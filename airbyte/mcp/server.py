# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MCP (Model Context Protocol) server for PyAirbyte connector management.

Supports two transport modes:

- **stdio** (default): For local MCP clients (Claude Desktop, etc.)
- **HTTP**: For hosted deployment. Start via `airbyte-mcp-http` entry point or
  `poe mcp-serve-http`. Transport auth is assembled by
  `fastmcp_extensions.build_mcp_auth`, which supports two client shapes on the
  same deployment:
    - **Interactive** (humans in a browser): Keycloak Authorization Code + PKCE
      via `OIDCProxy`, enabled when `OIDC_CONFIG_URL`, `OIDC_CLIENT_ID`, and
      `OIDC_CLIENT_SECRET` are all set.
    - **Headless** (agents, CI): the client mints its own short-lived bearer
      token via the OAuth 2.0 client credentials grant and sends it as
      `Authorization: Bearer <token>`. The server verifies it with a
      `JWTVerifier` (no browser, no stored/rotating refresh token), enabled
      when `MCP_AUTH_JWKS_URI` (or `MCP_AUTH_JWT_PUBLIC_KEY`) is set.
  When both are configured they are combined via `MultiAuth`.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import TYPE_CHECKING

from fastmcp_extensions import (
    JWTAuthConfig,
    OIDCAuthConfig,
    build_mcp_auth,
    mcp_server,
)
from starlette.responses import JSONResponse


if TYPE_CHECKING:
    from fastmcp.server.auth import AuthProvider
    from starlette.requests import Request

from airbyte._util.meta import set_mcp_mode
from airbyte.mcp._config import load_secrets_to_env_vars
from airbyte.mcp._tool_utils import (
    AIRBYTE_EXCLUDE_MODULES_CONFIG_ARG,
    AIRBYTE_INCLUDE_MODULES_CONFIG_ARG,
    AIRBYTE_READONLY_MODE_CONFIG_ARG,
    API_URL_CONFIG_ARG,
    BEARER_TOKEN_CONFIG_ARG,
    CLIENT_ID_CONFIG_ARG,
    CLIENT_SECRET_CONFIG_ARG,
    CONFIG_API_URL_CONFIG_ARG,
    WORKSPACE_ID_CONFIG_ARG,
    airbyte_module_filter,
    airbyte_readonly_mode_filter,
    airbyte_ui_support_filter,
)
from airbyte.mcp.cloud import register_cloud_tools
from airbyte.mcp.interactive import register_interactive_tools
from airbyte.mcp.local import register_local_tools
from airbyte.mcp.prompts import register_prompts
from airbyte.mcp.registry import register_registry_tools


# =============================================================================
# Server Instructions
# =============================================================================
# This text is provided to AI agents via the MCP protocol's "instructions" field.
# It helps agents understand when to use this server's tools, especially when
# tool search is enabled. For more context, see:
# - FastMCP docs: https://gofastmcp.com/servers/overview
# - Claude tool search: https://www.anthropic.com/news/tool-use-improvements
# =============================================================================

MCP_SERVER_INSTRUCTIONS = """
PyAirbyte connector management and data integration server for discovering,
deploying, and running Airbyte connectors.

Use this server for:
- Discovering connectors from the Airbyte registry (sources and destinations)
- Deploying sources, destinations, and connections to Airbyte Cloud
- Running cloud syncs and monitoring sync status
- Managing custom connector definitions in Airbyte Cloud
- Local connector execution for data extraction without cloud deployment
- Listing and describing environment variables for connector configuration

Operational modes:
- Cloud operations: Deploy and manage connectors on Airbyte Cloud (requires
  AIRBYTE_CLOUD_CLIENT_ID, AIRBYTE_CLOUD_CLIENT_SECRET, AIRBYTE_CLOUD_WORKSPACE_ID)
- Local operations: Run connectors locally for data extraction (requires
  AIRBYTE_PROJECT_DIR for artifact storage)

Safety features:
- Safe mode (default): Restricts destructive operations to objects created in
  the current session
- Read-only mode: Disables all write operations for cloud resources
""".strip()

logger = logging.getLogger(__name__)

# OIDC environment variable names (interactive Authorization Code + PKCE)
OIDC_CONFIG_URL_ENV = "OIDC_CONFIG_URL"
OIDC_CLIENT_ID_ENV = "OIDC_CLIENT_ID"
OIDC_CLIENT_SECRET_ENV = "OIDC_CLIENT_SECRET"
MCP_SERVER_URL_ENV = "MCP_SERVER_URL"

# Headless bearer-token verification environment variable names.
# Agents/CI mint a short-lived token via the client credentials grant and the
# server verifies it here — no interactive flow, no refresh token to rotate.
MCP_AUTH_JWKS_URI_ENV = "MCP_AUTH_JWKS_URI"
MCP_AUTH_JWT_PUBLIC_KEY_ENV = "MCP_AUTH_JWT_PUBLIC_KEY"
MCP_AUTH_ISSUER_ENV = "MCP_AUTH_ISSUER"
MCP_AUTH_AUDIENCE_ENV = "MCP_AUTH_AUDIENCE"
MCP_AUTH_ALGORITHM_ENV = "MCP_AUTH_ALGORITHM"

DEFAULT_HTTP_HOST = "0.0.0.0"
DEFAULT_HTTP_PORT = 8080


def _create_auth() -> AuthProvider | None:
    """Assemble the transport auth provider from environment configuration.

    Supports two client shapes on the same deployment, combined via `MultiAuth`
    when both are configured (see `fastmcp_extensions.build_mcp_auth`):

    - **Interactive** `OIDCProxy` (Keycloak Authorization Code + PKCE) when
      `OIDC_CONFIG_URL`, `OIDC_CLIENT_ID`, and `OIDC_CLIENT_SECRET` are set.
    - **Headless** `JWTVerifier` for pre-minted bearer tokens when
      `MCP_AUTH_JWKS_URI` or `MCP_AUTH_JWT_PUBLIC_KEY` is set.

    Returns `None` when neither is configured, so the server falls back to
    standard local (no-auth) behavior.
    """
    server_url = os.getenv(
        MCP_SERVER_URL_ENV,
        f"http://localhost:{DEFAULT_HTTP_PORT}",
    )

    oidc: OIDCAuthConfig | None = None
    config_url = os.getenv(OIDC_CONFIG_URL_ENV, "")
    oidc_client_id = os.getenv(OIDC_CLIENT_ID_ENV, "")
    oidc_client_secret = os.getenv(OIDC_CLIENT_SECRET_ENV, "")
    if config_url and oidc_client_id and oidc_client_secret:
        logger.info(
            "Interactive OIDC auth enabled (issuer=%s, client_id=%s, base_url=%s)",
            config_url,
            oidc_client_id,
            server_url,
        )
        oidc = OIDCAuthConfig(
            config_url=config_url,
            client_id=oidc_client_id,
            client_secret=oidc_client_secret,
            base_url=server_url,
        )

    jwt: JWTAuthConfig | None = None
    jwks_uri = os.getenv(MCP_AUTH_JWKS_URI_ENV, "")
    jwt_public_key = os.getenv(MCP_AUTH_JWT_PUBLIC_KEY_ENV, "")
    if jwks_uri or jwt_public_key:
        issuer = os.getenv(MCP_AUTH_ISSUER_ENV) or None
        audience = os.getenv(MCP_AUTH_AUDIENCE_ENV) or None
        logger.info(
            "Headless bearer-token auth enabled (jwks_uri=%s, issuer=%s, audience=%s)",
            jwks_uri or "<static public key>",
            issuer,
            audience,
        )
        jwt = JWTAuthConfig(
            jwks_uri=jwks_uri or None,
            public_key=jwt_public_key or None,
            issuer=issuer,
            audience=audience,
            algorithm=os.getenv(MCP_AUTH_ALGORITHM_ENV) or None,
        )

    return build_mcp_auth(oidc=oidc, jwt=jwt, base_url=server_url)


set_mcp_mode()
load_secrets_to_env_vars()

app = mcp_server(
    name="airbyte-mcp",
    package_name="airbyte",
    instructions=MCP_SERVER_INSTRUCTIONS,
    include_standard_tool_filters=True,
    server_config_args=[
        AIRBYTE_READONLY_MODE_CONFIG_ARG,
        AIRBYTE_EXCLUDE_MODULES_CONFIG_ARG,
        AIRBYTE_INCLUDE_MODULES_CONFIG_ARG,
        WORKSPACE_ID_CONFIG_ARG,
        BEARER_TOKEN_CONFIG_ARG,
        CLIENT_ID_CONFIG_ARG,
        CLIENT_SECRET_CONFIG_ARG,
        API_URL_CONFIG_ARG,
        CONFIG_API_URL_CONFIG_ARG,
    ],
    tool_filters=[
        airbyte_readonly_mode_filter,
        airbyte_module_filter,
        airbyte_ui_support_filter,
    ],
    auth=_create_auth(),
)
"""The Airbyte MCP Server application instance."""

# Register tools from each module
register_cloud_tools(app)
register_local_tools(app)
register_registry_tools(app)
register_interactive_tools(app)
register_prompts(app)


@app.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> JSONResponse:  # noqa: ARG001, RUF029
    """Health check endpoint for load balancer probes."""
    return JSONResponse({"status": "ok"})


def main() -> None:
    """@private Main entry point for the MCP server.

    This function starts the FastMCP server to handle MCP requests.

    It should not be called directly; instead, consult the MCP client documentation
    for instructions on how to connect to the server.
    """
    print("Starting Airbyte MCP server.", file=sys.stderr)
    try:
        asyncio.run(app.run_stdio_async())
    except KeyboardInterrupt:
        print("Airbyte MCP server interrupted by user.", file=sys.stderr)
    except Exception as ex:
        print(f"Error running Airbyte MCP server: {ex}", file=sys.stderr)
        sys.exit(1)

    print("Airbyte MCP server stopped.", file=sys.stderr)


if __name__ == "__main__":
    main()
