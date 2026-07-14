# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MCP (Model Context Protocol) server for PyAirbyte connector management.

Supports two transport modes:

- **stdio** (default): For local MCP clients (Claude Desktop, etc.)
- **HTTP**: For hosted deployment. Start via `airbyte-mcp-http` entry point or
  `poe mcp-serve-http`. Transport auth is assembled by
  `fastmcp_extensions.resolve_mcp_auth`, which supports two client shapes on the
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

For Airbyte Cloud, set `MCP_AUTH_AIRBYTE_CLOUD=true` to verify against Airbyte
Cloud's application-client realm without hand-configuring URLs. An agent then
mints an Airbyte Cloud access token from its `AIRBYTE_CLOUD_CLIENT_ID` /
`AIRBYTE_CLOUD_CLIENT_SECRET` (the `<api_root>/applications/token` endpoint) and
sends it as `Authorization: Bearer`. That single token both authenticates
transport (verified here) and authorizes downstream Cloud API calls (the same
header feeds the Cloud bearer token), because an Airbyte-Cloud-issued JWT is
itself a valid Cloud API bearer.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import TYPE_CHECKING

from fastmcp_extensions import (
    JWTAuthConfig,
    mcp_server,
    resolve_mcp_auth,
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

# Public base URL of this deployment; consumed by `http_main` to derive the
# mounted MCP path. All other transport auth env vars (`OIDC_*` interactive,
# `MCP_AUTH_*` headless) are read by `fastmcp_extensions.resolve_mcp_auth`.
MCP_SERVER_URL_ENV = "MCP_SERVER_URL"

# Flag that opts headless verification into Airbyte Cloud's application-client
# realm; this server owns only this provider literal.
MCP_AUTH_AIRBYTE_CLOUD_ENV = "MCP_AUTH_AIRBYTE_CLOUD"

# Airbyte Cloud's application-client realm. Tokens minted from an Airbyte Cloud
# `client_id`/`client_secret` via `<api_root>/applications/token` are RS256 JWTs
# issued by this realm, and the same token is a valid Airbyte Cloud API bearer.
# Verifying against this realm lets one token both authenticate transport and
# authorize downstream Cloud calls. Enable with `MCP_AUTH_AIRBYTE_CLOUD=true`.
AIRBYTE_CLOUD_REALM_ISSUER = "https://cloud.airbyte.com/auth/realms/_airbyte-application-clients"
AIRBYTE_CLOUD_JWKS_URI = f"{AIRBYTE_CLOUD_REALM_ISSUER}/protocol/openid-connect/certs"
AIRBYTE_CLOUD_JWT_AUDIENCE = "account"
AIRBYTE_CLOUD_JWT_ALGORITHM = "RS256"

DEFAULT_HTTP_HOST = "0.0.0.0"
DEFAULT_HTTP_PORT = 8080


def _create_auth() -> AuthProvider | None:
    """Assemble the transport auth provider from environment configuration.

    Delegates env parsing to `fastmcp_extensions.resolve_mcp_auth`, which wires
    up interactive `OIDCProxy` (from `OIDC_*`) and/or headless `JWTVerifier`
    (from `MCP_AUTH_*`), combining them via `MultiAuth` when both are set and
    returning `None` when neither is — so the server falls back to standard
    local (no-auth) behavior.

    When `MCP_AUTH_AIRBYTE_CLOUD` is truthy, the headless verifier defaults to
    Airbyte Cloud's application-client realm (JWKS / issuer / audience /
    algorithm); individual `MCP_AUTH_*` vars still override those fields. This
    is the only provider literal the server owns.
    """
    jwt_defaults: JWTAuthConfig | None = None
    if os.getenv(MCP_AUTH_AIRBYTE_CLOUD_ENV, "").strip().lower() in {"1", "true", "yes"}:
        jwt_defaults = JWTAuthConfig(
            jwks_uri=AIRBYTE_CLOUD_JWKS_URI,
            issuer=AIRBYTE_CLOUD_REALM_ISSUER,
            audience=AIRBYTE_CLOUD_JWT_AUDIENCE,
            algorithm=AIRBYTE_CLOUD_JWT_ALGORITHM,
        )
    return resolve_mcp_auth(jwt_defaults=jwt_defaults)


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
