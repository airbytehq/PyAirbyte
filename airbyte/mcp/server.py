# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MCP (Model Context Protocol) server for PyAirbyte connector management.

Supports two transport modes:

- **stdio** (default): For local MCP clients (Claude Desktop, etc.)
- **HTTP**: For hosted deployment. Start via `airbyte-mcp-http` entry point or
  `poe mcp-serve-http`. When `OIDC_CONFIG_URL`, `OIDC_CLIENT_ID`, and
  `OIDC_CLIENT_SECRET` are all set, enables Keycloak OIDC authentication
  via `OIDCProxy`.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import TYPE_CHECKING

from fastmcp.server.auth.oidc_proxy import OIDCProxy
from fastmcp_extensions import mcp_server
from starlette.responses import JSONResponse


if TYPE_CHECKING:
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

OIDC_CONFIG_URL_ENV = "OIDC_CONFIG_URL"
OIDC_CLIENT_ID_ENV = "OIDC_CLIENT_ID"
OIDC_CLIENT_SECRET_ENV = "OIDC_CLIENT_SECRET"
MCP_SERVER_URL_ENV = "MCP_SERVER_URL"

DEFAULT_HTTP_HOST = "0.0.0.0"
DEFAULT_HTTP_PORT = 8080


def _create_oidc_auth() -> OIDCProxy | None:
    """Create an `OIDCProxy` auth provider when OIDC env vars are configured.

    When `OIDC_CONFIG_URL`, `OIDC_CLIENT_ID`, and `OIDC_CLIENT_SECRET` are all
    set, returns an `OIDCProxy` that handles the Keycloak Authorization Code +
    PKCE flow. Otherwise returns `None` (no auth, standard local behavior).
    """
    config_url = os.getenv(OIDC_CONFIG_URL_ENV, "")
    client_id = os.getenv(OIDC_CLIENT_ID_ENV, "")
    client_secret = os.getenv(OIDC_CLIENT_SECRET_ENV, "")

    if not config_url or not client_id or not client_secret:
        return None

    server_url = os.getenv(
        MCP_SERVER_URL_ENV,
        f"http://localhost:{DEFAULT_HTTP_PORT}",
    )

    logger.info(
        "OIDC auth enabled (issuer=%s, client_id=%s, base_url=%s)",
        config_url,
        client_id,
        server_url,
    )
    return OIDCProxy(
        config_url=config_url,
        client_id=client_id,
        client_secret=client_secret,
        base_url=server_url,
    )


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
    auth=_create_oidc_auth(),
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
    print("Starting Airbyte MCP server.", file=sys.stderr)  # noqa: T201
    try:
        asyncio.run(app.run_stdio_async())
    except KeyboardInterrupt:
        print("Airbyte MCP server interrupted by user.", file=sys.stderr)  # noqa: T201
    except Exception as ex:
        print(f"Error running Airbyte MCP server: {ex}", file=sys.stderr)  # noqa: T201
        sys.exit(1)

    print("Airbyte MCP server stopped.", file=sys.stderr)  # noqa: T201


if __name__ == "__main__":
    main()
