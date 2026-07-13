# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""HTTP transport entry point for the Airbyte MCP server.

Starts the MCP server with HTTP transport, suitable for hosted deployment
behind a load balancer. When OIDC env vars are set, authentication is
handled by `OIDCProxy` (configured in `server.py`).

Environment variables:

- `MCP_SERVER_URL`: Public base URL. Used for OIDC redirect callbacks and to
  derive the MCP endpoint mount path (serves at `/` when the URL has a path
  prefix, otherwise defaults to `/mcp`).
- `OIDC_CONFIG_URL`: Keycloak OIDC discovery URL (enables auth with all three OIDC vars)
- `OIDC_CLIENT_ID`: OIDC client identifier
- `OIDC_CLIENT_SECRET`: OIDC client secret
"""

from __future__ import annotations

import logging
import os
from urllib.parse import urlparse

from fastmcp_extensions import register_landing_page

from airbyte.mcp.server import (
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    MCP_SERVER_URL_ENV,
    app,
)


logger = logging.getLogger(__name__)

# Human-facing landing page shown when a browser GETs the MCP endpoint.
MCP_LANDING_TITLE = "Airbyte MCP Server"
MCP_LANDING_DOCS_URL = "https://docs.airbyte.com/ai-agents/"


def _get_server_url() -> str:
    """Return the public base URL from `MCP_SERVER_URL`, defaulting to localhost."""
    return os.getenv(
        MCP_SERVER_URL_ENV,
        f"http://localhost:{DEFAULT_HTTP_PORT}",
    )


def main() -> None:
    """Start the Airbyte MCP server with HTTP transport."""
    logging.basicConfig(level=logging.INFO)

    # When deployed behind a path-stripping LB (MCP_SERVER_URL has a path
    # component like /cloud-mcp), serve the MCP endpoint at root so the
    # public URL is just the base path. Otherwise keep the FastMCP default.
    server_url = _get_server_url()
    mcp_path = "/" if urlparse(server_url).path.strip("/") else "/mcp"

    # The advertised endpoint must match where the MCP route is actually mounted:
    # the bare server URL when mounted at root, otherwise the server URL + mcp_path.
    endpoint_url = server_url if mcp_path == "/" else server_url.rstrip("/") + mcp_path

    # Serve a browser-friendly landing page on GET at the MCP path. In stateless
    # mode FastMCP only binds POST/DELETE there, so this GET route does not
    # interfere with MCP traffic.
    register_landing_page(
        app,
        path=mcp_path,
        title=MCP_LANDING_TITLE,
        endpoint_url=endpoint_url,
        docs_url=MCP_LANDING_DOCS_URL,
    )

    logger.info(
        "Starting Airbyte MCP HTTP server on %s:%d (mcp_path=%r)",
        DEFAULT_HTTP_HOST,
        DEFAULT_HTTP_PORT,
        mcp_path,
    )

    app.run(
        transport="streamable-http",
        host=DEFAULT_HTTP_HOST,
        port=DEFAULT_HTTP_PORT,
        path=mcp_path,
        stateless_http=True,
    )


if __name__ == "__main__":
    main()
