# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""HTTP transport entry point for the Airbyte MCP server.

Starts the MCP server with HTTP transport, suitable for hosted deployment
behind a load balancer. When OIDC env vars are set, authentication is
handled by `OIDCProxy` (configured in `server.py`).

Environment variables:

- `MCP_SERVER_URL`: Public base URL (for OIDC redirect callbacks)
- `OIDC_CONFIG_URL`: Keycloak OIDC discovery URL (enables auth with all three OIDC vars)
- `OIDC_CLIENT_ID`: OIDC client identifier
- `OIDC_CLIENT_SECRET`: OIDC client secret
"""

from __future__ import annotations

import logging
import os
from urllib.parse import urlparse

from airbyte.mcp.server import (
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    MCP_SERVER_URL_ENV,
    app,
)


logger = logging.getLogger(__name__)


def main() -> None:
    """Start the Airbyte MCP server with HTTP transport."""
    logging.basicConfig(level=logging.INFO)

    # When deployed behind a path-stripping LB (MCP_SERVER_URL has a path
    # component like /cloud-mcp), serve the MCP endpoint at root so the
    # public URL is just the base path. Otherwise keep the FastMCP default.
    server_url = os.getenv(
        MCP_SERVER_URL_ENV,
        f"http://localhost:{DEFAULT_HTTP_PORT}",
    )
    mcp_path = "/" if urlparse(server_url).path.strip("/") else "/mcp"

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
