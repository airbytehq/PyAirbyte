# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""HTTP transport entry point for the Airbyte MCP server.

Starts the MCP server with HTTP transport, suitable for hosted deployment
behind a load balancer. When OIDC env vars are set, authentication is
handled by `OIDCProxy` (configured in `server.py`).

Environment variables:

- `MCP_HTTP_HOST`: Bind address (default `0.0.0.0` for containers)
- `MCP_HTTP_PORT`: Listen port (default `8080`)
- `OIDC_CONFIG_URL`: Keycloak OIDC discovery URL (enables auth when set)
- `OIDC_CLIENT_ID`: OIDC client identifier
- `OIDC_CLIENT_SECRET`: OIDC client secret
- `MCP_SERVER_URL`: Public base URL (for OIDC redirect callbacks)
"""

from __future__ import annotations

import logging
import os

from airbyte.mcp.server import app


logger = logging.getLogger(__name__)

DEFAULT_HTTP_HOST = "0.0.0.0"
DEFAULT_HTTP_PORT = 8080


def main() -> None:
    """Start the Airbyte MCP server with HTTP transport."""
    host = os.getenv("MCP_HTTP_HOST", DEFAULT_HTTP_HOST)
    port = int(os.getenv("MCP_HTTP_PORT", str(DEFAULT_HTTP_PORT)))

    logging.basicConfig(level=logging.INFO)
    logger.info("Starting Airbyte MCP HTTP server on %s:%d", host, port)

    app.run(
        transport="streamable-http",
        host=host,
        port=port,
        stateless_http=True,
    )


if __name__ == "__main__":
    main()
