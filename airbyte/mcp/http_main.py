# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""HTTP transport entry point for the Airbyte MCP server.

Starts the MCP server with HTTP transport, suitable for hosted deployment
behind a load balancer. When OIDC env vars are set, authentication is
handled by `OIDCProxy` (configured in `server.py`).

Environment variables:

- `MCP_SERVER_URL`: Public base URL (for OIDC redirect callbacks)
- `OIDC_CONFIG_URL`: Keycloak OIDC discovery URL (enables auth when set)
- `OIDC_CLIENT_ID`: OIDC client identifier
- `OIDC_CLIENT_SECRET`: OIDC client secret
"""

from __future__ import annotations

import logging

from airbyte.mcp.server import DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT, app


logger = logging.getLogger(__name__)


def main() -> None:
    """Start the Airbyte MCP server with HTTP transport."""
    logging.basicConfig(level=logging.INFO)
    logger.info(
        "Starting Airbyte MCP HTTP server on %s:%d",
        DEFAULT_HTTP_HOST,
        DEFAULT_HTTP_PORT,
    )

    app.run(
        transport="streamable-http",
        host=DEFAULT_HTTP_HOST,
        port=DEFAULT_HTTP_PORT,
        stateless_http=True,
    )


if __name__ == "__main__":
    main()
