# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""HTTP transport entry point for the Airbyte MCP server.

Starts the MCP server with HTTP transport, suitable for hosted deployment
behind a load balancer. Transport auth is assembled in `server.py`, which maps
this server's branded `AIRBYTE_MCP_*` env vars into the typed configs consumed
by `fastmcp_extensions.build_mcp_auth` (interactive OIDC and/or headless
bearer-token verification, combined via `MultiAuth`). HTTP transport is
**always authenticated**, defaulting to the Airbyte Cloud realm with zero auth
config. See `server.py` for details.

Environment variables:

- `MCP_SERVER_URL`: Public base URL. Used for OIDC redirect callbacks and to
  derive the MCP endpoint mount path (serves at `/` when the URL has a path
  prefix, otherwise defaults to `/mcp`).

Interactive OIDC (Keycloak Authorization Code + PKCE), enabled when the client
credentials are set (the discovery URL defaults to Airbyte Cloud):

- `AIRBYTE_MCP_OIDC_CLIENT_ID`: OIDC client identifier
- `AIRBYTE_MCP_OIDC_CLIENT_SECRET`: OIDC client secret
- `AIRBYTE_MCP_OIDC_CONFIG_URL`: OIDC discovery URL (defaults to Airbyte Cloud)
- `AIRBYTE_MCP_OIDC_CLIENT_STORAGE_FACTORY`: optional `"package.module:callable"`
  naming a durable OAuth-state store factory (defaults to in-memory)

Headless bearer-token verification (for agents/CI that mint their own
short-lived token via the client credentials grant). Each var defaults to the
Airbyte Cloud application-client realm, so a self-hosted deployment overrides
only the field(s) pointing at its own realm:

- `AIRBYTE_MCP_AUTH_JWKS_URI`: JWKS endpoint used to verify token signatures
- `AIRBYTE_MCP_AUTH_JWT_PUBLIC_KEY`: static public key (alternative to the JWKS URI)
- `AIRBYTE_MCP_AUTH_ISSUER`: expected token issuer
- `AIRBYTE_MCP_AUTH_AUDIENCE`: expected token audience
- `AIRBYTE_MCP_AUTH_ALGORITHM`: signing algorithm override
"""

from __future__ import annotations

import logging
import os
from urllib.parse import urlparse

from fastmcp_extensions import (
    assert_http_trusted_execution_disabled,
    register_landing_page,
)

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

    # Trusted execution grants local filesystem, connector-execution, and
    # server-side secret-resolution capability, which must never be reachable
    # over HTTP. Hard-fail startup if it was explicitly enabled on this hosted
    # entrypoint (a permanent gate; the per-request filter also forces it off).
    assert_http_trusted_execution_disabled(app)

    app.run(
        transport="streamable-http",
        host=DEFAULT_HTTP_HOST,
        port=DEFAULT_HTTP_PORT,
        path=mcp_path,
        stateless_http=True,
    )


if __name__ == "__main__":
    main()
