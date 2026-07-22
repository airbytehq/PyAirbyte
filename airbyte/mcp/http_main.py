# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""HTTP transport entry point for the Airbyte MCP server.

Starts the MCP server with HTTP transport, suitable for hosted deployment
behind a load balancer. Transport auth is assembled in `server.py`, which
defaults every setting to Airbyte Cloud's public Keycloak realms and supports
interactive OIDC plus headless bearer-token verification (combined via
`MultiAuth`). See `server.py` for details.

Environment variables:

- `MCP_SERVER_URL`: Public base URL. Used for OIDC redirect callbacks and to
  derive the MCP endpoint mount path (serves at `/` when the URL has a path
  prefix, otherwise defaults to `/mcp`).

Interactive OIDC (Keycloak Authorization Code + PKCE), active once the client
credentials are supplied:

- `AIRBYTE_MCP_OIDC_CLIENT_ID`: OIDC client identifier
- `AIRBYTE_MCP_OIDC_CLIENT_SECRET`: OIDC client secret
- `AIRBYTE_MCP_OIDC_CONFIG_URL`: Keycloak OIDC discovery URL (defaults to
  Airbyte Cloud)

Headless bearer-token verification (for agents/CI that mint their own
short-lived token via the client credentials grant); on by default against
Airbyte Cloud, overridable for self-hosted deployments:

- `AIRBYTE_MCP_AUTH_JWKS_URI`: JWKS endpoint used to verify token signatures
- `AIRBYTE_MCP_AUTH_JWT_PUBLIC_KEY`: static public key (alternative to
  `AIRBYTE_MCP_AUTH_JWKS_URI` for realms without a JWKS endpoint)
- `AIRBYTE_MCP_AUTH_ISSUER`: expected token issuer
- `AIRBYTE_MCP_AUTH_AUDIENCE`: expected token audience
- `AIRBYTE_MCP_AUTH_ALGORITHM`: signing algorithm

Opt-in HTTP Basic client-credentials transport auth, for a headless agent that
can only set a static `Authorization` header and cannot re-mint short-lived
tokens itself (off by default):

- `AIRBYTE_MCP_AUTH_ALLOW_CLIENT_CREDENTIALS`: when truthy, accept
  `Authorization: Basic base64(client_id:client_secret)`, exchange it for a
  short-lived bearer token server-side, and verify that token via the headless
  path above
- `AIRBYTE_MCP_AUTH_CLIENT_CREDENTIALS_TOKEN_URL`: token endpoint used for the
  exchange (defaults to Airbyte Cloud)

Running over HTTP always requires transport auth: startup fails fast if no auth
provider resolves (e.g. an invalid `AIRBYTE_MCP_AUTH_*` override, since blank
values fall back to the Airbyte Cloud defaults rather than disabling auth).
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse

import uvicorn
from fastmcp_extensions import (
    assert_http_trusted_execution_disabled,
    register_landing_page,
)

from airbyte.mcp._client_credentials import wrap_if_enabled
from airbyte.mcp.server import (
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    DEFAULT_MCP_SERVER_URL,
    MCP_SERVER_URL_ENV,
    _env_or_default,
    app,
)


logger = logging.getLogger(__name__)

# Human-facing landing page shown when a browser GETs the MCP endpoint.
MCP_LANDING_TITLE = "Airbyte MCP Server"
MCP_LANDING_DOCS_URL = "https://docs.airbyte.com/ai-agents/"


def _get_server_url() -> str:
    """Return the public base URL from `MCP_SERVER_URL`, defaulting to localhost.

    A blank or whitespace-only value is treated as unset (matching `_create_auth`
    in `server.py`), so the mount-path and landing-page URL derived here can't
    disagree with the OIDC redirect base derived there.
    """
    return _env_or_default(MCP_SERVER_URL_ENV, DEFAULT_MCP_SERVER_URL)


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

    if getattr(app, "auth", None) is None:
        # HTTP transport is always authenticated by design: launching over HTTP
        # declares the server needs auth. Auth defaults to Airbyte Cloud and blank
        # env vars fall back to those defaults, so a `None` provider means auth
        # resolution produced nothing at all — e.g. an invalid `AIRBYTE_MCP_AUTH_*`
        # override. Fail fast rather than fall open to unauthenticated serving.
        raise RuntimeError(
            "HTTP transport requires transport auth, but none resolved. Auth "
            "defaults to Airbyte Cloud; leave the `AIRBYTE_MCP_AUTH_*` settings "
            "unset to use those defaults, or set them to valid values. Check "
            "startup logs for auth-resolution warnings before serving over HTTP."
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

    # Build the ASGI app ourselves (rather than `app.run`) so the optional
    # client-credentials exchange can wrap it as the *outermost* layer — ahead
    # of FastMCP's auth middleware — so its Basic-to-Bearer rewrite is what the
    # verifier sees. When the opt-in flag is unset, `wrap_if_enabled` returns the
    # app unchanged. The Starlette app owns the session-manager lifespan, so
    # running it under uvicorn directly is equivalent to `app.run`.
    http_app = app.http_app(
        path=mcp_path,
        transport="streamable-http",
        stateless_http=True,
    )
    uvicorn.run(
        wrap_if_enabled(http_app),
        host=DEFAULT_HTTP_HOST,
        port=DEFAULT_HTTP_PORT,
    )


if __name__ == "__main__":
    main()
