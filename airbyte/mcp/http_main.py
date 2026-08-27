# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""HTTP transport entry point for the Airbyte MCP server.

Starts the MCP server with HTTP transport, suitable for hosted deployment
behind a load balancer. Transport auth is assembled in `server.py`, which maps
this server's branded `AIRBYTE_MCP_*` env vars into the typed configs consumed
by `fastmcp_extensions.build_mcp_auth` (interactive OIDC and/or headless
bearer-token verification, combined via `MultiAuth`). Auth activates only for
the paths a deployment configures via env; with no auth env set the server
falls back to unauthenticated local behavior. This module declares only the env
var *names* — the concrete values are supplied at deploy time by the
deployment's own repo. See `server.py` and `_client_credentials.py` for
details.

Stateless streamable HTTP does not retain initialize-time client capabilities
internally. For clients that declare extensions at initialize, the server
returns a self-describing `Mcp-Session-Id`, and spec-compliant clients echo it
on subsequent requests. This makes MCP Apps `interactive-ui` tools available
without a client-specific header. Clients that do not echo session IDs can use
the explicit fallback `X-MCP-Extensions: io.modelcontextprotocol/ui` header on
each HTTP request. Multiple extension IDs may be comma-separated (recommended)
or whitespace-separated. The capability-token and SSE GET middleware are
provided by the installed `fastmcp-extensions` package.

The eventual spec-aligned replacement is per-request `_meta` under
`io.modelcontextprotocol/clientCapabilities`. That path exists in the modern
`mcp` 2.x server architecture, while this project currently resolves the
legacy `fastmcp` 3.x and `mcp` 1.x stack. Using it requires a stack migration
rather than a version-only change.

Environment variables:

- `MCP_SERVER_URL`: Public base URL. Used for OIDC redirect callbacks and to
  derive the MCP endpoint mount path (serves at `/` when the URL has a path
  prefix, otherwise defaults to `/mcp`).

Interactive OIDC (Keycloak Authorization Code + PKCE), enabled when the client
credentials are set:

- `AIRBYTE_MCP_OIDC_CLIENT_ID`: OIDC client identifier
- `AIRBYTE_MCP_OIDC_CLIENT_SECRET`: OIDC client secret
- `AIRBYTE_MCP_OIDC_CONFIG_URL`: OIDC discovery URL (required when the client
  credentials are set)
- `AIRBYTE_MCP_OIDC_CLIENT_STORAGE_FACTORY`: optional `"package.module:callable"`
  naming a durable OAuth-state store factory (defaults to in-memory)

Headless bearer-token verification (for agents/CI that mint their own
short-lived token via the client credentials grant). The verifier activates
once a signing-key source — the JWKS URI or a static public key — is set;
issuer, audience, and algorithm refine verification when provided:

- `AIRBYTE_MCP_AUTH_JWKS_URI`: JWKS endpoint used to verify token signatures
- `AIRBYTE_MCP_AUTH_JWT_PUBLIC_KEY`: static public key (alternative to the JWKS
  URI)
- `AIRBYTE_MCP_AUTH_ISSUER`: expected token issuer
- `AIRBYTE_MCP_AUTH_AUDIENCE`: expected token audience
- `AIRBYTE_MCP_AUTH_ALGORITHM`: signing algorithm override

Opt-in static client credentials:

- `AIRBYTE_MCP_AUTH_ALLOW_CLIENT_CREDENTIALS`: enable `Client-Id` /
  `Client-Secret` headers and HTTP Basic credentials. This is an exchange-and-
  rewrite layer, not a bearer-token verifier; configure `AIRBYTE_MCP_AUTH_JWKS_URI`
  or `AIRBYTE_MCP_AUTH_JWT_PUBLIC_KEY` as well. Without a verifier, minted token
  claims and requests with no credentials are not checked.
- `AIRBYTE_MCP_AUTH_CLIENT_CREDENTIALS_TOKEN_URL`: OAuth token endpoint for the
  exchange; defaults to the Airbyte Cloud application-token endpoint
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from fastmcp.server.auth import MultiAuth
from fastmcp_extensions import (
    assert_http_trusted_execution_disabled,
    register_landing_page,
    run_mcp_http_server,
)

from airbyte.constants import set_hosted_mcp_mode
from airbyte.mcp._client_credentials import (
    client_credentials_enabled,
    wrap_if_enabled,
)
from airbyte.mcp.server import (
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    DEFAULT_MCP_SERVER_URL,
    MCP_SERVER_URL_ENV,
    _env_or_default,
    app,
)
from airbyte.version import get_version


if TYPE_CHECKING:
    from fastmcp.server.auth import AuthProvider

logger = logging.getLogger(__name__)

# Human-facing landing page shown when a browser GETs the MCP endpoint.
MCP_LANDING_TITLE = "Airbyte MCP Server"
MCP_LANDING_DOCS_URL = "https://docs.airbyte.com/ai-agents/"
RELEASE_TAG_URL_TEMPLATE = "https://github.com/airbytehq/PyAirbyte/releases/tag/v{}"
COMMIT_URL_TEMPLATE = "https://github.com/airbytehq/PyAirbyte/commit/{}"
RELEASES_URL = "https://github.com/airbytehq/PyAirbyte/releases"
_FINAL_VERSION_PATTERN = re.compile(r"\d+(?:\.\d+)*")
_COMMIT_SHA_PATTERN = re.compile(r"[0-9a-f]{7,40}")


def _landing_version_str() -> str:
    """Return the installed PyAirbyte version for the landing-page footer."""
    return f"v{get_version()}"


def _landing_version_url() -> str:
    """Return the URL the landing-page version footer links to.

    A final version links to its release page. A dev build has no release of its
    own, so it links to the commit it was cut from, which its local segment
    carries (`0.54.0.post4.dev0+32b9886`). A non-final version built without a
    local segment (the prerelease workflow's `{base}.dev{pr}{run_id}`) identifies
    no commit, so it falls back to the release list.
    """
    public, _, local = get_version().partition("+")
    commit_sha = local.split(".")[0]
    if _COMMIT_SHA_PATTERN.fullmatch(commit_sha):
        return COMMIT_URL_TEMPLATE.format(commit_sha)
    if not _FINAL_VERSION_PATTERN.fullmatch(public):
        return RELEASES_URL
    return RELEASE_TAG_URL_TEMPLATE.format(public)


def _get_server_url() -> str:
    """Return the public base URL from `MCP_SERVER_URL`, defaulting to localhost.

    Uses the same blank-as-unset handling as `server._create_auth` so the HTTP
    mount/landing URL and the auth redirect/base URL agree on the effective
    server URL even when `MCP_SERVER_URL` is set but blank.
    """
    return _env_or_default(MCP_SERVER_URL_ENV, DEFAULT_MCP_SERVER_URL)


def _advertise_root_mount_resource(auth: AuthProvider) -> None:
    """Advertise the slash-less public URL as the RFC 9728 resource at a root mount.

    Behind a path-stripping load balancer the MCP endpoint is mounted at root
    (`mcp_path="/"`), and FastMCP derives the protected-resource identifier from
    that mount path — appending a trailing slash (e.g. `.../cloud-mcp/`). Strict
    RFC 9728 clients canonicalize the connection URL to the slash-less form
    (`.../cloud-mcp`) and reject the mismatch, so they cannot attach. FastMCP
    already returns the bare base URL for a *root* mount path (`None`/`""`), so
    this maps the `"/"` mount path onto that root case, leaving non-root mounts
    (e.g. the local `"/mcp"` default) untouched.

    Applied to every provider in the tree because the protected-resource
    metadata document and the `WWW-Authenticate` challenge are built from
    different providers (the interactive server versus the top-level `MultiAuth`).
    """
    original = auth._get_resource_url  # noqa: SLF001  # FastMCP has no public seam for this.

    def resolve_resource_url(path: str | None = None):  # noqa: ANN202
        normalized = path if path and path != "/" else None
        return original(normalized)

    auth._get_resource_url = resolve_resource_url  # type: ignore[method-assign]  # noqa: SLF001

    if isinstance(auth, MultiAuth):
        if auth.server is not None:
            _advertise_root_mount_resource(auth.server)
        for verifier in auth.verifiers:
            _advertise_root_mount_resource(verifier)


def _log_auth_status() -> None:
    """Log the configured HTTP transport authentication state."""
    if app.auth is None and client_credentials_enabled():
        logger.warning(
            "HTTP transport starting with client credentials enabled but without "
            "bearer-token verification: the token endpoint rejects invalid "
            "credentials, but minted token claims and requests with no credentials "
            "are not checked. Set `AIRBYTE_MCP_AUTH_JWKS_URI` or "
            "`AIRBYTE_MCP_AUTH_JWT_PUBLIC_KEY` to require bearer verification."
        )
    elif app.auth is None:
        logger.warning(
            "HTTP transport starting without authentication: no interactive "
            "OIDC or headless bearer-token auth is configured, so every request "
            "is unauthenticated. Set `AIRBYTE_MCP_OIDC_CLIENT_ID`/"
            "`AIRBYTE_MCP_OIDC_CLIENT_SECRET`/`AIRBYTE_MCP_OIDC_CONFIG_URL` "
            "(interactive) or `AIRBYTE_MCP_AUTH_JWKS_URI`/"
            "`AIRBYTE_MCP_AUTH_JWT_PUBLIC_KEY` (headless) to require auth."
        )
    else:
        logger.info("HTTP transport authentication is enabled (%s).", type(app.auth).__name__)


def main() -> None:
    """Start the Airbyte MCP server with HTTP transport."""
    logging.basicConfig(level=logging.INFO)
    set_hosted_mcp_mode()

    # When deployed behind a path-stripping LB (MCP_SERVER_URL has a path
    # component like /cloud-mcp), serve the MCP endpoint at root so the
    # public URL is just the base path. Otherwise keep the FastMCP default.
    server_url = _get_server_url()
    mcp_path = "/" if urlparse(server_url).path.strip("/") else "/mcp"

    # The advertised endpoint must match where the MCP route is actually mounted:
    # the bare server URL when mounted at root, otherwise the server URL + mcp_path.
    endpoint_url = server_url if mcp_path == "/" else server_url.rstrip("/") + mcp_path

    # At a root mount FastMCP would advertise a trailing-slash resource that
    # strict RFC 9728 clients reject; pin it to the slash-less public URL.
    if mcp_path == "/" and app.auth is not None:
        _advertise_root_mount_resource(app.auth)

    # Serve a browser-friendly landing page on GET at the MCP path. In stateless
    # mode FastMCP only binds POST/DELETE there, so this GET route does not
    # interfere with MCP traffic.
    register_landing_page(
        app,
        path=mcp_path,
        title=MCP_LANDING_TITLE,
        endpoint_url=endpoint_url,
        docs_url=MCP_LANDING_DOCS_URL,
        version_str=_landing_version_str(),
        version_url=_landing_version_url(),
    )

    _log_auth_status()

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

    try:
        run_mcp_http_server(
            app,
            path=mcp_path,
            transport="streamable-http",
            stateless_http=True,
            wrapper=wrap_if_enabled,
            host=DEFAULT_HTTP_HOST,
            port=DEFAULT_HTTP_PORT,
        )
    except KeyboardInterrupt:
        logger.info("Airbyte MCP HTTP server interrupted by user.")


if __name__ == "__main__":
    main()
