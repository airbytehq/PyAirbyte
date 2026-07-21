# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MCP (Model Context Protocol) server for PyAirbyte connector management.

Supports two transport modes:

- **stdio** (default): For local MCP clients (Claude Desktop, etc.)
- **HTTP**: For hosted deployment. Start via `airbyte-mcp-http` entry point or
  `poe mcp-serve-http`. Running over HTTP always requires transport auth, and it
  targets Airbyte Cloud out of the box — no auth env vars are needed for the
  hosted case beyond the interactive OIDC client credentials. Two client shapes
  are supported on the same deployment:
    - **Interactive** (humans in a browser): Keycloak Authorization Code + PKCE
      via `OIDCProxy`, active once `AIRBYTE_MCP_OIDC_CLIENT_ID` and
      `AIRBYTE_MCP_OIDC_CLIENT_SECRET` are supplied (the OIDC discovery URL
      defaults to Airbyte Cloud).
    - **Headless** (agents, CI): the client mints its own short-lived bearer
      token via the OAuth 2.0 client credentials grant and sends it as
      `Authorization: Bearer <token>`. The server verifies it with a
      `JWTVerifier` against Airbyte Cloud's application-client realm by default.
  When both are configured they are combined via `MultiAuth`.

All auth settings default to Airbyte Cloud's public (non-secret) Keycloak realms.
A self-hosted deployment pointing at its own Airbyte instance overrides any of
them via the matching env var (`AIRBYTE_MCP_OIDC_CONFIG_URL`,
`AIRBYTE_MCP_AUTH_JWKS_URI`, `AIRBYTE_MCP_AUTH_JWT_PUBLIC_KEY`,
`AIRBYTE_MCP_AUTH_ISSUER`, `AIRBYTE_MCP_AUTH_AUDIENCE`,
`AIRBYTE_MCP_AUTH_ALGORITHM`). This module owns those
Airbyte-specific names and translates them to the generic names that
`fastmcp_extensions.resolve_mcp_auth` consumes, so the extensions library stays
provider-agnostic.

For the headless path a token minted from an `AIRBYTE_CLOUD_CLIENT_ID` /
`AIRBYTE_CLOUD_CLIENT_SECRET` (the `<api_root>/applications/token` endpoint) both
authenticates transport (verified here) and authorizes downstream Cloud API
calls, because an Airbyte-Cloud-issued JWT is itself a valid Cloud API bearer.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import TYPE_CHECKING

from fastmcp_extensions import (
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
    TRUSTED_EXECUTION_CONFIG_ARG,
    WORKSPACE_ID_CONFIG_ARG,
    airbyte_module_filter,
    airbyte_readonly_mode_filter,
    airbyte_ui_support_filter,
    validate_airbyte_domains,
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
# mounted MCP path and by `resolve_mcp_auth` as the OIDC redirect base.
MCP_SERVER_URL_ENV = "MCP_SERVER_URL"

DEFAULT_HTTP_HOST = "0.0.0.0"
DEFAULT_HTTP_PORT = 8080

# Airbyte Cloud's public Keycloak realms. These are non-secret, publicly
# discoverable endpoints used as the zero-config auth defaults so the hosted
# Airbyte Cloud MCP server needs no auth env beyond its OIDC client credentials.
# Interactive human login uses the `airbyte` realm; headless application-client
# tokens are issued by (and verified against) the `_airbyte-application-clients`
# realm.
AIRBYTE_CLOUD_OIDC_CONFIG_URL = (
    "https://cloud.airbyte.com/auth/realms/airbyte/.well-known/openid-configuration"
)
AIRBYTE_CLOUD_ISSUER = "https://cloud.airbyte.com/auth/realms/_airbyte-application-clients"
AIRBYTE_CLOUD_JWKS_URI = f"{AIRBYTE_CLOUD_ISSUER}/protocol/openid-connect/certs"
AIRBYTE_CLOUD_AUDIENCE = "account"
AIRBYTE_CLOUD_ALGORITHM = "RS256"

# Default public base URL, mirroring `http_main`'s default so the OIDC redirect
# base is well-formed even when `MCP_SERVER_URL` is unset (e.g. local dev).
DEFAULT_MCP_SERVER_URL = f"http://localhost:{DEFAULT_HTTP_PORT}"

# Headless JWT verifier claim/algorithm family. Maps this server's
# Airbyte-branded env vars to the generic names `fastmcp_extensions`
# consumes, paired with the Airbyte Cloud default. Because these defaults are
# always present, HTTP transport always verifies bearer tokens. Setting any
# matching env var overrides the Cloud default — the escape hatch for
# self-hosted deployments pointing at their own Airbyte instance. These carry
# the `AUTH` segment; `OIDC_*` vars keep `OIDC` alone (it already denotes auth).
#
# The signing-key source (`AIRBYTE_MCP_AUTH_JWKS_URI` /
# `AIRBYTE_MCP_AUTH_JWT_PUBLIC_KEY`) is resolved separately in `_create_auth`,
# because the JWKS default must apply only when neither key source is set (see
# `_resolve_signing_key`).
_JWT_ENV_MAP: dict[str, tuple[str, str]] = {
    "AIRBYTE_MCP_AUTH_ISSUER": ("MCP_AUTH_ISSUER", AIRBYTE_CLOUD_ISSUER),
    "AIRBYTE_MCP_AUTH_AUDIENCE": ("MCP_AUTH_AUDIENCE", AIRBYTE_CLOUD_AUDIENCE),
    "AIRBYTE_MCP_AUTH_ALGORITHM": ("MCP_AUTH_ALGORITHM", AIRBYTE_CLOUD_ALGORITHM),
}

# Signing-key sources for the headless JWT verifier. A deployment may point at a
# JWKS endpoint (`AIRBYTE_MCP_AUTH_JWKS_URI`) or supply a static public key
# (`AIRBYTE_MCP_AUTH_JWT_PUBLIC_KEY`, for self-hosted realms without a JWKS
# endpoint). The Airbyte Cloud JWKS default applies only when neither is set.
JWKS_URI_ENV = "AIRBYTE_MCP_AUTH_JWKS_URI"
JWT_PUBLIC_KEY_ENV = "AIRBYTE_MCP_AUTH_JWT_PUBLIC_KEY"

# Interactive OIDC env vars. The client credentials are secret, so they have no
# default and must be supplied by the deployment to activate the interactive
# path. The discovery URL defaults to Airbyte Cloud but is only injected when
# the credentials are present (see `_create_auth`).
OIDC_CLIENT_ID_ENV = "AIRBYTE_MCP_OIDC_CLIENT_ID"
OIDC_CLIENT_SECRET_ENV = "AIRBYTE_MCP_OIDC_CLIENT_SECRET"
OIDC_CONFIG_URL_ENV = "AIRBYTE_MCP_OIDC_CONFIG_URL"

# Pre-rename generic names this server no longer reads, mapped to the branded
# name that replaced each. Used only to warn about a stale deployment config.
_LEGACY_OIDC_ENV: dict[str, str] = {
    "OIDC_CLIENT_ID": OIDC_CLIENT_ID_ENV,
    "OIDC_CLIENT_SECRET": OIDC_CLIENT_SECRET_ENV,
    "OIDC_CONFIG_URL": OIDC_CONFIG_URL_ENV,
}


def _warn_on_legacy_oidc_env() -> None:
    """Warn when a deployment still sets the pre-rename generic `OIDC_*` vars.

    `_create_auth` reads only the `AIRBYTE_MCP_OIDC_*` names now, so a bare
    `OIDC_CLIENT_ID` / `OIDC_CLIENT_SECRET` / `OIDC_CONFIG_URL` carried over from
    the old contract is silently ignored. Surfacing it turns a silent
    no-interactive-OIDC misconfiguration into a visible migration hint.
    """
    # Membership checks (not `os.getenv`) so the secret *values* are never read;
    # only the env var *names* are ever surfaced in the log.
    ignored = [
        f"`{legacy}` (rename to `{branded}`)"
        for legacy, branded in _LEGACY_OIDC_ENV.items()
        if legacy in os.environ and branded not in os.environ
    ]
    if ignored:
        logger.warning(
            "Ignoring legacy transport-auth env var(s): %s. This server reads the "
            "Airbyte-branded `AIRBYTE_MCP_OIDC_*` names now.",
            "; ".join(ignored),
        )


def _resolve_signing_key() -> dict[str, str]:
    """Resolve the headless JWT verifier's signing-key source.

    Returns the generic `MCP_AUTH_JWKS_URI` / `MCP_AUTH_JWT_PUBLIC_KEY` pair.
    A deployment may set either env var to point at its own realm; the Airbyte
    Cloud JWKS default applies only when *neither* is set, so a self-hosted
    static public key isn't shadowed by a leftover Cloud JWKS URI.
    """
    jwks_uri = os.getenv(JWKS_URI_ENV, "")
    public_key = os.getenv(JWT_PUBLIC_KEY_ENV, "")
    if not jwks_uri and not public_key:
        jwks_uri = AIRBYTE_CLOUD_JWKS_URI
    return {
        "MCP_AUTH_JWKS_URI": jwks_uri,
        "MCP_AUTH_JWT_PUBLIC_KEY": public_key,
    }


def _create_auth() -> AuthProvider | None:
    """Assemble the transport auth provider, defaulting to Airbyte Cloud.

    Reads this server's `AIRBYTE_MCP_*` env vars (falling back to Airbyte Cloud's
    public realm defaults), translates them to the generic names that
    `fastmcp_extensions.resolve_mcp_auth` consumes, and lets it wire up an
    interactive `OIDCProxy` and/or a headless `JWTVerifier`, combined via
    `MultiAuth`. Because a JWKS default is always present, HTTP transport
    always verifies bearer tokens; the interactive path additionally activates
    once the OIDC client credentials are supplied.
    """
    _warn_on_legacy_oidc_env()
    resolved_env: dict[str, str] = {
        generic_name: os.getenv(our_name, default)
        for our_name, (generic_name, default) in _JWT_ENV_MAP.items()
    }
    resolved_env.update(_resolve_signing_key())
    resolved_env[MCP_SERVER_URL_ENV] = os.getenv(MCP_SERVER_URL_ENV, DEFAULT_MCP_SERVER_URL)

    oidc_client_id = os.getenv(OIDC_CLIENT_ID_ENV, "")
    oidc_client_secret = os.getenv(OIDC_CLIENT_SECRET_ENV, "")
    resolved_env["OIDC_CLIENT_ID"] = oidc_client_id
    resolved_env["OIDC_CLIENT_SECRET"] = oidc_client_secret
    # Only advertise the OIDC discovery URL (defaulting to Airbyte Cloud) once
    # both client credentials are present. Otherwise `resolve_mcp_auth` sees a
    # config URL with no credentials and logs a spurious "incomplete OIDC"
    # warning on every headless/bearer-only startup.
    if oidc_client_id and oidc_client_secret:
        resolved_env["OIDC_CONFIG_URL"] = os.getenv(
            OIDC_CONFIG_URL_ENV, AIRBYTE_CLOUD_OIDC_CONFIG_URL
        )
    return resolve_mcp_auth(env=resolved_env)


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
        TRUSTED_EXECUTION_CONFIG_ARG,
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

validate_airbyte_domains(app)


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
