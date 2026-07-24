# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MCP (Model Context Protocol) server for PyAirbyte connector management.

Supports two transport modes:

- **stdio** (default): For local MCP clients (Claude Desktop, etc.). Auth is not
  enforced; the provider assembled below is ignored by the stdio transport.
- **HTTP**: For hosted deployment. Start via `airbyte-mcp-http` entry point or
  `poe mcp-serve-http`. HTTP transport is **always authenticated**, defaulting
  to Airbyte Cloud with zero auth config. This server maps its own branded
  `AIRBYTE_MCP_*` env vars into the typed configs that
  `fastmcp_extensions.build_mcp_auth` consumes, which supports two client shapes
  on the same deployment:
    - **Interactive** (humans in a browser): Keycloak Authorization Code + PKCE
      via `OIDCProxy`, active once `AIRBYTE_MCP_OIDC_CLIENT_ID` and
      `AIRBYTE_MCP_OIDC_CLIENT_SECRET` are supplied (the OIDC discovery URL
      defaults to Airbyte Cloud).
    - **Headless** (agents, CI): the client mints its own short-lived bearer
      token via the OAuth 2.0 client credentials grant and sends it as
      `Authorization: Bearer <token>`. The server verifies it with a
      `JWTVerifier` against Airbyte Cloud's application-client realm by default
      (no browser, no stored/rotating refresh token).
  When both are active they are combined via `MultiAuth`.

This module owns the Airbyte Cloud realm defaults (non-secret, publicly
discoverable) and maps its branded `AIRBYTE_MCP_OIDC_*` / `AIRBYTE_MCP_AUTH_*`
env vars into the typed `OIDCAuthConfig` / `JWTAuthConfig` objects that
`build_mcp_auth` consumes, so the extensions library stays provider-neutral and
reads no env itself. A self-hosted deployment pointing at its own Airbyte
instance overrides any default via the matching env var.

An agent mints an Airbyte Cloud access token from its `AIRBYTE_CLOUD_CLIENT_ID` /
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
import pkgutil
import sys
from typing import TYPE_CHECKING, Protocol

from fastmcp_extensions import (
    JWTAuthConfig,
    OIDCAuthConfig,
    build_mcp_auth,
    mcp_server,
)
from starlette.responses import JSONResponse


if TYPE_CHECKING:
    from fastmcp.server.auth import AuthProvider
    from key_value.aio.protocols.key_value import AsyncKeyValue
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

# This server's own (branded) transport-auth env vars. It owns these names and
# maps them into the typed `OIDCAuthConfig` / `JWTAuthConfig` objects that
# `build_mcp_auth` consumes; the extensions library reads no env itself. The
# branded `AIRBYTE_MCP_*` namespace is an added layer over generic OAuth names.

# Public base URL of this deployment (also used for OIDC redirect callbacks);
# `http_main` reuses it to derive the mounted MCP path.
MCP_SERVER_URL_ENV = "MCP_SERVER_URL"

# Interactive OIDC (`OIDCProxy`). Client id + secret gate the interactive path;
# the discovery URL defaults to Airbyte Cloud when unset.
OIDC_CLIENT_ID_ENV = "AIRBYTE_MCP_OIDC_CLIENT_ID"
OIDC_CLIENT_SECRET_ENV = "AIRBYTE_MCP_OIDC_CLIENT_SECRET"
OIDC_CONFIG_URL_ENV = "AIRBYTE_MCP_OIDC_CONFIG_URL"

# Headless JWT verifier. Each defaults to the Airbyte Cloud realm below, so a
# self-hosted deployment overrides only the field(s) pointing at its own realm.
JWKS_URI_ENV = "AIRBYTE_MCP_AUTH_JWKS_URI"
JWT_PUBLIC_KEY_ENV = "AIRBYTE_MCP_AUTH_JWT_PUBLIC_KEY"
JWT_ISSUER_ENV = "AIRBYTE_MCP_AUTH_ISSUER"
JWT_AUDIENCE_ENV = "AIRBYTE_MCP_AUTH_AUDIENCE"
JWT_ALGORITHM_ENV = "AIRBYTE_MCP_AUTH_ALGORITHM"

# Names a durable-storage factory (`"package.module:callable"`) for the
# interactive `OIDCProxy`'s OAuth state. The concrete backend (and its infra
# config) lives in the deployment's own package, keeping PyAirbyte generic.
OIDC_CLIENT_STORAGE_FACTORY_ENV = "AIRBYTE_MCP_OIDC_CLIENT_STORAGE_FACTORY"

# Airbyte Cloud's application-client realm (non-secret, publicly discoverable).
# Tokens minted from an Airbyte Cloud `client_id`/`client_secret` via
# `<api_root>/applications/token` are RS256 JWTs issued by this realm, and the
# same token is a valid Airbyte Cloud API bearer — so verifying against this
# realm by default lets one token both authenticate transport and authorize
# downstream Cloud calls.
AIRBYTE_CLOUD_OIDC_CONFIG_URL = (
    "https://cloud.airbyte.com/auth/realms/airbyte/.well-known/openid-configuration"
)
AIRBYTE_CLOUD_ISSUER = "https://cloud.airbyte.com/auth/realms/_airbyte-application-clients"
AIRBYTE_CLOUD_JWKS_URI = f"{AIRBYTE_CLOUD_ISSUER}/protocol/openid-connect/certs"
AIRBYTE_CLOUD_AUDIENCE = "account"
AIRBYTE_CLOUD_ALGORITHM = "RS256"

DEFAULT_HTTP_HOST = "0.0.0.0"
DEFAULT_HTTP_PORT = 8080
DEFAULT_MCP_SERVER_URL = f"http://localhost:{DEFAULT_HTTP_PORT}"


class _ClientStorageFactory(Protocol):
    """Callable that builds a durable `OIDCProxy` OAuth-state backend.

    A deployment names its factory via
    `AIRBYTE_MCP_OIDC_CLIENT_STORAGE_FACTORY` (`"package.module:callable"`). The
    callable receives the OIDC client secret as `encryption_source_material` so
    it can derive an at-rest encryption key, and returns an `AsyncKeyValue`
    store. Keeping the concrete backend (Firestore, Redis, ...) behind this hook
    lets PyAirbyte stay generic — the infrastructure-specific factory ships in
    the deployment's own package (e.g. the hosted Cloud MCP image), not here.
    """

    def __call__(self, *, encryption_source_material: str) -> AsyncKeyValue: ...


def _env_or_default(name: str, default: str) -> str:
    """Return the stripped value of env var `name`, or `default` when blank/unset.

    Blank and whitespace-only values are treated as unset so an empty deployment
    override falls back to the Airbyte Cloud realm default rather than an empty
    string.
    """
    value = os.getenv(name, "").strip()
    return value or default


def _resolve_signing_key() -> tuple[str, str]:
    """Resolve the headless JWT verifier's signing-key source.

    Returns the `(jwks_uri, public_key)` pair. A deployment may set either env
    var to point at its own realm; the Airbyte Cloud JWKS default applies only
    when *neither* is set, so a self-hosted static public key isn't shadowed by
    a leftover Cloud JWKS URI. Blank or whitespace-only values are treated as
    unset, and an unset member is returned as the empty string.
    """
    jwks_uri = os.getenv(JWKS_URI_ENV, "").strip()
    public_key = os.getenv(JWT_PUBLIC_KEY_ENV, "").strip()
    if not jwks_uri and not public_key:
        jwks_uri = AIRBYTE_CLOUD_JWKS_URI
    return jwks_uri, public_key


def _resolve_client_storage(*, encryption_source_material: str) -> AsyncKeyValue | None:
    """Resolve the durable `OIDCProxy` OAuth-state store, if one is configured.

    Reads `AIRBYTE_MCP_OIDC_CLIENT_STORAGE_FACTORY` (`"package.module:callable"`),
    imports the named factory, and calls it to build the store. Returns `None`
    when the var is unset/blank, keeping `OIDCProxy`'s in-memory default (fine
    for single-instance local dev). PyAirbyte stays backend-agnostic: it never
    imports a concrete store, so the infrastructure-specific factory (e.g. the
    Fernet-wrapped Firestore store for the hosted Cloud MCP image) ships in the
    deployment's own package.
    """
    factory_spec = os.getenv(OIDC_CLIENT_STORAGE_FACTORY_ENV, "").strip()
    if not factory_spec:
        return None
    factory: _ClientStorageFactory = pkgutil.resolve_name(factory_spec)
    return factory(encryption_source_material=encryption_source_material)


def _create_auth() -> AuthProvider | None:
    """Assemble the transport auth provider, defaulting to Airbyte Cloud.

    Reads this server's branded `AIRBYTE_MCP_*` env vars (falling back to
    Airbyte Cloud's public realm defaults), maps them into the typed
    `JWTAuthConfig` / `OIDCAuthConfig` objects that
    `fastmcp_extensions.build_mcp_auth` consumes, and lets it wire up a headless
    `JWTVerifier` and/or an interactive `OIDCProxy`, combined via `MultiAuth`.
    Because a JWKS default is always present, HTTP transport always verifies
    bearer tokens against Airbyte Cloud's application-client realm; the
    interactive path additionally activates once the OIDC client credentials are
    supplied. The `stdio` transport ignores the provider entirely.
    """
    base_url = _env_or_default(MCP_SERVER_URL_ENV, DEFAULT_MCP_SERVER_URL)

    jwks_uri, public_key = _resolve_signing_key()
    jwt = JWTAuthConfig(
        jwks_uri=jwks_uri or None,
        public_key=public_key or None,
        issuer=_env_or_default(JWT_ISSUER_ENV, AIRBYTE_CLOUD_ISSUER),
        audience=_env_or_default(JWT_AUDIENCE_ENV, AIRBYTE_CLOUD_AUDIENCE),
        algorithm=_env_or_default(JWT_ALGORITHM_ENV, AIRBYTE_CLOUD_ALGORITHM),
        base_url=base_url,
    )

    oidc: OIDCAuthConfig | None = None
    oidc_client_id = os.getenv(OIDC_CLIENT_ID_ENV, "").strip()
    oidc_client_secret = os.getenv(OIDC_CLIENT_SECRET_ENV, "").strip()
    if oidc_client_id and oidc_client_secret:
        oidc = OIDCAuthConfig(
            config_url=_env_or_default(OIDC_CONFIG_URL_ENV, AIRBYTE_CLOUD_OIDC_CONFIG_URL),
            client_id=oidc_client_id,
            client_secret=oidc_client_secret,
            base_url=base_url,
            client_storage=_resolve_client_storage(encryption_source_material=oidc_client_secret),
        )

    return build_mcp_auth(oidc=oidc, jwt=jwt, base_url=base_url)


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
