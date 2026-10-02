# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MCP (Model Context Protocol) server for PyAirbyte connector management.

Supports two transport modes:

- **stdio** (default): For local MCP clients (Claude Desktop, etc.). Auth is not
  enforced; the provider assembled below is ignored by the stdio transport.
- **HTTP**: For hosted deployment. Start via `airbyte-mcp-http` entry point or
  `poe mcp-serve-http`. This server maps its own branded `AIRBYTE_MCP_*` env vars
  into the typed configs that `fastmcp_extensions.build_mcp_auth` consumes, which
  supports two client shapes on the same deployment:
    - **Interactive** (humans in a browser): Keycloak Authorization Code + PKCE
      via `OIDCProxy`, active once `AIRBYTE_MCP_OIDC_CLIENT_ID`,
      `AIRBYTE_MCP_OIDC_CLIENT_SECRET`, and `AIRBYTE_MCP_OIDC_CONFIG_URL` (the
      OIDC discovery URL) are supplied. Consent is collected on the IdP's own
      branded login page; `OIDCProxy`'s generic consent screen is skipped.
      Setting `AIRBYTE_MCP_SSO_OIDC_CONFIG_URL_TEMPLATE` as well puts an
      identifier-entry page in front of that flow, so SSO customers can type
      their company identifier and authenticate against that Keycloak realm
      with the same OIDC client (see `airbyte.mcp._sso_auth`).
    - **Headless** (agents, CI): the client mints its own short-lived bearer
      token via the OAuth 2.0 client credentials grant and sends it as
      `Authorization: Bearer <token>`. The server verifies it with a
      `JWTVerifier`, active once a signing-key source (`AIRBYTE_MCP_AUTH_JWKS_URI`
      or `AIRBYTE_MCP_AUTH_JWT_PUBLIC_KEY`) is configured (no browser, no
      stored/rotating refresh token). Setting `AIRBYTE_MCP_AUTH_USER_JWKS_URI`
      adds a second headless verifier for user-realm tokens forwarded by a
      trusted first-party app, pinned via the `azp` allowlist in
      `AIRBYTE_MCP_AUTH_USER_TOKEN_CLIENT_IDS`.
  When both are active they are combined via `MultiAuth`; when neither is
  configured `_create_auth` returns `None` and HTTP transport runs
  unauthenticated (a startup warning is logged in `http_main`).

This module declares only the env var *names* and maps their values into the
typed `OIDCAuthConfig` / `JWTAuthConfig` objects that `build_mcp_auth` consumes,
so the extensions library stays provider-neutral and reads no env itself. It
embeds no provider-specific configuration *values* (a realm's discovery URL,
issuer, JWKS URI, audience, algorithm, etc.); those are supplied at deploy time
by the deployment's own repo — e.g. the hosted Cloud MCP image in
`airbyte-ops-mcp` sets the `AIRBYTE_MCP_*` env for the Airbyte Cloud realm.

For the headless path, an agent mints an access token from its client id/secret
(via the deployment's `<api_root>/applications/token` endpoint) and sends it as
`Authorization: Bearer`. When the deployment's realm is Airbyte Cloud, that
single token both authenticates transport (verified here) and authorizes
downstream Cloud API calls, because an Airbyte-Cloud-issued JWT is itself a valid
Cloud API bearer.
"""

from __future__ import annotations

import asyncio
import logging
import os
import pkgutil
import sys
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Protocol

from fastmcp_extensions import (
    JWTAuthConfig,
    OIDCAuthConfig,
    TelemetryConfig,
    TelemetrySinks,
    build_mcp_auth,
    mcp_server,
)
from starlette.responses import JSONResponse


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from fastmcp import FastMCP
    from fastmcp.server.auth import AuthProvider
    from key_value.aio.protocols.key_value import AsyncKeyValue
    from starlette.requests import Request

from airbyte._util.meta import set_mcp_mode
from airbyte._util.telemetry import DO_NOT_TRACK, PYAIRBYTE_MCP_TRACKING_KEY
from airbyte.constants import AIRBYTE_OFFLINE_MODE, _str_to_bool, is_hosted_mcp_mode
from airbyte.mcp._config import load_secrets_to_env_vars
from airbyte.mcp._error_handling import (
    MCP_TOOL_USER_FACING_ERRORS,
    format_user_facing_error,
)
from airbyte.mcp._scope import CallScopeMiddleware, call_scope_properties
from airbyte.mcp._sso_auth import SsoRealmConfig, make_sso_proxy_factory
from airbyte.mcp._telemetry import ServerConnectedTelemetryMiddleware, request_properties
from airbyte.mcp._tool_utils import (
    AIRBYTE_EXCLUDE_MODULES_CONFIG_ARG,
    AIRBYTE_INCLUDE_MODULES_CONFIG_ARG,
    AIRBYTE_READONLY_MODE_CONFIG_ARG,
    API_URL_CONFIG_ARG,
    BEARER_TOKEN_CONFIG_ARG,
    CLIENT_ID_CONFIG_ARG,
    CLIENT_SECRET_CONFIG_ARG,
    CONFIG_API_URL_CONFIG_ARG,
    INSIDERS_CONFIG_ARG,
    ORGANIZATION_ID_CONFIG_ARG,
    TRUSTED_EXECUTION_CONFIG_ARG,
    WORKSPACE_ID_CONFIG_ARG,
    airbyte_module_filter,
    airbyte_readonly_mode_filter,
    validate_airbyte_domains,
)
from airbyte.mcp._user_identity import (
    AirbyteUserMiddleware,
    airbyte_user_properties,
    current_airbyte_user_id,
)
from airbyte.mcp.cloud import register_cloud_tools
from airbyte.mcp.guidance import (
    KAPA_API_KEY_CONFIG_ARG,
    KAPA_RETRIEVAL_API_URL_CONFIG_ARG,
    KNOWLEDGE_SEARCH_CAPABILITY,
    is_knowledge_search_available,
    register_guidance_tools,
)
from airbyte.mcp.interactive import register_interactive_tools
from airbyte.mcp.local import register_local_tools
from airbyte.mcp.registry import register_registry_tools
from airbyte.secrets import SecretSourceEnum
from airbyte.secrets.config import disable_secret_source


# =============================================================================
# Server Instructions
# =============================================================================
# This text is provided to AI agents via the MCP protocol's "instructions" field.
# It helps agents understand when to use this server's tools, especially when
# tool search is enabled. For more context, see:
# - FastMCP docs: https://gofastmcp.com/servers/overview
# - Claude tool search: https://www.anthropic.com/news/tool-use-improvements
# =============================================================================

_INSTRUCTIONS_INTRO = """\
PyAirbyte connector management and data integration server for discovering,
deploying, and running Airbyte connectors.

Use this server for:
- Discovering connectors from the Airbyte registry (sources and destinations)
- Deploying sources, destinations, and connections to Airbyte Cloud
- Running cloud syncs and monitoring sync status
- Managing custom connector definitions in Airbyte Cloud"""

_INSTRUCTIONS_LOCAL_USES = """
- Local connector execution for data extraction without cloud deployment
- Listing and describing environment variables for connector configuration"""

_INSTRUCTIONS_CLOUD_MODE = """

Operational modes:
- Cloud operations: Deploy and manage connectors on Airbyte Cloud."""

_INSTRUCTIONS_STDIO_CLOUD_AUTH = """
  Authenticate with AIRBYTE_CLOUD_CLIENT_ID + AIRBYTE_CLOUD_CLIENT_SECRET (or
  AIRBYTE_CLOUD_BEARER_TOKEN), and optionally set AIRBYTE_CLOUD_WORKSPACE_ID."""

_INSTRUCTIONS_WORKSPACE_GUIDANCE = """
  When a tool's workspace_id is omitted, the session's workspace is used: the
  workspace configured for the connection if one is set, otherwise the
  authenticated user's default (or only) workspace. Use get_default_cloud_context or
  list_cloud_workspaces to discover workspaces. Only call list_cloud_organizations
  when you need to search organizations by name, passing name_contains. If multiple
  organizations or workspaces are candidates, ask the user to choose; never select
  automatically."""

_INSTRUCTIONS_LOCAL_MODE = """
- Local operations: Run connectors locally for data extraction (requires
  AIRBYTE_PROJECT_DIR for artifact storage)"""

_INSTRUCTIONS_SAFETY = """

Safety features:
- Safe mode (default): Restricts destructive operations to objects created in
  the current session
- Read-only mode: Disables all write operations for cloud resources"""


def build_mcp_server_instructions(*, hosted: bool) -> str:
    """Return the server instructions; local and env-var guidance is stdio-only."""
    parts = [_INSTRUCTIONS_INTRO]
    if not hosted:
        parts.append(_INSTRUCTIONS_LOCAL_USES)
    parts.append(_INSTRUCTIONS_CLOUD_MODE)
    if not hosted:
        parts.append(_INSTRUCTIONS_STDIO_CLOUD_AUTH)
    parts.append(_INSTRUCTIONS_WORKSPACE_GUIDANCE)
    if not hosted:
        parts.append(_INSTRUCTIONS_LOCAL_MODE)
    parts.append(_INSTRUCTIONS_SAFETY)
    return "".join(parts)


MCP_SERVER_INSTRUCTIONS = build_mcp_server_instructions(hosted=is_hosted_mcp_mode())

logger = logging.getLogger(__name__)

# This server's own transport-auth env vars. It owns these *names* and maps the
# *values* into the typed `OIDCAuthConfig` / `JWTAuthConfig` objects that
# `build_mcp_auth` consumes; the extensions library reads no env itself. The
# auth vars use the branded `AIRBYTE_MCP_*` namespace as an added layer over
# generic OAuth names; `MCP_SERVER_URL` (a deployment URL, not an auth var)
# stays unbranded. Only names live here — the concrete values (e.g. a specific
# realm's endpoints) are supplied at deploy time by the deployment's own repo,
# keeping infrastructure configuration out of this generic library.

# Public base URL of this deployment (also used for OIDC redirect callbacks);
# `http_main` reuses it to derive the mounted MCP path.
MCP_SERVER_URL_ENV = "MCP_SERVER_URL"

# Interactive OIDC (`OIDCProxy`). Client id + secret gate the interactive path;
# the discovery URL comes from the deployment.
OIDC_CLIENT_ID_ENV = "AIRBYTE_MCP_OIDC_CLIENT_ID"
OIDC_CLIENT_SECRET_ENV = "AIRBYTE_MCP_OIDC_CLIENT_SECRET"
OIDC_CONFIG_URL_ENV = "AIRBYTE_MCP_OIDC_CONFIG_URL"

# Upstream authorize scopes requested for the interactive OIDC flow, also
# advertised to clients via DCR/`.well-known` and enforced on the verified
# upstream token. `openid` is required for OIDC: without it the IdP may issue an
# identity-only token that downstream APIs reject.
AIRBYTE_CLOUD_REQUIRED_OIDC_SCOPES: str = "openid email profile"

# Forwarded to the upstream authorize endpoint. `prompt=consent` makes the IdP
# render its own login/consent page rather than silently reusing an existing
# browser session, so the branded page is what the user actually sees.
AIRBYTE_CLOUD_EXTRA_AUTHORIZE_PARAMS: dict[str, str] = {"prompt": "consent"}

# Headless JWT verifier. A signing-key source (`JWKS_URI_ENV` or
# `JWT_PUBLIC_KEY_ENV`) activates it; issuer/audience/algorithm refine it.
JWKS_URI_ENV = "AIRBYTE_MCP_AUTH_JWKS_URI"
JWT_PUBLIC_KEY_ENV = "AIRBYTE_MCP_AUTH_JWT_PUBLIC_KEY"
JWT_ISSUER_ENV = "AIRBYTE_MCP_AUTH_ISSUER"
JWT_AUDIENCE_ENV = "AIRBYTE_MCP_AUTH_AUDIENCE"
JWT_ALGORITHM_ENV = "AIRBYTE_MCP_AUTH_ALGORITHM"

# Optional second headless verifier for user-realm tokens forwarded by a
# trusted first-party app (e.g. the Ops Webapp session token). Activated by
# `USER_JWKS_URI_ENV`, which also requires `USER_ISSUER_ENV` (pinned issuer)
# and `USER_TOKEN_CLIENT_IDS_ENV`; `aud` is not checked (Keycloak user-token
# audiences vary by client) — the `azp` allowlist is the trust boundary.
USER_JWKS_URI_ENV = "AIRBYTE_MCP_AUTH_USER_JWKS_URI"
USER_ISSUER_ENV = "AIRBYTE_MCP_AUTH_USER_ISSUER"
USER_ALGORITHM_ENV = "AIRBYTE_MCP_AUTH_USER_ALGORITHM"
USER_TOKEN_CLIENT_IDS_ENV = "AIRBYTE_MCP_AUTH_USER_TOKEN_CLIENT_IDS"

# Names a durable-storage factory (`"package.module:callable"`) for the
# interactive `OIDCProxy`'s OAuth state. The concrete backend (and its infra
# config) lives in the deployment's own package, keeping PyAirbyte generic.
OIDC_CLIENT_STORAGE_FACTORY_ENV = "AIRBYTE_MCP_OIDC_CLIENT_STORAGE_FACTORY"

# SSO realm login. Setting the discovery-URL template activates the
# identifier-entry page in front of the interactive OIDC flow; the template is
# the default realm's discovery URL with the realm name replaced by a `{realm}`
# path segment that the user's company identifier is substituted into. The IdP
# hint, when set, is forwarded as Keycloak's `kc_idp_hint` so the realm hands
# straight off to the customer's identity provider.
SSO_OIDC_CONFIG_URL_TEMPLATE_ENV = "AIRBYTE_MCP_SSO_OIDC_CONFIG_URL_TEMPLATE"
SSO_IDP_HINT_ENV = "AIRBYTE_MCP_SSO_IDP_HINT"

# Realm names that can never be a customer's SSO realm. Airbyte's internal realms
# all start with `_`, which the identifier pattern already rejects, so only
# Keycloak's own admin realm needs listing. `airbyte` is a regular customer realm.
AIRBYTE_CLOUD_RESERVED_SSO_REALMS: frozenset[str] = frozenset({"master"})

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
    override falls back to `default` rather than an empty string.
    """
    value = os.getenv(name, "").strip()
    return value or default


def _resolve_client_storage(*, encryption_source_material: str) -> AsyncKeyValue | None:
    """Resolve the durable `OIDCProxy` OAuth-state store, if one is configured.

    Reads `AIRBYTE_MCP_OIDC_CLIENT_STORAGE_FACTORY` (`"package.module:callable"`),
    imports the named factory, and calls it to build the store. Returns `None`
    when the var is unset/blank, keeping `OIDCProxy`'s in-memory default (fine
    for single-instance local dev). PyAirbyte stays backend-agnostic: it never
    imports a concrete store, so the infrastructure-specific factory (e.g. the
    Fernet-wrapped Firestore store for the hosted Cloud MCP image) ships in the
    deployment's own package.

    Raises `ValueError` (naming the env var and expected format) when the
    factory reference is malformed or points at a missing symbol, so a
    misconfigured deployment fails with a clear message instead of a bare
    import traceback.
    """
    factory_spec = os.getenv(OIDC_CLIENT_STORAGE_FACTORY_ENV, "").strip()
    if not factory_spec:
        return None
    try:
        factory: _ClientStorageFactory = pkgutil.resolve_name(factory_spec)
    except (ImportError, AttributeError, ValueError) as exc:
        msg = (
            f"{OIDC_CLIENT_STORAGE_FACTORY_ENV}={factory_spec!r} could not be "
            "resolved; expected a 'package.module:callable' reference to an "
            "importable OAuth-state store factory."
        )
        raise ValueError(msg) from exc
    return factory(encryption_source_material=encryption_source_material)


def _resolve_sso_config(*, interactive_oidc_configured: bool) -> SsoRealmConfig | None:
    """Resolve the SSO realm-login settings, if a deployment enabled them.

    `AIRBYTE_MCP_SSO_OIDC_CONFIG_URL_TEMPLATE` activates the feature. It is the
    default realm's discovery URL with the realm name replaced by `{realm}`, e.g.
    `https://cloud.airbyte.com/auth/realms/{realm}/.well-known/openid-configuration`.
    `AIRBYTE_MCP_SSO_IDP_HINT` optionally names the realm's identity-provider
    alias (`default` on Airbyte Cloud) so Keycloak hands straight off to the
    customer IdP instead of showing its own login form. Returns `None` when the
    template is unset or blank, leaving the interactive path exactly as before.

    Raises `ValueError` naming the env var when the template is malformed, or
    when it is set without the interactive OIDC client credentials it extends.
    """
    template = os.getenv(SSO_OIDC_CONFIG_URL_TEMPLATE_ENV, "").strip()
    if not template:
        return None
    if not interactive_oidc_configured:
        msg = (
            f"{SSO_OIDC_CONFIG_URL_TEMPLATE_ENV} is set but the interactive OIDC path "
            f"is not configured; SSO login extends it, so also set {OIDC_CLIENT_ID_ENV}, "
            f"{OIDC_CLIENT_SECRET_ENV}, and {OIDC_CONFIG_URL_ENV}."
        )
        raise ValueError(msg)
    try:
        return SsoRealmConfig(
            discovery_url_template=template,
            idp_hint=os.getenv(SSO_IDP_HINT_ENV, "").strip() or None,
            reserved_realms=AIRBYTE_CLOUD_RESERVED_SSO_REALMS,
        )
    except ValueError as exc:
        msg = f"{SSO_OIDC_CONFIG_URL_TEMPLATE_ENV}={template!r} is invalid: {exc}"
        raise ValueError(msg) from exc


def _create_auth() -> AuthProvider | None:
    """Assemble the transport auth provider from this server's env configuration.

    Reads this server's branded `AIRBYTE_MCP_*` env vars and maps them into the
    typed `JWTAuthConfig` / `OIDCAuthConfig` objects that
    `fastmcp_extensions.build_mcp_auth` consumes, which wires up a headless
    `JWTVerifier` and/or an interactive `OIDCProxy`, combined via `MultiAuth`.
    The headless verifier activates once a signing-key source
    (`AIRBYTE_MCP_AUTH_JWKS_URI` or `AIRBYTE_MCP_AUTH_JWT_PUBLIC_KEY`) is
    configured; setting `AIRBYTE_MCP_AUTH_USER_JWKS_URI` adds a second,
    `azp`-allowlisted headless verifier for user-realm tokens forwarded by a
    trusted first-party app; the interactive path activates once the OIDC
    client credentials are supplied, and gains the SSO identifier-entry page
    (an `OIDCProxy` subclass supplied through `OIDCAuthConfig.proxy_factory`)
    once `AIRBYTE_MCP_SSO_OIDC_CONFIG_URL_TEMPLATE` is set too. Returns `None` when
    neither path is configured, so the server falls back to unauthenticated
    local behavior. The `stdio` transport ignores the provider entirely.

    This server declares only the env var *names*; the concrete values (e.g. a
    deployment's realm endpoints, issuer, audience, and discovery URL) are
    supplied at deploy time by the deployment's own repo, keeping
    infrastructure configuration out of this generic library.
    """
    base_url = _env_or_default(MCP_SERVER_URL_ENV, DEFAULT_MCP_SERVER_URL)

    jwt_configs: list[JWTAuthConfig] = []
    jwks_uri = os.getenv(JWKS_URI_ENV, "").strip()
    public_key = os.getenv(JWT_PUBLIC_KEY_ENV, "").strip()
    if jwks_uri or public_key:
        jwt_configs.append(
            JWTAuthConfig(
                jwks_uri=jwks_uri or None,
                public_key=public_key or None,
                issuer=os.getenv(JWT_ISSUER_ENV, "").strip() or None,
                audience=os.getenv(JWT_AUDIENCE_ENV, "").strip() or None,
                algorithm=os.getenv(JWT_ALGORITHM_ENV, "").strip() or None,
                base_url=base_url,
            )
        )

    user_jwks_uri = os.getenv(USER_JWKS_URI_ENV, "").strip()
    if user_jwks_uri:
        client_ids = frozenset(
            entry.strip()
            for entry in os.getenv(USER_TOKEN_CLIENT_IDS_ENV, "").split(",")
            if entry.strip()
        )
        if not client_ids:
            msg = (
                f"{USER_JWKS_URI_ENV} is set but {USER_TOKEN_CLIENT_IDS_ENV} is "
                "empty; the user-realm verifier needs a non-empty "
                "comma-separated azp allowlist."
            )
            raise ValueError(msg)
        user_issuer = os.getenv(USER_ISSUER_ENV, "").strip()
        if not user_issuer:
            msg = (
                f"{USER_JWKS_URI_ENV} is set but {USER_ISSUER_ENV} is empty; "
                "the user-realm verifier must pin the token issuer."
            )
            raise ValueError(msg)
        jwt_configs.append(
            JWTAuthConfig(
                jwks_uri=user_jwks_uri,
                issuer=user_issuer,
                algorithm=os.getenv(USER_ALGORITHM_ENV, "").strip() or None,
                base_url=base_url,
                allowed_client_ids=client_ids,
            )
        )

    oidc: OIDCAuthConfig | None = None
    oidc_client_id = os.getenv(OIDC_CLIENT_ID_ENV, "").strip()
    oidc_client_secret = os.getenv(OIDC_CLIENT_SECRET_ENV, "").strip()
    if bool(oidc_client_id) != bool(oidc_client_secret):
        present, missing = (
            (OIDC_CLIENT_ID_ENV, OIDC_CLIENT_SECRET_ENV)
            if oidc_client_id
            else (OIDC_CLIENT_SECRET_ENV, OIDC_CLIENT_ID_ENV)
        )
        msg = (
            f"{present} is set but {missing} is not; the interactive OIDC path "
            "needs both client credentials. Set both, or neither."
        )
        raise ValueError(msg)
    sso_config = _resolve_sso_config(
        interactive_oidc_configured=bool(oidc_client_id and oidc_client_secret)
    )
    if oidc_client_id and oidc_client_secret:
        config_url = os.getenv(OIDC_CONFIG_URL_ENV, "").strip()
        if not config_url:
            msg = (
                f"{OIDC_CLIENT_ID_ENV} and {OIDC_CLIENT_SECRET_ENV} are set but "
                f"{OIDC_CONFIG_URL_ENV} is not; the interactive OIDC path needs "
                "an OpenID Connect discovery URL."
            )
            raise ValueError(msg)
        oidc = OIDCAuthConfig(
            config_url=config_url,
            client_id=oidc_client_id,
            client_secret=oidc_client_secret,
            base_url=base_url,
            required_scopes=AIRBYTE_CLOUD_REQUIRED_OIDC_SCOPES.split(),
            client_storage=_resolve_client_storage(encryption_source_material=oidc_client_secret),
            # Consent is collected upstream on the branded IdP login page.
            # Leaving `OIDCProxy`'s own consent screen on would show the user a
            # generic FastMCP page first; `"external"` skips it without the
            # "consent disabled" warning that `False` logs on every startup.
            require_authorization_consent="external",
            extra_authorize_params=AIRBYTE_CLOUD_EXTRA_AUTHORIZE_PARAMS,
            # Swaps in the realm-per-login proxy only when SSO is configured;
            # the stock `OIDCProxy` is built otherwise.
            proxy_factory=make_sso_proxy_factory(sso_config) if sso_config else None,
        )

    return build_mcp_auth(oidc=oidc, jwt=jwt_configs or None, base_url=base_url)


SEGMENT_USER_ID = "airbyte-mcp"
"""Fallback Segment user ID for server telemetry when caller identity is unavailable."""


def _segment_write_key() -> str | None:
    """Return the Segment write key for tool-call telemetry, or `None` when opted out."""
    offline_mode_from_env = os.environ.get("AIRBYTE_OFFLINE_MODE")
    # Dotenv secrets load after constants are imported, so check the environment at call time.
    offline_mode = AIRBYTE_OFFLINE_MODE or _str_to_bool(offline_mode_from_env, default=False)
    if os.environ.get(DO_NOT_TRACK) or offline_mode:
        return None

    return PYAIRBYTE_MCP_TRACKING_KEY


load_secrets_to_env_vars()

segment_write_key = _segment_write_key()
if segment_write_key is None:
    logger.info("Segment telemetry is disabled; MCP tool-call telemetry remains log-only.")

lifecycle_telemetry_sinks = TelemetrySinks(
    package_name="airbyte",
    segment_write_key=segment_write_key,
    segment_user_id=lambda: current_airbyte_user_id() or SEGMENT_USER_ID,
)
"""Sinks for MCP session lifecycle events, configured like tool-call telemetry."""


@asynccontextmanager
async def _mcp_mode_lifespan(  # noqa: RUF029
    server: FastMCP,  # noqa: ARG001
) -> AsyncIterator[dict[str, object]]:
    """Mark the process as running in MCP mode for the lifetime of the server."""
    set_mcp_mode()
    # Secrets were loaded at import, before MCP mode was known; prompts would read
    # from stdin, which belongs to the transport now.
    disable_secret_source(SecretSourceEnum.PROMPT)
    yield {}


app = mcp_server(
    name="airbyte-mcp",
    package_name="airbyte",
    instructions=MCP_SERVER_INSTRUCTIONS,
    include_standard_tool_filters=True,
    server_config_args=[
        AIRBYTE_READONLY_MODE_CONFIG_ARG,
        AIRBYTE_EXCLUDE_MODULES_CONFIG_ARG,
        AIRBYTE_INCLUDE_MODULES_CONFIG_ARG,
        INSIDERS_CONFIG_ARG,
        WORKSPACE_ID_CONFIG_ARG,
        ORGANIZATION_ID_CONFIG_ARG,
        BEARER_TOKEN_CONFIG_ARG,
        CLIENT_ID_CONFIG_ARG,
        CLIENT_SECRET_CONFIG_ARG,
        API_URL_CONFIG_ARG,
        CONFIG_API_URL_CONFIG_ARG,
        TRUSTED_EXECUTION_CONFIG_ARG,
        KAPA_API_KEY_CONFIG_ARG,
        KAPA_RETRIEVAL_API_URL_CONFIG_ARG,
    ],
    capability_resolvers={
        KNOWLEDGE_SEARCH_CAPABILITY: is_knowledge_search_available,
    },
    tool_filters=[
        airbyte_readonly_mode_filter,
        airbyte_module_filter,
    ],
    auth=_create_auth(),
    lifespan=_mcp_mode_lifespan,
    telemetry=TelemetryConfig(
        package_name="airbyte",
        segment_write_key=segment_write_key,
        segment_user_id=lambda: current_airbyte_user_id() or SEGMENT_USER_ID,
        extra_properties=lambda: {
            **request_properties(),
            **call_scope_properties(),
            **airbyte_user_properties(),
        },
    ),
    user_facing_errors=MCP_TOOL_USER_FACING_ERRORS,
    user_facing_error_formatter=format_user_facing_error,
)
"""The Airbyte MCP Server application instance."""

app.add_middleware(ServerConnectedTelemetryMiddleware(lifecycle_telemetry_sinks))
app.middleware.insert(0, CallScopeMiddleware())
app.middleware.insert(0, AirbyteUserMiddleware())

# Register tools from each module
register_cloud_tools(app)
register_local_tools(app)
register_registry_tools(app)
register_interactive_tools(app)
register_guidance_tools(app)

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
