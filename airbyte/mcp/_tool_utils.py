# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MCP tool utility functions for policy and config args.

This module provides:
- Safe mode functionality for MCP tools, allowing tracking of resources created
  during a session to prevent accidental deletion of pre-existing resources.
- Config args and filters for backward compatibility with legacy Airbyte env vars.
"""

from __future__ import annotations

import inspect
import os
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

from fastmcp.server.dependencies import get_access_token, get_http_headers
from fastmcp_extensions import (
    ANNOTATION_INTERACTIVE_UI,
    MCPServerConfigArg,
    get_mcp_config,
)
from fastmcp_extensions import mcp_tool as _mcp_tool
from fastmcp_extensions.decorators import (
    _REGISTERED_PROVIDERS,  # noqa: PLC2701
    _REGISTERED_TOOLS,  # noqa: PLC2701
)
from fastmcp_extensions.registration import _ProviderToolAnnotations  # noqa: PLC2701
from fastmcp_extensions.tool_filters import (
    ANNOTATION_MCP_MODULE,
    ANNOTATION_READ_ONLY_HINT,
    CONFIG_INCLUDE_MODULES,
    CONFIG_TRUSTED_EXECUTION,
    get_annotation,
)

from airbyte._util.deployment import is_agents_api_available as _is_agents_api_available
from airbyte.constants import (
    ANNOTATION_EXTERNAL_ACCESS,
    ANNOTATION_PIPELINE_CHANGE,
    CLOUD_API_ROOT_ENV_VAR,
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CLIENT_SECRET_ENV_VAR,
    CLOUD_CONFIG_API_ROOT_ENV_VAR,
    CLOUD_MCP_SAFE_MODE_ENV_VAR,
    CLOUD_ORGANIZATION_ID_ENV_VAR,
    CLOUD_WORKSPACE_ID_ENV_VAR,
    MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR,
    MCP_ALLOW_EXTERNAL_ACCESS_HEADER,
    MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR,
    MCP_ALLOW_PIPELINE_CHANGES_HEADER,
    MCP_BEARER_TOKEN_HEADER,
    MCP_CONFIG_ALLOW_EXTERNAL_ACCESS,
    MCP_CONFIG_ALLOW_PIPELINE_CHANGES,
    MCP_CONFIG_API_URL,
    MCP_CONFIG_BEARER_TOKEN,
    MCP_CONFIG_CLIENT_ID,
    MCP_CONFIG_CLIENT_SECRET,
    MCP_CONFIG_CONFIG_API_URL,
    MCP_CONFIG_EXCLUDE_MODULES,
    MCP_CONFIG_INCLUDE_MODULES,
    MCP_CONFIG_INSIDERS,
    MCP_CONFIG_ORGANIZATION_ID,
    MCP_CONFIG_READONLY_MODE,
    MCP_CONFIG_WORKSPACE_ID,
    MCP_DOMAINS_DISABLED_ENV_VAR,
    MCP_DOMAINS_ENV_VAR,
    MCP_INSIDERS_ENV_VAR,
    MCP_INSIDERS_HEADER,
    MCP_INSIDERS_MODULES,
    MCP_ORGANIZATION_ID_HEADER,
    MCP_READONLY_MODE_ENV_VAR,
    MCP_TRUSTED_EXECUTION_ENV_VAR,
    MCP_WORKSPACE_ID_HEADER,
    _str_to_bool,
)
from airbyte.exceptions import ExternalAccessDisabledError, PyAirbyteInputError


if TYPE_CHECKING:
    from fastmcp import Context, FastMCP
    from mcp.types import Tool

_MCP_TOOL_FUNC = TypeVar("_MCP_TOOL_FUNC", bound=Callable[..., object])
_TOOL_APP_KEY = "_airbyte_tool_app"
_TOOL_META_KEY = "_airbyte_tool_meta"

INTERACTIVE_UI_ANNOTATION = ANNOTATION_INTERACTIVE_UI
"""Annotation indicating the tool requires MCP Apps UI support."""

_AGENTS_MCP_MODULE = "agents"
"""Module whose tools are only advertised when an Agents API is available."""


def is_agents_api_available(config_source: FastMCP | Context) -> bool:
    """Return whether the MCP server's deployment has an Agents API."""
    return _is_agents_api_available(
        public_api_root=get_mcp_config(config_source, MCP_CONFIG_API_URL),
        config_api_root=get_mcp_config(config_source, MCP_CONFIG_CONFIG_API_URL),
    )


# =============================================================================
# Safe Mode Configuration
# =============================================================================


def _resolve_safe_mode() -> bool:
    """Resolve Cloud safe mode, enabled unless explicitly disabled."""
    value = os.environ.get(CLOUD_MCP_SAFE_MODE_ENV_VAR)
    return _str_to_bool(value) is not False


def _safe_mode_explicitly_enabled() -> bool:
    """Return True only when safe mode is explicitly enabled via its environment variable."""
    return _str_to_bool(os.environ.get(CLOUD_MCP_SAFE_MODE_ENV_VAR)) is True


AIRBYTE_CLOUD_MCP_SAFE_MODE = _resolve_safe_mode()
"""Whether safe mode is enabled for Cloud operations."""


AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET = bool(os.environ.get("AIRBYTE_CLOUD_WORKSPACE_ID", "").strip())
"""Whether the AIRBYTE_CLOUD_WORKSPACE_ID environment variable is set.

When set, the workspace_id parameter is hidden from cloud tools.
"""

_GUIDS_CREATED_IN_SESSION: set[str] = set()


class SafeModeError(Exception):
    """Raised when a tool is blocked by safe mode restrictions."""

    pass


def register_guid_created_in_session(guid: str) -> None:
    """Register a GUID as created in this session.

    Args:
        guid: The GUID to register
    """
    _GUIDS_CREATED_IN_SESSION.add(guid)


def check_guid_created_in_session(guid: str) -> None:
    """Check if a GUID was created in this session.

    This is a no-op if AIRBYTE_CLOUD_MCP_SAFE_MODE is set to "0".

    Raises SafeModeError if the GUID was not created in this session and
    AIRBYTE_CLOUD_MCP_SAFE_MODE is set to 1.

    Args:
        guid: The GUID to check
    """
    if AIRBYTE_CLOUD_MCP_SAFE_MODE and guid not in _GUIDS_CREATED_IN_SESSION:
        raise SafeModeError(
            f"Cannot perform destructive operation on '{guid}': "
            f"Object was not created in this session. "
            f"{CLOUD_MCP_SAFE_MODE_ENV_VAR} is set to '1'."
        )


# =============================================================================
# Backward-Compatible Config Args
# =============================================================================
# These config args support the legacy Airbyte-specific environment variables
# while the standard fastmcp-extensions config args support the new MCP_* vars.
# Both sets of filters are applied, so either env var will work.
# =============================================================================

AIRBYTE_READONLY_MODE_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_READONLY_MODE,
    env_var=MCP_READONLY_MODE_ENV_VAR,
    default="0",
    required=False,
)
"""Config arg for legacy AIRBYTE_CLOUD_MCP_READONLY_MODE env var."""

AIRBYTE_EXCLUDE_MODULES_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_EXCLUDE_MODULES,
    env_var=MCP_DOMAINS_DISABLED_ENV_VAR,
    default="",
    required=False,
)
"""Config arg for legacy AIRBYTE_MCP_DOMAINS_DISABLED env var."""

AIRBYTE_INCLUDE_MODULES_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_INCLUDE_MODULES,
    env_var=MCP_DOMAINS_ENV_VAR,
    default="",
    required=False,
)
"""Config arg for legacy AIRBYTE_MCP_DOMAINS env var."""

INSIDERS_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_INSIDERS,
    http_header_key=MCP_INSIDERS_HEADER,
    env_var=MCP_INSIDERS_ENV_VAR,
    default="",
    required=False,
)
"""Config arg for the insiders tools gate.

The default is empty rather than `0`, because `0` is an explicit denial that also refuses
the include-list opt-in.
"""

ALLOW_PIPELINE_CHANGES_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_ALLOW_PIPELINE_CHANGES,
    http_header_key=MCP_ALLOW_PIPELINE_CHANGES_HEADER,
    env_var=MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR,
    default="",
    required=False,
)
"""Config arg for the pipeline-change permission."""

ALLOW_EXTERNAL_ACCESS_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_ALLOW_EXTERNAL_ACCESS,
    http_header_key=MCP_ALLOW_EXTERNAL_ACCESS_HEADER,
    env_var=MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR,
    default="",
    required=False,
)
"""Config arg for the Agents external access permission."""

TRUSTED_EXECUTION_CONFIG_ARG = MCPServerConfigArg(
    name=CONFIG_TRUSTED_EXECUTION,
    env_var=MCP_TRUSTED_EXECUTION_ENV_VAR,
    default="0",
    required=False,
)
"""Config arg mapping the generic `trusted_execution` gate to `AIRBYTE_MCP_TRUSTED_EXECUTION`.

Registering this lets `fastmcp_extensions` resolve the trusted-execution gate from the
Airbyte-specific env var while keeping the generic library Airbyte-agnostic. It
deliberately has no `http_header_key`: the gate *widens* the tool surface, so it must never
be caller-controllable.
"""

WORKSPACE_ID_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_WORKSPACE_ID,
    http_header_key=MCP_WORKSPACE_ID_HEADER,
    env_var=CLOUD_WORKSPACE_ID_ENV_VAR,
    required=False,
    sensitive=False,
)
"""Config arg for workspace ID, supporting both HTTP header and env var."""

ORGANIZATION_ID_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_ORGANIZATION_ID,
    http_header_key=MCP_ORGANIZATION_ID_HEADER,
    env_var=CLOUD_ORGANIZATION_ID_ENV_VAR,
    required=False,
    sensitive=False,
)
"""Config arg for organization ID, supporting both HTTP header and env var.

Only the tools that scope a listing to an organization use it; workspace-scoped tools
resolve their organization from the resolved workspace when none is configured.
"""


def _normalize_bearer_token(value: str) -> str | None:
    """Strip an optional `Bearer ` prefix from an `Authorization` value.

    Accepts either a full `Authorization` header value (`Bearer <token>`,
    case-insensitive prefix) or a bare token, and returns the bare token so it
    can be forwarded downstream. Returns `None` for an empty value so config
    resolution falls through to the next source.
    """
    stripped = value.strip()
    if stripped.lower().startswith("bearer "):
        stripped = stripped[len("bearer ") :].strip()
    return stripped or None


def _resolve_transport_bearer_token() -> str:
    """Resolve the bearer token to forward to the downstream Airbyte Cloud API.

    Prefers the token the transport auth provider *verified* for the request,
    exposed by `get_access_token`. Behind a token-swapping proxy (`OAuthProxy`/
    `OIDCProxy`) this is the upstream Airbyte access token, not the reference
    JWT the proxy minted for the MCP client and put in the raw `Authorization`
    header — forwarding that reference JWT downstream yields a `401` because
    Airbyte never issued it.

    Falls back to the raw `Authorization` header only when there is no verified
    token (a server with no transport auth provider, where the client passes a
    real Airbyte token directly), and to an empty string when neither is present
    (for example stdio mode), so config resolution can reach client-credentials.
    """
    access_token = get_access_token()
    if access_token and access_token.token:
        return access_token.token

    headers = get_http_headers(include={MCP_BEARER_TOKEN_HEADER.lower()})
    header_lower = MCP_BEARER_TOKEN_HEADER.lower()
    for key, value in headers.items():
        if key.lower() == header_lower:
            normalized = _normalize_bearer_token(value)
            if normalized:
                return normalized
    return ""


BEARER_TOKEN_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_BEARER_TOKEN,
    env_var=CLOUD_BEARER_TOKEN_ENV_VAR,
    normalize_fn=_normalize_bearer_token,
    default=_resolve_transport_bearer_token,
    required=False,
    sensitive=True,
)
"""Config arg for the downstream Airbyte Cloud bearer token.

Resolves an explicit `AIRBYTE_CLOUD_BEARER_TOKEN` override first, then defers to
`_resolve_transport_bearer_token`. The raw `Authorization` header is
deliberately *not* a first-class source: behind `OAuthProxy`/`OIDCProxy` it
carries the proxy's self-minted reference JWT, which Airbyte Cloud rejects with
`401`; the resolver consults it only as a last-resort fallback."""

CLIENT_ID_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_CLIENT_ID,
    env_var=CLOUD_CLIENT_ID_ENV_VAR,
    required=False,
    sensitive=True,
)
"""Config arg for client ID, supporting env var only.

Deliberately has no `http_header_key`: the supported headless transport path
uses standard `Client-Id` and `Client-Secret` headers, or
`Authorization: Basic base64(client_id:client_secret)`, handled by
`airbyte/mcp/_client_credentials.py` and
`fastmcp_extensions.wrap_client_credentials`. That exchange produces a
short-lived bearer token server-side and rewrites the request to
`Authorization: Bearer`. A per-request downstream credential header would let
a caller act as a Cloud identity other than the authenticated one.
"""

CLIENT_SECRET_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_CLIENT_SECRET,
    env_var=CLOUD_CLIENT_SECRET_ENV_VAR,
    required=False,
    sensitive=True,
)
"""Config arg for client secret, supporting env var only.

Deliberately has no `http_header_key`: the supported headless transport path
uses standard `Client-Id` and `Client-Secret` headers, or
`Authorization: Basic base64(client_id:client_secret)`, handled by
`airbyte/mcp/_client_credentials.py` and
`fastmcp_extensions.wrap_client_credentials`. That exchange produces a
short-lived bearer token server-side and rewrites the request to
`Authorization: Bearer`. A per-request downstream credential header would let
a caller act as a Cloud identity other than the authenticated one.
"""

API_URL_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_API_URL,
    env_var=CLOUD_API_ROOT_ENV_VAR,
    required=False,
    sensitive=False,
)
"""Config arg for API URL, supporting env var only.

Deliberately has no `http_header_key`: each hosted deployment is paired to a
single backend, so the API root must not be caller-controllable via an HTTP
header. Accepting it from a header would let a caller redirect the server's
credentialed requests to an arbitrary URL and exfiltrate them. The base URLs
are still configurable via env var for local (stdio) deployments.
"""

CONFIG_API_URL_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_CONFIG_API_URL,
    env_var=CLOUD_CONFIG_API_ROOT_ENV_VAR,
    required=False,
    sensitive=False,
)
"""Config arg for Config API URL, supporting env var only.

See `API_URL_CONFIG_ARG` for why no `http_header_key` is exposed.
"""


# =============================================================================
# Tool Filters for Backward Compatibility
# =============================================================================


def _parse_csv_config(value: str) -> list[str]:
    """Parse a comma-separated config value into a list of strings."""
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def mcp_tool(  # noqa: PLR0913
    *,
    read_only: bool = False,
    pipeline_change: bool | None = None,
    external_access: bool = False,
    destructive: bool = False,
    idempotent: bool = False,
    open_world: bool = False,
    annotations: Mapping[str, object] | None = None,
    meta: Mapping[str, object] | None = None,
    app: object | None = None,
    extra_help_text: str | None = None,
) -> Callable[[_MCP_TOOL_FUNC], _MCP_TOOL_FUNC]:
    """Decorate an MCP tool with deferred Airbyte registration metadata."""
    base_decorator = _mcp_tool(
        read_only=read_only,
        destructive=destructive,
        idempotent=idempotent,
        open_world=open_world,
        extra_help_text=extra_help_text,
    )

    def decorator(func: _MCP_TOOL_FUNC) -> _MCP_TOOL_FUNC:
        decorated = base_decorator(func)
        registered_func, registered_annotations = _REGISTERED_TOOLS[-1]
        if registered_func is not decorated:
            raise RuntimeError("Unexpected MCP tool registration state.")
        registered_annotations[ANNOTATION_MCP_MODULE] = _mcp_module_for_tool(decorated)
        registered_annotations.update(annotations or {})
        registered_annotations[ANNOTATION_PIPELINE_CHANGE] = (
            pipeline_change if pipeline_change is not None else not read_only
        )
        registered_annotations[ANNOTATION_EXTERNAL_ACCESS] = external_access
        if meta:
            registered_annotations[_TOOL_META_KEY] = dict(meta)
        if app is not None:
            registered_annotations[_TOOL_APP_KEY] = app
        return decorated

    return decorator


def _mcp_module_for_tool(func: Callable[..., object]) -> str:
    module_parts = func.__module__.split(".")
    return next(
        module_part for module_part in reversed(module_parts) if not module_part.startswith("_")
    )


def _get_caller_file_stem() -> str:
    for frame_info in inspect.stack():
        if frame_info.filename != __file__:
            return Path(frame_info.filename).stem
    return "unknown"


def register_mcp_tools(
    app: FastMCP,
    mcp_module: str | None = None,
    *,
    exclude_args: list[str] | None = None,
) -> None:
    """Register deferred MCP tools with Airbyte-specific metadata support."""
    if mcp_module is None:
        mcp_module = _get_caller_file_stem()
    mcp_module = _normalize_mcp_module(mcp_module)
    matching_tools = [
        (func, tool_annotations)
        for func, tool_annotations in _REGISTERED_TOOLS
        if tool_annotations.get(ANNOTATION_MCP_MODULE) == mcp_module
    ]

    for func, tool_annotations in matching_tools:
        tool_exclude_args: list[str] | None = None
        if exclude_args:
            params = set(inspect.signature(func).parameters.keys())
            excluded = [name for name in exclude_args if name in params]
            tool_exclude_args = excluded or None

        app.tool(
            func,
            annotations={
                key: value
                for key, value in tool_annotations.items()
                if key not in {_TOOL_APP_KEY, _TOOL_META_KEY}
            },
            exclude_args=tool_exclude_args,
            meta=tool_annotations.get(_TOOL_META_KEY),
            app=tool_annotations.get(_TOOL_APP_KEY),
        )

    matching_providers = [
        (provider_factory, tool_annotations)
        for provider_factory, tool_annotations in _REGISTERED_PROVIDERS
        if _normalize_mcp_module(str(tool_annotations.get(ANNOTATION_MCP_MODULE))) == mcp_module
    ]
    for provider_factory, tool_annotations in matching_providers:
        provider = provider_factory()
        provider.add_transform(_ProviderToolAnnotations(tool_annotations))
        app.add_provider(provider)


def _normalize_mcp_module(mcp_module: str) -> str:
    for module_part in reversed(mcp_module.split(".")):
        if not module_part.startswith("_"):
            return module_part
    return mcp_module


def _resolve_policy(
    app_or_ctx: FastMCP | Context,
    config_name: str,
    env_var: str,
) -> bool | None:
    """Resolve a tri-state policy whose request value can only narrow its env value."""
    environment_value = _str_to_bool(os.environ.get(env_var))
    if environment_value is False:
        return False
    request_value = _str_to_bool(get_mcp_config(app_or_ctx, config_name))

    if environment_value is True:
        return request_value is not False
    return request_value


def pipeline_changes_allowed(app_or_ctx: FastMCP | Context) -> bool | None:
    """Return the effective permission for pipeline-changing tools."""
    if _str_to_bool(os.environ.get(MCP_READONLY_MODE_ENV_VAR)) is True:
        return False
    return _resolve_policy(
        app_or_ctx,
        MCP_CONFIG_ALLOW_PIPELINE_CHANGES,
        MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR,
    )


def external_access_allowed(app_or_ctx: FastMCP | Context) -> bool | None:
    """Return the effective permission for Agents external access.

    When no explicit permission is configured, explicitly enabled safe mode and disabled
    pipeline changes both disable external access; otherwise the result follows the insiders
    default.
    """
    explicit_value = _resolve_policy(
        app_or_ctx,
        MCP_CONFIG_ALLOW_EXTERNAL_ACCESS,
        MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR,
    )
    if explicit_value is not None:
        return explicit_value
    return (
        False
        if (pipeline_changes_allowed(app_or_ctx) is False or _safe_mode_explicitly_enabled())
        else None
    )


def check_external_access_allowed(ctx: Context) -> None:
    """Raise `ExternalAccessDisabledError` when Agents external access is disabled."""
    if external_access_allowed(ctx) is False:
        raise ExternalAccessDisabledError


def airbyte_readonly_mode_filter(tool: Tool, app: FastMCP) -> bool:
    """Advertise only read-only tools when pipeline changes are disabled."""
    if pipeline_changes_allowed(app) is False:
        read_only = bool(get_annotation(tool, ANNOTATION_READ_ONLY_HINT, default=False))
        return (
            get_annotation(
                tool,
                ANNOTATION_PIPELINE_CHANGE,
                default=not read_only,
            )
            is not True
        )
    return True


def _insiders_mode(app: FastMCP) -> bool | None:
    """Return whether insiders tool modules are advertised for this request.

    `AIRBYTE_MCP_INSIDERS` sets the deployment default and callers may only narrow it: a
    falsy host value denies insiders tools outright, while a truthy one still honors an
    explicit `X-MCP-Insiders: 0`. Returns `None` when neither is set to a recognized value.
    """
    hosted_mode = _str_to_bool(os.environ.get(MCP_INSIDERS_ENV_VAR))
    caller_mode = _str_to_bool(get_mcp_config(app, MCP_CONFIG_INSIDERS))

    if hosted_mode is False:
        return False
    if hosted_mode is True:
        return caller_mode is not False

    return caller_mode


def airbyte_module_filter(tool: Tool, app: FastMCP) -> bool:  # noqa: PLR0911
    """Filter tools based on legacy AIRBYTE_MCP_DOMAINS and AIRBYTE_MCP_DOMAINS_DISABLED.

    When AIRBYTE_MCP_DOMAINS_DISABLED is set, hide tools from those modules.
    When AIRBYTE_MCP_DOMAINS is set, only show tools from those modules.

    Modules in `MCP_INSIDERS_MODULES` are hidden unless insiders mode is on or the include
    list names them. `AIRBYTE_MCP_INSIDERS=0` hides them outright, including from an
    include list.
    Agents tools are hidden when external access is disabled. Explicit external access
    permission bypasses the normal insiders requirement, but does not override the
    insiders environment hard deny or module/API availability checks.
    """
    tool_module = get_annotation(tool, ANNOTATION_MCP_MODULE, None)

    exclude_modules = _parse_csv_config(get_mcp_config(app, MCP_CONFIG_EXCLUDE_MODULES) or "")
    include_modules = [
        *_parse_csv_config(get_mcp_config(app, MCP_CONFIG_INCLUDE_MODULES) or ""),
        *_parse_csv_config(get_mcp_config(app, CONFIG_INCLUDE_MODULES) or ""),
    ]

    # Hide tools from excluded modules.
    if exclude_modules and tool_module and tool_module in exclude_modules:
        return False

    if (
        get_annotation(tool, ANNOTATION_EXTERNAL_ACCESS, default=False) is True
        and external_access_allowed(app) is False
    ):
        return False

    if tool_module == _AGENTS_MCP_MODULE and not is_agents_api_available(app):
        return False

    if tool_module in MCP_INSIDERS_MODULES:
        external_access_mode = (
            external_access_allowed(app) if tool_module == _AGENTS_MCP_MODULE else None
        )
        if external_access_mode is True:
            if _str_to_bool(os.environ.get(MCP_INSIDERS_ENV_VAR)) is False:
                return False
        else:
            insiders_mode = _insiders_mode(app)
            if insiders_mode is False or (
                insiders_mode is None and tool_module not in include_modules
            ):
                return False

    if include_modules:
        # Only show tools from included modules
        return bool(tool_module and tool_module in include_modules)

    return True


def _known_mcp_modules() -> set[str]:
    """Return the set of Airbyte MCP domain (module) names that have registered tools."""
    modules: set[str] = set()
    for _func, tool_annotations in _REGISTERED_TOOLS:
        module = tool_annotations.get(ANNOTATION_MCP_MODULE)
        if module:
            modules.add(_normalize_mcp_module(str(module)))
    for _provider_factory, tool_annotations in _REGISTERED_PROVIDERS:
        module = tool_annotations.get(ANNOTATION_MCP_MODULE)
        if module:
            modules.add(_normalize_mcp_module(str(module)))
    return modules


def validate_airbyte_domains(app: FastMCP) -> None:
    """Validate the `AIRBYTE_MCP_DOMAINS` / `AIRBYTE_MCP_DOMAINS_DISABLED` selection.

    Hard-fails at startup instead of silently dropping a domain that was explicitly
    requested. Two incompatibilities are rejected:

    1. Setting both `AIRBYTE_MCP_DOMAINS` (include) and `AIRBYTE_MCP_DOMAINS_DISABLED`
       (exclude), which are mutually exclusive -- `airbyte_module_filter` would otherwise
       silently honor only the exclude list and drop the requested includes.
    2. Naming a domain that has no registered tools (typically a typo), which would
       otherwise silently expose or hide nothing for that name.

    Call this once at startup, after all tools are registered.

    Args:
        app: The FastMCP app instance.

    Raises:
        PyAirbyteInputError: If the domain configuration is incompatible.
    """
    exclude_modules = _parse_csv_config(get_mcp_config(app, MCP_CONFIG_EXCLUDE_MODULES) or "")
    include_modules = _parse_csv_config(get_mcp_config(app, MCP_CONFIG_INCLUDE_MODULES) or "")

    if include_modules and exclude_modules:
        raise PyAirbyteInputError(
            message=(
                "AIRBYTE_MCP_DOMAINS and AIRBYTE_MCP_DOMAINS_DISABLED are mutually exclusive."
            ),
            guidance=(
                "Clear one of `AIRBYTE_MCP_DOMAINS` (include) or `AIRBYTE_MCP_DOMAINS_DISABLED` "
                "(exclude) so only one is set, then restart the MCP server process."
            ),
            context={
                "include_domains": include_modules,
                "exclude_domains": exclude_modules,
            },
        )

    known_modules = _known_mcp_modules()
    unknown_modules = sorted(
        {module for module in (*include_modules, *exclude_modules) if module not in known_modules}
    )
    if unknown_modules:
        raise PyAirbyteInputError(
            message="One or more requested MCP domains are not recognized.",
            guidance=(
                "Correct the unknown domain name(s) in `AIRBYTE_MCP_DOMAINS` / "
                "`AIRBYTE_MCP_DOMAINS_DISABLED` (or clear the variable), then restart the MCP "
                "server process."
            ),
            context={
                "unknown_domains": unknown_modules,
                "known_domains": sorted(known_modules),
            },
        )
