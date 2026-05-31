# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MCP tool utility functions for safe mode and config args.

This module provides:
- Safe mode functionality for MCP tools, allowing tracking of resources created
  during a session to prevent accidental deletion of pre-existing resources.
- Config args and filters for backward compatibility with legacy Airbyte env vars.
"""

from __future__ import annotations

import inspect
import os
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

from fastmcp.apps import UI_EXTENSION_ID
from fastmcp.server.dependencies import get_context
from fastmcp.server.providers import Provider
from fastmcp.server.transforms import GetToolNext, Transform
from fastmcp_extensions import MCPServerConfigArg, get_mcp_config
from fastmcp_extensions import mcp_tool as _mcp_tool
from fastmcp_extensions.decorators import _REGISTERED_TOOLS  # noqa: PLC2701
from fastmcp_extensions.tool_filters import (
    ANNOTATION_MCP_MODULE,
    ANNOTATION_READ_ONLY_HINT,
    get_annotation,
)
from mcp.types import ToolAnnotations

from airbyte.constants import (
    CLOUD_API_ROOT_ENV_VAR,
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CLIENT_SECRET_ENV_VAR,
    CLOUD_CONFIG_API_ROOT_ENV_VAR,
    CLOUD_WORKSPACE_ID_ENV_VAR,
    MCP_API_URL_HEADER,
    MCP_BEARER_TOKEN_HEADER,
    MCP_CLIENT_ID_HEADER,
    MCP_CLIENT_SECRET_HEADER,
    MCP_CONFIG_API_URL,
    MCP_CONFIG_API_URL_HEADER,
    MCP_CONFIG_BEARER_TOKEN,
    MCP_CONFIG_CLIENT_ID,
    MCP_CONFIG_CLIENT_SECRET,
    MCP_CONFIG_CONFIG_API_URL,
    MCP_CONFIG_EXCLUDE_MODULES,
    MCP_CONFIG_INCLUDE_MODULES,
    MCP_CONFIG_READONLY_MODE,
    MCP_CONFIG_WORKSPACE_ID,
    MCP_DOMAINS_DISABLED_ENV_VAR,
    MCP_DOMAINS_ENV_VAR,
    MCP_READONLY_MODE_ENV_VAR,
    MCP_WORKSPACE_ID_HEADER,
)


if TYPE_CHECKING:
    from fastmcp import FastMCP
    from fastmcp.server.context import Context
    from fastmcp.tools import Tool as FastMCPTool
    from fastmcp.utilities.versions import VersionSpec
    from mcp.types import Tool

_MCP_TOOL_FUNC = TypeVar("_MCP_TOOL_FUNC", bound=Callable[..., object])
_MCP_PROVIDER_FUNC = TypeVar("_MCP_PROVIDER_FUNC", bound=Callable[[], Provider])
_TOOL_APP_KEY = "_airbyte_tool_app"
_TOOL_META_KEY = "_airbyte_tool_meta"
_REGISTERED_PROVIDERS: list[tuple[Callable[[], Provider], dict[str, object]]] = []

INTERACTIVE_UI_ANNOTATION = "interactive-ui"
"""Annotation indicating the tool requires MCP Apps UI support."""


# =============================================================================
# Safe Mode Configuration
# =============================================================================

AIRBYTE_CLOUD_MCP_SAFE_MODE = os.environ.get("AIRBYTE_CLOUD_MCP_SAFE_MODE", "1").strip() != "0"
"""Whether safe mode is enabled for cloud operations.

When enabled (default), destructive operations are only allowed on resources
created during the current session.
"""

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
            f"AIRBYTE_CLOUD_MCP_SAFE_MODE is set to '1'."
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

WORKSPACE_ID_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_WORKSPACE_ID,
    http_header_key=MCP_WORKSPACE_ID_HEADER,
    env_var=CLOUD_WORKSPACE_ID_ENV_VAR,
    required=False,
    sensitive=False,
)
"""Config arg for workspace ID, supporting both HTTP header and env var."""

BEARER_TOKEN_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_BEARER_TOKEN,
    http_header_key=MCP_BEARER_TOKEN_HEADER,
    env_var=CLOUD_BEARER_TOKEN_ENV_VAR,
    required=False,
    sensitive=True,
)
"""Config arg for bearer token, supporting Authorization header and env var."""

CLIENT_ID_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_CLIENT_ID,
    http_header_key=MCP_CLIENT_ID_HEADER,
    env_var=CLOUD_CLIENT_ID_ENV_VAR,
    required=False,
    sensitive=True,
)
"""Config arg for client ID, supporting HTTP header and env var."""

CLIENT_SECRET_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_CLIENT_SECRET,
    http_header_key=MCP_CLIENT_SECRET_HEADER,
    env_var=CLOUD_CLIENT_SECRET_ENV_VAR,
    required=False,
    sensitive=True,
)
"""Config arg for client secret, supporting HTTP header and env var."""

API_URL_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_API_URL,
    http_header_key=MCP_API_URL_HEADER,
    env_var=CLOUD_API_ROOT_ENV_VAR,
    required=False,
    sensitive=False,
)
"""Config arg for API URL, supporting HTTP header and env var."""

CONFIG_API_URL_CONFIG_ARG = MCPServerConfigArg(
    name=MCP_CONFIG_CONFIG_API_URL,
    http_header_key=MCP_CONFIG_API_URL_HEADER,
    env_var=CLOUD_CONFIG_API_ROOT_ENV_VAR,
    required=False,
    sensitive=False,
)
"""Config arg for Config API URL, supporting HTTP header and env var."""


# =============================================================================
# Tool Filters for Backward Compatibility
# =============================================================================


def _parse_csv_config(value: str) -> list[str]:
    """Parse a comma-separated config value into a list of strings."""
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def mcp_tool(
    *,
    read_only: bool = False,
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
        if meta:
            registered_annotations[_TOOL_META_KEY] = dict(meta)
        if app is not None:
            registered_annotations[_TOOL_APP_KEY] = app
        return decorated

    return decorator


def mcp_provider(
    *,
    read_only: bool = False,
    destructive: bool = False,
    idempotent: bool = False,
    open_world: bool = False,
    annotations: Mapping[str, object] | None = None,
) -> Callable[[_MCP_PROVIDER_FUNC], _MCP_PROVIDER_FUNC]:
    """Decorate an MCP provider factory with deferred Airbyte registration metadata."""
    tool_annotations: dict[str, object] = {
        ANNOTATION_READ_ONLY_HINT: read_only,
        "destructiveHint": destructive,
        "idempotentHint": idempotent,
        "openWorldHint": open_world,
    }
    tool_annotations.update(annotations or {})

    def decorator(func: _MCP_PROVIDER_FUNC) -> _MCP_PROVIDER_FUNC:
        tool_annotations[ANNOTATION_MCP_MODULE] = _mcp_module_for_tool(func)
        _REGISTERED_PROVIDERS.append((func, tool_annotations))
        return func

    return decorator


class _ProviderToolAnnotations(Transform):
    def __init__(self, annotations: Mapping[str, object]) -> None:
        self._annotations = annotations

    async def list_tools(self, tools: Sequence[FastMCPTool]) -> Sequence[FastMCPTool]:
        return [self._apply_annotations(tool) for tool in tools]

    async def get_tool(
        self,
        name: str,
        call_next: GetToolNext,
        *,
        version: VersionSpec | None = None,
    ) -> FastMCPTool | None:
        tool = await call_next(name, version=version)
        if tool is None:
            return None
        return self._apply_annotations(tool)

    def _apply_annotations(self, tool: FastMCPTool) -> FastMCPTool:
        annotations = tool.annotations.model_dump(exclude_none=True) if tool.annotations else {}
        annotations.update(self._annotations)
        return tool.model_copy(
            update={
                "annotations": ToolAnnotations(**annotations),
            },
        )


def _mcp_module_for_tool(func: Callable[..., object]) -> str:
    module_name = func.__module__
    if module_name.startswith("airbyte.mcp.interactive."):
        return "interactive"
    return module_name.rsplit(".", 1)[-1]


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
    mcp_module = mcp_module.rsplit(".", 1)[-1]
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
        if tool_annotations.get(ANNOTATION_MCP_MODULE) == mcp_module
    ]
    for provider_factory, tool_annotations in matching_providers:
        provider = provider_factory()
        provider.add_transform(_ProviderToolAnnotations(tool_annotations))
        app.add_provider(provider)


def airbyte_readonly_mode_filter(tool: Tool, app: FastMCP) -> bool:
    """Filter tools based on legacy AIRBYTE_CLOUD_MCP_READONLY_MODE env var.

    When set to "1", only show tools with readOnlyHint=True.
    """
    config_value = (get_mcp_config(app, MCP_CONFIG_READONLY_MODE) or "").lower()
    if config_value in {"1", "true"}:
        return bool(get_annotation(tool, ANNOTATION_READ_ONLY_HINT, default=False))
    return True


def airbyte_module_filter(tool: Tool, app: FastMCP) -> bool:
    """Filter tools based on legacy AIRBYTE_MCP_DOMAINS and AIRBYTE_MCP_DOMAINS_DISABLED.

    When AIRBYTE_MCP_DOMAINS_DISABLED is set, hide tools from those modules.
    When AIRBYTE_MCP_DOMAINS is set, only show tools from those modules.
    """
    exclude_modules = _parse_csv_config(get_mcp_config(app, MCP_CONFIG_EXCLUDE_MODULES) or "")
    include_modules = _parse_csv_config(get_mcp_config(app, MCP_CONFIG_INCLUDE_MODULES) or "")

    # Get the tool's mcp_module from annotations
    tool_module = get_annotation(tool, ANNOTATION_MCP_MODULE, None)

    if exclude_modules:
        # Hide tools from excluded modules
        return not (tool_module and tool_module in exclude_modules)

    if include_modules:
        # Only show tools from included modules
        return bool(tool_module and tool_module in include_modules)

    return True


def airbyte_ui_support_filter(tool: Tool, _app: FastMCP) -> bool:
    """Filter tools that require MCP Apps UI support."""
    if not get_annotation(tool, INTERACTIVE_UI_ANNOTATION, default=False):
        return True
    return _client_supports_ui()


def _client_supports_ui() -> bool:
    try:
        context = get_context()
    except RuntimeError:
        return False
    return _fastmcp_context_supports_ui(context)


def _fastmcp_context_supports_ui(context: Context) -> bool:
    return context.client_supports_extension(UI_EXTENSION_ID)
