# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MCP tool utility functions for safe mode and config args.

This module provides:
- Safe mode functionality for MCP tools, allowing tracking of resources created
  during a session to prevent accidental deletion of pre-existing resources.
- Config args and filters for backward compatibility with legacy Airbyte env vars.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from fastmcp_extensions import MCPServerConfigArg, get_mcp_config
from fastmcp_extensions.tool_filters import (
    ANNOTATION_MCP_MODULE,
    ANNOTATION_READ_ONLY_HINT,
    get_annotation,
)

from airbyte.constants import (
    CLOUD_API_ROOT_ENV_VAR,
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CLIENT_SECRET_ENV_VAR,
    CLOUD_WORKSPACE_ID_ENV_VAR,
    MCP_API_URL_HEADER,
    MCP_BEARER_TOKEN_HEADER,
    MCP_CLIENT_ID_HEADER,
    MCP_CLIENT_SECRET_HEADER,
    MCP_CONFIG_API_URL,
    MCP_CONFIG_BEARER_TOKEN,
    MCP_CONFIG_CLIENT_ID,
    MCP_CONFIG_CLIENT_SECRET,
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
    from mcp.types import Tool


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

AIRBYTE_CONFIG_ARGS: list[MCPServerConfigArg] = [
    AIRBYTE_READONLY_MODE_CONFIG_ARG,
    AIRBYTE_EXCLUDE_MODULES_CONFIG_ARG,
    AIRBYTE_INCLUDE_MODULES_CONFIG_ARG,
    WORKSPACE_ID_CONFIG_ARG,
    BEARER_TOKEN_CONFIG_ARG,
    CLIENT_ID_CONFIG_ARG,
    CLIENT_SECRET_CONFIG_ARG,
    API_URL_CONFIG_ARG,
]
"""List of Airbyte-specific config args for backward compatibility."""


# =============================================================================
# Tool Filters for Backward Compatibility
# =============================================================================


def _parse_csv_config(value: str) -> list[str]:
    """Parse a comma-separated config value into a list of strings."""
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


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
