# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Experimental MCP (Model Context Protocol) server for PyAirbyte connector management."""

from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

from fastmcp_extensions import (
    MCPServerConfigArg,
    get_mcp_config,
    mcp_server,
)
from fastmcp_extensions.tool_filters import (
    ANNOTATION_MCP_MODULE,
    ANNOTATION_READ_ONLY_HINT,
    get_annotation,
)


if TYPE_CHECKING:
    from fastmcp import FastMCP
    from mcp.types import Tool

from airbyte._util.meta import set_mcp_mode
from airbyte.constants import (
    CLOUD_WORKSPACE_ID_ENV_VAR,
    MCP_CONFIG_EXCLUDE_MODULES,
    MCP_CONFIG_INCLUDE_MODULES,
    MCP_CONFIG_READONLY_MODE,
    MCP_CONFIG_WORKSPACE_ID,
    MCP_DOMAINS_DISABLED_ENV_VAR,
    MCP_DOMAINS_ENV_VAR,
    MCP_READONLY_MODE_ENV_VAR,
    MCP_WORKSPACE_ID_HEADER,
)
from airbyte.mcp._tool_utils import AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET
from airbyte.mcp._util import initialize_secrets
from airbyte.mcp.cloud import register_cloud_tools
from airbyte.mcp.local import register_local_tools
from airbyte.mcp.prompts import register_prompts
from airbyte.mcp.registry import register_registry_tools


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


AIRBYTE_CONFIG_ARGS: list[MCPServerConfigArg] = [
    AIRBYTE_READONLY_MODE_CONFIG_ARG,
    AIRBYTE_EXCLUDE_MODULES_CONFIG_ARG,
    AIRBYTE_INCLUDE_MODULES_CONFIG_ARG,
    WORKSPACE_ID_CONFIG_ARG,
]
"""List of Airbyte-specific config args for backward compatibility."""


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

set_mcp_mode()
initialize_secrets()

app = mcp_server(
    name="airbyte-mcp",
    package_name="airbyte",
    instructions=MCP_SERVER_INSTRUCTIONS,
    include_standard_tool_filters=True,
    server_config_args=AIRBYTE_CONFIG_ARGS,
    tool_filters=[airbyte_readonly_mode_filter, airbyte_module_filter],
)
"""The Airbyte MCP Server application instance."""

# Register tools from each module
register_cloud_tools(app, exclude_workspace_id_arg=AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET)
register_local_tools(app)
register_registry_tools(app)
register_prompts(app)


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
