# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Interactive MCP tools for UI-capable clients."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastmcp.apps import UI_EXTENSION_ID
from fastmcp.server.dependencies import get_context
from fastmcp_extensions import register_mcp_tools

from airbyte.mcp._ui_builders import register_prefab_tool_metadata
from airbyte.mcp.interactive._registry import show_connectors_list


if TYPE_CHECKING:
    from fastmcp import FastMCP
    from fastmcp.server.context import Context
    from mcp.types import Tool


INTERACTIVE_TOOL_NAMES = frozenset({"show_connectors_list"})


def interactive_tool_filter(tool: Tool, _app: FastMCP) -> bool:
    """Return whether an interactive tool is available to the current client."""
    if tool.name not in INTERACTIVE_TOOL_NAMES:
        return True
    return _client_supports_ui()


def register_interactive_tools(app: FastMCP) -> None:
    """Register UI-presenting tools."""
    register_mcp_tools(app, mcp_module="_registry")
    register_prefab_tool_metadata(app, tuple(INTERACTIVE_TOOL_NAMES))


def _client_supports_ui() -> bool:
    try:
        context = get_context()
    except RuntimeError:
        return False
    return _fastmcp_context_supports_ui(context)


def _fastmcp_context_supports_ui(context: Context) -> bool:
    return context.client_supports_extension(UI_EXTENSION_ID)


__all__ = [
    "INTERACTIVE_TOOL_NAMES",
    "interactive_tool_filter",
    "register_interactive_tools",
    "show_connectors_list",
]
