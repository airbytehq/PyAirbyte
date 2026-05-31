# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Interactive MCP tools for UI-capable clients."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastmcp_extensions import register_mcp_tools

from airbyte.mcp._ui_builders import register_prefab_tool_metadata
from airbyte.mcp.interactive._registry import show_connectors_list


if TYPE_CHECKING:
    from fastmcp import FastMCP


INTERACTIVE_TOOL_NAMES = frozenset({"show_connectors_list"})


def register_interactive_tools(app: FastMCP) -> None:
    """Register UI-presenting tools."""
    register_mcp_tools(app, mcp_module="_registry")
    register_prefab_tool_metadata(app, tuple(INTERACTIVE_TOOL_NAMES))


__all__ = [
    "INTERACTIVE_TOOL_NAMES",
    "register_interactive_tools",
    "show_connectors_list",
]
