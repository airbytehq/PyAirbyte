# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Interactive MCP tools for UI-capable clients."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte.mcp._tool_utils import register_mcp_tools
from airbyte.mcp.interactive._prefab import register_generative_ui_tools
from airbyte.mcp.interactive._registry import show_connectors_list


if TYPE_CHECKING:
    from fastmcp import FastMCP


def register_interactive_tools(app: FastMCP) -> None:
    """Register UI-presenting tools."""
    register_mcp_tools(app, mcp_module="interactive", include_providers=False)
    register_generative_ui_tools(app)


__all__ = [
    "register_interactive_tools",
    "show_connectors_list",
]
