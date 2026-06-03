# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Interactive MCP tools for UI-capable clients."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte.mcp._tool_utils import register_mcp_tools
from airbyte.mcp.interactive import _prefab as _prefab_module  # noqa: F401
from airbyte.mcp.interactive._registry_ui import show_connectors_list
from airbyte.mcp.interactive._sync_history_ui import show_connection_sync_history
from airbyte.mcp.interactive._workspace_sync_status_ui import show_workspace_sync_status


if TYPE_CHECKING:
    from fastmcp import FastMCP


def register_interactive_tools(app: FastMCP) -> None:
    """Register UI-presenting tools."""
    register_mcp_tools(app, mcp_module="interactive")


__all__ = [
    "register_interactive_tools",
    "show_connectors_list",
    "show_connection_sync_history",
    "show_workspace_sync_status",
]
