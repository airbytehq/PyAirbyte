# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Interactive MCP tools for UI-capable clients."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastmcp.apps import UI_EXTENSION_ID
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp_extensions import register_mcp_tools

from airbyte.mcp.interactive._registry import show_connectors_list
from airbyte.mcp.ui_builders import register_prefab_tool_metadata


if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Any

    from fastmcp import FastMCP
    from fastmcp.server.context import Context
    from fastmcp.tools.base import Tool, ToolResult
    from mcp import types as mt


INTERACTIVE_TOOL_NAMES = frozenset({"show_connectors_list"})


class InteractiveToolMiddleware(Middleware):
    """Hide interactive tools unless the client supports MCP Apps UI."""

    async def on_list_tools(
        self,
        context: MiddlewareContext[mt.ListToolsRequest],
        call_next: CallNext[mt.ListToolsRequest, Sequence[Tool]],
    ) -> Sequence[Tool]:
        """Return interactive tools only for MCP Apps UI-capable clients."""
        tools = await call_next(context)
        if _client_supports_ui(context):
            return tools
        return [tool for tool in tools if tool.name not in INTERACTIVE_TOOL_NAMES]

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        """Reject direct interactive tool calls from clients without MCP Apps UI."""
        if context.message.name in INTERACTIVE_TOOL_NAMES and not _client_supports_ui(context):
            raise ValueError(
                f"Tool '{context.message.name}' requires MCP Apps UI support from the client."
            )
        return await call_next(context)


def register_interactive_tools(app: FastMCP) -> None:
    """Register UI-presenting tools and hide them from non-UI clients."""
    register_mcp_tools(app, mcp_module="_registry")
    app.add_middleware(InteractiveToolMiddleware())
    register_prefab_tool_metadata(app, tuple(INTERACTIVE_TOOL_NAMES))


def _client_supports_ui(context: MiddlewareContext[Any]) -> bool:
    fastmcp_context = context.fastmcp_context
    if fastmcp_context is None:
        return False
    return _fastmcp_context_supports_ui(fastmcp_context)


def _fastmcp_context_supports_ui(context: Context) -> bool:
    return context.client_supports_extension(UI_EXTENSION_ID)


__all__ = [
    "INTERACTIVE_TOOL_NAMES",
    "InteractiveToolMiddleware",
    "register_interactive_tools",
    "show_connectors_list",
]
