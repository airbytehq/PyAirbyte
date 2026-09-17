# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""MCP middleware enforcing pipeline-change policy at call time."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext

from airbyte.exceptions import PipelineChangesDisabledError
from airbyte.mcp._tool_utils import pipeline_changes_allowed


if TYPE_CHECKING:
    import mcp.types as mt
    from fastmcp.tools.base import ToolResult


class PipelineChangesGuardMiddleware(Middleware):
    """Reject calls to non-read-only tools when pipeline changes are disabled."""

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        """Enforce the pipeline policy before dispatching a tool call."""
        fastmcp_context = context.fastmcp_context
        if fastmcp_context is None:
            return await call_next(context)
        tool = await fastmcp_context.fastmcp.get_tool(context.message.name)
        if (
            tool is not None
            and pipeline_changes_allowed(fastmcp_context) is False
            and (tool.annotations is None or tool.annotations.readOnlyHint is not True)
        ):
            raise PipelineChangesDisabledError
        return await call_next(context)
