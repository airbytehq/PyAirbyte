# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""MCP middleware enforcing pipeline-change policy at call time."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext

from airbyte.constants import ANNOTATION_EXTERNAL_ACCESS, ANNOTATION_PIPELINE_CHANGE
from airbyte.exceptions import ExternalAccessDisabledError, PipelineChangesDisabledError
from airbyte.mcp._tool_utils import external_access_allowed, pipeline_changes_allowed


if TYPE_CHECKING:
    import mcp.types as mt
    from fastmcp.tools.base import ToolResult


class PolicyGuardMiddleware(Middleware):
    """Reject calls that violate MCP policy annotations."""

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
        if tool is not None:
            annotations = tool.annotations.model_extra if tool.annotations is not None else None
            read_only = (
                tool.annotations.readOnlyHint is True if tool.annotations is not None else False
            )
            pipeline_change = (
                annotations.get(ANNOTATION_PIPELINE_CHANGE, not read_only)
                if annotations is not None
                else not read_only
            )
            external_access = (
                annotations.get(ANNOTATION_EXTERNAL_ACCESS, False)
                if annotations is not None
                else False
            )
            if pipeline_changes_allowed(fastmcp_context) is False and pipeline_change is True:
                raise PipelineChangesDisabledError
            if external_access is True and external_access_allowed(fastmcp_context) is False:
                raise ExternalAccessDisabledError
        return await call_next(context)
