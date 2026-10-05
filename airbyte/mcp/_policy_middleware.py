# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Runtime enforcement for MCP tool policies."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastmcp.server.middleware import Middleware

from airbyte.exceptions import (
    AirbyteExternalAccessDisabledError,
    AirbytePipelineChangesDisabledError,
)
from airbyte.mcp._tool_utils import (
    external_access_allowed,
    get_tool_policy,
    pipeline_changes_allowed,
)


if TYPE_CHECKING:
    from fastmcp.server.middleware import CallNext, MiddlewareContext
    from fastmcp.tools import ToolResult
    from mcp.types import CallToolRequestParams


class PolicyGuardMiddleware(Middleware):
    """Enforce pipeline-change and external-access policies before tool execution."""

    async def on_call_tool(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        """Reject calls denied by effective pipeline or external-access settings."""
        fastmcp_context = context.fastmcp_context
        if fastmcp_context is None:
            return await call_next(context)

        tool = await fastmcp_context.fastmcp.get_tool(context.message.name)
        if tool is None:
            return await call_next(context)

        policy = get_tool_policy(tool)
        if policy.pipeline_change and pipeline_changes_allowed(fastmcp_context) is False:
            raise AirbytePipelineChangesDisabledError(
                message="Pipeline-changing tools are disabled by the MCP server policy."
            )
        if policy.external_access and external_access_allowed(fastmcp_context) is False:
            raise AirbyteExternalAccessDisabledError(
                message="External-access tools are disabled by the MCP server policy."
            )
        return await call_next(context)
