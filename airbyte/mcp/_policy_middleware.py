# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Runtime enforcement for MCP tool policies."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from fastmcp.server.middleware import Middleware
from fastmcp_extensions.tool_filters import (
    ANNOTATION_DESTRUCTIVE_HINT,
    ANNOTATION_READ_ONLY_HINT,
    get_annotation,
)
from mcp.types import Tool

from airbyte.exceptions import (
    AirbyteDestructiveToolsDisabledError,
    AirbyteExternalAccessDisabledError,
    AirbytePipelineChangesDisabledError,
    AirbyteReadOnlyModeError,
)
from airbyte.mcp._tool_utils import (
    _is_cloud_tool,
    airbyte_readonly_mode_enabled,
    check_unsupported_mcp_headers,
    destructive_tools_allowed,
    external_access_allowed,
    get_tool_policy,
    pipeline_changes_allowed,
)


if TYPE_CHECKING:
    from collections.abc import Sequence

    from fastmcp.server.middleware import CallNext, MiddlewareContext
    from fastmcp.tools import Tool as FastMCPTool
    from fastmcp.tools import ToolResult
    from mcp.types import CallToolRequestParams, ListToolsRequest


class PolicyGuardMiddleware(Middleware):
    """Enforce pipeline-change and external-access policies before tool execution."""

    async def on_call_tool(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        """Reject calls denied by effective pipeline or external-access settings."""
        check_unsupported_mcp_headers()
        fastmcp_context = context.fastmcp_context
        if fastmcp_context is None:
            return await call_next(context)

        tool = await fastmcp_context.fastmcp.get_tool(context.message.name)
        if tool is None:
            return await call_next(context)

        cloud_tool = _is_cloud_tool(fastmcp_context.fastmcp, cast(Tool, tool))
        if (
            cloud_tool
            and airbyte_readonly_mode_enabled()
            and not get_annotation(cast(Tool, tool), ANNOTATION_READ_ONLY_HINT, default=False)
        ):
            raise AirbyteReadOnlyModeError(
                message="Read-only mode is enabled; only read-only tools are available."
            )
        policy = get_tool_policy(tool)
        if (
            cloud_tool
            and policy.pipeline_change
            and pipeline_changes_allowed(fastmcp_context) is False
        ):
            raise AirbytePipelineChangesDisabledError(
                message="Pipeline-changing tools are disabled by the MCP server policy."
            )
        if policy.external_access and external_access_allowed(fastmcp_context) is False:
            raise AirbyteExternalAccessDisabledError(
                message="External-access tools are disabled by the MCP server policy."
            )
        if (
            cloud_tool
            and get_annotation(cast(Tool, tool), ANNOTATION_DESTRUCTIVE_HINT, default=False)
            and destructive_tools_allowed(fastmcp_context) is False
        ):
            raise AirbyteDestructiveToolsDisabledError(
                message="Destructive tools are disabled by the MCP server policy."
            )
        return await call_next(context)

    async def on_list_tools(
        self,
        context: MiddlewareContext[ListToolsRequest],
        call_next: CallNext[ListToolsRequest, Sequence[FastMCPTool]],
    ) -> Sequence[FastMCPTool]:
        """Reject listings when a removed MCP request header is present."""
        check_unsupported_mcp_headers()
        return await call_next(context)
