# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""FastMCP middleware for MCP tool-call telemetry."""

from __future__ import annotations

from contextlib import suppress
from time import perf_counter
from typing import TYPE_CHECKING

from fastmcp.server.middleware import Middleware

from airbyte._util.telemetry import EventState, log_mcp_tool_call


if TYPE_CHECKING:
    from fastmcp.server.middleware import CallNext, MiddlewareContext
    from fastmcp.tools.base import ToolResult
    from mcp.types import CallToolRequestParams


class MCPToolCallTelemetryMiddleware(Middleware):
    """Record the outcome and duration of each MCP tool call."""

    async def on_call_tool(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        started_at = perf_counter()
        try:
            result = await call_next(context)
        except Exception as exception:
            self._log(
                tool_name=context.message.name,
                state=EventState.FAILED,
                duration_ms=self._duration_ms(started_at),
                exception=exception,
            )
            raise
        else:
            self._log(
                tool_name=context.message.name,
                state=EventState.SUCCEEDED,
                duration_ms=self._duration_ms(started_at),
            )
            return result

    @staticmethod
    def _duration_ms(started_at: float) -> int:
        return int((perf_counter() - started_at) * 1000)

    @staticmethod
    def _log(
        *,
        tool_name: str,
        state: EventState,
        duration_ms: int,
        exception: Exception | None = None,
    ) -> None:
        with suppress(Exception):
            log_mcp_tool_call(
                tool_name=tool_name,
                state=state,
                duration_ms=duration_ms,
                exception=exception,
            )
