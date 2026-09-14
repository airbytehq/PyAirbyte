# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Error handling middleware for the MCP server."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import (
    CallNext,
    Middleware,
    MiddlewareContext,
)

from airbyte.exceptions import (
    AirbyteAgentsUnavailableError,
    AirbyteMCPError,
    PyAirbyteError,
    PyAirbyteInputError,
)


if TYPE_CHECKING:
    import mcp.types as mt
    from fastmcp.tools.base import ToolResult


MCP_TOOL_USER_FACING_ERRORS: tuple[type[PyAirbyteError], ...] = (
    PyAirbyteInputError,
    AirbyteMCPError,
    AirbyteAgentsUnavailableError,
)
"""Expected errors returned to MCP clients as concise message and guidance text."""


class UserFacingErrorMiddleware(Middleware):
    """Return concise `ToolError`s for expected PyAirbyte errors.

    The error message and guidance are returned without a traceback.
    """

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        try:
            return await call_next(context)
        except MCP_TOOL_USER_FACING_ERRORS as error:
            text = error.get_message()
            if error.guidance:
                text = f"{text} {error.guidance}"
            raise ToolError(text) from None
