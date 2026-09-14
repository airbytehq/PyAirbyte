# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for concise user-facing MCP tool errors."""

from __future__ import annotations

import asyncio

from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from airbyte.exceptions import PyAirbyteInputError
from airbyte.mcp._error_handling import UserFacingErrorMiddleware


def test_user_facing_errors_are_concise() -> None:
    server = FastMCP("test", middleware=[UserFacingErrorMiddleware()])

    @server.tool
    def raise_input_error() -> None:
        raise PyAirbyteInputError(message="bad", guidance="fix it")

    async def call_tool() -> None:
        async with Client(server) as client:
            try:
                await client.call_tool("raise_input_error")
            except ToolError as error:
                text = str(error)
                assert "bad" in text
                assert "fix it" in text
                assert "Traceback" not in text
                return
        raise AssertionError("Expected ToolError")

    asyncio.run(call_tool())


def test_unexpected_errors_keep_fastmcp_default_handling() -> None:
    server = FastMCP("test", middleware=[UserFacingErrorMiddleware()])

    @server.tool
    def raise_runtime_error() -> None:
        raise RuntimeError("boom")

    async def call_tool() -> None:
        async with Client(server) as client:
            try:
                await client.call_tool("raise_runtime_error")
            except ToolError as error:
                assert "boom" in str(error)
                assert "fix it" not in str(error)
                return
        raise AssertionError("Expected ToolError")

    asyncio.run(call_tool())
