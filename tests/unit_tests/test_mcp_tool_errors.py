# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for concise user-facing MCP tool errors."""

from __future__ import annotations

import asyncio

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError

from airbyte.exceptions import (
    AirbyteConnectionSyncError,
    AirbyteConnectionSyncTimeoutError,
    AirbyteConnectorInUseError,
    AirbyteMissingResourceError,
    AirbyteLibError,
    AirbyteLibInputError,
)
from airbyte.mcp._error_handling import (
    MCP_TOOL_USER_FACING_ERRORS,
    classify_mcp_tool_error,
    format_user_facing_error,
)
from fastmcp_extensions import mcp_server


def test_unexpected_errors_keep_fastmcp_default_handling() -> None:
    server = mcp_server(
        "test",
        telemetry=False,
        user_facing_errors=MCP_TOOL_USER_FACING_ERRORS,
        user_facing_error_formatter=format_user_facing_error,
    )

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


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(
            AirbyteLibInputError(message="bad", guidance="fix it"),
            id="input-error",
        ),
        pytest.param(
            AirbyteConnectorInUseError(
                message="Connector is in use.",
                guidance="Delete its connections first.",
            ),
            id="connector-in-use",
        ),
        pytest.param(
            AirbyteMissingResourceError(
                resource_type="sync job",
                resource_name_or_id="42",
                message="Job 42 is unavailable.",
                guidance="Find a valid job ID.",
            ),
            id="missing-resource",
        ),
    ],
)
def test_expected_errors_are_presented_concisely(error: AirbyteLibError) -> None:
    server = mcp_server(
        "test",
        telemetry=False,
        user_facing_errors=MCP_TOOL_USER_FACING_ERRORS,
        user_facing_error_formatter=format_user_facing_error,
    )

    @server.tool
    def raise_resource_error() -> None:
        raise error

    async def call_tool() -> None:
        async with Client(server) as client:
            try:
                await client.call_tool("raise_resource_error")
            except ToolError as tool_error:
                text = str(tool_error)
                assert "Traceback" not in text
                assert error.get_message() in text
                assert error.guidance in text
                return
        raise AssertionError("Expected ToolError")

    asyncio.run(call_tool())


def test_format_user_facing_error() -> None:
    assert (
        format_user_facing_error(AirbyteLibInputError(message="bad", guidance="fix it"))
        == "bad fix it"
    )
    assert format_user_facing_error(AirbyteLibInputError(message="bad")) == "bad"
    assert format_user_facing_error(ValueError("plain")) == "plain"


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        pytest.param(
            AirbyteConnectionSyncError(
                connection_id="c", job_id=1, job_status="failed"
            ),
            "upstream_error",
            id="failed-job",
        ),
        pytest.param(
            AirbyteConnectionSyncTimeoutError(
                connection_id="c", job_id=1, job_status="running", timeout=5
            ),
            "upstream_timeout",
            id="timed-out-job",
        ),
        pytest.param(
            AirbyteConnectionSyncError(connection_id="c"),
            None,
            id="api-error-keeps-status-category",
        ),
        pytest.param(AirbyteLibInputError(message="bad input"), None, id="other-error"),
        pytest.param(ValueError("unexpected"), None, id="non-airbyte-error"),
    ],
)
def test_classify_mcp_tool_error(error: BaseException, expected: str | None) -> None:
    assert classify_mcp_tool_error(error) == expected
