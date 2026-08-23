# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import asyncio
import json

import pytest
import responses
from mcp.types import CallToolRequestParams

from airbyte import constants
from airbyte._util import telemetry
from airbyte.mcp import _telemetry as mcp_telemetry
from airbyte.mcp._telemetry import MCPToolCallTelemetryMiddleware
from fastmcp.server.middleware import MiddlewareContext


def _context() -> MiddlewareContext[CallToolRequestParams]:
    return MiddlewareContext(
        message=CallToolRequestParams(
            name="secret_tool",
            arguments={"api_key": "secret_argument"},
        ),
        method="tools/call",
    )


@responses.activate
def test_mcp_tool_call_telemetry_success_does_not_capture_arguments_or_result(
    monkeypatch,
):
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    responses.add(responses.POST, "https://api.segment.io/v1/track", status=200)

    async def call_next(
        _context: MiddlewareContext[CallToolRequestParams],
    ) -> dict[str, str]:
        return {"secret_result": "result_value"}

    result = asyncio.run(
        MCPToolCallTelemetryMiddleware().on_call_tool(_context(), call_next)
    )

    assert result == {"secret_result": "result_value"}
    assert len(responses.calls) == 1
    body = json.loads(responses.calls[0].request.body)
    properties = body["properties"]
    assert body["event"] == telemetry.EventType.MCP_TOOL_CALL
    assert properties["tool_name"] == "secret_tool"
    assert properties["state"] == telemetry.EventState.SUCCEEDED
    assert isinstance(properties["duration_ms"], int)
    assert "secret_argument" not in body["properties"]
    assert "result_value" not in body["properties"]


@responses.activate
def test_mcp_tool_call_telemetry_failure_reraises_original_exception(monkeypatch):
    monkeypatch.delenv("DO_NOT_TRACK", raising=False)
    responses.add(responses.POST, "https://api.segment.io/v1/track", status=200)
    error = RuntimeError("tool failed")

    async def call_next(_context: MiddlewareContext[CallToolRequestParams]) -> object:
        raise error

    with pytest.raises(RuntimeError) as raised:
        asyncio.run(
            MCPToolCallTelemetryMiddleware().on_call_tool(_context(), call_next)
        )

    assert raised.value is error
    body = json.loads(responses.calls[0].request.body)
    properties = body["properties"]
    assert properties["state"] == telemetry.EventState.FAILED
    assert properties["exception"] == {"class": "RuntimeError"}


@pytest.mark.parametrize("do_not_track", ["1", "true", "t"])
@responses.activate
def test_mcp_tool_call_telemetry_respects_do_not_track(monkeypatch, do_not_track):
    monkeypatch.setenv("DO_NOT_TRACK", do_not_track)
    responses.add(responses.POST, "https://api.segment.io/v1/track", status=200)

    async def call_next(_context: MiddlewareContext[CallToolRequestParams]) -> str:
        return "result"

    assert (
        asyncio.run(
            MCPToolCallTelemetryMiddleware().on_call_tool(_context(), call_next)
        )
        == "result"
    )
    assert not responses.calls


def test_mcp_tool_call_telemetry_swallows_telemetry_errors(monkeypatch):
    def fail_logging(**kwargs: object) -> None:
        raise RuntimeError("telemetry failed")

    monkeypatch.setattr(
        mcp_telemetry,
        "log_mcp_tool_call",
        fail_logging,
    )

    async def call_next(_context: MiddlewareContext[CallToolRequestParams]) -> str:
        return "result"

    assert (
        asyncio.run(
            MCPToolCallTelemetryMiddleware().on_call_tool(_context(), call_next)
        )
        == "result"
    )


def test_mcp_tool_call_telemetry_middleware_is_registered():
    from airbyte.mcp.server import app

    assert any(
        isinstance(middleware, MCPToolCallTelemetryMiddleware)
        for middleware in app.middleware
    )


def test_hosted_mcp_flag_is_evaluated_for_each_event(monkeypatch):
    monkeypatch.setattr(constants, "_HOSTED_MCP_MODE_ENABLED", False)

    before = telemetry.get_env_flags()
    constants.set_hosted_mcp_mode()
    after = telemetry.get_env_flags()

    assert "HOSTED_MCP" not in before
    assert after["HOSTED_MCP"] is True
