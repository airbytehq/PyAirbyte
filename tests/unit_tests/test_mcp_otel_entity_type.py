# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Bounded entity names on upstream OpenTelemetry tool spans."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator

import pytest
from fastmcp import Client
from fastmcp_extensions import (
    TelemetryConfig,
    ToolTracingConfig,
    capture_tool_spans,
    mcp_server,
    register_tool_call_telemetry,
)
from opentelemetry.sdk.trace import ReadableSpan

from airbyte.mcp._tool_utils import mcp_tool, register_mcp_tools
from airbyte.mcp._trace_attributes import agent_action_attributes


_MCP_MODULE = "test_mcp_otel_entity_type"


@mcp_tool(
    read_only=True,
    tracing=lambda args: agent_action_attributes("execute_external_api_query", args),
)
def trace_entity_query(entity_type: str, action: str = "list") -> str:
    return entity_type


@pytest.fixture(autouse=True)
def _disable_remote_export(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", raising=False)
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    yield


def _app():
    app = mcp_server(
        display_name="entity-tracing-tests",
        telemetry=TelemetryConfig(package_name="airbyte"),
    )
    register_mcp_tools(app, mcp_module=_MCP_MODULE)
    register_tool_call_telemetry(
        app,
        TelemetryConfig(
            package_name="airbyte",
            tool_tracing=ToolTracingConfig(attribute_prefix="airbyte.mcp"),
        ),
    )
    return app


def _tool_span(spans: list[ReadableSpan]) -> ReadableSpan:
    return next(span for span in spans if span.name == "tools/call trace_entity_query")


@pytest.mark.parametrize(
    ("entity_type", "expected"),
    [
        ("issues", "issues"),
        ("x" * 256, "x" * 256),
        ("x" * 257, "x" * 256),
        (" leading-space", None),
        ("trailing-space ", None),
        ("\n", None),
    ],
)
def test_entity_type_is_validated_and_bounded_without_mutating_tool_input(
    entity_type: str,
    expected: str | None,
) -> None:
    async def scenario() -> None:
        app = _app()
        with capture_tool_spans() as spans:
            async with Client(app) as client:
                result = await client.call_tool(
                    "trace_entity_query", {"entity_type": entity_type}
                )

        assert result.content[0].text == entity_type
        attributes = _tool_span(spans).attributes or {}
        assert attributes["airbyte.mcp.agent.action"] == "list"
        if expected is None:
            assert "airbyte.mcp.agent.entity_type" not in attributes
        else:
            assert attributes["airbyte.mcp.agent.entity_type"] == expected

    asyncio.run(scenario())
