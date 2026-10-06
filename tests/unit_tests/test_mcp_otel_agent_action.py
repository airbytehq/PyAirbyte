# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Per-tool action attributes on upstream OpenTelemetry spans."""

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


_MCP_MODULE = "test_mcp_otel_agent_action"


@mcp_tool(
    read_only=True,
    tracing=lambda args: agent_action_attributes("execute_external_api_query", args),
)
def trace_api_query(action: str = "list", entity_type: str = "datasets") -> str:
    return "private-result"


@mcp_tool(
    read_only=True,
    tracing=lambda args: agent_action_attributes("execute_external_sql_query", args),
)
def trace_sql_query(sql: str) -> str:
    return "private-result"


@mcp_tool(
    read_only=True,
    tracing=lambda args: agent_action_attributes("execute_external_search_query", args),
)
def trace_search_query(prompt: str, search_type: str = "hybrid") -> str:
    return "private-result"


@pytest.fixture(autouse=True)
def _disable_remote_export(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", raising=False)
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    yield


def _traced_app():
    app = mcp_server(
        display_name="agent-action-tests",
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


def _tool_span(spans: list[ReadableSpan], name: str) -> ReadableSpan:
    return next(span for span in spans if span.name == f"tools/call {name}")


def test_action_attributes_cover_defaults_valid_and_invalid_values() -> None:
    async def scenario() -> None:
        app = _traced_app()
        with capture_tool_spans() as spans:
            async with Client(app) as client:
                await client.call_tool("trace_api_query", {"entity_type": "projects"})
                await client.call_tool(
                    "trace_api_query", {"action": "get", "entity_type": "users"}
                )
                await client.call_tool(
                    "trace_api_query", {"action": "search", "entity_type": "issues"}
                )
                await client.call_tool("trace_sql_query", {"sql": "SELECT 1"})
                await client.call_tool("trace_search_query", {"prompt": "find records"})
                await client.call_tool(
                    "trace_search_query",
                    {"prompt": "find records", "search_type": "keyword"},
                )
                await client.call_tool(
                    "trace_api_query", {"action": "delete", "entity_type": "secrets"}
                )
                await client.call_tool(
                    "trace_search_query",
                    {"prompt": "find records", "search_type": "invalid"},
                )

        api_spans = [
            span for span in spans if span.name == "tools/call trace_api_query"
        ]
        assert len(api_spans) == 4
        assert (api_spans[0].attributes or {})["airbyte.mcp.agent.action"] == "list"
        assert (api_spans[0].attributes or {})[
            "airbyte.mcp.agent.entity_type"
        ] == "projects"
        assert (api_spans[1].attributes or {})["airbyte.mcp.agent.action"] == "get"
        assert (api_spans[1].attributes or {})[
            "airbyte.mcp.agent.entity_type"
        ] == "users"
        assert (api_spans[2].attributes or {})["airbyte.mcp.agent.action"] == "search"
        assert (api_spans[2].attributes or {})[
            "airbyte.mcp.agent.entity_type"
        ] == "issues"
        assert "airbyte.mcp.agent.action" not in (api_spans[3].attributes or {})
        assert "airbyte.mcp.agent.entity_type" not in (api_spans[3].attributes or {})

        sql_attributes = _tool_span(spans, "trace_sql_query").attributes or {}
        assert sql_attributes["airbyte.mcp.agent.action"] == "sql_select"
        search_spans = [
            span for span in spans if span.name == "tools/call trace_search_query"
        ]
        assert len(search_spans) == 3
        assert (search_spans[0].attributes or {})[
            "airbyte.mcp.agent.action"
        ] == "search_hybrid"
        assert (search_spans[1].attributes or {})[
            "airbyte.mcp.agent.action"
        ] == "search_keyword"
        assert "airbyte.mcp.agent.action" not in (search_spans[2].attributes or {})

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("tool_name", "arguments", "expected"),
    [
        ("execute_external_api_query", {}, {"agent.action": "list"}),
        (
            "execute_external_api_query",
            {"action": "get", "entity_type": "accounts"},
            {"agent.action": "get", "agent.entity_type": "accounts"},
        ),
        (
            "execute_external_sql_query",
            {"action": "delete"},
            {"agent.action": "sql_select"},
        ),
        (
            "execute_external_search_query",
            {},
            {"agent.action": "search_hybrid"},
        ),
        (
            "execute_external_search_query",
            {"search_type": "invalid"},
            {},
        ),
    ],
)
def test_action_attribute_helper_defaults_and_rejects_invalid_values(
    tool_name: str,
    arguments: dict[str, object],
    expected: dict[str, str],
) -> None:
    assert agent_action_attributes(tool_name, arguments) == expected
