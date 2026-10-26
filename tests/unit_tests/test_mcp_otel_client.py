# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""MCP client trace context and session identity at the tracing boundary."""

from __future__ import annotations

import asyncio
import hashlib
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
from mcp.types import Implementation
from opentelemetry import trace

from airbyte.mcp._otel import SessionIdHeaderDigest


@pytest.fixture(autouse=True)
def _disable_remote_export(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", raising=False)
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    yield


def _app():
    app = mcp_server(
        display_name="client-tracing-tests",
        telemetry=TelemetryConfig(package_name="airbyte"),
    )

    @app.tool()
    def echo(value: str) -> str:
        return value

    register_tool_call_telemetry(
        app,
        TelemetryConfig(
            package_name="airbyte",
            tool_tracing=ToolTracingConfig(attribute_prefix="airbyte.mcp"),
        ),
    )
    return app


def test_client_trace_id_is_kept_without_parent_and_unknown_tools_are_exported() -> (
    None
):
    async def scenario() -> None:
        app = _app()
        trace_id = "0123456789abcdef0123456789abcdef"
        parent_id = "0123456789abcdef"

        with capture_tool_spans() as spans:
            async with Client(
                app,
                client_info=Implementation(name="trace-test-client", version="1.2.3"),
            ) as client:
                await client.list_tools()
                with trace.use_span(
                    trace.NonRecordingSpan(
                        trace.SpanContext(
                            trace_id=int(trace_id, 16),
                            span_id=int(parent_id, 16),
                            is_remote=True,
                            trace_flags=trace.TraceFlags(trace.TraceFlags.SAMPLED),
                        )
                    ),
                    end_on_exit=False,
                ):
                    await client.call_tool("echo", {"value": "private-value"})
                await client.call_tool("unregistered_tool", {}, raise_on_error=False)

        echo_span = next(span for span in spans if span.name == "tools/call echo")
        assert f"{echo_span.context.trace_id:032x}" == trace_id
        assert echo_span.parent is None
        echo_attributes = echo_span.attributes or {}
        assert echo_attributes["airbyte.mcp.client_name"] == "trace-test-client"
        assert echo_attributes["airbyte.mcp.client_version"] == "1.2.3"
        assert any(span.name == "tools/list" for span in spans)
        unknown_span = next(
            span
            for span in spans
            if (span.attributes or {}).get("airbyte.mcp.outcome") == "unknown_tool"
        )
        assert (unknown_span.attributes or {})[
            "airbyte.mcp.tool_requested_name"
        ] == "unregistered_tool"
        assert "private-value" not in repr([span.attributes for span in spans])

    asyncio.run(scenario())


def test_session_header_digest_replaces_raw_client_value() -> None:
    async def scenario() -> None:
        raw_session_id = b"unsigned-client-session-id"
        seen_scope: dict[str, object] = {}

        async def downstream(scope, _receive, _send):
            seen_scope.update(scope)

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(_message):
            return None

        scope = {
            "type": "http",
            "headers": [(b"mcp-session-id", raw_session_id)],
        }
        await SessionIdHeaderDigest(downstream)(scope, receive, send)

        digest = hashlib.sha256(raw_session_id).hexdigest()
        assert seen_scope["headers"] == [(b"mcp-session-id", digest.encode())]
        assert seen_scope["state"]["airbyte_mcp_session_id"] == digest

    asyncio.run(scenario())
