# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Native Datadog payload policy and real HTTP/LLM span contracts."""

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from airbyte.mcp import _datadog


@pytest.mark.parametrize("name", ["config", "testing_values", "api_args"])
def test_selected_argument_redaction_preserves_other_payloads(name):
    original = {
        "method": "tools/call",
        "params": {"arguments": {name: "private", "other": "visible"}},
    }
    span = SimpleNamespace(
        input=[{"content": json.dumps(original)}],
        output=[{"content": "visible result"}],
        get_tag=lambda key: {"mcp_tool": "example"}.get(key),
    )
    _datadog.redact_tool_span(span)
    assert json.loads(span.input[0]["content"])["params"]["arguments"] == {
        name: "[REDACTED]",
        "other": "visible",
    }
    assert span.output == [{"content": "visible result"}]
    assert original["params"]["arguments"][name] == "private"


@pytest.mark.parametrize(
    "tool",
    [
        "execute_agent_connector",
        "execute_agent_connector_ro",
        "get_cloud_sync_logs",
        "get_connection_artifact",
        "get_stream_previews",
        "read_source_stream_records",
        "run_sql_query",
    ],
)
def test_selected_outputs_and_malformed_inputs_are_fully_redacted(tool):
    span = SimpleNamespace(
        input=[object()],
        output=[{"content": "private"}],
        get_tag=lambda key: tool if key == "mcp_tool" else None,
    )
    _datadog.redact_tool_span(span)
    assert span.input == [{"content": "[REDACTED]", "role": ""}]
    assert span.output == [{"content": "[REDACTED]", "role": ""}]


def test_outbound_mcp_payloads_are_fully_redacted():
    span = SimpleNamespace(
        input=[{"content": "private"}],
        output=[{"content": "private"}],
        get_tag=lambda key: "client" if key == "mcp_tool_kind" else None,
    )
    _datadog.redact_tool_span(span)
    assert span.input == span.output == [{"content": "[REDACTED]", "role": ""}]


@pytest.mark.parametrize("conflict", ["otel_requests", "native_mcp"])
def test_native_startup_rejects_duplicate_instrumentation(monkeypatch, conflict):
    import mcp
    from fastmcp import FastMCP

    monkeypatch.setattr(
        _datadog,
        "RequestsInstrumentor",
        lambda: SimpleNamespace(
            is_instrumented_by_opentelemetry=conflict == "otel_requests"
        ),
    )
    monkeypatch.setattr(mcp, "__datadog_patch", conflict == "native_mcp", raising=False)
    with pytest.raises(RuntimeError, match="OTel requests|automatic MCP"):
        _datadog.install(FastMCP("duplicate-test"))


@pytest.mark.parametrize("digest_key_mode", ["enabled", "missing", "blank", "invalid"])
def test_native_datadog_http_contract_in_fresh_process(digest_key_mode):
    # ddtrace and OTel own process-global state. Keep this real integration test
    # independent from the suite's OTel provider and prohibit external exporters.
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("DD_", "OTEL_", "AIRBYTE_MCP_"))
    }
    env.update(
        DIGEST_KEY_MODE=digest_key_mode,
        DO_NOT_TRACK="1",
        DD_TRACE_HTTPX_ENABLED="false",
        DD_TRACE_MCP_ENABLED="false",
        DD_TRACE_REQUESTS_ENABLED="true",
        DD_TRACE_URLLIB3_ENABLED="true",
        DD_TRACE_STARTUP_LOGS="false",
        DD_INSTRUMENTATION_TELEMETRY_ENABLED="false",
        DD_REMOTE_CONFIGURATION_ENABLED="false",
        DD_SERVICE="native-mcp-test",
    )
    completed = subprocess.run(
        [sys.executable, str(Path(__file__).resolve())],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def _native_http_contract():
    import asyncio
    import hashlib
    import logging
    import socket
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from unittest.mock import patch

    import ddtrace.auto  # noqa: F401  # Install native HTTP instrumentation for the contract.
    from ddtrace import tracer
    from ddtrace._trace.processor import TraceProcessor
    from ddtrace.llmobs import LLMObs
    import httpx
    import requests
    from fastmcp import FastMCP
    from fastmcp.tools import ToolResult
    from mcp.types import CallToolRequest, CallToolResult
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )
    from airbyte.mcp import _otel
    from airbyte.mcp._args_digest import args_digest

    # Assert every socket is loopback; writers are captured and never started.
    connect = socket.socket.connect

    def loopback(sock, address):
        if isinstance(address, tuple):
            assert address[0] in {"127.0.0.1", "::1"}, address
        return connect(sock, address)

    socket.socket.connect = loopback
    spans = []

    class Capture(TraceProcessor):
        def process_trace(self, finished):
            spans.extend(finished)
            return None

    LLMObs._start_service = lambda self: None
    LLMObs._stop_service = lambda self: None
    LLMObs.enable(
        agent_service="native-mcp-test",
        integrations_enabled=False,
        agentless_enabled=False,
    )
    tracer.configure(trace_processors=[Capture()])
    logging.disable(logging.CRITICAL)
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"visible synthetic result")

        def log_message(self, *_):
            pass

    upstream = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    app = FastMCP("native-mcp-contract")
    correlations = []
    calls = []

    @app.tool(name="execute_external_api_query")
    def execute(
        mode: str = "ok", action: str = "list", config: dict | None = None
    ) -> ToolResult:
        calls.append(mode)
        active = tracer.current_span()
        correlations.append((active.span_id, tracer.get_log_correlation_context()))
        requests.get(
            f"http://127.0.0.1:{upstream.server_port}/synthetic", timeout=3
        ).raise_for_status()
        if mode == "raise":
            raise ValueError("visible synthetic error")
        return ToolResult(content="visible synthetic result", is_error=mode == "error")

    @app.tool()
    def run_sql_query() -> str:
        return "private synthetic result"

    @app.tool()
    async def handle_error() -> str:
        from fastmcp.exceptions import ToolError

        try:
            await app.call_tool("execute_external_api_query", {"mode": "raise"})
        except ToolError:
            return "handled"
        raise AssertionError("Expected inner tool failure")

    environment = {
        "AIRBYTE_MCP_TRACING_BACKEND": "datadog",
        "AIRBYTE_MCP_INTENT_CAPTURE": "1",
    }
    key_mode = os.environ.get("DIGEST_KEY_MODE", "missing")
    if key_mode != "missing":
        environment["AIRBYTE_MCP_OTEL_DIGEST_KEY"] = {
            "enabled": " synthetic-native-key ",
            "blank": " ",
            "invalid": "\ud800",
        }[key_mode]
    digest_key = b" synthetic-native-key " if key_mode == "enabled" else None
    _otel.install(app, environ=environment)
    # Configuration is read once; later environment changes cannot rotate a key.
    environment["AIRBYTE_MCP_OTEL_DIGEST_KEY"] = "changed-after-install"

    assert isinstance(
        trace.get_tracer_provider(), TracerProvider
    )  # Existing provider is untouched.
    responses = []

    async def run():
        raw = app.http_app(path="/mcp", stateless_http=True, json_response=True)
        async with raw.router.lifespan_context(raw):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=_otel.SessionIdHeaderDigest(raw)),
                base_url="http://testserver",
            ) as client:
                sequence = [
                    (
                        "initialize",
                        {
                            "protocolVersion": "2025-11-25",
                            "capabilities": {},
                            "clientInfo": {"name": "custom-agent", "version": "1.2"},
                        },
                    ),
                    ("tools/list", {}),
                    *[
                        (
                            "tools/call",
                            {
                                "name": "execute_external_api_query",
                                "arguments": {
                                    "mode": mode,
                                    "action": "list",
                                    "config": {"secret": "private argument"},
                                    "intent": "Keep Mixed Case Intent",
                                },
                            },
                        )
                        for mode in ("ok", "error", "raise")
                    ],
                    ("tools/call", {"name": "run_sql_query", "arguments": {}}),
                    ("tools/call", {"name": "unknown_tool", "arguments": {}}),
                ]
                for index, (method, params) in enumerate(sequence):
                    response = await client.post(
                        "/mcp",
                        headers={
                            "accept": "application/json, text/event-stream",
                            "mcp-session-id": "synthetic-session",
                        },
                        json={
                            "jsonrpc": "2.0",
                            "id": index + 1,
                            "method": method,
                            "params": params,
                        },
                    )
                    assert response.status_code == 200
                    responses.append((method, params, response.json()))
                # Telemetry failures must not abort dispatch or replace its result.
                for target in ("tool", "annotate"):
                    before = len(calls)
                    with patch.object(
                        LLMObs,
                        target,
                        side_effect=RuntimeError("instrumentation failure"),
                    ):
                        response = await client.post(
                            "/mcp",
                            headers={"accept": "application/json, text/event-stream"},
                            json={
                                "jsonrpc": "2.0",
                                "id": 20,
                                "method": "tools/call",
                                "params": {
                                    "name": "execute_external_api_query",
                                    "arguments": {"mode": "ok"},
                                },
                            },
                        )
                    assert (
                        response.status_code == 200
                        and not response.json()["result"]["isError"]
                    )
                    assert len(calls) == before + 1

                # Failed request serialization must not bypass sensitive output redaction.
                with patch.object(
                    _datadog,
                    "CallToolRequest",
                    SimpleNamespace(
                        model_validate=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                            ValueError("capture failure")
                        )
                    ),
                ):
                    response = await client.post(
                        "/mcp",
                        headers={"accept": "application/json, text/event-stream"},
                        json={
                            "jsonrpc": "2.0",
                            "id": 30,
                            "method": "tools/call",
                            "params": {"name": "run_sql_query", "arguments": {}},
                        },
                    )
                assert (
                    response.json()["result"]["content"][0]["text"]
                    == "private synthetic result"
                )
                failed_capture = next(
                    span for span in reversed(spans) if span.span_type == "llm"
                )
                assert (
                    failed_capture._get_ctx_item("_llmobs.cached_event")["meta"][
                        "output"
                    ]["value"]
                    == "[REDACTED]"
                )
                before = len([span for span in spans if span.span_type == "llm"])
                response = await client.post(
                    "/mcp",
                    headers={"accept": "application/json, text/event-stream"},
                    json={
                        "jsonrpc": "2.0",
                        "id": 40,
                        "method": "tools/call",
                        "params": {"name": "handle_error", "arguments": {}},
                    },
                )
                assert not response.json()["result"]["isError"]
                native_after = [span for span in spans if span.span_type == "llm"]
                assert len(native_after) == before + 1
                handled = native_after[-1]
                assert not handled.error
                assert handled.get_tag("airbyte.mcp.error_type") is None
                assert handled.get_tag("airbyte.mcp.args_digest") == (
                    args_digest("handle_error", {}, digest_key) if digest_key else None
                )
                assert (
                    handled._get_ctx_item("_llmobs.cached_event")["name"]
                    == "handle_error"
                )
        # Cancellation must propagate and restore the previous Datadog context.
        with tracer.trace("cancellation-parent") as parent:
            before = tracer.current_span()

            async def cancel(_ctx):
                raise asyncio.CancelledError()

            with pytest.raises(asyncio.CancelledError):
                await _datadog._DatadogRequestMiddleware()(
                    SimpleNamespace(
                        method="tools/call", params={"name": "cancel", "arguments": {}}
                    ),
                    cancel,
                )
            assert tracer.current_span() is before is parent

    try:
        asyncio.run(run())
        assert not exporter.get_finished_spans(), (
            "Native backend must not create duplicate OTel spans"
        )
        native = [span for span in spans if span.span_type == "llm"]
        primary = native[:7]
        assert len(primary) == 7
        lookup = {span.span_id: span for span in spans}
        for span in primary:
            assert lookup[span.parent_id].name == "starlette.request"
        events = [span._get_ctx_item("_llmobs.cached_event") for span in primary]
        assert [event["meta"]["span"]["kind"] for event in events] == [
            "task",
            "task",
            "tool",
            "tool",
            "tool",
            "tool",
            "tool",
        ]
        assert events[0]["name"] == "mcp.initialize"
        assert "client_name:custom-agent" in events[0]["tags"]
        assert "client_version:custom-agent_1.2" in events[0]["tags"]
        assert [span.resource for span in primary] == [
            "server_request",
            "server_request",
        ] + ["server_tool_call"] * 5
        for index in (2, 3, 4):
            span, event = primary[index], events[index]
            method, params, response = responses[index]
            expected_input = CallToolRequest.model_validate({
                "method": method,
                "params": params,
            }).model_dump(exclude={"params": {"meta": "_dd_trace_context"}})
            expected_input["params"]["arguments"]["config"] = "[REDACTED]"
            assert json.loads(event["meta"]["input"]["value"]) == expected_input
            expected_output = CallToolResult.model_validate(
                response["result"]
            ).model_dump(mode="json")
            assert json.loads(event["meta"]["output"]["value"]) == expected_output, (
                json.loads(event["meta"]["output"]["value"]),
                expected_output,
            )
            assert event["meta"]["metadata"]["intent"] == "Keep Mixed Case Intent"
            assert event["meta"]["metadata"]["agent.action"] == "list"
            expected_digest = (
                args_digest(
                    "execute_external_api_query",
                    {k: v for k, v in params["arguments"].items() if k != "intent"},
                    digest_key,
                )
                if digest_key
                else None
            )
            assert event["meta"]["metadata"].get("args_digest") == expected_digest
            assert span.get_tag("airbyte.mcp.args_digest") == expected_digest
            assert (
                event["meta"]["metadata"]["tool_id"]
                == hashlib.sha256(str(index + 1).encode()).hexdigest()
            )
            assert "mcp_tool_kind:server" in event["tags"]
            assert (
                "mcp_session_id:" + hashlib.sha256(b"synthetic-session").hexdigest()
                in event["tags"]
            )
            requests_children = [
                s
                for s in spans
                if s.parent_id == span.span_id and s.name == "requests.request"
            ]
            assert len(requests_children) == 1
            assert any(
                s.parent_id == requests_children[0].span_id
                and s.name == "urllib3.request"
                for s in spans
            )
        assert primary[2].error == 0
        assert all(
            span.error == 1 and span.get_tag("error.type") == "ToolError"
            for span in primary[3:5]
        )
        assert events[4]["meta"]["metadata"]["error_type"] == "ValueError"
        assert events[5]["meta"]["output"]["value"] == "[REDACTED]"
        assert (
            responses[5][2]["result"]["content"][0]["text"]
            == "private synthetic result"
        )
        assert events[6]["name"] == "unknown_tool" and primary[6].error
        # Initialization, listing, and unknown tools never inherit another call's digest.
        for index in (0, 1, 6):
            assert primary[index].get_tag("airbyte.mcp.args_digest") is None
            assert "args_digest" not in events[index]["meta"].get("metadata", {})
        assert events[5]["meta"]["metadata"].get("args_digest") == (
            args_digest("run_sql_query", {}, digest_key) if digest_key else None
        )
        serialized = json.dumps(events)
        assert "private argument" not in serialized
        assert "synthetic-native-key" not in serialized
        assert "changed-after-install" not in serialized
        for span_id, correlation in correlations[:3]:
            assert str(span_id) == correlation["dd.span_id"]
        print(
            "Native Datadog HTTP, payload, redaction, hierarchy, logs, and fault contracts passed"
        )
    finally:
        tracer.shutdown()
        provider.shutdown()
        upstream.shutdown()


if __name__ == "__main__":
    _native_http_contract()
