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
from airbyte.version import get_version


@pytest.mark.parametrize("original_input", [[{"content": "private"}], [object()], []])
def test_tool_payloads_rebuilt_only_from_approved_metadata(original_input):
    span = SimpleNamespace(
        input=original_input,
        output=[{"content": "private output"}],
        metadata={
            "intent": "Find matching records",
            "agent.action": "list",
            "agent.entity_type": "contacts",
            "unrelated": "private metadata",
        },
        get_tag=lambda key: {"mcp_tool": "example"}.get(key),
    )
    _datadog.redact_tool_span(span)
    assert json.loads(span.input[0]["content"]) == {
        "method": "tools/call",
        "params": {
            "name": "example",
            "arguments": {
                "intent": "Find matching records",
                "action": "list",
                "entity_type": "contacts",
            },
        },
    }
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


def test_native_datadog_http_contract_in_fresh_process():
    # ddtrace and OTel own process-global state. Keep this real integration test
    # independent from the suite's OTel provider and prohibit external exporters.
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("DD_", "OTEL_", "AIRBYTE_MCP_"))
    }
    env.update(
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


async def _native_distributed_context_contract(app):
    import asyncio
    from unittest.mock import patch

    import httpx
    from ddtrace import tracer
    from ddtrace.llmobs import LLMObs

    headers = {
        "x-datadog-trace-id": "123456",
        "x-datadog-parent-id": "654321",
        "x-datadog-sampling-priority": "1",
    }
    llm_provider = LLMObs._instance._llmobs_context_provider
    # Use SDK-produced headers as well as a plain APM carrier: LLM and APM
    # have separate parent/trace IDs, and both must survive propagation.
    with LLMObs.task(name="remote-agent") as remote:
        llm_headers = LLMObs.inject_distributed_headers({})
        remote_ids = LLMObs.export_span(remote)
    llm_provider.activate(None)
    # A broken telemetry provider must neither abort the tool nor replace its
    # result while unwinding the span. Do not activate remote context if its
    # previous LLM context could not be saved.
    for failing_method in ("active", "activate"):
        with tracer.trace("provider-failure-parent") as parent:

            async def unaffected_call(_ctx):
                return {"content": []}

            with patch.object(
                llm_provider,
                failing_method,
                side_effect=RuntimeError("provider failure"),
            ):
                result = await _datadog._DatadogRequestMiddleware()(
                    SimpleNamespace(
                        method="tools/call",
                        params={
                            "name": "synthetic",
                            "_meta": {"_dd_trace_context": headers},
                        },
                    ),
                    unaffected_call,
                )
            assert result == {"content": []}
            assert tracer.current_span() is parent
            llm_provider.activate(None)
    for incoming, environ, inherited in [
        ({"_dd_trace_context": headers}, {}, True),
        ({"_dd_trace_context": llm_headers}, {}, True),
        (
            {"_dd_trace_context": headers},
            {"DD_MCP_DISTRIBUTED_TRACING": "false"},
            False,
        ),
        ({"_dd_trace_context": headers}, {"DD_MCP_DISTRIBUTED_TRACING": "0"}, False),
        ({}, {}, False),
        (None, {}, False),
        ({"_dd_trace_context": "invalid"}, {}, False),
        ({"_dd_trace_context": {"x-datadog-trace-id": []}}, {}, False),
        ({"_dd_trace_context": {"x-datadog-trace-id": "bad"}}, {}, False),
    ]:
        for outcome in ("success", "exception", "cancel"):
            with LLMObs.task(name="local-agent") as local:
                with tracer.trace("http-parent") as http_parent:
                    before_llm = llm_provider.active()
                    seen = []

                    async def call_next(_ctx):
                        seen.append(tracer.current_span())
                        if outcome == "exception":
                            raise ValueError("synthetic failure")
                        if outcome == "cancel":
                            raise asyncio.CancelledError()
                        return {"content": [], "isError": False}

                    ctx = SimpleNamespace(
                        method="tools/call",
                        params={
                            "name": "synthetic",
                            "arguments": {},
                            "_meta": incoming,
                        },
                    )
                    middleware = _datadog._DatadogRequestMiddleware(environ=environ)
                    if outcome == "success":
                        assert await middleware(ctx, call_next) == {
                            "content": [],
                            "isError": False,
                        }
                    else:
                        error = (
                            ValueError
                            if outcome == "exception"
                            else asyncio.CancelledError
                        )
                        with pytest.raises(error):
                            await middleware(ctx, call_next)
                    assert tracer.current_span() is http_parent
                    assert llm_provider.active() is before_llm is local
                    span = seen[0]
                    if inherited:
                        carrier = incoming["_dd_trace_context"]
                        assert span.trace_id == (
                            remote.trace_id if carrier is llm_headers else 123456
                        )
                        assert span.parent_id == (
                            remote.span_id if carrier is llm_headers else 654321
                        )
                    else:
                        assert span.trace_id == http_parent.trace_id
                        assert span.parent_id == http_parent.span_id
                    event = span._get_ctx_item("_llmobs.cached_event")
                    assert "_dd_trace_context" not in json.dumps(event)
                    assert "x-datadog-" not in json.dumps(event)
                    if inherited and incoming["_dd_trace_context"] is llm_headers:
                        assert event["parent_id"] == remote_ids["span_id"]
                        assert event["trace_id"] == remote_ids["trace_id"]

    # HTTP may have joined the APM trace without activating the separate LLM
    # context. Keep that nearer HTTP parent and inherit the remote LLM parent.
    from ddtrace.propagation.http import HTTPPropagator

    for carrier in (headers, llm_headers):
        for outcome in ("success", "exception", "cancel"):
            llm_provider.activate(None)
            with tracer.start_span(
                "http-parent", child_of=HTTPPropagator.extract(carrier), activate=True
            ) as http_parent:
                seen = []

                async def same_trace_call(_ctx):
                    seen.append(tracer.current_span())
                    assert seen[0].parent_id == http_parent.span_id
                    if outcome == "exception":
                        raise ValueError("synthetic failure")
                    if outcome == "cancel":
                        raise asyncio.CancelledError()
                    return {"content": []}

                ctx = SimpleNamespace(
                    method="tools/call",
                    params={
                        "name": "synthetic",
                        "_meta": {"_dd_trace_context": carrier},
                    },
                )
                middleware = _datadog._DatadogRequestMiddleware()
                if outcome == "success":
                    assert await middleware(ctx, same_trace_call) == {"content": []}
                else:
                    error = (
                        ValueError if outcome == "exception" else asyncio.CancelledError
                    )
                    with pytest.raises(error):
                        await middleware(ctx, same_trace_call)
                assert tracer.current_span() is http_parent
                assert llm_provider.active() is None
                assert seen[0].trace_id == http_parent.trace_id
                if carrier is llm_headers:
                    event = seen[0]._get_ctx_item("_llmobs.cached_event")
                    assert event["parent_id"] == remote_ids["span_id"]
                    assert event["trace_id"] == remote_ids["trace_id"]

    # Exercise the actual MCP/HTTP boundary to verify the wire `_meta` carrier
    # reaches the middleware, rather than testing only a synthetic context.
    raw = app.http_app(path="/mcp", stateless_http=True, json_response=True)
    async with raw.router.lifespan_context(raw):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=raw), base_url="http://testserver"
        ) as client:
            captured = []

            @app.tool()
            def propagated_tool() -> str:
                captured.append(tracer.current_span())
                return "synthetic response"

            response = await client.post(
                "/mcp",
                headers={"accept": "application/json, text/event-stream"},
                json={
                    "jsonrpc": "2.0",
                    "id": 123,
                    "method": "tools/call",
                    "params": {
                        "name": "propagated_tool",
                        "arguments": {},
                        "_meta": {"_dd_trace_context": headers},
                    },
                },
            )
            assert (
                response.status_code == 200 and not response.json()["result"]["isError"]
            )
            assert captured[0].trace_id == 123456
            assert captured[0].parent_id == 654321


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
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )
    from airbyte.mcp import _otel, _scope, _telemetry
    from fastmcp_extensions import ToolCallTelemetryMiddleware
    from unittest.mock import AsyncMock

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
    records = []
    app.add_middleware(_scope.CallScopeMiddleware())
    telemetry = ToolCallTelemetryMiddleware(
        extra_properties=_scope.call_scope_properties
    )
    telemetry._sinks.emit = records.append
    app.add_middleware(telemetry)
    correlations = []
    entities = []
    calls = []
    private_arguments = {
        "name_contains": "private-name-canary",
        "stream_name": "private-stream-canary",
        "connection_id": "private-id-canary",
        "manifest_yaml": "private-manifest-canary",
        "nested": {"values": ["private-nested-canary"]},
    }
    private_payload = json.dumps(private_arguments, sort_keys=True)

    @app.tool(name="execute_external_api_query")
    def execute(
        mode: str = "ok",
        action: str = "list",
        config: dict | None = None,
        entity_type: str = "",
        name_contains: str = "",
        stream_name: str = "",
        connection_id: str = "",
        manifest_yaml: str = "",
        nested: dict | None = None,
    ) -> ToolResult:
        if name_contains:
            assert {
                "name_contains": name_contains,
                "stream_name": stream_name,
                "connection_id": connection_id,
                "manifest_yaml": manifest_yaml,
                "nested": nested,
            } == private_arguments
        _scope.record_default_workspace("11111111-1111-1111-1111-111111111111")
        calls.append(mode)
        entities.append(entity_type)
        active = tracer.current_span()
        correlations.append((active.span_id, tracer.get_log_correlation_context()))
        requests.get(
            f"http://127.0.0.1:{upstream.server_port}/synthetic", timeout=3
        ).raise_for_status()
        if mode == "raise":
            raise ValueError(private_payload)
        return ToolResult(content=private_payload, is_error=mode == "error")

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

    _otel.install(
        app,
        environ={
            "DD_LLMOBS_ENABLED": "true",
            "AIRBYTE_MCP_INTENT_CAPTURE": "1",
        },
    )
    assert isinstance(
        trace.get_tracer_provider(), TracerProvider
    )  # Existing provider is untouched.
    responses = []
    # Client attribution comes from the same resolver analytics uses. Native
    # tracing must apply its label bounds without modifying analytics values.
    client_properties = {
        "mcp_client_name": " Custom Agent ",
        "mcp_client_version": "v" * 300,
        "auth_method": "client_credentials",
        "mcp_protocol_version": "2025-11-25",
        "session_id": hashlib.sha256(b"synthetic-session").hexdigest(),
    }
    original_properties = dict(client_properties)

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
                                    **private_arguments,
                                    "mode": mode,
                                    "entity_type": "Custom Entities/東京" * 20,
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
                assert (
                    handled._get_ctx_item("_llmobs.cached_event")["name"]
                    == "handle_error"
                )

        # An exception escaping the protocol boundary must retain its class/status
        # without exporting a payload-containing message or traceback.
        async def fail(_ctx):
            raise ValueError(private_payload)

        with pytest.raises(ValueError, match="private-name-canary"):
            await _datadog._DatadogRequestMiddleware()(
                SimpleNamespace(
                    method="tools/call",
                    params={
                        "name": "escaped_error",
                        "arguments": {
                            **private_arguments,
                            "workspace_id": "12345678-1234-1234-1234-123456789abc",
                            "organization_id": "87654321-4321-4321-4321-abcdef123456",
                        },
                    },
                ),
                fail,
            )
        escaped = next(span for span in reversed(spans) if span.span_type == "llm")
        assert escaped.error and escaped.get_tag("error.type") == "ValueError"
        assert escaped.get_tag("error.message") is None
        assert escaped.get_tag("error.stack") is None
        escaped_metadata = escaped._get_ctx_item("_llmobs.cached_event")["meta"][
            "metadata"
        ]
        assert escaped_metadata["outcome"] == "exception"
        assert escaped_metadata["error_type"] == "ValueError"
        assert escaped_metadata["auth_method"] == "client_credentials"
        assert escaped_metadata["mcp_protocol_version"] == "2025-11-25"
        assert escaped_metadata["session_id"] == client_properties["session_id"]
        assert (
            escaped_metadata["workspace_id"] == "12345678-1234-1234-1234-123456789abc"
        )
        assert (
            escaped_metadata["organization_id"]
            == "87654321-4321-4321-4321-abcdef123456"
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
        cancelled = next(span for span in reversed(spans) if span.span_type == "llm")
        cancelled_metadata = cancelled._get_ctx_item("_llmobs.cached_event")["meta"][
            "metadata"
        ]
        assert cancelled_metadata["outcome"] == "cancelled"
        assert cancelled_metadata["error_type"] == "CancelledError"

        # Cancellation arriving after tool completion must still reach the native
        # outcome and retain already-known metadata while restoring its parent.
        intent_middleware = next(
            item
            for item in app.middleware
            if isinstance(item, _datadog._DatadogIntentMiddleware)
        )
        for tool_fails in (False, True):
            enriching = asyncio.Event()

            async def enrich(_ctx):
                enriching.set()
                await asyncio.Future()

            async def tool(_ctx):
                if tool_fails:
                    raise ValueError("private synthetic result")
                return ToolResult(content="private synthetic result")

            async def dispatch(_ctx):
                return await intent_middleware._trace_call(
                    SimpleNamespace(fastmcp_context=None),
                    tool,
                    {"airbyte.mcp.intent": "Preserve safe metadata on cancellation"},
                )

            with patch.object(_scope, "enrich_call_scope", enrich):
                task = asyncio.create_task(
                    _datadog._DatadogRequestMiddleware()(
                        SimpleNamespace(
                            method="tools/call", params={"name": "enrich_cancel"}
                        ),
                        dispatch,
                    )
                )
                await asyncio.wait_for(enriching.wait(), 5)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            span = next(span for span in reversed(spans) if span.span_type == "llm")
            metadata = span._get_ctx_item("_llmobs.cached_event")["meta"]["metadata"]
            assert metadata["outcome"] == "cancelled"
            assert (
                metadata["error_type"] == span.get_tag("error.type") == "CancelledError"
            )
            assert metadata["intent"] == "Preserve safe metadata on cancellation"
            assert span.get_tag("error.stack") is None

    try:
        with (
            patch.object(
                _telemetry, "request_properties", return_value=client_properties
            ),
            patch.object(
                _scope,
                "resolve_user_default_organization_id",
                new=AsyncMock(return_value="33333333-3333-3333-3333-333333333333"),
            ),
            patch.object(
                _scope,
                "resolve_call_workspace_organization_id",
                new=AsyncMock(return_value="44444444-4444-4444-4444-444444444444"),
            ),
        ):
            asyncio.run(run())
        assert client_properties == original_properties
        assert not exporter.get_finished_spans(), (
            "Native backend must not create duplicate OTel spans"
        )
        native = [span for span in spans if span.span_type == "llm"]
        serialized_native = json.dumps([
            {"apm": span.get_tags(), "llm": span._get_ctx_item("_llmobs.cached_event")}
            for span in native
        ])
        for secret in (
            "private-name-canary",
            "private-stream-canary",
            "private-id-canary",
            "private-manifest-canary",
            "private-nested-canary",
            "private argument",
            "private synthetic result",
        ):
            assert secret not in serialized_native
        primary = native[:7]
        assert len(primary) == 7
        lookup = {span.span_id: span for span in spans}
        for span in primary:
            assert lookup[span.parent_id].name == "starlette.request"
        events = [span._get_ctx_item("_llmobs.cached_event") for span in primary]
        for event in events:
            metadata = event["meta"]["metadata"]
            assert metadata["pyairbyte.version"] == get_version()
            assert metadata["auth_method"] == "client_credentials"
            assert metadata["mcp_protocol_version"] == "2025-11-25"
            assert metadata["session_id"] == client_properties["session_id"]
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
            assert json.loads(event["meta"]["input"]["value"]) == {
                "method": "tools/call",
                "params": {
                    "name": "execute_external_api_query",
                    "arguments": {
                        "intent": "Keep Mixed Case Intent",
                        "action": "list",
                        "entity_type": ("Custom Entities/東京" * 20)[:256].rstrip(),
                    },
                },
            }
            assert event["meta"]["output"]["value"] == "[REDACTED]"
            assert event["meta"]["metadata"]["intent"] == "Keep Mixed Case Intent"
            assert event["meta"]["metadata"]["agent.action"] == "list"
            entity = "Custom Entities/東京" * 20
            # Only bounded approved telemetry leaves the process; execution
            # receives the original argument.
            assert entities[index - 2] == entity
            assert (
                event["meta"]["metadata"]["agent.entity_type"] == entity[:256].rstrip()
            )
            assert (
                span.get_tag("airbyte.mcp.agent.entity_type") == entity[:256].rstrip()
            )
            assert (
                event["meta"]["metadata"]["workspace_id"]
                == "11111111-1111-1111-1111-111111111111"
            )
            assert (
                event["meta"]["metadata"]["organization_id"]
                == "44444444-4444-4444-4444-444444444444"
            )
            assert event["meta"]["metadata"]["scope_source"] == "default"
            assert event["meta"]["metadata"]["client_name"] == "Custom Agent"
            assert event["meta"]["metadata"]["client_version"] == "v" * 256
            assert span.get_tag("airbyte.mcp.client_name") == "Custom Agent"
            assert span.get_tag("airbyte.mcp.client_version") == "v" * 256
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
        assert (
            json.loads(responses[2][2]["result"]["content"][0]["text"])
            == private_arguments
        )
        assert (
            json.loads(responses[3][2]["result"]["content"][0]["text"])
            == private_arguments
        )
        assert primary[2].error == 0
        tool_records = [
            record for record in records if record.name == "execute_external_api_query"
        ]
        for record in tool_records[:3]:
            assert (
                record.extra["workspace_id"] == "11111111-1111-1111-1111-111111111111"
            )
            assert (
                record.extra["organization_id"]
                == "44444444-4444-4444-4444-444444444444"
            )
        assert (
            events[5]["meta"]["metadata"]["organization_id"]
            == "33333333-3333-3333-3333-333333333333"
        )
        assert "workspace_id" not in events[5]["meta"]["metadata"]
        for index, outcome, error_type in (
            (2, "success", None),
            (3, "tool_error", "ToolError"),
            (4, "exception", "ValueError"),
        ):
            span, event = primary[index], events[index]
            assert bool(span.error) is (error_type is not None)
            assert span.get_tag("error.type") == error_type
            assert span.get_tag("airbyte.mcp.outcome") == outcome
            assert event["meta"]["metadata"]["outcome"] == outcome
            assert event["meta"]["metadata"].get("error_type") == error_type
        assert events[4]["meta"]["metadata"]["error_type"] == "ValueError"
        assert events[5]["meta"]["output"]["value"] == "[REDACTED]"
        assert (
            responses[5][2]["result"]["content"][0]["text"]
            == "private synthetic result"
        )
        assert events[6]["name"] == "unknown_tool" and primary[6].error
        for span_id, correlation in correlations[:3]:
            assert str(span_id) == correlation["dd.span_id"]
        asyncio.run(_native_distributed_context_contract(app))
        print(
            "Native Datadog HTTP, payload, redaction, hierarchy, logs, and fault contracts passed"
        )
    finally:
        tracer.shutdown()
        provider.shutdown()
        upstream.shutdown()


if __name__ == "__main__":
    _native_http_contract()
