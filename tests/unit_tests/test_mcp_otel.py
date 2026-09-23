# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Hosted OpenTelemetry contracts at the real, redacted exporter boundary."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import os
import socket
import subprocess
import sys
import textwrap
from collections.abc import Iterator
from unittest.mock import Mock

import httpx
import pytest
import requests
from fastmcp import Client, FastMCP
from fastmcp import telemetry as fastmcp_telemetry
from fastmcp.exceptions import NotFoundError
from fastmcp_extensions import CapabilityTokenMiddleware
from jsonschema import ValidationError
from opentelemetry import trace
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode

from airbyte._direct_connectors import api_util as agents_api
from airbyte.agents.connectors import AgentConnector
from airbyte.cloud._credentials import _AirbyteCredentials
from airbyte.mcp import _otel as observability
from airbyte.mcp import agents as agents_mcp
from airbyte.version import get_version


@pytest.fixture(scope="session")
def otel_provider() -> Iterator[tuple[TracerProvider, InMemorySpanExporter]]:
    """Set the global provider once; use production's complete processor chain."""
    with pytest.MonkeyPatch.context() as environment:
        for key in os.environ:
            if key.startswith("OTEL_"):
                environment.delenv(key)
        exporter = InMemorySpanExporter()
        provider = observability._build_provider(exporter)
    trace.set_tracer_provider(provider)
    yield provider, exporter
    if RequestsInstrumentor().is_instrumented_by_opentelemetry:
        RequestsInstrumentor().uninstrument()
    provider.shutdown()


def _loopback_only(original, address_index):
    """Reject every socket target except loopback and AF_UNIX paths.

    Windows' ProactorEventLoop connects a loopback socketpair for its self-pipe
    whenever a loop is created, so a guard that rejects every connect breaks
    ``asyncio.run`` there before any exporter could run.
    """

    def guarded(*args, **kwargs):
        address = kwargs.get("address", args[address_index])
        host = address[0] if isinstance(address, tuple) else address
        if not isinstance(address, tuple) or (
            isinstance(host, str)
            and (host in ("::1", "localhost") or host.startswith("127."))
        ):
            return original(*args, **kwargs)
        raise AssertionError("unexpected network connection")

    return guarded


@pytest.fixture(autouse=True)
def isolated_otel(
    monkeypatch: pytest.MonkeyPatch,
    otel_provider: tuple[TracerProvider, InMemorySpanExporter],
) -> Iterator[None]:
    """Reset module state and exported spans, and prohibit real network traffic."""
    provider, exporter = otel_provider
    provider.force_flush()
    exporter.clear()
    observability._reset_for_tests()
    for key in os.environ:
        if key.startswith(("OTEL_", "AIRBYTE_MCP_")):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    for owner, name, index in (
        (socket.socket, "connect", 1),
        (socket.socket, "connect_ex", 1),
        (socket, "create_connection", 0),
    ):
        monkeypatch.setattr(owner, name, _loopback_only(getattr(owner, name), index))
    yield
    if RequestsInstrumentor().is_instrumented_by_opentelemetry:
        RequestsInstrumentor().uninstrument()
    provider.force_flush()
    exporter.clear()
    observability._reset_for_tests()


@pytest.fixture
def uninitialized_provider(monkeypatch):
    """Model the one-shot provider API without replacing the session provider."""
    current = [trace.ProxyTracerProvider()]
    setter = Mock(side_effect=lambda provider: current.__setitem__(0, provider))
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: current[0])
    monkeypatch.setattr(trace, "set_tracer_provider", setter)
    return setter


def _spans(
    otel_provider: tuple[TracerProvider, InMemorySpanExporter],
) -> tuple[ReadableSpan, ...]:
    provider, exporter = otel_provider
    assert provider.force_flush()
    return exporter.get_finished_spans()


def _tool_span(
    otel_provider: tuple[TracerProvider, InMemorySpanExporter],
) -> ReadableSpan:
    spans = [span for span in _spans(otel_provider) if span.kind == SpanKind.SERVER]
    assert len(spans) == 1, [span.name for span in spans]
    return spans[0]


def _export_text(otel_provider: tuple[TracerProvider, InMemorySpanExporter]) -> str:
    return "\n".join(span.to_json() for span in _spans(otel_provider))


def _capture(app: FastMCP, *, capture: bool = True) -> None:
    observability._build_tool_maps()
    app.add_middleware(
        observability.IntentCaptureMiddleware(
            app, environ={"AIRBYTE_MCP_INTENT_CAPTURE": str(int(capture))}
        )
    )


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch) -> FastMCP:
    """Use a real tool signature so unstripped intent fails validation."""
    server = FastMCP("otel-tests")

    @server.tool()
    def echo(value: str) -> str:
        return value

    _capture(server)
    monkeypatch.setitem(observability._TOOL_MODULES, "echo", "cloud")
    return server


async def _call(app: FastMCP, arguments: dict, *, name: str = "echo", **kwargs):
    async with Client(app) as client:
        return await client.call_tool(name, arguments, **kwargs)


async def _http_rpc(
    app: FastMCP,
    method: str,
    params: dict,
    headers: dict | None = None,
    *,
    request_id: int | str = 42,
):
    raw = app.http_app(path="/mcp", stateless_http=True, json_response=True)
    wrapped = CapabilityTokenMiddleware(observability.SessionIdHeaderDigest(raw))
    async with raw.router.lifespan_context(raw):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=wrapped), base_url="http://testserver"
        ) as client:
            response = await client.post(
                "/mcp",
                json={
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": params,
                },
                headers={
                    "accept": "application/json, text/event-stream",
                    **(headers or {}),
                },
            )
            assert response.status_code == 200, response.text
            return response


@pytest.fixture
def agents_app(monkeypatch: pytest.MonkeyPatch) -> FastMCP:
    """Include the production Agents tools without contacting an Agents API."""
    from airbyte.mcp import _tool_utils, server

    monkeypatch.setenv("AIRBYTE_MCP_INSIDERS", "1")
    monkeypatch.setenv("AIRBYTE_AGENTS_API_URL", "https://agents.example.com/api/v1")
    monkeypatch.setattr(_tool_utils, "is_agents_api_available", lambda context: True)
    monkeypatch.setattr(server.app, "middleware", list(server.app.middleware))
    monkeypatch.setattr(server.app, "instructions", server.app.instructions)
    _capture(server.app)
    return server.app


def test_list_tools_advertises_optional_intent_without_mutating_tool_parameters(
    agents_app,
    monkeypatch,
):
    """Repeated listings preserve both plain and Agents server-owned schemas."""

    async def check():
        names = ("list_cloud_workspaces", "execute_agent_connector_ro")
        original = {
            name: copy.deepcopy((await agents_app.get_tool(name)).parameters)
            for name in names
        }
        async with Client(agents_app) as client:
            with monkeypatch.context() as disabled:
                for middleware in agents_app.middleware:
                    if isinstance(middleware, observability.IntentCaptureMiddleware):
                        disabled.setattr(middleware, "_environ", {})
                baseline = {
                    tool.name: tool.inputSchema for tool in await client.list_tools()
                }
            first = {tool.name: tool.inputSchema for tool in await client.list_tools()}
            second = {tool.name: tool.inputSchema for tool in await client.list_tools()}
        assert first == second
        for name in names:
            assert "intent" not in first[name].get("required", [])
            assert "telemetry" not in first[name]["properties"]
            assert first[name].get("required", []) == original[name].get("required", [])
            assert (await agents_app.get_tool(name)).parameters == original[name]
            if "intent" in original[name]["properties"]:
                assert first[name] == baseline[name]
            else:
                assert "intent" not in baseline[name]["properties"]
                assert first[name]["properties"]["intent"]["type"] == "string"
                advertised = copy.deepcopy(first[name])
                advertised["properties"].pop("intent")
                assert advertised == baseline[name]

    asyncio.run(check())


def test_call_strips_intent_and_tool_receives_clean_arguments(app):
    result = asyncio.run(
        _call(app, {"value": "argument-SENTINEL", "intent": "Inspect"})
    )
    assert result.data == "argument-SENTINEL"


def test_call_strips_intent_when_capture_disabled(monkeypatch, otel_provider):
    server = FastMCP("capture-disabled")

    @server.tool()
    def echo(value: str) -> str:
        return value

    _capture(server, capture=False)
    monkeypatch.setitem(observability._TOOL_MODULES, "echo", "cloud")

    async def check():
        async with Client(server) as client:
            assert (
                "intent" not in (await client.list_tools())[0].inputSchema["properties"]
            )
            return await client.call_tool(
                "echo", {"value": "clean", "intent": "Still recorded"}
            )

    assert asyncio.run(check()).data == "clean"
    assert (
        _tool_span(otel_provider).attributes["airbyte.mcp.intent"] == "Still recorded"
    )


@pytest.mark.parametrize("capture", [False, True])
@pytest.mark.parametrize(
    "arguments",
    [
        {"intent": "Inspect"},
        {"telemetry": {"intent": "Inspect"}},
        {},
    ],
)
def test_export_disabled_keeps_advertisement_and_cached_calls(
    monkeypatch, otel_provider, uninitialized_provider, capture, arguments
):
    server = FastMCP("export-disabled", instructions="original")

    @server.tool()
    def echo(value: str) -> str:
        return value

    monkeypatch.setattr(
        fastmcp_telemetry, "otel_get_tracer", trace.NoOpTracerProvider().get_tracer
    )
    build = Mock()
    monkeypatch.setattr(observability, "_build_provider", build)
    observability.install(
        server, environ={"AIRBYTE_MCP_INTENT_CAPTURE": str(int(capture))}
    )
    monkeypatch.setitem(observability._TOOL_MODULES, "echo", "cloud")

    async def check():
        async with Client(server) as client:
            properties = (await client.list_tools())[0].inputSchema["properties"]
            assert ("intent" in properties) == capture
            assert "telemetry" not in properties
            return await client.call_tool("echo", {"value": "ok", **arguments})

    assert asyncio.run(check()).data == "ok"
    assert (
        observability.INTENT_INSTRUCTIONS_SENTENCE.strip() in server.instructions
    ) == capture
    build.assert_not_called()
    uninitialized_provider.assert_not_called()
    assert not _spans(otel_provider)


@pytest.mark.parametrize("capture", [False, True])
@pytest.mark.parametrize(
    "arguments,expected",
    [
        ({"telemetry": {"intent": "  Legacy intent  "}}, "Legacy intent"),
        ({"telemetry": {"intent": 123}}, None),
        ({"telemetry": "argument-SENTINEL"}, None),
        (
            {"telemetry": {"intent": "argument-SENTINEL"}, "intent": "Top level"},
            "Top level",
        ),
        ({"telemetry": {"intent": "argument-SENTINEL"}, "intent": None}, None),
        ({"telemetry": {"intent": "argument-SENTINEL"}, "intent": "  "}, None),
    ],
)
def test_legacy_intent_is_receive_only_with_top_level_precedence(
    monkeypatch, otel_provider, capture, arguments, expected
):
    server = FastMCP("legacy")

    @server.tool()
    def echo(value: str) -> str:
        return value

    _capture(server, capture=capture)
    monkeypatch.setitem(observability._TOOL_MODULES, "echo", "cloud")
    original = copy.deepcopy(arguments)
    assert asyncio.run(_call(server, {"value": "ok", **arguments})).data == "ok"
    assert arguments == original
    attrs = _tool_span(otel_provider).attributes
    assert attrs.get("airbyte.mcp.intent") == expected
    assert attrs["airbyte.mcp.intent_present"] == bool(expected)
    assert "SENTINEL" not in _export_text(otel_provider)


@pytest.mark.parametrize("tracing", [False, True])
@pytest.mark.parametrize("capture", [False, True])
@pytest.mark.parametrize(
    "intent", [None, "  Inspect state  ", "  " + "x" * 5000 + "  "]
)
def test_agents_intent_reaches_api_unchanged_with_bounded_trace_copy(
    agents_app, monkeypatch, otel_provider, tracing, capture, intent
):
    credentials = _AirbyteCredentials.from_auth(
        bearer_token="bearer-SENTINEL", env_vars=False
    )
    connector = AgentConnector("connector-SENTINEL", credentials=credentials)
    monkeypatch.setattr(agents_mcp, "_get_agent_connector", lambda **kwargs: connector)
    execute = Mock(return_value={"status": "success", "result": ["result-SENTINEL"]})
    monkeypatch.setattr(agents_api, "execute_agent_connector_action", execute)
    for middleware in agents_app.middleware:
        if isinstance(middleware, observability.IntentCaptureMiddleware):
            monkeypatch.setattr(
                middleware,
                "_environ",
                {"AIRBYTE_MCP_INTENT_CAPTURE": str(int(capture))},
            )
    if not tracing:
        monkeypatch.setattr(
            fastmcp_telemetry, "otel_get_tracer", trace.NoOpTracerProvider().get_tracer
        )
    result = asyncio.run(
        _call(
            agents_app,
            {
                "connector_id": "connector-SENTINEL",
                "entity_type": "issues",
                "action": "list",
                **({"intent": intent} if intent is not None else {}),
            },
            name="execute_agent_connector_ro",
        )
    )
    assert not result.is_error
    execute.assert_called_once()
    body = execute.call_args.kwargs["request_body"]
    if intent is None:
        assert "intent" not in body
    else:
        assert body["intent"] == intent
    if tracing:
        attrs = _tool_span(otel_provider).attributes
        if intent is None:
            assert "airbyte.mcp.intent" not in attrs
            assert attrs["airbyte.mcp.intent_present"] is False
        else:
            expected = intent.strip()
            if len(expected) > 4096:
                expected = expected[: 4096 - len("...[truncated]")] + "...[truncated]"
            assert attrs["airbyte.mcp.intent"] == expected
            assert attrs["airbyte.mcp.intent_present"] is True
        assert "SENTINEL" not in _export_text(otel_provider)
    else:
        assert not _spans(otel_provider)


@pytest.mark.parametrize("intent", [123, {"secret": "argument-SENTINEL"}, []])
def test_invalid_declared_intent_still_fails_validation(
    agents_app, monkeypatch, otel_provider, intent
):
    execute = Mock()
    monkeypatch.setattr(agents_api, "execute_agent_connector_action", execute)
    result = asyncio.run(
        _call(
            agents_app,
            {
                "connector_id": "connector-SENTINEL",
                "entity_type": "issues",
                "action": "list",
                "intent": intent,
            },
            name="execute_agent_connector_ro",
            raise_on_error=False,
        )
    )
    assert result.is_error
    execute.assert_not_called()
    attrs = _tool_span(otel_provider).attributes
    assert "airbyte.mcp.intent" not in attrs
    assert attrs["airbyte.mcp.intent_present"] is False
    assert "SENTINEL" not in _export_text(otel_provider)


def test_required_non_string_intent_keeps_schema_and_validation(
    monkeypatch, otel_provider
):
    server = FastMCP("declared-intent")

    @server.tool()
    def real(intent: int) -> int:
        return intent

    _capture(server)
    monkeypatch.setitem(observability._TOOL_MODULES, "real", "cloud")

    async def check():
        original = copy.deepcopy((await server.get_tool("real")).parameters)
        async with Client(server) as client:
            assert (await client.list_tools())[0].inputSchema == original
            assert "intent" in original["required"]
            assert original["properties"]["intent"]["type"] == "integer"
            return await client.call_tool("real", {"intent": 123})

    assert asyncio.run(check()).data == 123
    assert "airbyte.mcp.intent" not in _tool_span(otel_provider).attributes


def test_strip_preserves_real_string_telemetry_parameter(monkeypatch, otel_provider):
    server = FastMCP("real-telemetry")

    @server.tool()
    def real(telemetry: str) -> str:
        return telemetry

    _capture(server)
    monkeypatch.setitem(observability._TOOL_MODULES, "real", "cloud")

    async def check():
        async with Client(server) as client:
            assert (await client.list_tools())[0].inputSchema["properties"][
                "telemetry"
            ]["type"] == "string"
            return await client.call_tool("real", {"telemetry": "argument-SENTINEL"})

    assert asyncio.run(check()).data == "argument-SENTINEL"
    assert "airbyte.mcp.intent" not in _tool_span(otel_provider).attributes
    assert "SENTINEL" not in _export_text(otel_provider)


@pytest.mark.parametrize("arguments", [{}, {"intent": "Inspect"}])
def test_strip_preserves_real_telemetry_parameter(
    monkeypatch, otel_provider, arguments
):
    server = FastMCP("real-telemetry")

    @server.tool()
    def real(telemetry: dict[str, str]) -> str:
        return telemetry["intent"]

    _capture(server)
    monkeypatch.setitem(observability._TOOL_MODULES, "real", "cloud")

    async def check():
        async with Client(server) as client:
            assert (await client.list_tools())[0].inputSchema["properties"][
                "telemetry"
            ]["type"] == "object"
            return await client.call_tool(
                "real",
                {
                    "telemetry": {"intent": "argument-SENTINEL"},
                    **arguments,
                },
            )

    assert asyncio.run(check()).data == "argument-SENTINEL"
    assert _tool_span(otel_provider).attributes.get(
        "airbyte.mcp.intent"
    ) == arguments.get("intent")
    assert "SENTINEL" not in _export_text(otel_provider)


def test_tool_span_carries_intent_and_gen_ai_attributes_in_memory(app, otel_provider):
    asyncio.run(
        _call(
            app,
            {
                "value": "argument-SENTINEL",
                "intent": "  Inspect state  ",
            },
        )
    )
    span = _tool_span(otel_provider)
    assert span.name == "tools/call echo"
    assert span.attributes["gen_ai.operation.name"] == "execute_tool"
    assert span.attributes["gen_ai.tool.name"] == "echo"
    assert span.attributes["gen_ai.tool.call.id"]
    assert span.attributes["airbyte.mcp.intent"] == "Inspect state"
    assert span.attributes["airbyte.mcp.intent_present"] is True
    assert span.attributes["airbyte.mcp.tool_module"] == "cloud"
    assert "SENTINEL" not in _export_text(otel_provider)


def test_tool_span_carries_intent_over_http(app, otel_provider):
    response = asyncio.run(
        _http_rpc(
            app,
            "tools/call",
            {
                "name": "echo",
                "arguments": {
                    "value": "result-SENTINEL",
                    "intent": "Inspect HTTP",
                },
            },
        )
    )
    assert not response.json()["result"].get("isError")
    span = _tool_span(otel_provider)
    assert span.attributes["airbyte.mcp.intent"] == "Inspect HTTP"
    assert span.attributes["gen_ai.tool.call.id"] == hashlib.sha256(b"42").hexdigest()
    assert span.parent is None
    assert "SENTINEL" not in _export_text(otel_provider)


def test_tool_span_carries_intent_when_client_sends_meta_traceparent_and_is_root(
    app, otel_provider
):
    parent = "00-12345678901234567890123456789012-1234567890123456-01"
    response = asyncio.run(
        _http_rpc(
            app,
            "tools/call",
            {
                "name": "echo",
                "arguments": {"value": "ok", "intent": "Own trace"},
                "_meta": {
                    "traceparent": parent,
                    "tracestate": "vendor=meta-SENTINEL",
                    "progressToken": 17,
                },
            },
            {"traceparent": parent},
        )
    )
    assert not response.json()["result"].get("isError")
    span = _tool_span(otel_provider)
    assert span.parent is None
    assert span.context.trace_id != int("12345678901234567890123456789012", 16)
    assert span.attributes["airbyte.mcp.intent"] == "Own trace"
    assert "SENTINEL" not in _export_text(otel_provider)


@pytest.mark.parametrize("error", [ValueError, NotFoundError])
def test_error_span_has_error_type_and_no_exception_text(
    app, monkeypatch, otel_provider, error
):
    @app.tool()
    def fail() -> str:
        raise error("error-SENTINEL")

    monkeypatch.setitem(observability._TOOL_MODULES, "fail", "cloud")
    result = asyncio.run(_call(app, {}, name="fail", raise_on_error=False))
    assert result.is_error
    span = _tool_span(otel_provider)
    assert span.status.status_code == StatusCode.ERROR
    assert not span.status.description
    assert span.attributes["airbyte.mcp.error_type"] == error.__name__
    assert span.events
    assert all(set(event.attributes) == {"exception.type"} for event in span.events)
    assert "SENTINEL" not in _export_text(otel_provider)


class _Adapter(requests.adapters.BaseAdapter):
    """Exercise requests instrumentation without opening sockets."""

    def send(self, request, **kwargs):
        if "/sources" in request.url:
            raise requests.ConnectionError("request-error-SENTINEL")
        response = requests.Response()
        response.status_code = 200
        response._content = b"result-SENTINEL"
        response.url = request.url
        return response

    def close(self):
        pass


def test_requests_child_span_has_no_query_string_and_no_error_text(
    app, monkeypatch, otel_provider
):
    RequestsInstrumentor().instrument(excluded_urls="api.segment.io")
    session = requests.Session()
    session.mount("https://", _Adapter())

    @app.tool()
    def request_probe(secret: str) -> str:
        session.get("https://api.airbyte.com/v1/connections?token=query-SENTINEL")
        try:
            raise ValidationError("validation-SENTINEL " + secret)
        except ValidationError:
            session.get("https://api.airbyte.com/v1/sources?token=query-SENTINEL")
        return "unreachable"

    monkeypatch.setitem(observability._TOOL_MODULES, "request_probe", "cloud")
    result = asyncio.run(
        _call(
            app,
            {"secret": "argument-SENTINEL"},
            name="request_probe",
            raise_on_error=False,
        )
    )
    assert result.is_error
    root = _tool_span(otel_provider)
    children = [
        span
        for span in _spans(otel_provider)
        if span.parent and span.parent.span_id == root.context.span_id
    ]
    assert len(children) == 2
    assert {span.attributes["http.url"] for span in children} == {
        "https://api.airbyte.com/v1/connections",
        "https://api.airbyte.com/v1/sources",
    }
    failed = next(
        span for span in children if span.status.status_code == StatusCode.ERROR
    )
    assert not failed.status.description
    assert failed.events
    assert all(set(event.attributes) == {"exception.type"} for event in failed.events)
    assert "SENTINEL" not in _export_text(otel_provider)


def test_exporter_drops_enduser_and_user_agent(otel_provider):
    with trace.get_tracer(__name__).start_as_current_span(
        "GET",
        attributes={
            "enduser.id": "identity-SENTINEL",
            "enduser.scope": "scope-SENTINEL",
            "user_agent.original": "useragent-SENTINEL",
            "url.query": "query-SENTINEL",
            "url.full": "https://user-SENTINEL:password-SENTINEL@api.airbyte.com/v1/connections?q=SENTINEL#SENTINEL",
        },
    ):
        pass
    (span,) = _spans(otel_provider)
    assert span.attributes == {"url.full": "https://api.airbyte.com/v1/connections"}
    assert "SENTINEL" not in _export_text(otel_provider)


def test_exporter_drops_span_it_cannot_rebuild(monkeypatch, otel_provider):
    monkeypatch.setattr(
        observability, "ReadableSpan", Mock(side_effect=ValueError("rebuild-SENTINEL"))
    )
    with trace.get_tracer(__name__).start_as_current_span("GET"):
        pass
    assert not _spans(otel_provider)


def test_exporter_drops_unregistered_tool_name(app, otel_provider):
    result = asyncio.run(
        _call(app, {}, name="unknown_tool_name_SENTINEL", raise_on_error=False)
    )
    assert result.is_error
    assert "unknown_tool_name_SENTINEL" in str(result)
    assert not _spans(otel_provider)


def test_unknown_tool_name_does_not_escape_in_export(app, otel_provider):
    """Carry the draft's unknown-name regression through the actual exporter."""
    test_exporter_drops_unregistered_tool_name(app, otel_provider)
    asyncio.run(_call(app, {"value": "ok"}))
    assert _tool_span(otel_provider).attributes["gen_ai.tool.name"] == "echo"
    assert "SENTINEL" not in _export_text(otel_provider)


def test_session_digest_replaces_header_and_keeps_extension_gated_tools(
    agents_app, monkeypatch, otel_provider
):
    from airbyte.mcp.interactive import _registry_ui

    monkeypatch.setattr(
        _registry_ui, "_list_public_registry_connectors", lambda **kwargs: []
    )

    async def check():
        initialized = await _http_rpc(
            agents_app,
            "initialize",
            {
                "protocolVersion": "2025-11-25",
                "capabilities": {"extensions": {"io.modelcontextprotocol/ui": {}}},
                "clientInfo": {"name": "otel-tests", "version": "1"},
            },
        )
        token = initialized.headers["mcp-session-id"]
        listing = await _http_rpc(
            agents_app, "tools/list", {}, {"mcp-session-id": token}
        )
        names = {tool["name"] for tool in listing.json()["result"]["tools"]}
        assert {
            "show_connectors_list",
            "show_workspace_sync_status",
            "show_connection_sync_history",
        } <= names

        response = await _http_rpc(
            agents_app,
            "tools/call",
            {"name": "show_connectors_list", "arguments": {}},
            {"mcp-session-id": token},
        )
        assert not response.json()["result"].get("isError")
        return token

    token = asyncio.run(check())
    span = _tool_span(otel_provider)
    digest = hashlib.sha256(token.encode()).hexdigest()
    assert span.attributes["mcp.session.id"] == digest
    assert span.attributes["gen_ai.conversation.id"] == digest
    assert token not in _export_text(otel_provider)


def test_segment_urls_excluded(otel_provider):
    RequestsInstrumentor().instrument(excluded_urls="api.segment.io")
    session = requests.Session()
    session.mount("https://", _Adapter())
    session.post("https://api.segment.io/v1/batch")
    session.post("https://api.segment.io/v1/track")
    session.get("https://api.airbyte.com/v1/connections")
    spans = _spans(otel_provider)
    assert len(spans) == 1
    assert spans[0].attributes["http.url"] == "https://api.airbyte.com/v1/connections"


def test_install_is_noop_without_endpoint_and_idempotent(
    monkeypatch, uninitialized_provider
):
    exporter = Mock()
    monkeypatch.setattr(
        "opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter",
        exporter,
    )
    server = FastMCP("disabled")
    before = list(server.middleware)
    observability.install(server, environ={})
    observability.install(
        server,
        environ={
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://example.invalid/v1/traces"
        },
    )
    exporter.assert_not_called()
    assert server.middleware[:-1] == before
    assert isinstance(server.middleware[-1], observability.IntentCaptureMiddleware)


def test_install_leaves_provider_unset_when_build_fails(
    monkeypatch, caplog, uninitialized_provider
):
    monkeypatch.setattr(
        "opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter", Mock()
    )
    monkeypatch.setattr(
        observability, "_build_provider", Mock(side_effect=ValueError("build failed"))
    )
    setter = uninitialized_provider
    # PyAirbyte's file logger stops `airbyte.*` propagation once any test creates it.
    monkeypatch.setattr(logging.getLogger("airbyte"), "propagate", True)
    server = FastMCP("failed")
    observability.install(
        server,
        environ={
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://example.invalid/v1/traces"
        },
    )
    setter.assert_not_called()
    assert any(record.levelname == "ERROR" for record in caplog.records)
    assert any(
        isinstance(item, observability.IntentCaptureMiddleware)
        for item in server.middleware
    )


@pytest.mark.parametrize(
    "enabled,capture", [(False, False), (False, True), (True, False), (True, True)]
)
def test_install_respects_explicit_environ(
    monkeypatch, enabled, capture, uninitialized_provider
):
    """The mapping controls our switches; the SDK reads ambient configuration."""
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://ambient.invalid/v1/traces"
    )
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "x-source=ambient")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "ambient-service")
    monkeypatch.setenv("AIRBYTE_MCP_INTENT_CAPTURE", str(int(not capture)))
    factory = Mock(wraps=OTLPSpanExporter)
    build = Mock(wraps=observability._build_provider)
    monkeypatch.setattr(
        "opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter",
        factory,
    )
    monkeypatch.setattr(observability, "_build_provider", build)
    setter = uninitialized_provider
    monkeypatch.setattr(
        observability,
        "RequestsInstrumentor",
        Mock(return_value=Mock(is_instrumented_by_opentelemetry=False)),
    )
    server = FastMCP("explicit", instructions="original")
    environment = (
        {
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://mapping.invalid/v1/traces",
            "OTEL_EXPORTER_OTLP_TRACES_HEADERS": "x-source=mapping",
            "OTEL_SERVICE_NAME": "mapping-service",
            "AIRBYTE_MCP_INTENT_CAPTURE": str(int(capture)),
        }
        if enabled
        else {"AIRBYTE_MCP_INTENT_CAPTURE": str(int(capture))}
    )
    observability.install(server, environ=environment)
    if not enabled:
        factory.assert_not_called()
        setter.assert_not_called()
        assert (
            observability.INTENT_INSTRUCTIONS_SENTENCE.strip() in server.instructions
        ) == capture
        return
    provider = setter.call_args.args[0]
    try:
        factory.assert_called_once_with()
        exporter = build.call_args.args[0]
        # The exporter has no public configuration getters; never export to it.
        assert exporter._endpoint == "https://ambient.invalid/v1/traces"
        assert exporter._headers["x-source"] == "ambient"
        assert provider.resource.attributes["service.name"] == "ambient-service"
        assert (
            observability.INTENT_INSTRUCTIONS_SENTENCE.strip() in server.instructions
        ) == capture
    finally:
        provider.shutdown()


@pytest.mark.parametrize("vendor", ["", "other", "datadog"])
@pytest.mark.parametrize("valid_ids", [False, True])
def test_datadog_metadata_attribute_only_with_vendor_opt_in(
    agents_app, monkeypatch, otel_provider, vendor, valid_ids
):
    """UUID config headers are validated before ordinary or vendor export."""
    from airbyte.mcp.interactive import _registry_ui

    monkeypatch.setattr(
        _registry_ui, "_list_public_registry_connectors", lambda **kwargs: []
    )
    monkeypatch.setenv("AIRBYTE_MCP_OTEL_VENDOR", vendor)
    workspace = (
        "12345678-1234-1234-1234-123456789ABC" if valid_ids else "workspace-SENTINEL"
    )
    organization = (
        "87654321-4321-4321-4321-ABCDEF123456" if valid_ids else "organization-SENTINEL"
    )
    result = asyncio.run(
        _http_rpc(
            agents_app,
            "tools/call",
            {
                "name": "show_connectors_list",
                "arguments": {"intent": "Inspect state"},
            },
            {
                "x-mcp-extensions": "io.modelcontextprotocol/ui",
                "x-airbyte-workspace-id": workspace,
                "x-airbyte-organization-id": organization,
            },
        )
    )
    assert not result.json()["result"].get("isError")
    attributes = _tool_span(otel_provider).attributes
    metadata = (
        json.loads(attributes["_dd.ml_obs.metadata"]) if vendor == "datadog" else {}
    )
    for field, value in (
        ("workspace_id", workspace),
        ("organization_id", organization),
    ):
        if valid_ids:
            assert attributes[f"airbyte.mcp.{field}"] == value.lower()
            if vendor == "datadog":
                assert metadata[field] == value.lower()
        else:
            assert f"airbyte.mcp.{field}" not in attributes
            assert field not in metadata
    if vendor == "datadog":
        assert metadata["intent"] == "Inspect state"
        assert metadata["intent_present"] is True
        assert (
            metadata["tool_module"]
            == observability._TOOL_MODULES["show_connectors_list"]
        )
    else:
        assert "_dd.ml_obs.metadata" not in attributes
    assert "SENTINEL" not in _export_text(otel_provider)


@pytest.mark.parametrize(
    "endpoint,capture", [(False, False), (False, True), (True, False), (True, True)]
)
def test_instructions_sentence_requires_capture_flag(
    monkeypatch, otel_provider, endpoint, capture, uninitialized_provider
):
    provider, exporter = otel_provider
    monkeypatch.setattr(
        "opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter",
        Mock(return_value=exporter),
    )
    monkeypatch.setattr(observability, "_build_provider", Mock(return_value=provider))
    instrument = Mock()
    monkeypatch.setattr(
        observability,
        "RequestsInstrumentor",
        Mock(
            return_value=Mock(
                instrument=instrument, is_instrumented_by_opentelemetry=False
            )
        ),
    )
    environ = {"AIRBYTE_MCP_INTENT_CAPTURE": str(int(capture))}
    if endpoint:
        environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = (
            "https://example.invalid/v1/traces"
        )
    server = FastMCP("instructions", instructions="original")
    observability.install(server, environ=environ)
    observability.install(server, environ=environ)
    assert server.instructions.count(
        observability.INTENT_INSTRUCTIONS_SENTENCE.strip()
    ) == int(capture)
    assert server.instructions.startswith("original")
    if endpoint:
        instrument.assert_called_once_with(excluded_urls="api.segment.io")
    else:
        instrument.assert_not_called()


@pytest.mark.parametrize("version", [None, "operator-version"])
def test_service_version_set_only_when_absent(monkeypatch, version):
    monkeypatch.setenv("OTEL_SERVICE_NAME", "test-service")
    if version:
        monkeypatch.setenv("OTEL_RESOURCE_ATTRIBUTES", "service.version=" + version)
    provider = observability._build_provider(InMemorySpanExporter())
    try:
        assert provider.resource.attributes["service.version"] == (
            version or get_version()
        )
        assert provider.resource.attributes["service.name"] == "test-service"
    finally:
        provider.shutdown()


def test_wrap_http_app_places_session_digest_innermost(
    monkeypatch, uninitialized_provider
):
    """Inspect the wrapper passed by the actual hosted entrypoint."""
    from airbyte.mcp import http_main

    captured = {}
    monkeypatch.setattr(http_main.app, "middleware", list(http_main.app.middleware))
    monkeypatch.setattr(
        http_main, "run_mcp_http_server", lambda app, **kwargs: captured.update(kwargs)
    )
    monkeypatch.setattr(http_main, "set_hosted_mcp_mode", lambda: None)
    http_main.main()
    sentinel = object()
    wrapped = captured["wrapper"](sentinel)
    chain = []
    while wrapped is not sentinel:
        chain.append(type(wrapped).__name__)
        wrapped = getattr(wrapped, "_app", getattr(wrapped, "app", None))
        assert wrapped is not None, chain
    assert chain[-1] == "SessionIdHeaderDigest"
    assert chain.count("SessionIdHeaderDigest") == 1
    assert captured["stateless_http"] is True


@pytest.mark.parametrize(
    "connection_id",
    ["connection-secret-SENTINEL", "12345678-1234-1234-1234-123456789abc"],
)
def test_real_cloud_connection_argument_cannot_escape_through_url(
    agents_app, monkeypatch, otel_provider, connection_id
):
    """The real Cloud SDK must not export arbitrary connection IDs as URL paths."""
    from airbyte.cloud import CloudWorkspace
    from airbyte.mcp import cloud

    RequestsInstrumentor().instrument(excluded_urls="api.segment.io")
    workspace = CloudWorkspace(
        workspace_id="12345678-1234-1234-1234-123456789abc",
        bearer_token="cloud-bearer-SENTINEL",
    )
    monkeypatch.setattr(
        cloud, "_get_cloud_workspace", lambda *args, **kwargs: workspace
    )
    monkeypatch.setattr(requests.Session, "get_adapter", lambda *args: _Adapter())
    asyncio.run(
        _http_rpc(
            agents_app,
            "tools/call",
            {
                "name": "describe_cloud_connection",
                "arguments": {"connection_id": connection_id},
            },
        )
    )
    children = [span for span in _spans(otel_provider) if span.kind == SpanKind.CLIENT]
    assert children
    assert "SENTINEL" not in _export_text(otel_provider)
    assert children[0].attributes["http.url"] == (
        observability.REDACTED_PLACEHOLDER
        if "SENTINEL" in connection_id
        else "https://api.airbyte.com/v1/connections/" + connection_id
    )


@pytest.mark.parametrize("intent", [None, 123, {}, [], "  ", "x" * 5000])
def test_intent_is_optional_and_bounded(app, otel_provider, intent):
    result = asyncio.run(_call(app, {"value": "ok", "intent": intent}))
    assert result.data == "ok"
    attrs = _tool_span(otel_provider).attributes
    if isinstance(intent, str) and intent.strip():
        assert len(attrs["airbyte.mcp.intent"]) == 4096
        assert attrs["airbyte.mcp.intent"].endswith("[truncated]")
        assert attrs["airbyte.mcp.intent_present"] is True
    else:
        assert "airbyte.mcp.intent" not in attrs
        assert attrs["airbyte.mcp.intent_present"] is False


@pytest.mark.parametrize(
    "method,params",
    [
        ("resources/read", {"uri": "secret://resource-SENTINEL?token=query-SENTINEL"}),
        ("prompts/get", {"name": "prompt-SENTINEL"}),
        (
            "prompts/get",
            {"name": "test-my-tools", "arguments": {"scope": "scope-SENTINEL"}},
        ),
    ],
)
def test_non_tool_requests_do_not_export_caller_text_or_trace_context(
    agents_app, monkeypatch, otel_provider, method, params
):
    """Real FastMCP resources/prompts must never escape the tool-only boundary."""
    from airbyte.mcp.interactive import _registry_ui

    monkeypatch.setattr(
        _registry_ui, "_list_public_registry_connectors", lambda **kwargs: []
    )
    request = {
        **params,
        "_meta": {
            "traceparent": "00-12345678901234567890123456789012-1234567890123456-01",
            "tracestate": "vendor=state-SENTINEL",
        },
    }
    result = asyncio.run(_http_rpc(agents_app, method, request))
    assert "result" in result.json() or "error" in result.json()
    assert not _spans(otel_provider)
    result = asyncio.run(
        _http_rpc(
            agents_app,
            "tools/call",
            {
                "name": "show_connectors_list",
                "arguments": {"intent": "Inspect tools"},
            },
            {"x-mcp-extensions": "io.modelcontextprotocol/ui"},
        )
    )
    assert not result.json()["result"].get("isError")
    span = _tool_span(otel_provider)
    assert span.attributes["gen_ai.tool.name"] == "show_connectors_list"
    assert span.parent is None
    assert span.context.trace_id != int("12345678901234567890123456789012", 16)
    assert "SENTINEL" not in _export_text(otel_provider)


@pytest.mark.parametrize("mode", ["http", "http/dup"])
def test_requests_stable_http_conventions_do_not_export_custom_origins(
    app, monkeypatch, otel_provider, mode
):
    """Stable HTTP host attributes cannot bypass the URL origin redaction."""
    import opentelemetry.instrumentation.requests as requests_otel

    # The pinned instrumentor caches process opt-ins; isolate the mode getter
    # instead of resetting its singleton or replacing the real requests wrapper.
    monkeypatch.setattr(
        requests_otel._OpenTelemetrySemanticConventionStability,
        "_get_opentelemetry_stability_opt_in_mode",
        lambda signal: requests_otel._StabilityMode(mode),
    )
    raw_hosts = []
    RequestsInstrumentor().instrument(
        excluded_urls="api.segment.io",
        request_hook=lambda span, request: raw_hosts.append(
            span.attributes.get("server.address")
        ),
    )
    session = requests.Session()
    session.mount("https://", _Adapter())

    @app.tool()
    def origin_probe() -> str:
        session.get(
            "https://private-origin-SENTINEL.example/custom?token=query-SENTINEL"
        )
        session.get("https://api.airbyte.com/v1/connections?token=query-SENTINEL")
        return "ok"

    monkeypatch.setitem(observability._TOOL_MODULES, "origin_probe", "cloud")
    assert asyncio.run(_call(app, {}, name="origin_probe")).data == "ok"
    assert raw_hosts == ["private-origin-sentinel.example", "api.airbyte.com"]
    root = _tool_span(otel_provider)
    children = [span for span in _spans(otel_provider) if span.kind == SpanKind.CLIENT]
    assert len(children) == 2
    assert all(span.parent.span_id == root.context.span_id for span in children)
    assert {span.attributes["url.full"] for span in children} == {
        observability.REDACTED_PLACEHOLDER,
        "https://api.airbyte.com/v1/connections",
    }
    for span in children:
        assert span.attributes["http.response.status_code"] == 200
        assert {"http.host", "server.address", "network.peer.address"}.isdisjoint(
            span.attributes
        )
        if mode == "http/dup":
            assert span.attributes["http.url"] == span.attributes["url.full"]
            assert span.attributes["http.status_code"] == 200
    assert "sentinel" not in _export_text(otel_provider).casefold()


@pytest.mark.parametrize("preinitialized", ["global", "requests"])
@pytest.mark.parametrize("endpoint", [False, True])
def test_hosted_startup_refuses_preexisting_exporting_provider(
    preinitialized, endpoint
):
    """A preinstalled provider or instrumentor cannot bypass the privacy boundary."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent("""
            import os
            import socket
            from unittest.mock import Mock, patch
            # Same loopback-only guard as the ``isolated_otel`` fixture; duplicated
            # because importing the test module would import airbyte.mcp first.
            def loopback_only(original, address_index):
                def guarded(*args, **kwargs):
                    address = kwargs.get("address", args[address_index])
                    host = address[0] if isinstance(address, tuple) else address
                    if not isinstance(address, tuple) or (
                        isinstance(host, str)
                        and (host in ("::1", "localhost") or host.startswith("127."))
                    ):
                        return original(*args, **kwargs)
                    raise AssertionError("unexpected network connection")
                return guarded
            socket.socket.connect = loopback_only(socket.socket.connect, 1)
            socket.socket.connect_ex = loopback_only(socket.socket.connect_ex, 1)
            socket.create_connection = loopback_only(socket.create_connection, 0)

            from opentelemetry import trace
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import SimpleSpanProcessor
            from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
            from opentelemetry.instrumentation.requests import RequestsInstrumentor
            foreign_exporter = InMemorySpanExporter()
            owned_exporter = InMemorySpanExporter()
            foreign_provider = TracerProvider()
            foreign_provider.add_span_processor(SimpleSpanProcessor(foreign_exporter))
            if os.environ['TEST_OTEL_PREINITIALIZE'] == 'global':
                trace.set_tracer_provider(foreign_provider)
            else:
                RequestsInstrumentor().instrument(tracer_provider=foreign_provider)
            from airbyte.mcp import http_main

            with patch('opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter', return_value=owned_exporter) as create_exporter:
                with patch.object(http_main, 'run_mcp_http_server') as run_server:
                    with patch.object(RequestsInstrumentor, 'instrument') as instrument:
                        for attempt in range(2):
                            try:
                                http_main.main()
                            except RuntimeError:
                                pass
                            else:
                                raise AssertionError('Hosted startup accepted an unprotected provider')
                        create_exporter.assert_not_called()
                        run_server.assert_not_called()
                        instrument.assert_not_called()
            if os.environ['TEST_OTEL_PREINITIALIZE'] == 'global':
                assert trace.get_tracer_provider() is foreign_provider
            else:
                assert isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider)
                RequestsInstrumentor().uninstrument()
            foreign_provider.force_flush()
            assert not foreign_exporter.get_finished_spans()
            assert not owned_exporter.get_finished_spans()
            foreign_provider.shutdown()
            owned_exporter.shutdown()
        """),
        ],
        env={
            **{
                key: value
                for key, value in os.environ.items()
                if not key.startswith(("OTEL_", "AIRBYTE_", "MCP_"))
            },
            **(
                {
                    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://example.invalid/v1/traces"
                }
                if endpoint
                else {}
            ),
            "DO_NOT_TRACK": "1",
            "PYDANTIC_DISABLE_PLUGINS": "logfire-plugin",
            "TEST_OTEL_PREINITIALIZE": preinitialized,
        },
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_install_refuses_silent_global_provider_rejection(monkeypatch):
    """A provider race cannot turn an ignored SDK setter into successful startup."""
    monkeypatch.setattr(
        trace, "get_tracer_provider", lambda: trace.ProxyTracerProvider()
    )
    monkeypatch.setattr(trace, "set_tracer_provider", Mock())
    monkeypatch.setattr(
        "opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter", Mock()
    )
    provider = Mock()
    monkeypatch.setattr(observability, "_build_provider", Mock(return_value=provider))
    instrument = Mock(return_value=Mock(is_instrumented_by_opentelemetry=False))
    monkeypatch.setattr(observability, "RequestsInstrumentor", instrument)
    with pytest.raises(RuntimeError):
        observability.install(
            FastMCP("rejected"),
            environ={
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://example.invalid/v1/traces",
            },
        )
    provider.shutdown.assert_called_once_with()
    instrument.return_value.instrument.assert_not_called()


def test_exporter_drops_captured_http_header_attributes(
    app, monkeypatch, otel_provider
):
    """Header capture opt-ins must never export Authorization or cookie values.

    The pinned requests instrumentor (0.60b1) has no client header capture; the
    hook writes the attribute names ``opentelemetry-util-http`` would produce.
    """
    RequestsInstrumentor().instrument(
        excluded_urls="api.segment.io",
        request_hook=lambda span, request: span.set_attributes({
            "http.request.header.authorization": request.headers["Authorization"],
            "http.request.header.cookie": "cookie-SENTINEL",
        }),
        response_hook=lambda span, request, response: span.set_attribute(
            "http.response.header.set_cookie", "set-cookie-SENTINEL"
        ),
    )
    session = requests.Session()
    session.mount("https://", _Adapter())

    @app.tool()
    def header_probe() -> str:
        session.get(
            "https://api.airbyte.com/v1/connections",
            headers={"Authorization": "Bearer authorization-SENTINEL"},
        )
        return "ok"

    monkeypatch.setitem(observability._TOOL_MODULES, "header_probe", "cloud")
    assert asyncio.run(_call(app, {}, name="header_probe")).data == "ok"
    (child,) = [span for span in _spans(otel_provider) if span.kind == SpanKind.CLIENT]
    assert child.attributes["http.url"] == "https://api.airbyte.com/v1/connections"
    assert not any(
        key.startswith("http.re") and "header" in key for key in child.attributes
    )
    assert "SENTINEL" not in _export_text(otel_provider)


@pytest.mark.parametrize(
    "request_id",
    [
        7,
        10**40,
        "call_abc-1.2_XYZ",
        "customer_123-SENTINEL",
        "user@example-SENTINEL.com",
        "x" * 65,
    ],
)
def test_tool_call_id_is_always_digested(app, otel_provider, request_id):
    """Client-chosen JSON-RPC ids never export verbatim, whatever their shape."""
    response = asyncio.run(
        _http_rpc(
            app,
            "tools/call",
            {"name": "echo", "arguments": {"value": "ok"}},
            request_id=request_id,
        )
    )
    assert not response.json()["result"].get("isError")
    attributes = _tool_span(otel_provider).attributes
    assert attributes["gen_ai.tool.call.id"] == (
        hashlib.sha256(str(request_id).encode()).hexdigest()
    )
    assert str(request_id) not in attributes.values()
    assert "SENTINEL" not in _export_text(otel_provider)


@pytest.mark.parametrize(
    "path,exported",
    [
        ("/jobs/get", "https://cloud.airbyte.com/api/v1/jobs/get"),
        ("/jobs/list_for_workspaces-SENTINEL", observability.REDACTED_PLACEHOLDER),
    ],
)
def test_config_api_jobs_get_route_is_exported_and_unknown_routes_are_not(
    app, monkeypatch, otel_provider, path, exported
):
    RequestsInstrumentor().instrument(excluded_urls="api.segment.io")
    session = requests.Session()
    session.mount("https://", _Adapter())

    @app.tool()
    def config_probe() -> str:
        session.post(observability.CLOUD_CONFIG_API_ROOT + path, json={"id": 7})
        return "ok"

    monkeypatch.setitem(observability._TOOL_MODULES, "config_probe", "cloud")
    assert asyncio.run(_call(app, {}, name="config_probe")).data == "ok"
    (child,) = [span for span in _spans(otel_provider) if span.kind == SpanKind.CLIENT]
    assert child.attributes["http.url"] == exported
    assert "SENTINEL" not in _export_text(otel_provider)
