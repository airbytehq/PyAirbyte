# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Bounded requested actions at the final hosted tracing export boundary."""

from __future__ import annotations

import asyncio
import json
import os
import socket
from collections.abc import Iterator
from unittest.mock import Mock

import fastmcp.telemetry
import pytest
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import Middleware, MiddlewareContext
from mcp.types import CallToolRequestParams
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from airbyte.agents.connectors import AgentAction, AgentReadAction
from airbyte.agents.models import AgentExecuteResult
from airbyte.mcp import _otel as observability
from airbyte.mcp import agents
from tests.unit_tests import test_mcp_otel as otel_tests
from tests.unit_tests.test_mcp_otel import (
    _call,
    _capture,
    _export_text,
    _loopback_only,
    _spans,
    _tool_span,
)


agents_app = otel_tests.agents_app
ACTION = "airbyte.mcp.agent.action"
GENERAL = "execute_agent_connector"
READ_ONLY = "execute_agent_connector_ro"
SENTINEL = "private-payload-must-not-export"
VALID_ACTIONS = [
    (GENERAL, action)
    for action in ("list", "get", "search", "sql_select", "create", "update", "delete")
] + [(READ_ONLY, action) for action in ("list", "get", "search", "sql_select")]


@pytest.fixture(scope="module")
def otel_provider() -> Iterator[tuple[TracerProvider, InMemorySpanExporter]]:
    exporter = InMemorySpanExporter()
    provider = observability._build_provider(exporter)
    with pytest.MonkeyPatch.context() as instrumentation:
        instrumentation.setattr(
            fastmcp.telemetry, "otel_get_tracer", provider.get_tracer
        )
        instrumentation.setattr(trace, "get_tracer", provider.get_tracer)
        yield provider, exporter
    provider.shutdown()


@pytest.fixture(autouse=True)
def isolated_otel(
    monkeypatch: pytest.MonkeyPatch,
    otel_provider: tuple[TracerProvider, InMemorySpanExporter],
) -> Iterator[None]:
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
    provider.force_flush()
    exporter.clear()
    observability._reset_for_tests()


@pytest.fixture
def uninitialized_provider(monkeypatch: pytest.MonkeyPatch) -> Mock:
    current = [trace.ProxyTracerProvider()]
    setter = Mock(side_effect=lambda provider: current.__setitem__(0, provider))
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: current[0])
    monkeypatch.setattr(trace, "set_tracer_provider", setter)
    return setter


@pytest.mark.parametrize(("name", "action"), VALID_ACTIONS)
@pytest.mark.parametrize("capture", [True, False])
@pytest.mark.parametrize("vendor", ["", "datadog"])
def test_requested_action_survives_real_tool_execution(
    monkeypatch, agents_app, otel_provider, name, action, capture, vendor
):
    monkeypatch.setenv("AIRBYTE_MCP_OTEL_VENDOR", vendor)
    agents_app.middleware = []
    _capture(agents_app, capture=capture)
    connector = Mock(
        execute=Mock(return_value=AgentExecuteResult(status="success", result=SENTINEL))
    )
    monkeypatch.setattr(agents, "_get_agent_connector", Mock(return_value=connector))
    asyncio.run(
        _call(
            agents_app,
            {
                "action": action,
                "connector_id": SENTINEL,
                "entity_type": SENTINEL,
                "api_args": {"credential": SENTINEL, "sql": SENTINEL},
                "cursor": SENTINEL,
            },
            name=name,
        )
    )
    attrs = _tool_span(otel_provider).attributes
    assert attrs[ACTION] == action
    if vendor:
        assert json.loads(attrs["_dd.ml_obs.metadata"])["agent.action"] == action
    else:
        assert "_dd.ml_obs.metadata" not in attrs
    assert SENTINEL not in _export_text(otel_provider)


@pytest.mark.parametrize(
    "failure", ["validation", "read_only", "lookup", "exception", "shaped"]
)
def test_valid_request_is_recorded_despite_later_failure(
    monkeypatch, agents_app, otel_provider, failure
):
    connector = Mock(
        execute=Mock(return_value=AgentExecuteResult(status="error", result=SENTINEL))
    )
    lookup = Mock(return_value=connector)
    monkeypatch.setattr(agents, "_get_agent_connector", lookup)
    arguments = {"action": "create", "connector_id": SENTINEL, "entity_type": SENTINEL}
    if failure == "validation":
        arguments.pop("connector_id")
    elif failure == "read_only":
        arguments["read_only"] = True
    elif failure == "lookup":
        lookup.side_effect = ValueError(SENTINEL)
    elif failure == "exception":
        connector.execute.side_effect = RuntimeError(SENTINEL)
    if failure == "shaped":
        asyncio.run(_call(agents_app, arguments, name=GENERAL))
    else:
        with pytest.raises(ToolError):
            asyncio.run(_call(agents_app, arguments, name=GENERAL))
    assert _tool_span(otel_provider).attributes[ACTION] == "create"
    assert SENTINEL not in _export_text(otel_provider)
    if failure in {"validation", "read_only"}:
        lookup.assert_not_called()


@pytest.mark.parametrize("name", [GENERAL, READ_ONLY])
@pytest.mark.parametrize(
    "action",
    [
        None,
        "",
        0,
        -1,
        False,
        [],
        {},
        ["list"],
        " LIST ",
        "LIST",
        "list ",
        "download",
        "api_search",
        SENTINEL,
        SENTINEL * 1000,
    ],
)
def test_invalid_action_is_omitted_on_validation_failure(
    agents_app, otel_provider, name, action
):
    with pytest.raises(ToolError):
        asyncio.run(_call(agents_app, {"action": action}, name=name))
    assert ACTION not in _tool_span(otel_provider).attributes
    assert SENTINEL not in _export_text(otel_provider)


@pytest.mark.parametrize("action", ["create", "update", "delete"])
def test_read_only_tool_rejects_write_action_metadata(
    agents_app, otel_provider, action
):
    with pytest.raises(ToolError):
        asyncio.run(_call(agents_app, {"action": action}, name=READ_ONLY))
    assert ACTION not in _tool_span(otel_provider).attributes


@pytest.mark.parametrize("arguments", [{}, {"api_args": {"action": "list"}}])
def test_missing_and_nested_only_action_is_omitted(
    agents_app, otel_provider, arguments
):
    with pytest.raises(ToolError):
        asyncio.run(_call(agents_app, arguments, name=GENERAL))
    assert ACTION not in _tool_span(otel_provider).attributes


@pytest.mark.parametrize(
    "name", ["list_agent_connectors", "inspect_agent_connector", "other"]
)
def test_unrelated_tools_do_not_infer_action(monkeypatch, otel_provider, name):
    app = FastMCP("unrelated")

    @app.tool(name=name)
    def unrelated(action: str = "list") -> str:
        return action

    _capture(app)
    monkeypatch.setitem(observability._TOOL_MODULES, name, "agents")
    asyncio.run(_call(app, {"action": "list"}, name=name))
    assert ACTION not in _tool_span(otel_provider).attributes


@pytest.mark.parametrize("processor", [SimpleSpanProcessor, BatchSpanProcessor])
@pytest.mark.parametrize("vendor", ["", "datadog"])
@pytest.mark.parametrize(
    ("name", "action", "expected"),
    [
        (GENERAL, "list", "list"),
        (GENERAL, SENTINEL, None),
        (READ_ONLY, "delete", None),
        (GENERAL, None, None),
    ],
)
def test_exporter_rebuilds_injected_vendor_metadata(
    monkeypatch, processor, vendor, name, action, expected
):
    sink = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(
        processor(
            observability.RedactingExporter(
                sink, environ={"AIRBYTE_MCP_OTEL_VENDOR": vendor}
            )
        )
    )
    monkeypatch.setitem(observability._TOOL_MODULES, name, "agents")
    attributes = {
        "_dd.ml_obs.metadata": json.dumps({
            "agent.action": SENTINEL,
            "unapproved": SENTINEL,
        })
    }
    if action is not None:
        attributes[ACTION] = action
    try:
        with provider.get_tracer("test-action-metadata").start_as_current_span(
            f"tools/call {name}", kind=SpanKind.SERVER, attributes=attributes
        ):
            pass
        assert provider.force_flush()
        [span] = sink.get_finished_spans()
        assert span.attributes.get(ACTION) == expected
        if vendor == "datadog" and expected is not None:
            assert json.loads(span.attributes["_dd.ml_obs.metadata"]) == {
                "agent.action": expected
            }
        else:
            assert "_dd.ml_obs.metadata" not in span.attributes
        assert SENTINEL not in span.to_json()
    finally:
        provider.shutdown()


@pytest.mark.parametrize("processor", [SimpleSpanProcessor, BatchSpanProcessor])
@pytest.mark.parametrize("vendor", ["", "datadog"])
def test_exporter_revalidates_action_and_root_span_scope(
    monkeypatch, processor, vendor
):
    sink = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(
        processor(
            observability.RedactingExporter(
                sink, environ={"AIRBYTE_MCP_OTEL_VENDOR": vendor}
            )
        )
    )
    tracer = provider.get_tracer("test-action-boundary")
    for tool in (GENERAL, READ_ONLY, "unrelated"):
        monkeypatch.setitem(observability._TOOL_MODULES, tool, "agents")
    cases = [
        (GENERAL, SpanKind.SERVER, "create", "create"),
        (READ_ONLY, SpanKind.SERVER, "sql_select", "sql_select"),
        (READ_ONLY, SpanKind.SERVER, "delete", None),
        (GENERAL, SpanKind.SERVER, SENTINEL, None),
        (GENERAL, SpanKind.SERVER, ("list",), None),
        (GENERAL, SpanKind.INTERNAL, "list", None),
        ("unrelated", SpanKind.SERVER, "list", None),
    ]
    try:
        for name, kind, value, expected in cases:
            with tracer.start_as_current_span(
                f"tools/call {name}",
                kind=kind,
                attributes={ACTION: value, "safe": "kept"},
            ):
                pass
            assert provider.force_flush()
            span = sink.get_finished_spans()[-1]
            assert span.attributes.get(ACTION) == expected
            assert span.attributes["safe"] == "kept"
            metadata = json.loads(span.attributes.get("_dd.ml_obs.metadata", "{}"))
            assert metadata.get("agent.action") == (expected if vendor else None)
        with tracer.start_as_current_span(
            "HTTP", kind=SpanKind.CLIENT, attributes={ACTION: "get"}
        ):
            with tracer.start_as_current_span(
                f"tools/call {GENERAL}",
                kind=SpanKind.SERVER,
                attributes={ACTION: "list"},
            ):
                pass
        assert provider.force_flush()
        assert all(
            ACTION not in span.attributes for span in sink.get_finished_spans()[-2:]
        )
        assert SENTINEL not in "\n".join(
            span.to_json() for span in sink.get_finished_spans()
        )
    finally:
        provider.shutdown()


def test_concurrent_and_nested_calls_do_not_mix_actions(monkeypatch, otel_provider):
    app = FastMCP("action-lifecycle")
    _capture(app)
    monkeypatch.setitem(observability._TOOL_MODULES, GENERAL, "agents")
    monkeypatch.setitem(observability._TOOL_MODULES, READ_ONLY, "agents")

    @app.tool(name=READ_ONLY)
    async def inner(action: AgentReadAction) -> str:
        return action.value

    @app.tool(name=GENERAL)
    async def outer(action: AgentAction) -> str:
        before = dict(observability._INTENT_ATTRIBUTES.get() or {})
        await app.call_tool(READ_ONLY, {"action": "get"})
        await asyncio.sleep(0)
        assert observability._INTENT_ATTRIBUTES.get() == before
        return action.value

    async def run():
        await asyncio.gather(
            _call(app, {"action": "create"}, name=GENERAL),
            _call(app, {"action": "search"}, name=GENERAL),
        )
        assert observability._INTENT_ATTRIBUTES.get() is None

    asyncio.run(run())
    spans = _spans(otel_provider)
    roots = [span for span in spans if span.parent is None]
    assert sorted(span.attributes[ACTION] for span in roots) == ["create", "search"]
    assert all(
        ACTION not in span.attributes for span in spans if span.parent is not None
    )


@pytest.mark.parametrize("timed_out", [False, True])
def test_cancel_and_timeout_reset_action_context(monkeypatch, otel_provider, timed_out):
    app = FastMCP("cancel-action")
    middleware = observability.IntentCaptureMiddleware(app)
    context = MiddlewareContext(
        message=CallToolRequestParams(name=GENERAL, arguments={"action": "list"}),
        source="client",
        type="request",
        method="tools/call",
    )
    monkeypatch.setitem(observability._TOOL_MODULES, GENERAL, "agents")

    async def execute(context):
        with trace.get_tracer("action-cancel-test").start_as_current_span(
            f"tools/call {GENERAL}", kind=SpanKind.SERVER
        ):
            if timed_out:
                await asyncio.Event().wait()
            raise asyncio.CancelledError

    async def run():
        with pytest.raises(
            asyncio.TimeoutError if timed_out else asyncio.CancelledError
        ):
            if timed_out:
                await asyncio.wait_for(
                    middleware.on_call_tool(context, execute), timeout=0.1
                )
            else:
                await middleware.on_call_tool(context, execute)
        assert observability._INTENT_ATTRIBUTES.get() is None

    asyncio.run(run())
    assert _tool_span(otel_provider).attributes[ACTION] == "list"


def test_denied_before_span_does_not_manufacture_action_span(
    monkeypatch, otel_provider
):
    class Deny(Middleware):
        async def on_call_tool(self, context, call_next):
            raise PermissionError(SENTINEL)

    app = FastMCP("denied")
    _capture(app)
    app.add_middleware(Deny())
    monkeypatch.setitem(observability._TOOL_MODULES, GENERAL, "agents")

    async def run():
        with pytest.raises(PermissionError):
            await app.call_tool(GENERAL, {"action": "get"})
        assert observability._INTENT_ATTRIBUTES.get() is None

    asyncio.run(run())
    assert not _spans(otel_provider)


def test_disabled_hosted_tracing_leaves_middleware_and_call_untouched(
    uninitialized_provider,
):
    app = FastMCP("disabled")
    before = list(app.middleware)
    observability.install(app, environ={})
    assert app.middleware[:-1] == before
    assert isinstance(app.middleware[-1], observability.IntentCaptureMiddleware)
    uninitialized_provider.assert_not_called()


def test_nested_middleware_restores_previous_action():
    app = FastMCP("restore-context")
    middleware = observability.IntentCaptureMiddleware(app)
    context = MiddlewareContext(
        message=CallToolRequestParams(name=GENERAL, arguments={"action": "update"}),
        source="client",
        type="request",
        method="tools/call",
    )
    previous = {ACTION: "get"}
    token = observability._INTENT_ATTRIBUTES.set(previous)

    async def fail(context):
        assert observability._INTENT_ATTRIBUTES.get()[ACTION] == "update"
        raise RuntimeError(SENTINEL)

    try:
        with pytest.raises(RuntimeError):
            asyncio.run(middleware.on_call_tool(context, fail))
        assert observability._INTENT_ATTRIBUTES.get() is previous
    finally:
        observability._INTENT_ATTRIBUTES.reset(token)
