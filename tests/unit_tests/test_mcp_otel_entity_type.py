# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""AGENTIC-2260: bounded requested categories at the final exporter boundary."""

from __future__ import annotations

import asyncio
import copy
import json
from unittest.mock import Mock

import pytest
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server import telemetry as fastmcp_telemetry
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from airbyte.agents.models import AgentExecuteResult
from airbyte.mcp import _otel as observability
from airbyte.mcp import agents as agents_mcp


ATTRIBUTE = "airbyte.mcp.agent.entity_type"
pytest_plugins = ("tests.unit_tests.test_mcp_otel",)
TOOLS = ("execute_agent_connector_ro", "execute_agent_connector")
ENTITIES = (
    "issues",
    "comments",
    "projects",
    "repositories",
    "pull_requests",
    "teams",
    "users",
    "workflow_states",
)


@pytest.fixture(params=["", "datadog"])
def vendor(request, monkeypatch):
    monkeypatch.setenv("AIRBYTE_MCP_OTEL_VENDOR", request.param)
    return request.param


@pytest.fixture(params=["simple", "batch"])
def entity_export(request, monkeypatch, isolated_otel, vendor):
    """Keep FastMCP and production processors real without replacing the global provider."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(observability.IntentStampProcessor())
    redacted = observability.RedactingExporter(exporter)
    processor = (
        SimpleSpanProcessor(redacted)
        if request.param == "simple"
        else BatchSpanProcessor(redacted)
    )
    provider.add_span_processor(processor)
    monkeypatch.setattr(
        fastmcp_telemetry, "get_tracer", lambda: provider.get_tracer("fastmcp")
    )
    yield provider, exporter
    provider.shutdown()


def _finished(entity_export):
    provider, exporter = entity_export
    assert provider.force_flush()
    return exporter.get_finished_spans()


def _assert_entity(span, expected, vendor):
    attrs = span.attributes
    if expected is None:
        assert ATTRIBUTE not in attrs
    else:
        assert attrs[ATTRIBUTE] == expected
        assert isinstance(attrs[ATTRIBUTE], str)
    metadata = json.loads(attrs.get("_dd.ml_obs.metadata", "{}"))
    if vendor == "datadog" and expected is not None:
        assert metadata["agent.entity_type"] == expected
    else:
        assert "agent.entity_type" not in metadata
    assert "airbyte.mcp.entity_type_valid" not in attrs


async def _call(app, tool, arguments):
    async with Client(app) as client:
        return await client.call_tool(tool, arguments)


@pytest.mark.parametrize("tool", TOOLS)
@pytest.mark.parametrize("entity", ENTITIES)
def test_real_execution_tools_keep_requested_category_on_success_and_error(
    agents_app, entity_export, monkeypatch, vendor, tool, entity
):
    connector = Mock()
    connector.execute.return_value = AgentExecuteResult(
        status="success", result={"secret": "result-SENTINEL"}
    )
    monkeypatch.setattr(agents_mcp, "_get_agent_connector", lambda **kwargs: connector)
    arguments = {
        "connector_id": "connector-SENTINEL",
        "entity_type": entity,
        "action": "list",
        "api_args": {"token": "credential-SENTINEL", "sql": "sql-SENTINEL"},
        "cursor": "cursor-SENTINEL",
    }
    original = copy.deepcopy(arguments)
    result = asyncio.run(_call(agents_app, tool, arguments))
    assert result.structured_content["result"] == {"secret": "result-SENTINEL"}
    assert arguments == original
    assert connector.execute.call_count == 1
    assert connector.execute.call_args.kwargs["entity_type"] == entity
    assert connector.execute.call_args.kwargs["api_args"] == original["api_args"]
    assert connector.execute.call_args.kwargs["cursor"] == original["cursor"]
    success = _finished(entity_export)
    assert len(success) == 1
    _assert_entity(success[0], entity, vendor)
    assert success[0].parent is None

    connector.execute.side_effect = ValueError("exception-SENTINEL")
    with pytest.raises(ToolError, match="exception-SENTINEL"):
        asyncio.run(_call(agents_app, tool, arguments))
    spans = _finished(entity_export)
    assert len(spans) == 2
    _assert_entity(spans[-1], entity, vendor)
    assert observability._INTENT_ATTRIBUTES.get() is None
    assert "SENTINEL" not in "\n".join(span.to_json() for span in spans)


@pytest.fixture
def flexible_app():
    """Accept arbitrary synthetic arguments so telemetry cannot rely on schema rejection."""
    app = FastMCP("entity-type")

    async def execute(entity_type: object = None, action: object = None) -> str:
        return "result-SENTINEL"

    for name in (
        *TOOLS,
        "inspect_agent_connector",
        "list_agent_connectors",
        "read_agent_skill_docs",
    ):
        app.tool(name=name)(execute)
    observability._build_tool_maps()
    app.add_middleware(observability.IntentCaptureMiddleware(app))
    return app


@pytest.mark.parametrize("tool", TOOLS)
def test_action_matrix_including_sql_and_read_only_write_actions(
    flexible_app, entity_export, vendor, tool
):
    actions = ["list", "get", "search", "create", "update", "delete"]
    accepted = tuple(actions[:3] if tool.endswith("_ro") else actions)
    actions += [
        None,
        "",
        0,
        False,
        [],
        {},
        "LIST",
        " list",
        "download",
        "sql_select",
        "action-SENTINEL",
    ]

    async def exercise():
        async with Client(flexible_app) as client:
            for action in actions:
                result = await client.call_tool(
                    tool, {"action": action, "entity_type": "issues"}
                )
                assert result.content[0].text == "result-SENTINEL"
            await client.call_tool(tool, {"entity_type": "issues"})

    asyncio.run(exercise())
    spans = _finished(entity_export)
    assert len(spans) == len(actions) + 1
    for span, action in zip(spans, [*actions, None], strict=True):
        _assert_entity(span, "issues" if action in accepted else None, vendor)
    assert "SENTINEL" not in "\n".join(span.to_json() for span in spans)


@pytest.mark.parametrize("tool", TOOLS)
def test_invalid_categories_are_omitted_without_mutating_tool_input(
    flexible_app, entity_export, vendor, tool
):
    values = [
        None,
        "",
        0,
        -1,
        1.5,
        False,
        [],
        ["issues"],
        {},
        {"name": "issues"},
        " ",
        "Issues",
        "ISSUES",
        " issues",
        "issues ",
        "issues\n",
        "issues\x00",
        "issues-SENTINEL@example.com",
        "customer_private_table",
        "x" * 10_000,
    ]

    async def exercise():
        async with Client(flexible_app) as client:
            for entity in values:
                await client.call_tool(tool, {"action": "get", "entity_type": entity})
            await client.call_tool(tool, {"action": "get"})

    asyncio.run(exercise())
    spans = _finished(entity_export)
    assert len(spans) == len(values) + 1
    for span in spans:
        _assert_entity(span, None, vendor)
    exported = "\n".join(span.to_json() for span in spans)
    assert "SENTINEL" not in exported
    assert "customer_private_table" not in exported
    assert "x" * 10_000 not in exported


def test_non_execution_tools_never_get_category(flexible_app, entity_export, vendor):
    async def exercise():
        async with Client(flexible_app) as client:
            for tool in (
                "inspect_agent_connector",
                "list_agent_connectors",
                "read_agent_skill_docs",
            ):
                await client.call_tool(
                    tool, {"action": "list", "entity_type": "issues"}
                )

    asyncio.run(exercise())
    spans = _finished(entity_export)
    assert len(spans) == 3
    for span in spans:
        _assert_entity(span, None, vendor)


def test_exporter_validates_injected_type_value_and_root_scope(entity_export, vendor):
    provider, _ = entity_export
    tracer = provider.get_tracer("test-injection")
    observability._build_tool_maps()
    cases = [
        (f"tools/call {TOOLS[0]}", SpanKind.SERVER, "issues", "issues"),
        (f"tools/call {TOOLS[1]}", SpanKind.SERVER, "projects", "projects"),
        (f"tools/call {TOOLS[0]}", SpanKind.SERVER, "injected-SENTINEL", None),
        (f"tools/call {TOOLS[0]}", SpanKind.SERVER, 42, None),
        (f"tools/call {TOOLS[0]}", SpanKind.SERVER, True, None),
        (f"tools/call {TOOLS[0]}", SpanKind.SERVER, ["issues"], None),
        (f"tools/call {TOOLS[0]}", SpanKind.CLIENT, "issues", None),
        (f"tools/call {TOOLS[0]}", SpanKind.INTERNAL, "issues", None),
        ("tools/call inspect_agent_connector", SpanKind.SERVER, "issues", None),
        (TOOLS[0], SpanKind.SERVER, "issues", None),
        ("GET", SpanKind.CLIENT, "issues", None),
    ]
    for name, kind, value, _ in cases:
        with tracer.start_as_current_span(
            name, kind=kind, attributes={ATTRIBUTE: value}
        ):
            pass
    for span, (_, _, _, expected) in zip(_finished(entity_export), cases, strict=True):
        _assert_entity(span, expected, vendor)
    with tracer.start_as_current_span("parent"):
        with tracer.start_as_current_span(
            f"tools/call {TOOLS[0]}",
            kind=SpanKind.SERVER,
            attributes={ATTRIBUTE: "issues"},
        ):
            pass
    for span in _finished(entity_export)[-2:]:
        _assert_entity(span, None, vendor)
    with tracer.start_as_current_span(
        "tools/call unregistered-SENTINEL",
        kind=SpanKind.SERVER,
        attributes={ATTRIBUTE: "issues"},
    ):
        pass
    assert len(_finished(entity_export)) == len(cases) + 2
    assert "SENTINEL" not in "\n".join(
        span.to_json() for span in _finished(entity_export)
    )


def test_concurrent_and_nested_calls_keep_request_context(
    flexible_app, entity_export, vendor
):
    app = flexible_app
    entered = 0
    both_entered = asyncio.Event()

    @app.tool(name=TOOLS[0])
    async def execute(entity_type: str, action: str) -> str:
        nonlocal entered
        entered += 1
        if entered == 2:
            both_entered.set()
        await both_entered.wait()
        before = dict(observability._INTENT_ATTRIBUTES.get())
        await app.call_tool(TOOLS[1], {"action": "get", "entity_type": "comments"})
        assert observability._INTENT_ATTRIBUTES.get() == before
        return entity_type

    async def exercise():
        results = await asyncio.gather(
            _call(app, TOOLS[0], {"entity_type": "issues", "action": "list"}),
            _call(app, TOOLS[0], {"entity_type": "projects", "action": "get"}),
        )
        assert [result.content[0].text for result in results] == ["issues", "projects"]
        assert observability._INTENT_ATTRIBUTES.get() is None
        await _call(
            app, "inspect_agent_connector", {"entity_type": "issues", "action": "get"}
        )

    asyncio.run(exercise())
    spans = _finished(entity_export)
    roots = [span for span in spans if span.name == f"tools/call {TOOLS[0]}"]
    assert len(roots) == 2
    assert {span.attributes[ATTRIBUTE] for span in roots} == {"issues", "projects"}
    for span in roots:
        _assert_entity(span, span.attributes[ATTRIBUTE], vendor)
    for span in spans:
        if span not in roots:
            _assert_entity(span, None, vendor)


@pytest.mark.parametrize("failure", ["error", "cancel", "timeout"])
def test_failed_cancelled_timed_out_calls_reset_before_next_attempt(
    flexible_app, entity_export, vendor, failure
):
    app = flexible_app
    started = asyncio.Event()

    @app.tool(name=TOOLS[0])
    async def execute(entity_type: str, action: str) -> str:
        if failure == "error":
            raise ValueError("exception-SENTINEL")
        started.set()
        await asyncio.Event().wait()
        return "unreachable"

    async def exercise():
        async with Client(app) as client:
            if failure == "error":
                with pytest.raises(ToolError, match="exception-SENTINEL"):
                    await client.call_tool(
                        TOOLS[0], {"entity_type": "issues", "action": "get"}
                    )
            else:
                task = asyncio.create_task(
                    client.call_tool(
                        TOOLS[0], {"entity_type": "issues", "action": "get"}
                    )
                )
                await started.wait()
                if failure == "cancel":
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                else:
                    with pytest.raises(asyncio.TimeoutError):
                        await asyncio.wait_for(task, timeout=0)
            assert observability._INTENT_ATTRIBUTES.get() is None
            await client.call_tool(
                "inspect_agent_connector", {"entity_type": "issues", "action": "list"}
            )
            await client.call_tool(
                TOOLS[1], {"entity_type": "projects", "action": "list"}
            )

    asyncio.run(exercise())
    spans = _finished(entity_export)
    assert len(spans) == 3
    by_name = {span.name: span for span in spans}
    _assert_entity(by_name[f"tools/call {TOOLS[0]}"], "issues", vendor)
    _assert_entity(by_name["tools/call inspect_agent_connector"], None, vendor)
    _assert_entity(by_name[f"tools/call {TOOLS[1]}"], "projects", vendor)
    assert "SENTINEL" not in "\n".join(span.to_json() for span in spans)


def test_immediate_export_precedes_middleware_return(monkeypatch, isolated_otel):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(observability.IntentStampProcessor())
    provider.add_span_processor(
        SimpleSpanProcessor(observability.RedactingExporter(exporter))
    )
    monkeypatch.setattr(
        fastmcp_telemetry, "get_tracer", lambda: provider.get_tracer("fastmcp")
    )
    app = FastMCP("timing")

    @app.tool(name=TOOLS[0])
    async def execute(entity_type: str, action: str) -> str:
        assert not exporter.get_finished_spans()
        return "ok"

    class ObserveReturn(observability.IntentCaptureMiddleware):
        async def on_call_tool(self, context, call_next):
            async def observe(inner_context):
                result = await call_next(inner_context)
                spans = exporter.get_finished_spans()
                assert len(spans) == 1
                assert spans[0].attributes[ATTRIBUTE] == "issues"
                return result

            return await super().on_call_tool(context, observe)

    observability._build_tool_maps()
    app.add_middleware(ObserveReturn(app))
    try:
        asyncio.run(_call(app, TOOLS[0], {"entity_type": "issues", "action": "get"}))
    finally:
        provider.shutdown()


def test_tracing_disabled_preserves_default_schema_and_execution(
    monkeypatch, isolated_otel, uninitialized_provider, otel_provider
):
    monkeypatch.setattr(
        fastmcp_telemetry,
        "get_tracer",
        lambda: trace.NoOpTracerProvider().get_tracer("fastmcp"),
    )
    app = FastMCP("disabled")

    @app.tool(name=TOOLS[0])
    def execute(entity_type: str, action: str) -> str:
        return entity_type

    before = copy.deepcopy(asyncio.run(app.get_tool(TOOLS[0])).parameters)
    observability.install(app, environ={})
    uninitialized_provider.assert_not_called()
    listed = asyncio.run(app.list_tools())
    assert listed[0].parameters == before
    result = asyncio.run(
        _call(app, TOOLS[0], {"entity_type": "issues", "action": "list"})
    )
    assert result.content[0].text == "issues"
    assert not _finished(otel_provider)
