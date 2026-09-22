# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""AGENTIC-2256 classification contracts at the final exporter boundary."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
import json
import os
import socket
from unittest.mock import Mock

from fastmcp import Client, FastMCP, telemetry
from fastmcp.server.middleware import MiddlewareContext
from fastmcp.tools.tool import ToolResult
from fastmcp_extensions.decorators import _REGISTERED_PROVIDERS, _REGISTERED_TOOLS
from mcp.types import CallToolRequestParams
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode
import pytest

from airbyte.mcp import _otel as observability
from airbyte.mcp.agents import register_agents_tools
from airbyte.mcp.cloud import register_cloud_tools
from airbyte.mcp.interactive import register_interactive_tools
from airbyte.mcp.local import register_local_tools
from airbyte.mcp.registry import register_registry_tools
from tests.unit_tests.test_mcp_otel import _loopback_only


ATTRIBUTE = "airbyte.mcp.entity_kind"
# Source of truth: AGENTIC-2256's approved exact-name classification table.
EXPECTED_GROUPS = {
    "connection": (
        "create_connection_on_cloud",
        "describe_cloud_connection",
        "list_deployed_cloud_connections",
        "permanently_delete_cloud_connection",
        "rename_cloud_connection",
        "set_cloud_connection_table_prefix",
        "set_cloud_connection_selected_streams",
        "update_cloud_connection",
        "get_connection_artifact",
    ),
    "source": (
        "deploy_source_to_cloud",
        "list_deployed_cloud_source_connectors",
        "describe_cloud_source",
        "check_cloud_source",
        "permanently_delete_cloud_source",
        "rename_cloud_source",
        "update_cloud_source_config",
        "validate_connector_config",
    ),
    "destination": (
        "deploy_destination_to_cloud",
        "deploy_noop_destination_to_cloud",
        "list_deployed_cloud_destination_connectors",
        "describe_cloud_destination",
        "check_cloud_destination",
        "permanently_delete_cloud_destination",
        "rename_cloud_destination",
        "update_cloud_destination_config",
        "destination_smoke_test",
    ),
    "workspace": (
        "list_cloud_workspaces",
        "describe_cloud_workspace",
        "set_default_cloud_workspace",
        "list_agent_workspaces",
        "show_workspace_sync_status",
    ),
    "organization": (
        "list_cloud_organizations",
        "describe_cloud_organization",
        "get_cloud_organization_billing_status",
    ),
    "sync_job": (
        "run_cloud_sync",
        "get_cloud_sync_status",
        "list_cloud_sync_jobs",
        "cancel_cloud_sync",
        "get_cloud_sync_logs",
        "show_connection_sync_history",
    ),
    "connector_definition": (
        "publish_custom_source_definition",
        "list_custom_source_definitions",
        "get_custom_source_definition",
        "get_connector_builder_draft_manifest",
        "update_custom_source_definition",
        "permanently_delete_custom_source_definition",
        "list_connectors",
        "get_connector_info",
        "get_api_docs_urls",
        "get_connector_version_history",
        "show_connectors_list",
    ),
    "agent_connector": (
        "list_agent_connectors",
        "inspect_agent_connector",
        "execute_agent_connector_ro",
        "execute_agent_connector",
    ),
    "skill": ("list_agent_skills", "read_agent_skill_docs"),
    "stream": (
        "list_source_streams",
        "get_source_stream_json_schema",
        "read_source_stream_records",
        "get_stream_previews",
        "list_cached_streams",
    ),
    "cache": ("sync_source_to_cache", "describe_default_cache", "run_sql_query"),
}
EXPECTED_KINDS = {
    name: kind for kind, names in EXPECTED_GROUPS.items() for name in names
}
OMITTED = {
    "get_default_cloud_context",
    "list_connector_config_secrets",
    "list_dotenv_secrets",
}


@pytest.fixture(autouse=True)
def isolated_entity_kind(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Prevent network access and cross-test telemetry configuration."""
    observability._reset_for_tests()
    for key in os.environ:
        if key.startswith(("OTEL_", "AIRBYTE_MCP_")):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    monkeypatch.setattr(
        socket.socket, "connect", _loopback_only(socket.socket.connect, 1)
    )
    monkeypatch.setattr(
        socket.socket, "connect_ex", _loopback_only(socket.socket.connect_ex, 1)
    )
    monkeypatch.setattr(
        socket, "create_connection", _loopback_only(socket.create_connection, 0)
    )
    observability._build_tool_maps()
    yield
    observability._reset_for_tests()


@pytest.fixture(
    params=[SimpleSpanProcessor, BatchSpanProcessor], ids=["immediate", "batch"]
)
def entity_provider(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> Iterator[tuple[TracerProvider, InMemorySpanExporter]]:
    """Exercise real FastMCP spans without replacing the process-global provider."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(observability.IntentStampProcessor())
    provider.add_span_processor(
        request.param(observability.RedactingExporter(exporter))
    )
    monkeypatch.setattr(telemetry, "otel_get_tracer", provider.get_tracer)
    yield provider, exporter
    provider.shutdown()


def _synthetic_app(*names: str, capture: bool = False) -> FastMCP:
    app = FastMCP("entity-kind")

    def operation(
        payload: str = "",
        entity_type: str = "",
        action: str = "",
        fail: bool = False,
    ) -> str:
        if fail:
            raise ValueError("exception-SENTINEL")
        return f"response-SENTINEL:{payload}:{entity_type}:{action}"

    for name in names:
        app.tool(operation, name=name)
    app.add_middleware(
        observability.IntentCaptureMiddleware(
            app, environ={"AIRBYTE_MCP_INTENT_CAPTURE": str(int(capture))}
        )
    )
    return app


def _finished(
    provider_and_exporter: tuple[TracerProvider, InMemorySpanExporter],
) -> tuple[ReadableSpan, ...]:
    provider, exporter = provider_and_exporter
    assert provider.force_flush()
    spans = exporter.get_finished_spans()
    assert "SENTINEL" not in "\n".join(span.to_json() for span in spans)
    return spans


def test_exact_classification_covers_every_registered_and_unfiltered_tool() -> None:
    """New/stale tools, duplicate names, providers and wrong vocabulary fail loudly."""
    app = FastMCP("unfiltered-inventory")
    for register in (
        register_agents_tools,
        register_cloud_tools,
        register_local_tools,
        register_registry_tools,
        register_interactive_tools,
    ):
        register(app)
    names = [function.__name__ for function, _ in _REGISTERED_TOOLS]
    assert len(names) == len(set(names)) == 68
    assert not _REGISTERED_PROVIDERS
    assert set(EXPECTED_KINDS).isdisjoint(OMITTED)
    assert observability._TOOL_ENTITY_KINDS == EXPECTED_KINDS
    assert (
        set(EXPECTED_KINDS) | OMITTED
        == set(names)
        == {tool.name for tool in asyncio.run(app.list_tools())}
    )


@pytest.mark.parametrize("vendor", ["", "datadog"])
@pytest.mark.parametrize(
    ("name", "expected"),
    [*EXPECTED_KINDS.items(), *((name, None) for name in sorted(OMITTED))],
)
def test_every_tool_exports_its_static_kind(
    entity_provider, monkeypatch, vendor, name, expected
) -> None:
    """Final exported values match the ticket for both processors and vendors."""
    monkeypatch.setenv("AIRBYTE_MCP_OTEL_VENDOR", vendor)
    app = _synthetic_app(name)

    async def call() -> None:
        async with Client(app) as client:
            await client.call_tool(
                name,
                {
                    "payload": "credential-SENTINEL",
                    "entity_type": "sql-SENTINEL",
                    "action": "sql_select",
                    "telemetry": {"entity_kind": "forged-SENTINEL"},
                },
            )

    asyncio.run(call())
    (span,) = _finished(entity_provider)
    assert span.parent is None
    assert span.kind == SpanKind.SERVER
    assert span.end_time >= span.start_time
    attrs = span.attributes
    if expected is None:
        assert ATTRIBUTE not in attrs
    else:
        assert attrs[ATTRIBUTE] == expected
    if vendor == "datadog":
        metadata = json.loads(attrs["_dd.ml_obs.metadata"])
        assert metadata.get("entity_kind") == expected
    else:
        assert "_dd.ml_obs.metadata" not in attrs


@pytest.mark.parametrize("capture", [False, True])
@pytest.mark.parametrize(
    "arguments",
    [
        None,
        {},
        {"payload": None},
        {"payload": ["malformed-SENTINEL"]},
        {"fail": True, "payload": "argument-SENTINEL"},
    ],
)
def test_kind_survives_missing_arguments_validation_and_tool_failure(
    entity_provider, arguments, capture
) -> None:
    app = _synthetic_app("validate_connector_config", capture=capture)

    async def call() -> None:
        async with Client(app) as client:
            await client.call_tool(
                "validate_connector_config", arguments, raise_on_error=False
            )

    asyncio.run(call())
    (span,) = _finished(entity_provider)
    assert span.attributes[ATTRIBUTE] == "source"
    if arguments:
        assert span.status.status_code == StatusCode.ERROR
    assert observability._INTENT_ATTRIBUTES.get() is None


@pytest.mark.parametrize("vendor", ["", "datadog"])
@pytest.mark.parametrize(
    ("name", "kind", "value", "nested", "expected"),
    [
        (
            "tools/call validate_connector_config",
            SpanKind.SERVER,
            "source",
            False,
            "source",
        ),
        ("tools/call validate_connector_config", SpanKind.SERVER, None, False, None),
        ("tools/call validate_connector_config", SpanKind.SERVER, "", False, None),
        (
            "tools/call validate_connector_config",
            SpanKind.SERVER,
            "connection",
            False,
            None,
        ),
        (
            "tools/call validate_connector_config",
            SpanKind.SERVER,
            "rejected-SENTINEL",
            False,
            None,
        ),
        (
            "tools/call validate_connector_config",
            SpanKind.SERVER,
            ("source",),
            False,
            None,
        ),
        ("tools/call validate_connector_config", SpanKind.SERVER, False, False, None),
        ("tools/call validate_connector_config", SpanKind.SERVER, 0, False, None),
        (
            "tools/call validate_connector_config",
            SpanKind.CLIENT,
            "source",
            False,
            None,
        ),
        ("tools/call validate_connector_config", SpanKind.SERVER, "source", True, None),
        ("validate_connector_config", SpanKind.SERVER, "source", False, None),
        ("GET", SpanKind.CLIENT, "source", False, None),
        (
            "tools/call get_default_cloud_context",
            SpanKind.SERVER,
            "source",
            False,
            None,
        ),
    ],
)
def test_exporter_rejects_wrong_value_or_span(
    entity_provider, monkeypatch, vendor, name, kind, value, nested, expected
) -> None:
    monkeypatch.setenv("AIRBYTE_MCP_OTEL_VENDOR", vendor)
    provider, _ = entity_provider
    tracer = provider.get_tracer("boundary-test")
    parent = tracer.start_span("parent") if nested else None
    context = trace.set_span_in_context(parent) if parent else None
    with tracer.start_as_current_span(name, kind=kind, context=context) as span:
        if value is not None:
            span.set_attribute(ATTRIBUTE, value)
    if parent:
        parent.end()
    exported = next(span for span in _finished(entity_provider) if span.name == name)
    if expected is None:
        assert ATTRIBUTE not in exported.attributes
    else:
        assert exported.attributes[ATTRIBUTE] == expected
    metadata = json.loads(exported.attributes.get("_dd.ml_obs.metadata", "{}"))
    assert metadata.get("entity_kind") == (expected if vendor == "datadog" else None)


@pytest.mark.parametrize(
    "name",
    [
        "describe_cloud_source_extra",
        "execute_agent_connector_ro_extra",
        "unknown-SENTINEL",
    ],
)
def test_unclassified_names_never_infer_kind(
    entity_provider, monkeypatch, name
) -> None:
    app = _synthetic_app(name)
    monkeypatch.setitem(observability._TOOL_MODULES, name, "cloud")

    async def call() -> None:
        async with Client(app) as client:
            await client.call_tool(name, {"entity_type": "source"})

    if "SENTINEL" in name:
        monkeypatch.delitem(observability._TOOL_MODULES, name)
    asyncio.run(call())
    spans = _finished(entity_provider)
    if "SENTINEL" in name:
        assert not spans
    else:
        (span,) = spans
        assert ATTRIBUTE not in span.attributes


def test_interleaved_calls_preserve_request_local_kind(entity_provider) -> None:
    app = FastMCP("interleaved")
    entered = asyncio.Event()
    release = asyncio.Event()

    @app.tool(name="describe_cloud_source")
    async def slow() -> str:
        entered.set()
        await release.wait()
        return "source-result-SENTINEL"

    @app.tool(name="describe_cloud_destination")
    async def fast() -> str:
        await entered.wait()
        release.set()
        return "destination-result-SENTINEL"

    app.add_middleware(observability.IntentCaptureMiddleware(app, environ={}))

    async def call() -> None:
        async with Client(app) as client:
            await asyncio.gather(
                client.call_tool("describe_cloud_source"),
                client.call_tool("describe_cloud_destination"),
            )
        assert observability._INTENT_ATTRIBUTES.get() is None

    asyncio.run(call())
    spans = _finished(entity_provider)
    assert {span.name: span.attributes[ATTRIBUTE] for span in spans} == {
        "tools/call describe_cloud_source": "source",
        "tools/call describe_cloud_destination": "destination",
    }


def test_nested_calls_restore_outer_attributes_and_only_root_exports_kind(
    entity_provider,
) -> None:
    app = _synthetic_app("describe_cloud_destination")

    @app.tool(name="describe_cloud_source")
    async def outer() -> str:
        before = observability._INTENT_ATTRIBUTES.get()
        assert before[ATTRIBUTE] == "source"
        await app.call_tool("describe_cloud_destination")
        assert observability._INTENT_ATTRIBUTES.get() is before
        return "nested-result-SENTINEL"

    async def call() -> None:
        async with Client(app) as client:
            await client.call_tool("describe_cloud_source")

    asyncio.run(call())
    spans = _finished(entity_provider)
    assert len(spans) == 2
    root = next(span for span in spans if span.parent is None)
    child = next(span for span in spans if span.parent is not None)
    assert root.attributes[ATTRIBUTE] == "source"
    assert ATTRIBUTE not in child.attributes
    assert child.parent.span_id == root.context.span_id
    assert observability._INTENT_ATTRIBUTES.get() is None


def test_cancelled_middleware_resets_context_before_next_call(entity_provider) -> None:
    middleware = observability.IntentCaptureMiddleware(FastMCP("cancel"), environ={})
    provider, _ = entity_provider
    entered = asyncio.Event()

    async def cancelled(
        context: MiddlewareContext[CallToolRequestParams],
    ) -> ToolResult:
        with provider.get_tracer("fastmcp").start_as_current_span(
            f"tools/call {context.message.name}", kind=SpanKind.SERVER
        ):
            entered.set()
            await asyncio.Future()
        raise AssertionError("unreachable")

    async def call() -> None:
        context = MiddlewareContext(message=CallToolRequestParams(name="run_sql_query"))

        async def invoke() -> None:
            try:
                await middleware.on_call_tool(context, cancelled)
            finally:
                assert observability._INTENT_ATTRIBUTES.get() is None

        task = asyncio.create_task(invoke())
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with Client(_synthetic_app("get_default_cloud_context")) as client:
            await client.call_tool("get_default_cloud_context")

    asyncio.run(call())
    spans = _finished(entity_provider)
    assert len(spans) == 2
    assert spans[0].attributes[ATTRIBUTE] == "cache"
    assert ATTRIBUTE not in spans[1].attributes


@pytest.mark.parametrize("environment", [{}, {"AIRBYTE_MCP_INTENT_CAPTURE": "1"}])
def test_install_without_endpoint_preserves_schema_and_never_builds_exporter(
    monkeypatch, environment
) -> None:
    provider = Mock()
    monkeypatch.setattr(observability, "_build_provider", provider)
    monkeypatch.setattr(trace, "get_tracer_provider", trace.ProxyTracerProvider)
    app = _synthetic_app("describe_cloud_source")
    tool = asyncio.run(app.get_tool("describe_cloud_source"))
    before = json.dumps(tool.parameters, sort_keys=True)
    observability.install(app, environ=environment)
    provider.assert_not_called()
    assert json.dumps(tool.parameters, sort_keys=True) == before
    assert ATTRIBUTE not in before
