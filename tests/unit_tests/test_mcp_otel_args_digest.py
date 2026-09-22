# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""AGENTIC-2257: bounded pseudonymous arguments at the final exporter boundary."""

from __future__ import annotations

import asyncio
from decimal import Decimal
import json
import logging
import os
import socket
from unittest.mock import AsyncMock, Mock

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import MiddlewareContext
from fastmcp.tools import ToolResult
from mcp.types import CallToolRequestParams
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
import pytest

from airbyte.mcp import _args_digest as canonical
from airbyte.mcp import _otel as observability
from tests.unit_tests.test_mcp_otel import _http_rpc, _loopback_only


KEY = b"synthetic-key"
ATTRIBUTE = "airbyte.mcp.args_digest"
SENTINEL = "never-export-this-argument-value"


@pytest.mark.parametrize(
    "tool,arguments,key,expected",
    [
        ("echo", {}, KEY, "336d57c10232fc85802e2e3ce1a8392e"),
        (
            "echo",
            {"z": [True, False, 0, -1, 1, 1.0, -0.0, 0.0], "a": None},
            KEY,
            "dc8c356fe54bcb47a3b2c52cf9038996",
        ),
        (
            "echo",
            {"é": "e\u0301", "😀": [-(2**63), 2**63 - 1, 1.25]},
            KEY,
            "48496e9cd4f7940ff761a2bbbf9cf8e3",
        ),
        ("other", {}, KEY, "bfc2f00b91155dbf2286aad6dcd599b2"),
        ("echo", {}, b" synthetic-key ", "d64bd4b2e1f2c196efcb90d4eaf3e0e2"),
    ],
)
def test_frozen_v1_vectors(tool, arguments, key, expected):
    assert canonical.args_digest(tool, arguments, key) == expected


@pytest.mark.parametrize(
    "left,right",
    [
        ({}, {"value": None}),
        ({}, {"value": "default"}),
        ({"api_args": '{"x":1}'}, {"api_args": {"x": 1}}),
        ({"v": 1}, {"v": 1.0}),
        ({"v": -0.0}, {"v": 0.0}),
        ({"v": [1, 2]}, {"v": [2, 1]}),
        ({"v": "é"}, {"v": "e\u0301"}),
    ],
)
def test_supplied_values_are_not_coerced_or_normalized(left, right):
    first = canonical.args_digest("echo", left, KEY)
    second = canonical.args_digest("echo", right, KEY)
    assert first and second and first != second


def test_mapping_order_absent_arguments_and_shared_acyclic_values():
    assert (
        canonical.args_digest("echo", None, KEY) == "336d57c10232fc85802e2e3ce1a8392e"
    )
    shared = [1, None]
    assert canonical.args_digest("echo", {"b": shared, "a": shared}, KEY) == (
        canonical.args_digest("echo", {"a": [1, None], "b": [1, None]}, KEY)
    )


def test_version_and_key_domain_separation(monkeypatch):
    original = canonical.args_digest("echo", {}, KEY)
    assert canonical.args_digest("echo", {}, b"rotated-synthetic-key") != original
    monkeypatch.setattr(canonical, "_DOMAIN", "airbyte.mcp.args_digest/v2")
    assert canonical.args_digest("echo", {}, KEY) != original


class CustomDict(dict):
    pass


class CustomString(str):
    pass


@pytest.mark.parametrize(
    "arguments",
    [
        [],
        CustomDict(),
        {"v": CustomDict()},
        {"v": CustomString("secret")},
        {"v": object()},
        {"v": (1,)},
        {"v": b"bytes"},
        {"v": Decimal("1")},
        {"v": float("nan")},
        {"v": float("inf")},
        {"v": -float("inf")},
        {"v": 2**63},
        {"v": -(2**63) - 1},
        {1: "value"},
        {"v": "\ud800"},
        {"\udfff": "value"},
    ],
)
def test_unsupported_arguments_are_omitted(arguments):
    assert canonical.args_digest("echo", arguments, KEY) is None


def test_cycles_and_tool_name_encoding_are_omitted():
    value = {}
    value["self"] = value
    assert canonical.args_digest("echo", value, KEY) is None
    value = []
    value.append(value)
    assert canonical.args_digest("echo", {"v": value}, KEY) is None
    assert canonical.args_digest("\ud800", {}, KEY) is None
    assert canonical.args_digest("a" * 65537, {}, KEY) is None
    assert canonical.args_digest("echo", {}, b"") is None


def test_depth_node_string_and_encoded_byte_boundaries():
    nested = None
    for _ in range(30):
        nested = [nested]
    assert canonical.args_digest("echo", {"v": nested}, KEY)
    assert canonical.args_digest("echo", {"v": [nested]}, KEY) is None
    assert canonical.args_digest("echo", {"v": [None] * 4090}, KEY)
    assert canonical.args_digest("echo", {"v": [None] * 4091}, KEY) is None
    assert canonical.args_digest("echo", {str(i): None for i in range(2046)}, KEY)
    assert (
        canonical.args_digest("echo", {str(i): None for i in range(2047)}, KEY) is None
    )
    string_budget = 65536 - len("airbyte.mcp.args_digest/v1") - len("echo")
    canonical._validate([
        "airbyte.mcp.args_digest/v1",
        "echo",
        {"": "a" * string_budget},
    ])
    with pytest.raises(ValueError):
        canonical._validate([
            "airbyte.mcp.args_digest/v1",
            "echo",
            {"a": "a" * string_budget},
        ])
    overhead = len(b'["airbyte.mcp.args_digest/v1","echo",{"":""}]')
    assert canonical.args_digest("echo", {"": "a" * (65536 - overhead)}, KEY)
    assert canonical.args_digest("echo", {"": "a" * (65537 - overhead)}, KEY) is None
    assert canonical.args_digest("echo", {"": "😀" * 5500}, KEY) is None


@pytest.fixture(params=["simple", "batch"])
def harness(monkeypatch, request):
    """Use the actual install path with local exporters and no network transport."""
    observability._reset_for_tests()
    for name in os.environ:
        if name.startswith(("OTEL_", "AIRBYTE_MCP_")):
            monkeypatch.delenv(name)
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    for owner, name, original, index in (
        (socket.socket, "connect", socket.socket.connect, 1),
        (socket.socket, "connect_ex", socket.socket.connect_ex, 1),
        (socket, "create_connection", socket.create_connection, 0),
    ):
        monkeypatch.setattr(owner, name, _loopback_only(original, index))
    current = [trace.ProxyTracerProvider()]
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: current[0])
    monkeypatch.setattr(
        trace, "set_tracer_provider", lambda provider: current.__setitem__(0, provider)
    )
    monkeypatch.setattr(
        observability,
        "RequestsInstrumentor",
        Mock(return_value=Mock(is_instrumented_by_opentelemetry=False)),
    )
    exporter = InMemorySpanExporter()
    providers = []

    def build(destination):
        provider = TracerProvider()
        providers.append(provider)
        provider.add_span_processor(observability.IntentStampProcessor())
        processor = (
            SimpleSpanProcessor if request.param == "simple" else BatchSpanProcessor
        )
        provider.add_span_processor(
            processor(observability.RedactingExporter(destination))
        )
        return provider

    monkeypatch.setattr(observability, "_build_provider", build)
    monkeypatch.setattr(
        "opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter",
        lambda: exporter,
    )

    def make(*, key="synthetic-key", vendor="", endpoint=True):
        app = FastMCP("digest-tests")

        @app.tool()
        async def echo(value: str = "default", fail: bool = False) -> str:
            await asyncio.sleep(0)
            if fail:
                raise RuntimeError("tool failure")
            return value

        @app.tool()
        async def real(telemetry: dict, payload: dict | None = None) -> str:
            assert isinstance(telemetry, dict)
            return json.dumps(payload)

        @app.tool()
        async def api(api_args: dict | str | None = None) -> str:
            return str(api_args)

        environment = {"AIRBYTE_MCP_OTEL_VENDOR": vendor}
        if key is not None:
            environment["AIRBYTE_MCP_OTEL_DIGEST_KEY"] = key
        if endpoint:
            environment["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = (
                "https://example.invalid"
            )
        observability.install(app, environ=environment)
        for name in ("echo", "real", "api"):
            monkeypatch.setitem(observability._TOOL_MODULES, name, "cloud")
        return app, current[0], exporter, environment

    yield make
    for provider in providers:
        provider.force_flush()
        provider.shutdown()
    observability._reset_for_tests()


def finished(provider, exporter):
    assert provider.force_flush()
    return exporter.get_finished_spans()


@pytest.mark.parametrize("vendor", ["", "other", "datadog"])
@pytest.mark.parametrize("fail", [False, True])
def test_real_tool_span_exports_only_digest_for_success_and_error(
    harness, vendor, fail
):
    app, provider, exporter, _ = harness(vendor=vendor)
    arguments = {"value": SENTINEL, "fail": fail}
    if fail:
        with pytest.raises(ToolError):
            asyncio.run(app.call_tool("echo", arguments))
    else:
        asyncio.run(app.call_tool("echo", arguments))
    spans = finished(provider, exporter)
    assert len(spans) == 1
    attrs = spans[0].attributes
    assert attrs[ATTRIBUTE] == canonical.args_digest("echo", arguments, KEY)
    assert (
        (json.loads(attrs["_dd.ml_obs.metadata"])["args_digest"] == attrs[ATTRIBUTE])
        if (vendor == "datadog")
        else "_dd.ml_obs.metadata" not in attrs
    )
    text = spans[0].to_json()
    assert SENTINEL not in text
    assert KEY.decode() not in text
    assert observability._INTENT_ATTRIBUTES.get() is None


def test_telemetry_and_pre_coercion_input_at_export_boundary(harness):
    app, provider, exporter, _ = harness()

    async def calls():
        await app.call_tool("echo", {})
        await app.call_tool("echo", {"telemetry": {"intent": "first"}})
        await app.call_tool("echo", {"telemetry": {"intent": "second"}})
        await app.call_tool("echo", {"value": "default"})
        await app.call_tool("real", {"telemetry": {"intent": "first"}})
        await app.call_tool("real", {"telemetry": {"intent": "second"}})
        await app.call_tool("real", {"telemetry": {}, "payload": {"telemetry": 1}})
        await app.call_tool("real", {"telemetry": {}, "payload": {"telemetry": 2}})
        await app.call_tool("api", {"api_args": '{"x":1}'})
        await app.call_tool("api", {"api_args": {"x": 1}})

    asyncio.run(calls())
    values = [span.attributes[ATTRIBUTE] for span in finished(provider, exporter)]
    assert values[:3] == ["336d57c10232fc85802e2e3ce1a8392e"] * 3
    assert values[3] != values[0]
    assert len(set(values[3:])) == 7


@pytest.mark.parametrize("key", [None, "", " \t\n", "\ud800"])
def test_unusable_key_omits_digest_without_hashing(harness, monkeypatch, key):
    app, provider, exporter, _ = harness(key=key)
    digest = Mock(side_effect=AssertionError("must not hash"))
    monkeypatch.setattr(observability, "args_digest", digest)
    asyncio.run(app.call_tool("echo", {"value": SENTINEL}))
    assert ATTRIBUTE not in finished(provider, exporter)[0].attributes
    digest.assert_not_called()


def test_key_bytes_are_exact_and_read_only_once(harness):
    app, provider, exporter, environment = harness(key=" synthetic-key ")
    environment["AIRBYTE_MCP_OTEL_DIGEST_KEY"] = "changed"
    observability.install(app, environ=environment)
    asyncio.run(app.call_tool("echo", {}))
    assert finished(provider, exporter)[0].attributes[ATTRIBUTE] == (
        "d64bd4b2e1f2c196efcb90d4eaf3e0e2"
    )


def test_digest_does_not_require_intent_collection(harness):
    app, provider, exporter, environment = harness()
    environment["AIRBYTE_MCP_INTENT_CAPTURE"] = "0"
    asyncio.run(app.call_tool("echo", {"value": SENTINEL}))
    assert finished(provider, exporter)[0].attributes[
        ATTRIBUTE
    ] == canonical.args_digest("echo", {"value": SENTINEL}, KEY)


def test_disabled_tracing_never_hashes(harness, monkeypatch):
    app, _, exporter, _ = harness(endpoint=False)
    digest = Mock(side_effect=AssertionError("must not hash"))
    monkeypatch.setattr(observability, "args_digest", digest)
    asyncio.run(app.call_tool("echo", {"value": SENTINEL}))
    digest.assert_not_called()
    assert not exporter.get_finished_spans()


def test_failed_provider_installation_never_enables_hashing(harness, monkeypatch):
    monkeypatch.setattr(
        observability, "_build_provider", Mock(side_effect=RuntimeError)
    )
    app, _, exporter, _ = harness()
    digest = Mock(side_effect=AssertionError("must not hash"))
    monkeypatch.setattr(observability, "args_digest", digest)
    asyncio.run(app.call_tool("echo", {}))
    digest.assert_not_called()
    assert not exporter.get_finished_spans()


def test_foreign_provider_refuses_install_before_loading_key(harness, monkeypatch):
    provider = TracerProvider()
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)
    try:
        with pytest.raises(RuntimeError, match="exclusive ownership"):
            harness()
    finally:
        provider.shutdown()


@pytest.mark.parametrize("failure", ["encode", "hash", "update", "attributes"])
def test_observation_failures_do_not_change_dispatch_or_log_payloads(
    harness, monkeypatch, caplog, failure
):
    app, provider, exporter, _ = harness()
    monkeypatch.setattr(logging.getLogger("airbyte"), "propagate", True)
    caplog.set_level(logging.DEBUG, logger="airbyte.mcp")
    with pytest.MonkeyPatch.context() as failure_patch:
        if failure == "encode":

            def broken_encoder(_value):
                yield "["
                raise ValueError(SENTINEL)

            failure_patch.setattr(
                canonical,
                "json",
                Mock(JSONEncoder=Mock(return_value=Mock(iterencode=broken_encoder))),
            )
        elif failure == "hash":
            failure_patch.setattr(
                canonical.hmac, "new", Mock(side_effect=ValueError(SENTINEL))
            )
        elif failure == "update":
            failure_patch.setattr(
                canonical.hmac,
                "new",
                Mock(
                    return_value=Mock(
                        update=Mock(side_effect=[None, ValueError(SENTINEL)])
                    )
                ),
            )
        else:
            failure_patch.setattr(
                observability.IntentCaptureMiddleware,
                "_attributes",
                Mock(side_effect=ValueError(SENTINEL)),
            )
        result = asyncio.run(app.call_tool("echo", {}))
    assert result.content[0].text == "default"
    attrs = finished(provider, exporter)[0].attributes
    assert (ATTRIBUTE in attrs) == (failure == "attributes")
    assert SENTINEL not in caplog.text
    assert KEY.decode() not in caplog.text


def test_http_transport_strips_synthetic_telemetry_before_fingerprinting(harness):
    app, provider, exporter, _ = harness()
    response = asyncio.run(
        _http_rpc(
            app,
            "tools/call",
            {"name": "echo", "arguments": {"telemetry": {"intent": "test"}}},
        )
    )
    assert "error" not in response.json()
    spans = finished(provider, exporter)
    assert spans[0].attributes[ATTRIBUTE] == "336d57c10232fc85802e2e3ce1a8392e"


@pytest.mark.parametrize("lookup", ["missing", "raises", "malformed"])
def test_ambiguous_telemetry_omits_digest_and_still_dispatches(
    harness, monkeypatch, lookup
):
    app, provider, exporter, _ = harness()
    middleware = app.middleware[-1]
    if lookup == "missing":
        monkeypatch.setattr(app, "get_tool", AsyncMock(return_value=None))
    elif lookup == "raises":
        monkeypatch.setattr(
            app, "get_tool", AsyncMock(side_effect=RuntimeError(SENTINEL))
        )
    else:
        monkeypatch.setattr(
            app, "get_tool", AsyncMock(return_value=Mock(parameters={"properties": []}))
        )
    context = MiddlewareContext(
        message=CallToolRequestParams(
            name="echo", arguments={"telemetry": {"intent": "test"}}
        ),
        source="client",
        type="request",
        method="tools/call",
    )

    async def dispatch(_context):
        with provider.get_tracer("fastmcp").start_as_current_span(
            "tools/call echo", kind=SpanKind.SERVER
        ):
            return ToolResult(content="ok")

    assert (
        asyncio.run(middleware.on_call_tool(context, dispatch)).content[0].text == "ok"
    )
    assert ATTRIBUTE not in finished(provider, exporter)[0].attributes
    assert observability._INTENT_ATTRIBUTES.get() is None


@pytest.mark.parametrize(
    "bad", ["unsupported", "cycle", "nodes", "bytes", "surrogate", "root"]
)
def test_rejected_arguments_preserve_other_telemetry_and_never_export_input(
    harness, bad
):
    app, provider, exporter, _ = harness(vendor="datadog")
    arguments = {"value": SENTINEL}
    if bad == "unsupported":
        arguments["secret"] = object()
    elif bad == "cycle":
        arguments["secret"] = arguments
    elif bad == "nodes":
        arguments["secret"] = [None] * 4096
    elif bad == "bytes":
        arguments["secret"] = "x" * 65536
    elif bad == "surrogate":
        arguments["secret"] = "\ud800"
    else:
        arguments = CustomDict(arguments)
    message = CallToolRequestParams.model_construct(name="echo", arguments=arguments)
    context = MiddlewareContext(
        message=message, source="client", type="request", method="tools/call"
    )

    async def dispatch(received):
        assert received.message.arguments is arguments
        with provider.get_tracer("fastmcp").start_as_current_span(
            "tools/call echo", kind=SpanKind.SERVER
        ):
            return ToolResult(content="ok")

    asyncio.run(app.middleware[-1].on_call_tool(context, dispatch))
    span = finished(provider, exporter)[0]
    assert ATTRIBUTE not in span.attributes
    assert span.attributes["airbyte.mcp.tool_module"] == "cloud"
    assert "args_digest" not in json.loads(span.attributes["_dd.ml_obs.metadata"])
    assert SENTINEL not in span.to_json()


@pytest.mark.parametrize(
    "bad", ["A" * 32, "a" * 31, "a" * 33, 123, True, ["a" * 32], SENTINEL]
)
@pytest.mark.parametrize("vendor", ["", "datadog"])
def test_exporter_rejects_bad_digest_values(harness, bad, vendor):
    _, provider, exporter, _ = harness(vendor=vendor)
    with provider.get_tracer("fastmcp").start_as_current_span(
        "tools/call echo", kind=SpanKind.SERVER
    ) as span:
        span.set_attribute(ATTRIBUTE, bad)
    attrs = finished(provider, exporter)[0].attributes
    assert ATTRIBUTE not in attrs
    assert "args_digest" not in json.loads(attrs.get("_dd.ml_obs.metadata", "{}"))
    assert SENTINEL not in json.dumps(dict(attrs))


def test_exporter_rejects_digest_on_child_client_internal_and_unknown_spans(harness):
    _, provider, exporter, _ = harness(vendor="datadog")
    tracer = provider.get_tracer("test")
    candidate = "a" * 32
    with tracer.start_as_current_span("outer"):
        with tracer.start_as_current_span(
            "tools/call echo", kind=SpanKind.SERVER
        ) as span:
            span.set_attribute(ATTRIBUTE, candidate)
    for name, kind in [
        ("tools/call echo", SpanKind.CLIENT),
        ("tools/call echo", SpanKind.INTERNAL),
        ("tools/call unknown", SpanKind.SERVER),
        ("other", SpanKind.SERVER),
    ]:
        with tracer.start_as_current_span(name, kind=kind) as span:
            span.set_attribute(ATTRIBUTE, candidate)
    spans = finished(provider, exporter)
    assert spans
    assert all(ATTRIBUTE not in span.attributes for span in spans)
    assert all(
        "args_digest"
        not in json.loads(span.attributes.get("_dd.ml_obs.metadata", "{}"))
        for span in spans
    )


def test_concurrency_nesting_cancellation_and_context_reset(harness):
    app, provider, exporter, _ = harness()
    middleware = app.middleware[-1]
    seen = {}

    async def call(value, next_call):
        context = MiddlewareContext(
            message=CallToolRequestParams(name="echo", arguments={"value": value}),
            source="client",
            type="request",
            method="tools/call",
        )
        return await middleware.on_call_tool(context, next_call)

    async def dispatch(context):
        before = dict(observability._INTENT_ATTRIBUTES.get())
        value = context.message.arguments["value"]
        await asyncio.sleep(0)
        if value == "outer":
            await call("inner", dispatch)
        if value == "cancel":
            raise asyncio.CancelledError
        assert observability._INTENT_ATTRIBUTES.get() == before
        seen[value] = before[ATTRIBUTE]
        with provider.get_tracer("fastmcp").start_as_current_span(
            "tools/call echo", context=Context(), kind=SpanKind.SERVER
        ):
            return ToolResult(content="ok")

    async def calls():
        await asyncio.gather(
            *(call(value, dispatch) for value in ["first", "second", "outer"])
        )
        with pytest.raises(asyncio.CancelledError):
            await call("cancel", dispatch)
        assert observability._INTENT_ATTRIBUTES.get() is None
        await call("after", dispatch)
        assert observability._INTENT_ATTRIBUTES.get() is None

    asyncio.run(calls())
    assert len(set(seen.values())) == 5
    for value, digest in seen.items():
        assert digest == canonical.args_digest("echo", {"value": value}, KEY)
    assert {span.attributes[ATTRIBUTE] for span in finished(provider, exporter)} == set(
        seen.values()
    )


def test_actual_fastmcp_concurrent_nested_and_cancelled_spans(harness, monkeypatch):
    app, provider, exporter, _ = harness(vendor="datadog")

    @app.tool()
    async def nested(value: str) -> str:
        await app.call_tool("echo", {"value": value})
        return "ok"

    @app.tool()
    async def cancelled() -> str:
        raise asyncio.CancelledError

    monkeypatch.setitem(observability._TOOL_MODULES, "nested", "cloud")
    monkeypatch.setitem(observability._TOOL_MODULES, "cancelled", "cloud")

    async def calls():
        await asyncio.gather(
            *(app.call_tool("echo", {"value": str(i)}) for i in range(5))
        )
        await app.call_tool("nested", {"value": SENTINEL})
        with pytest.raises(asyncio.CancelledError):
            await app.call_tool("cancelled", {})
        assert observability._INTENT_ATTRIBUTES.get() is None
        await app.call_tool("echo", {})
        assert observability._INTENT_ATTRIBUTES.get() is None

    asyncio.run(calls())
    spans = finished(provider, exporter)
    roots = [span for span in spans if span.parent is None]
    children = [span for span in spans if span.parent is not None]
    assert len(roots) == 8 and len(children) == 1
    assert ATTRIBUTE not in children[0].attributes
    assert "args_digest" not in json.loads(
        children[0].attributes.get("_dd.ml_obs.metadata", "{}")
    )
    expected = {canonical.args_digest("echo", {"value": str(i)}, KEY) for i in range(5)}
    expected.update([
        canonical.args_digest("nested", {"value": SENTINEL}, KEY),
        canonical.args_digest("cancelled", {}, KEY),
        "336d57c10232fc85802e2e3ce1a8392e",
    ])
    assert {span.attributes[ATTRIBUTE] for span in roots} == expected
    assert all(SENTINEL not in span.to_json() for span in spans)
