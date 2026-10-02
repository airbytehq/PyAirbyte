# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Argument tracing through the real middleware and both export boundaries."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import random
import threading
from collections.abc import Iterator
from typing import Literal
from unittest.mock import Mock

import fastmcp.client.telemetry
import fastmcp.server.dependencies
import fastmcp.telemetry
import mcp.shared._otel
import pytest
from fastmcp import Client, FastMCP
from fastmcp.server.auth import AccessToken
from fastmcp.tools import ToolResult
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from airbyte._direct_connectors import api_util as agents_api
from airbyte._direct_connectors.models import ExternalApiExecuteResult
from airbyte.cloud.connectors import CloudConnector
from airbyte.mcp import _arg_trace, _datadog, _telemetry, cloud
from airbyte.mcp import _otel as observability
from airbyte.mcp._telemetry import GroupingId
from airbyte.registry import ConnectorType
from tests.unit_tests import test_mcp_otel as otel_tests


@pytest.fixture(autouse=True)
def _propagate_airbyte_logs(monkeypatch):
    monkeypatch.setattr(logging.getLogger("airbyte"), "propagate", True)


agents_app = otel_tests.agents_app
isolated_otel = otel_tests.isolated_otel

ARG = _arg_trace.ARG_PREFIX
KEY = bytes(range(0x40, 0x60))
ISSUER = "https://issuer.example.test/realms/test"
API_TOOL = "execute_external_api_query"
PRIVACY_TOOLS = (
    API_TOOL,
    "execute_external_sql_query",
    "execute_external_search_query",
    "deploy_connector_to_cloud",
    "publish_custom_source_definition",
)


@pytest.fixture(scope="module")
def otel_provider() -> Iterator[tuple[TracerProvider, InMemorySpanExporter]]:
    exporter = InMemorySpanExporter()
    provider = observability._build_provider(exporter)
    yield provider, exporter
    provider.shutdown()


@pytest.fixture(params=["otel", "datadog-otlp"])
def backend(request, monkeypatch) -> str:
    monkeypatch.setenv("AIRBYTE_MCP_TRACING_BACKEND", request.param)
    return request.param


@pytest.fixture
def export(monkeypatch, isolated_otel, backend):
    """Real processors and exporter; in-process clients never parent server spans."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(observability.IntentStampProcessor())
    provider.add_span_processor(
        SimpleSpanProcessor(observability.RedactingExporter(exporter))
    )
    monkeypatch.setattr(fastmcp.telemetry, "otel_get_tracer", provider.get_tracer)
    monkeypatch.setattr(trace, "get_tracer", provider.get_tracer)
    monkeypatch.setattr(
        fastmcp.client.telemetry, "get_tracer", lambda *_, **__: trace.NoOpTracer()
    )
    monkeypatch.setattr(mcp.shared._otel, "_tracer", trace.NoOpTracer())
    yield provider, exporter
    provider.shutdown()


def _finished(export):
    provider, exporter = export
    assert provider.force_flush()
    return exporter.get_finished_spans()


def _root(export, tool: str):
    spans = [
        span
        for span in _finished(export)
        if span.name == f"tools/call {tool}" and span.parent is None
    ]
    assert len(spans) == 1, [span.name for span in _finished(export)]
    return dict(spans[0].attributes)


def _new_keys(attrs) -> dict:
    return {key: value for key, value in attrs.items() if _arg_trace.is_new_key(key)}


def _jwt(claims: dict) -> str:
    def part(data: dict) -> str:
        raw = json.dumps(data).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    return f"{part({'alg': 'none'})}.{part(claims)}.sig"


def _verified(monkeypatch, token: AccessToken | None) -> None:
    monkeypatch.setattr(fastmcp.server.dependencies, "get_access_token", lambda: token)


def _token(
    subject: str = "user-1", issuer: str | None = ISSUER, **claims
) -> AccessToken:
    payload = {"sub": subject, **claims}
    token_claims = (
        {"sub": subject} if issuer is None else {"iss": issuer, "sub": subject}
    )
    return AccessToken(
        token=_jwt(payload), client_id="client", scopes=[], claims=token_claims
    )


def _keyed(monkeypatch, principal: str = "user-1", grouping: GroupingId | None = None):
    monkeypatch.setattr(observability, "_ARG_MASTER", KEY)
    _verified(monkeypatch, _token(principal))
    if grouping is not None:
        monkeypatch.setattr(_telemetry, "current_grouping_id", lambda: grouping)


def _stub_api(monkeypatch, status: str = "success", side_effect=None) -> Mock:
    connector = CloudConnector(
        workspace=Mock(), connector_id="c", connector_type=ConnectorType.SOURCE
    )
    execute = Mock(
        return_value=ExternalApiExecuteResult(status=status, result={"rows": []}),
        side_effect=side_effect,
    )
    monkeypatch.setattr(agents_api, "execute_cloud_connector_action", execute)
    monkeypatch.setattr(
        cloud,
        "_get_cloud_workspace",
        lambda *args, **kwargs: Mock(get_connector=Mock(return_value=connector)),
    )
    return execute


async def _calls(app, calls):
    async with Client(app) as client:
        return [
            await client.call_tool(tool, arguments, raise_on_error=False)
            for tool, arguments in calls
        ]


def _run(app, *calls):
    return asyncio.run(_calls(app, calls))


def _record(attrs, name: str) -> dict:
    return json.loads(attrs[ARG + name])


def _sentinel() -> str:
    rng = random.SystemRandom()
    return "SENTINELqzx" + "".join(
        rng.choice("ghijklmnopqrstuvwxyz") for _ in range(16)
    )


def _sentinel_arguments(tool: str, sentinel: str) -> dict:
    values = {
        "api_args": {"filter": sentinel, "nested": [sentinel]},
        "config": {"password": sentinel},
        "testing_values": {"token": sentinel},
        "manifest_yaml": f"version: {sentinel}",
        "sql": f"select '{sentinel}' from t",
        "select_fields": [sentinel, "id"],
        "exclude_fields": [sentinel],
        "streams": [{"name": sentinel}],
        "entity_type": "contacts",
    }
    classes = observability._TOOL_ARG_CLASSES[tool]
    arguments: dict[str, object] = {
        name: values.get(name, sentinel)
        for name, cls in classes.items()
        if cls.cat is not _arg_trace.Cat.SKIP
    }
    arguments[sentinel] = sentinel
    return arguments


def _assert_absent(sentinel: str, text: str) -> None:
    # Every longer substring contains a six-character substring.
    for start in range(len(sentinel) - 5):
        assert sentinel[start : start + 6] not in text


@pytest.mark.parametrize("keyed", [False, True])
@pytest.mark.parametrize("tool", PRIVACY_TOOLS)
def test_privacy_sentinel_end_to_end(agents_app, export, monkeypatch, tool, keyed):
    if keyed:
        _keyed(monkeypatch)
    sentinel = _sentinel()
    arguments = _sentinel_arguments(tool, sentinel)
    _run(agents_app, (tool, {**arguments, "intent": "synthetic intent"}))
    attrs = _root(export, tool)
    # Positive control: every supplied registered argument has a record.
    expected = {ARG + name for name in arguments if name != sentinel}
    assert expected <= set(attrs)
    assert attrs[_arg_trace.TRACING_KEY] == ("ok" if keyed else "no_key")
    assert attrs["airbyte.mcp.intent"] == "synthetic intent"
    if tool == API_TOOL:
        assert _record(attrs, "entity_type") == {"value": "contacts"}
    for name, value in arguments.items():
        if name in {sentinel, "entity_type"}:
            continue
        record = _record(attrs, name)
        assert "value" not in record, name
        if observability._TOOL_ARG_CLASSES[tool][name].cat is _arg_trace.Cat.PRESENT:
            assert record == {"present": True}
        if not keyed:
            assert not {"eq", "fp"} & record.keys()
    text = "\n".join(span.to_json() for span in _finished(export))
    _assert_absent(sentinel, text)


def test_registered_records_have_specified_shapes(
    agents_app, export, monkeypatch, backend
):
    _keyed(monkeypatch)
    _stub_api(monkeypatch)
    _run(
        agents_app,
        (
            API_TOOL,
            {
                "connector_id": "11111111-1111-1111-1111-111111111111",
                "entity_type": "contacts",
                "action": "list",
                "api_args": {"q": "x"},
                "page_size": 0,
                "skip_truncation": False,
                "select_fields": ["id", "email", "id"],
                "cursor": "",
            },
        ),
    )
    attrs = _root(export, API_TOOL)
    assert _record(attrs, "action") == {"value": "list"}
    assert _record(attrs, "page_size") == {"value": 0}
    assert _record(attrs, "skip_truncation") == {"value": False}
    assert _record(attrs, "entity_type") == {"valid": True, "value": "contacts"}
    assert attrs[_arg_trace.ENTITY_VALID_KEY] is True
    fields = _record(attrs, "select_fields")
    assert fields.keys() == {"count", "eq", "fp"} and fields["count"] == 3
    for name in ("connector_id", "api_args", "cursor"):
        assert _record(attrs, name).keys() == {"eq"}, name
    assert attrs[_arg_trace.KEY_SCOPE_KEY] == "approximate"
    assert len(attrs[_arg_trace.SCOPE_ID_KEY]) == 16
    assert attrs[_arg_trace.RESULT_ERROR_LIKE_KEY] is False
    assert _arg_trace.DROPPED_KEY not in attrs
    metadata = json.loads(attrs.get("_dd.ml_obs.metadata", "{}"))
    new_metadata = {
        key for key in metadata if _arg_trace.is_new_key("airbyte.mcp." + key)
    }
    expected_metadata = (
        _arg_trace.LLMOBS_ALLOWED if backend == "datadog-otlp" else set()
    )
    assert new_metadata == expected_metadata


def _eq(attrs, name):
    return _record(attrs, name)["eq"]


def test_scope_ids_differ_across_principals_and_groupings(
    agents_app, export, monkeypatch
):
    _stub_api(monkeypatch)
    call = (API_TOOL, {"connector_id": "conn-a", "entity_type": "c", "action": "list"})
    scopes = []
    for principal, grouping in (
        ("user-1", GroupingId("transport_session", "a" * 64)),
        ("user-1", GroupingId("transport_session", "a" * 64)),
        ("user-2", GroupingId("transport_session", "a" * 64)),
        ("user-1", GroupingId("transport_session", "b" * 64)),
        ("user-1", GroupingId("none", None)),
    ):
        _keyed(monkeypatch, principal, grouping)
        _run(agents_app, call)
        attrs = [dict(s.attributes) for s in _finished(export) if s.parent is None][-1]
        scopes.append((
            attrs[_arg_trace.KEY_SCOPE_KEY],
            attrs[_arg_trace.SCOPE_ID_KEY],
            _eq(attrs, "connector_id"),
        ))
    assert scopes[0] == scopes[1]
    assert scopes[0][0] == "transport_session" and scopes[4][0] == "approximate"
    assert len({scope[1] for scope in scopes}) == 4
    assert len({scope[2] for scope in scopes}) == 4


def _assert_keyless(attrs, state: str, values=()):
    assert attrs[_arg_trace.TRACING_KEY] == state
    assert attrs[_arg_trace.KEY_SCOPE_KEY] == "none"
    assert _arg_trace.SCOPE_ID_KEY not in attrs
    for key, value in attrs.items():
        if key.startswith(ARG):
            assert not {"eq", "fp"} & json.loads(value).keys()
    blob = json.dumps(attrs)
    for value in values:
        assert hashlib.sha256(value.encode()).hexdigest()[:16] not in blob


@pytest.mark.parametrize(
    "token",
    [
        None,
        _token(issuer=None),
        _token(subject="user\x00x"),
        AccessToken(token="opaque", client_id="c", scopes=[], claims={"iss": ISSUER}),
    ],
    ids=["no-token", "no-iss", "nul-subject", "opaque"],
)
def test_no_verified_principal_is_no_scope(agents_app, export, monkeypatch, token):
    monkeypatch.setattr(observability, "_ARG_MASTER", KEY)
    _verified(monkeypatch, token)
    _run(agents_app, (API_TOOL, {"connector_id": "conn-a", "entity_type": "c"}))
    _assert_keyless(_root(export, API_TOOL), "no_scope", ["conn-a"])


def test_forged_authorization_header_without_verifier_is_no_scope(
    agents_app, export, monkeypatch
):
    monkeypatch.setattr(observability, "_ARG_MASTER", KEY)
    forged = _jwt({"iss": ISSUER, "sub": "forged"})
    asyncio.run(
        otel_tests._http_rpc(
            agents_app,
            "tools/call",
            {"name": API_TOOL, "arguments": {"connector_id": "conn-a"}},
            headers={"authorization": f"Bearer {forged}"},
        )
    )
    attrs = [dict(s.attributes) for s in _finished(export) if s.parent is None]
    assert attrs[-1][_arg_trace.TRACING_KEY] == "no_scope"


def test_no_key_is_keyless_even_with_principal(agents_app, export, monkeypatch):
    _verified(monkeypatch, _token())
    _run(agents_app, (API_TOOL, {"connector_id": "conn-a", "cursor": "cur-1"}))
    _assert_keyless(_root(export, API_TOOL), "no_key", ["conn-a", "cur-1", '"conn-a"'])


def test_client_credentials_subject_is_a_principal(agents_app, export, monkeypatch):
    """An exchanged application token is verified like any bearer; its `sub` is used."""
    monkeypatch.setattr(observability, "_ARG_MASTER", KEY)
    _verified(monkeypatch, _token(subject="application-client-id"))
    _run(agents_app, (API_TOOL, {"connector_id": "conn-a"}))
    attrs = _root(export, API_TOOL)
    assert attrs[_arg_trace.TRACING_KEY] == "ok"
    assert attrs[_arg_trace.KEY_SCOPE_KEY] == "approximate"


def test_hand_made_session_id_with_verified_token_is_approximate(
    agents_app, export, monkeypatch
):
    monkeypatch.setattr(observability, "_ARG_MASTER", KEY)
    _verified(monkeypatch, _token())
    asyncio.run(
        otel_tests._http_rpc(
            agents_app,
            "tools/call",
            {"name": API_TOOL, "arguments": {"connector_id": "conn-a"}},
            headers={"mcp-session-id": "test"},
        )
    )
    attrs = [dict(s.attributes) for s in _finished(export) if s.parent is None][-1]
    assert attrs[_arg_trace.KEY_SCOPE_KEY] == "approximate"


def test_key_is_loaded_without_logging_its_value(monkeypatch, caplog):
    raw = base64.urlsafe_b64encode(KEY).decode().rstrip("=")
    monkeypatch.setenv("AIRBYTE_MCP_TELEMETRY_HMAC_KEY", raw)
    with caplog.at_level(logging.INFO, logger="airbyte.mcp._otel"):
        observability._build_tool_maps()
    assert observability._ARG_MASTER == KEY
    assert f"key_id={_arg_trace.key_id(KEY)}" in caplog.text
    assert raw not in caplog.text


def test_unregistered_tool_gets_no_arg_tracing(export, monkeypatch):
    app = FastMCP("unregistered")

    @app.tool()
    def echo(value: str = "") -> str:
        return value

    otel_tests._capture(app)
    monkeypatch.setitem(observability._TOOL_MODULES, "echo", "cloud")
    sentinel = _sentinel()
    _run(app, ("echo", {"value": "v", sentinel: sentinel}))
    attrs = _root(export, "echo")
    assert not _new_keys(attrs)
    _assert_absent(sentinel, "\n".join(s.to_json() for s in _finished(export)))


@pytest.fixture
def lookup_app(monkeypatch):
    app = FastMCP("lookup")

    @app.tool()
    def lookup(
        mode: Literal["found", "missing", "error", "raise"] = "found",
    ) -> ToolResult:
        if mode == "raise":
            raise ValueError("boom")
        if mode == "error":
            return ToolResult(content="Not found.", is_error=True)
        return ToolResult(content="Not found." if mode == "missing" else "a row")

    otel_tests._capture(app)
    monkeypatch.setitem(observability._TOOL_MODULES, "lookup", "cloud")
    monkeypatch.setitem(
        observability._TOOL_ARG_CLASSES,
        "lookup",
        _arg_trace.classify_tool(lookup.fn if hasattr(lookup, "fn") else lookup),
    )
    monkeypatch.setitem(
        observability._TOOL_ERROR_STRINGS, "lookup", frozenset({"Not found."})
    )
    return app


def test_result_error_like_is_structural_and_separate_from_outcome(lookup_app, export):
    _run(
        lookup_app,
        ("lookup", {"mode": "missing"}),
        ("lookup", {"mode": "found"}),
        ("lookup", {"mode": "error"}),
        ("lookup", {"mode": "raise"}),
    )
    spans = [dict(s.attributes) for s in _finished(export) if s.parent is None]
    assert [s[_arg_trace.RESULT_ERROR_LIKE_KEY] for s in spans] == [
        True,
        False,
        False,
        False,
    ]
    assert spans[0]["airbyte.mcp.outcome"] == "success"
    assert spans[2]["airbyte.mcp.outcome"] == "tool_error"
    assert spans[3]["airbyte.mcp.outcome"] == "exception"
    assert _record(spans[0], "mode") == {"value": "missing"}


def test_build_records_failure_keeps_existing_attributes_and_logs_no_values(
    agents_app, export, monkeypatch, caplog
):
    sentinel = _sentinel()
    _stub_api(monkeypatch)

    def explode(*_args, **_kwargs):
        raise ValueError(sentinel)

    monkeypatch.setattr(_arg_trace, "build_records", explode)
    with caplog.at_level(logging.DEBUG, logger="airbyte.mcp._otel"):
        results = _run(
            agents_app,
            (
                API_TOOL,
                {
                    "connector_id": "c",
                    "entity_type": "contacts",
                    "action": "list",
                    "api_args": {"q": sentinel},
                    "intent": "kept intent",
                },
            ),
        )
    assert not results[0].is_error
    attrs = _root(export, API_TOOL)
    assert attrs[_arg_trace.TRACING_KEY] == "error"
    assert attrs[_arg_trace.KEY_SCOPE_KEY] == "none"
    assert not any(key.startswith(ARG) for key in attrs)
    assert attrs["airbyte.mcp.intent"] == "kept intent"
    assert attrs["airbyte.mcp.agent.action"] == "list"
    assert "Argument tracing failed: ValueError" in caplog.text
    _assert_absent(sentinel, caplog.text)


def test_span_attribute_hook_failure_never_changes_tool_result(
    agents_app, export, monkeypatch
):
    _stub_api(monkeypatch)
    monkeypatch.setattr(
        observability, "_record_late_attributes", Mock(side_effect=RuntimeError("x"))
    )
    results = _run(
        agents_app, (API_TOOL, {"connector_id": "c", "entity_type": "contacts"})
    )
    assert not results[0].is_error


@pytest.mark.parametrize(
    ("status", "side_effect", "entity"),
    [
        ("warning", None, "contacts"),
        ("success", TimeoutError("timeout"), "contacts"),
        ("success", None, "contacts\x00"),
    ],
)
def test_entity_valid_absent_on_failure_or_unbounded_entity(
    agents_app, export, monkeypatch, status, side_effect, entity
):
    _stub_api(monkeypatch, status=status, side_effect=side_effect)
    _run(agents_app, (API_TOOL, {"connector_id": "c", "entity_type": entity}))
    attrs = _root(export, API_TOOL)
    record = _record(attrs, "entity_type")
    if entity == "contacts\x00":
        assert record == {"present": True}
    else:
        assert record == {"value": entity}
    assert _arg_trace.ENTITY_VALID_KEY not in attrs


def _start(provider, name, attributes, kind=SpanKind.SERVER):
    return provider.get_tracer("arg-trace-test").start_as_current_span(
        name, kind=kind, attributes=attributes
    )


def test_exporter_validates_forged_non_root_and_orphan_flat(export):
    provider, _ = export
    observability._build_tool_maps()
    name = f"tools/call {API_TOOL}"
    sentinel = _sentinel()
    base = {
        _arg_trace.TRACING_KEY: "no_key",
        _arg_trace.KEY_SCOPE_KEY: "none",
    }
    with _start(
        provider, name, {**base, ARG + "page_size": f'{{"value":"{sentinel}"}}'}
    ):
        pass
    with _start(provider, name, base):
        observability.record_tool_span_attributes({_arg_trace.ENTITY_VALID_KEY: True})
    with _start(provider, "parent", {}, kind=SpanKind.INTERNAL):
        with _start(provider, name, {**base, ARG + "page_size": '{"value":5}'}):
            pass
    with _start(
        provider, "tools/call unregistered", {**base, ARG + "x": '{"present":true}'}
    ):
        pass
    spans = _finished(export)
    forged, orphan = (dict(s.attributes) for s in spans[:2])
    assert ARG + "page_size" not in forged
    assert forged[_arg_trace.DROPPED_KEY] == 1
    assert _arg_trace.ENTITY_VALID_KEY not in orphan
    assert _arg_trace.DROPPED_KEY not in orphan
    for span in spans[2:]:
        assert not _new_keys(span.attributes), span.name
    _assert_absent(sentinel, "\n".join(s.to_json() for s in _finished(export)))


def test_late_attributes_are_thread_safe(export):
    provider, _ = export
    observability._build_tool_maps()
    errors: list[BaseException] = []

    def work():
        try:
            for _ in range(200):
                with _start(provider, f"tools/call {API_TOOL}", {}):
                    observability.record_tool_span_attributes({"airbyte.mcp.x": "1"})
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=work) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    assert not observability._LATE_ATTRIBUTES
    assert len(_finished(export)) == 1600


# ----------------------------------------------------------------- native Datadog


class _FakeSpan:
    def __init__(self) -> None:
        self.tags: dict[str, str] = {}
        self.metrics: dict[str, int] = {}

    def set_tags(self, tags) -> None:
        self.tags.update(tags)

    def set_tag(self, key, value) -> None:
        self.tags[key] = value

    def set_metric(self, key, value) -> None:
        self.metrics[key] = value


@pytest.fixture
def native(monkeypatch, isolated_otel):
    from ddtrace import tracer
    from ddtrace.llmobs import LLMObs

    from airbyte.mcp import server

    span = _FakeSpan()
    annotations: list[dict] = []
    monkeypatch.setattr(tracer, "current_span", lambda: span)
    monkeypatch.setattr(
        LLMObs,
        "annotate",
        lambda _span, metadata=None, **_: annotations.append(metadata),
    )
    monkeypatch.setenv("AIRBYTE_MCP_INSIDERS", "1")
    monkeypatch.setattr(server.app, "middleware", list(server.app.middleware))
    monkeypatch.setattr(
        server.app._mcp_server, "middleware", list(server.app._mcp_server.middleware)
    )
    observability._build_tool_maps()
    server.app.add_middleware(_datadog._DatadogIntentMiddleware(server.app))
    return server.app, span, annotations


@pytest.mark.parametrize("keyed", [False, True])
def test_native_parity_types_and_llmobs_allowlist(native, monkeypatch, keyed):
    app, span, annotations = native
    if keyed:
        _keyed(monkeypatch)
    _stub_api(monkeypatch)
    sentinel = _sentinel()
    arguments = {
        "connector_id": sentinel,
        "entity_type": "contacts",
        "action": "list",
        "api_args": {"q": sentinel},
        "page_size": 7,
        "skip_truncation": True,
    }
    _run(app, (API_TOOL, arguments))
    expected = {ARG + name for name in arguments}
    assert expected <= span.tags.keys()
    assert span.tags[_arg_trace.TRACING_KEY] == ("ok" if keyed else "no_key")
    assert span.tags[_arg_trace.ENTITY_VALID_KEY] == "true"
    assert span.tags[_arg_trace.RESULT_ERROR_LIKE_KEY] == "false"
    assert json.loads(span.tags[ARG + "entity_type"]) == {
        "valid": True,
        "value": "contacts",
    }
    metadata = annotations[-1]
    new_metadata = {
        key for key in metadata if _arg_trace.is_new_key("airbyte.mcp." + key)
    }
    assert new_metadata == _arg_trace.LLMOBS_ALLOWED
    blob = json.dumps([span.tags, span.metrics, annotations], default=str)
    _run(app, (API_TOOL, {**arguments, sentinel: sentinel}))
    blob += json.dumps([span.tags, span.metrics, annotations], default=str)
    _assert_absent(sentinel, blob)


def test_native_rejects_forged_keys_and_uses_metrics():
    from unittest.mock import patch

    from ddtrace.llmobs import LLMObs

    observability._build_tool_maps()
    span = _FakeSpan()
    with patch.object(LLMObs, "annotate") as annotate:
        _datadog._annotate_attributes(
            span,
            {
                "gen_ai.tool.name": API_TOOL,
                "airbyte.mcp.intent": "i",
                _arg_trace.TRACING_KEY: "no_key",
                _arg_trace.KEY_SCOPE_KEY: "none",
                ARG + "page_size": '{"value":"forged"}',
                "airbyte.mcp.arg_foo": "x",
            },
        )
    assert span.metrics == {_arg_trace.DROPPED_KEY: 2}
    assert ARG + "page_size" not in span.tags
    assert "airbyte.mcp.arg_foo" not in span.tags
    assert span.tags["airbyte.mcp.intent"] == "i"
    metadata = annotate.call_args.kwargs["metadata"]
    assert "arg_foo" not in metadata
    assert metadata["arg_tracing"] == "no_key"
