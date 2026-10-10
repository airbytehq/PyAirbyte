# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Hosted OpenTelemetry tracing contracts at the fastmcp-extensions boundary."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from collections.abc import Callable, Iterator, Mapping, Sequence
from unittest.mock import AsyncMock

import pytest
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp_extensions import (
    TelemetryConfig,
    ToolCallTelemetryMiddleware,
    ToolTracingConfig,
    capture_tool_spans,
    mcp_server,
    register_tool_call_telemetry,
)
from fastmcp_extensions import UserFacingErrorMiddleware
from opentelemetry import trace
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.trace import SpanKind
from opentelemetry.util.types import AttributeValue

from airbyte._util import meta
from airbyte.exceptions import AirbyteConnectionSyncError
from airbyte.constants import CLOUD_API_ROOT
from airbyte.mcp import _otel as observability
from airbyte.mcp._error_handling import (
    MCP_TOOL_USER_FACING_ERRORS,
    AgentErrorTextMiddleware,
    classify_mcp_tool_error,
    format_user_facing_error,
    mcp_tool_error_reason,
)
from airbyte.mcp import _scope
from airbyte.mcp._scope import (
    CallScopeMiddleware,
    call_scope_properties,
    record_default_workspace,
    record_resolved_organization,
)
from airbyte.mcp._telemetry import request_properties


WORKSPACE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
ORGANIZATION_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


@pytest.fixture(autouse=True)
def _disable_remote_export(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name in (
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "AIRBYTE_CLOUD_WORKSPACE_ID",
        "AIRBYTE_CLOUD_ORGANIZATION_ID",
        "AIRBYTE_CLOUD_CLIENT_ID",
        "AIRBYTE_CLOUD_CLIENT_SECRET",
        "AIRBYTE_CLIENT_ID",
        "AIRBYTE_CLIENT_SECRET",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    observability._reset_for_tests()
    yield
    observability._reset_for_tests()


def _app(
    name: str,
    *,
    extra_properties: Callable[[], Mapping[str, object]]
    | Mapping[str, object]
    | None = None,
) -> FastMCP:
    return mcp_server(
        display_name=name,
        telemetry=TelemetryConfig(
            package_name="airbyte",
            extra_properties=extra_properties,
        ),
    )


def _register_tracing(
    app: FastMCP,
    *,
    attributes: (
        Mapping[str, AttributeValue] | Callable[[], Mapping[str, AttributeValue]] | None
    ) = observability._trace_attributes,
    shared_properties: tuple[str, ...] = (),
    capture_intent: bool = False,
    other_spans: Callable[[ReadableSpan], Mapping[str, object] | None] | None = None,
    arg_key: Callable[[], bytes | None] | None = None,
) -> None:
    register_tool_call_telemetry(
        app,
        TelemetryConfig(
            package_name="airbyte",
            tool_tracing=ToolTracingConfig(
                attribute_prefix="airbyte.mcp",
                attributes=attributes,
                shared_properties=shared_properties,
                capture_intent=capture_intent,
                other_spans=other_spans,
                arg_key=arg_key,
            ),
        ),
    )


def _tool_span(spans: Sequence[ReadableSpan], name: str) -> ReadableSpan:
    return next(span for span in spans if span.name == f"tools/call {name}")


@pytest.mark.parametrize(
    ("headers", "expected_application_name"),
    [
        pytest.param(
            {"x-airbyte-application-name": "My Agent!"},
            "my-agent",
            id="declared-application-name",
        ),
        pytest.param({}, None, id="no-application-name"),
    ],
)
def test_trace_attributes_include_application_name_when_declared(
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str],
    expected_application_name: str | None,
) -> None:
    monkeypatch.setattr(meta, "_MCP_MODE_ENABLED", True)
    monkeypatch.setattr(meta, "get_http_headers", lambda: headers)

    attributes = observability._trace_attributes()

    assert "analytic_source" in attributes
    if expected_application_name is None:
        assert "application_name" not in attributes
    else:
        assert attributes["application_name"] == expected_application_name


class _Collector(SpanExporter):
    def __init__(self) -> None:
        self.spans: list[ReadableSpan] = []

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        self.spans.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        pass


def test_existing_telemetry_properties_are_shared_after_tool_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        monkeypatch.setattr(
            _scope,
            "resolve_user_default_organization_id",
            AsyncMock(return_value=None),
        )
        monkeypatch.setattr(
            meta, "get_cloud_api_analytic_source", lambda: "mcp-tracing-test"
        )
        resolved_properties: list[dict[str, object]] = []

        def extra_properties() -> dict[str, object]:
            properties = {**request_properties(), **call_scope_properties()}
            resolved_properties.append(properties)
            return properties

        app = _app("shared-properties", extra_properties=extra_properties)
        app.middleware.insert(0, CallScopeMiddleware())

        @app.tool()
        def resolve_scope() -> str:
            record_default_workspace(WORKSPACE_ID)
            record_resolved_organization(ORGANIZATION_ID, workspace_id=WORKSPACE_ID)
            return "resolved"

        telemetry_count = sum(
            isinstance(middleware, ToolCallTelemetryMiddleware)
            for middleware in app.middleware
        )
        _register_tracing(
            app,
            shared_properties=(
                "workspace_id",
                "organization_id",
                "scope_source",
                "auth_method",
            ),
        )
        assert (
            sum(
                isinstance(middleware, ToolCallTelemetryMiddleware)
                for middleware in app.middleware
            )
            == telemetry_count
        )

        with capture_tool_spans() as spans:
            async with Client(app) as client:
                await client.call_tool("resolve_scope")

        attributes = _tool_span(spans, "resolve_scope").attributes or {}
        assert attributes["airbyte.mcp.workspace_id"] == WORKSPACE_ID
        assert attributes["airbyte.mcp.organization_id"] == ORGANIZATION_ID
        assert attributes["airbyte.mcp.scope_source"] == "default"
        assert attributes["airbyte.mcp.auth_method"] == "none"
        assert attributes["airbyte.mcp.analytic_source"] == "mcp-tracing-test"
        assert any(
            properties.get("workspace_id") == WORKSPACE_ID
            and properties.get("organization_id") == ORGANIZATION_ID
            for properties in resolved_properties
        )

    asyncio.run(scenario())


def test_hosted_install_enables_opt_in_intent_from_environ(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        app = _app("intent-capture")

        @app.tool()
        def echo(value: str) -> str:
            return value

        with monkeypatch.context() as patch:
            patch.setattr(
                observability.trace,
                "get_tracer_provider",
                lambda: trace.ProxyTracerProvider(),
            )
            patch.setattr(
                RequestsInstrumentor,
                "is_instrumented_by_opentelemetry",
                property(lambda _self: False),
            )
            observability.install(
                app,
                environ={
                    "AIRBYTE_MCP_TRACING_BACKEND": "otel",
                    "AIRBYTE_MCP_INTENT_CAPTURE": "1",
                },
            )

        with capture_tool_spans() as spans:
            async with Client(app) as client:
                result = await client.call_tool(
                    "echo",
                    {"value": "private-value", "intent": "Find the right record"},
                )

        assert result.content[0].text == "private-value"
        attributes = _tool_span(spans, "echo").attributes or {}
        assert attributes["airbyte.mcp.intent"] == "Find the right record"
        assert attributes["airbyte.mcp.intent_present"] is True

    asyncio.run(scenario())


def test_requests_spans_keep_allowlisted_urls_and_redact_queries() -> None:
    async def scenario() -> None:
        app = _app("requests-allowlist")

        @app.tool()
        def echo() -> str:
            return "ok"

        _register_tracing(app, other_spans=observability._http_client_span_attributes)
        tracer = trace.get_tracer("opentelemetry.instrumentation.requests")
        with capture_tool_spans() as spans:
            async with Client(app) as client:
                await client.call_tool("echo")
            with tracer.start_as_current_span(
                "safe-request", kind=SpanKind.CLIENT
            ) as span:
                span.set_attribute(
                    "http.url",
                    f"{CLOUD_API_ROOT}/applications/token?access_token=private",
                )
                span.set_attribute("http.request.method", "POST")
                span.set_attribute("url.query", "access_token=private")
                span.set_attribute("http.user_agent", "private-user-agent")
                span.set_attribute("server.address", "private-host")
            with tracer.start_as_current_span(
                "unknown-request", kind=SpanKind.CLIENT
            ) as span:
                span.set_attribute(
                    "url.full",
                    f"{CLOUD_API_ROOT}/private-route?token=private",
                )

        safe = next(span for span in spans if span.name == "safe-request")
        unknown = next(span for span in spans if span.name == "unknown-request")
        safe_attributes = safe.attributes or {}
        assert safe_attributes["http.url"] == f"{CLOUD_API_ROOT}/applications/token"
        assert safe_attributes["http.request.method"] == "POST"
        assert "url.query" not in safe_attributes
        assert "http.user_agent" not in safe_attributes
        assert "server.address" not in safe_attributes
        assert (unknown.attributes or {})[
            "url.full"
        ] == observability.REDACTED_PLACEHOLDER

    asyncio.run(scenario())


def test_datadog_metadata_exporter_maps_only_approved_tool_attributes() -> None:
    async def scenario() -> None:
        app = _app(
            "datadog-metadata",
            extra_properties={
                "auth_method": "bearer",
                "workspace_id": WORKSPACE_ID,
                "organization_id": ORGANIZATION_ID,
                "scope_source": "arg",
            },
        )

        @app.tool()
        def echo(value: str) -> str:
            return value

        _register_tracing(
            app,
            attributes=lambda: {
                "tool_module": "cloud",
                "agent.action": "list",
                "agent.entity_type": "users",
            },
            shared_properties=(
                "workspace_id",
                "organization_id",
                "scope_source",
                "auth_method",
            ),
            capture_intent=True,
        )
        with capture_tool_spans() as spans:
            async with Client(app) as client:
                await client.call_tool(
                    "echo",
                    {
                        "value": "private-tool-payload",
                        "intent": "Read the user records",
                    },
                )

        collector = _Collector()
        assert (
            observability._DatadogMetadataExporter(collector).export([
                _tool_span(spans, "echo")
            ])
            == SpanExportResult.SUCCESS
        )
        attributes = collector.spans[0].attributes or {}
        metadata = json.loads(attributes["_dd.ml_obs.metadata"])
        assert metadata["intent"] == "Read the user records"
        assert metadata["agent.action"] == "list"
        assert metadata["agent.entity_type"] == "users"
        assert metadata["workspace_id"] == WORKSPACE_ID
        assert metadata["organization_id"] == ORGANIZATION_ID
        assert metadata["auth_method"] == "bearer"
        assert metadata["arg.value"] == attributes["airbyte.mcp.arg.value"]
        assert metadata["arg_hash_status"] == attributes["airbyte.mcp.arg_hash_status"]
        assert json.loads(attributes["gen_ai.tool.call.arguments"]) == {
            "intent": "Read the user records",
            "action": "list",
            "entity_name": "users",
        }
        assert "gen_ai.tool.call.result" not in attributes
        assert "private-tool-payload" not in json.dumps(dict(attributes))

    asyncio.run(scenario())


def test_install_registers_tracing_then_instruments_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import fastmcp_extensions

    app = FastMCP("install-test")
    sdk_provider = TracerProvider()
    providers = iter((trace.ProxyTracerProvider(), sdk_provider))
    registrations: list[tuple[object, TelemetryConfig]] = []
    instrumented: list[dict[str, object]] = []

    def register(app_arg: object, config: TelemetryConfig) -> None:
        registrations.append((app_arg, config))

    monkeypatch.setattr(fastmcp_extensions, "register_tool_call_telemetry", register)
    monkeypatch.setattr(
        observability.trace, "get_tracer_provider", lambda: next(providers)
    )
    monkeypatch.setattr(
        RequestsInstrumentor,
        "is_instrumented_by_opentelemetry",
        property(lambda _self: False),
    )
    monkeypatch.setattr(
        RequestsInstrumentor,
        "instrument",
        lambda _self, **kwargs: instrumented.append(kwargs),
    )

    observability.install(
        app,
        environ={
            "AIRBYTE_MCP_TRACING_BACKEND": "otel",
            "AIRBYTE_MCP_INTENT_CAPTURE": "true",
        },
    )

    registered_app, config = registrations[0]
    assert registered_app is app
    tracing = config.tool_tracing
    assert tracing is not None
    assert tracing.attribute_prefix == "airbyte.mcp"
    assert tracing.attributes is observability._trace_attributes
    assert tracing.shared_properties == (
        "workspace_id",
        "organization_id",
        "scope_source",
        "auth_method",
    )
    assert tracing.capture_intent is True
    assert tracing.error_classifier is classify_mcp_tool_error
    assert tracing.error_reason is mcp_tool_error_reason
    assert tracing.other_spans is observability._http_client_span_attributes
    assert tracing.arg_key is observability._arg_key
    assert tracing.exporter == "otlp"
    assert instrumented == [{"excluded_urls": "api.segment.io"}]


@pytest.mark.parametrize(
    ("provider_kind", "requests_instrumented"),
    [("sdk", False), ("proxy", True)],
)
def test_install_refuses_unowned_otel_instrumentation(
    monkeypatch: pytest.MonkeyPatch,
    provider_kind: str,
    requests_instrumented: bool,
) -> None:
    provider = (
        TracerProvider() if provider_kind == "sdk" else trace.ProxyTracerProvider()
    )
    monkeypatch.setattr(observability.trace, "get_tracer_provider", lambda: provider)
    monkeypatch.setattr(
        RequestsInstrumentor,
        "is_instrumented_by_opentelemetry",
        property(lambda _self: requests_instrumented),
    )
    with pytest.raises(RuntimeError, match=observability._PROVIDER_OWNERSHIP_ERROR):
        observability.install(
            FastMCP("ownership-test"),
            environ={"AIRBYTE_MCP_TRACING_BACKEND": "datadog-otlp"},
        )


def test_datadog_otlp_exporter_is_only_constructed_when_endpoint_is_set() -> None:
    assert observability._exporter("otel", {}) == "otlp"
    assert observability._exporter("datadog-otlp", {}) == "otlp"
    exporter = observability._exporter(
        "datadog-otlp",
        {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318"},
    )
    assert isinstance(exporter, observability._DatadogMetadataExporter)


def test_argument_hmac_key_validation_is_private_and_warns_once(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(logging.getLogger("airbyte"), "propagate", True)
    monkeypatch.setenv(
        "AIRBYTE_MCP_TELEMETRY_HMAC_KEY",
        base64.urlsafe_b64encode(b"k" * 32).decode().rstrip("="),
    )
    assert observability._arg_key() == b"k" * 32

    monkeypatch.delenv("AIRBYTE_MCP_TELEMETRY_HMAC_KEY")
    assert observability._arg_key() is None

    monkeypatch.setenv("AIRBYTE_MCP_TELEMETRY_HMAC_KEY", "private-invalid-key!")
    with caplog.at_level(logging.WARNING):
        assert observability._arg_key() is None
        assert observability._arg_key() is None
    assert caplog.text.count("AIRBYTE_MCP_TELEMETRY_HMAC_KEY is invalid") == 1
    assert "private-invalid-key!" not in caplog.text


# -- T6: error telemetry facts are unchanged by the agent-facing text layer ----
#
# Expected values captured on the pre-change base commit (d74eeb30) by running
# the same scenario; the new middleware is outermost so classification sees the
# same innermost cause.


def _wrapped_sdk_error(status: int, body: str) -> tuple[Exception, Exception]:
    import requests
    from airbyte_api.errors import SDKError

    from airbyte._util import api_util

    raw = requests.Response()
    raw.status_code = status
    raw.url = "https://api.airbyte.com/v1/x"
    sdk = SDKError("API error occurred", status, body, raw)
    return sdk, api_util._wrap_sdk_error(sdk)


_ERROR_CASES: dict[str, tuple[Exception | None, Exception]] = {
    "forbidden": _wrapped_sdk_error(
        403, '{"type":"https://reference.airbyte.com/reference/errors#forbidden"}'
    ),
    "bad_request": _wrapped_sdk_error(
        400, '{"type":"https://reference.airbyte.com/reference/errors#bad-request"}'
    ),
    "load_shed": _wrapped_sdk_error(429, '{"message":"You are being rate limited."}'),
    "unexpected": _wrapped_sdk_error(
        500,
        '{"type":"https://reference.airbyte.com/reference/errors",'
        '"title":"unexpected-problem"}',
    ),
    "unavailable": _wrapped_sdk_error(503, '{"type":"error:service-unavailable"}'),
    "not_ready": _wrapped_sdk_error(
        409,
        '{"type":"https://reference.airbyte.com/reference/errors#409-actor-not-ready"}',
    ),
    "sync_failed": (
        None,
        AirbyteConnectionSyncError(connection_id="c", job_id=1, job_status="failed"),
    ),
    "runtime": (None, RuntimeError("boom with SECRET-ARG-VALUE")),
}


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        pytest.param(
            "forbidden",
            {
                "airbyte.mcp.error.category": "user_error",
                "airbyte.mcp.error.fault": "caller",
                "airbyte.mcp.upstream.status_code": 403,
                "airbyte.mcp.error.reason": "forbidden",
            },
            id="forbidden",
        ),
        pytest.param(
            "bad_request",
            {
                "airbyte.mcp.error.category": "upstream_error",
                "airbyte.mcp.error.fault": "upstream",
                "airbyte.mcp.upstream.status_code": 400,
                "airbyte.mcp.error.reason": "bad-request",
            },
            id="bad-request",
        ),
        pytest.param(
            "load_shed",
            {
                "airbyte.mcp.error.category": "rate_limited",
                "airbyte.mcp.error.fault": "upstream",
                "airbyte.mcp.upstream.status_code": 429,
            },
            id="load-shed",
        ),
        pytest.param(
            "unexpected",
            {
                "airbyte.mcp.error.category": "upstream_error",
                "airbyte.mcp.error.fault": "upstream",
                "airbyte.mcp.upstream.status_code": 500,
                "airbyte.mcp.error.reason": "unexpected-problem",
            },
            id="unexpected",
        ),
        pytest.param(
            "unavailable",
            {
                "airbyte.mcp.error.category": "upstream_error",
                "airbyte.mcp.error.fault": "upstream",
                "airbyte.mcp.upstream.status_code": 503,
                "airbyte.mcp.error.reason": "error:service-unavailable",
            },
            id="unavailable",
        ),
        pytest.param(
            "not_ready",
            {
                "airbyte.mcp.error.category": "user_error",
                "airbyte.mcp.error.fault": "caller",
                "airbyte.mcp.upstream.status_code": 409,
                "airbyte.mcp.error.reason": "409-actor-not-ready",
            },
            id="not-ready",
        ),
        pytest.param(
            "sync_failed",
            {
                "airbyte.mcp.error.category": "upstream_error",
                "airbyte.mcp.error.fault": "upstream",
            },
            id="sync-failed",
        ),
        pytest.param(
            "runtime",
            {
                "airbyte.mcp.error.category": "unclassified",
                "airbyte.mcp.error.fault": "unknown",
            },
            id="runtime",
        ),
        pytest.param(
            "bad_arg",
            {
                "airbyte.mcp.error.category": "invalid_arguments",
                "airbyte.mcp.error.fault": "caller",
            },
            id="bad-arg",
        ),
    ],
)
def test_error_telemetry_facts_unchanged(
    case: str, expected: Mapping[str, object]
) -> None:
    async def scenario() -> None:
        app = mcp_server(
            display_name="t6",
            telemetry=TelemetryConfig(package_name="airbyte"),
            user_facing_errors=MCP_TOOL_USER_FACING_ERRORS,
            user_facing_error_formatter=format_user_facing_error,
        )
        app.middleware.insert(
            next(
                index
                for index, middleware in enumerate(app.middleware)
                if isinstance(middleware, UserFacingErrorMiddleware)
            ),
            AgentErrorTextMiddleware(),
        )

        @app.tool()
        def tool() -> str:
            if case == "bad_arg":
                return "x"
            cause, wrapped = _ERROR_CASES[case]
            if cause is not None:
                raise wrapped from cause
            raise wrapped

        @app.tool()
        def bad_arg_tool(count: int) -> str:
            return "x"

        register_tool_call_telemetry(
            app,
            TelemetryConfig(
                package_name="airbyte",
                tool_tracing=ToolTracingConfig(
                    attribute_prefix="airbyte.mcp",
                    error_classifier=classify_mcp_tool_error,
                    error_reason=mcp_tool_error_reason,
                ),
            ),
        )

        with capture_tool_spans() as spans:
            async with Client(app) as client:
                with pytest.raises(ToolError):
                    if case == "bad_arg":
                        await client.call_tool(
                            "bad_arg_tool", {"count": "sk_live_FAKE1234567890"}
                        )
                    else:
                        await client.call_tool("tool")

        tool_name = "bad_arg_tool" if case == "bad_arg" else "tool"
        attributes = _tool_span(spans, tool_name).attributes or {}
        for key, value in expected.items():
            assert attributes.get(key) == value, f"{key}: {attributes.get(key)}"
        if "airbyte.mcp.error.reason" not in expected:
            assert "airbyte.mcp.error.reason" not in attributes

    asyncio.run(scenario())


def test_agent_error_text_sits_outside_user_facing_and_telemetry_middleware() -> None:
    from fastmcp_extensions import (
        ToolCallTelemetryMiddleware,
        UserFacingErrorMiddleware,
    )

    from airbyte.mcp import server as real_server
    from airbyte.mcp._error_handling import AgentErrorTextMiddleware

    kinds = [type(middleware) for middleware in real_server.app.middleware]
    agent_index = kinds.index(AgentErrorTextMiddleware)
    assert agent_index < kinds.index(UserFacingErrorMiddleware)
    telemetry_indexes = [
        index
        for index, middleware in enumerate(real_server.app.middleware)
        if isinstance(middleware, ToolCallTelemetryMiddleware)
    ]
    assert telemetry_indexes and all(index > agent_index for index in telemetry_indexes)
