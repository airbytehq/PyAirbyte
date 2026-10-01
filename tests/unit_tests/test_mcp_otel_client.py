# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Client attribution on real HTTP tool calls and the redacted export boundary."""

from __future__ import annotations

import asyncio
import hashlib
import json

import httpx
import pytest
from fastmcp import FastMCP
from fastmcp_extensions import (
    CapabilityTokenMiddleware,
    TelemetrySinks,
    ToolCallTelemetryMiddleware,
)
from opentelemetry.trace import SpanKind, StatusCode

from airbyte.mcp import _otel as observability
from airbyte.mcp import _telemetry
from tests.unit_tests import test_mcp_otel_agent_action as action_tests
from tests.unit_tests.test_mcp_otel import (
    _call,
    _capture,
    _export_text,
    _spans,
    _tool_span,
)


otel_provider = action_tests.otel_provider
isolated_otel = action_tests.isolated_otel
PRIVATE = "private-customer-argument-and-result"


@pytest.fixture
def client_app(monkeypatch):
    app = FastMCP("client-tracing-tests")

    @app.tool()
    def echo(value: str) -> str:
        return value

    _capture(app, capture=False)
    monkeypatch.setitem(observability._TOOL_MODULES, "echo", "cloud")
    return app


@pytest.mark.parametrize("modern", [True, False])
def test_http_client_identity_success_validation_and_request_isolation(
    monkeypatch, client_app, otel_provider, modern
):
    monkeypatch.setenv("AIRBYTE_MCP_TRACING_BACKEND", "datadog-otlp")
    sinks = TelemetrySinks(package_name="airbyte")
    monkeypatch.setattr(sinks, "emit", lambda _: None)
    raw = client_app.http_app(path="/mcp", stateless_http=True, json_response=True)
    wrapped = _telemetry.McpRequestTelemetryMiddleware(
        CapabilityTokenMiddleware(observability.SessionIdHeaderDigest(raw)),
        sinks=sinks,
        mcp_path="/mcp",
    )

    async def check():
        async with raw.router.lifespan_context(raw):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=wrapped), base_url="http://testserver"
            ) as client:
                for index, info in enumerate([
                    {"name": " Claude Code ", "version": " 2.1.0 "},
                    {"name": "Cursor", "version": "3.0"},
                    None,
                ]):
                    headers = {"accept": "application/json, text/event-stream"}
                    meta = {}
                    if modern:
                        headers["mcp-protocol-version"] = "2026-07-28"
                        headers["mcp-method"] = "tools/call"
                        headers["mcp-name"] = "echo"
                        meta = {
                            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                            "io.modelcontextprotocol/clientInfo": info,
                            "io.modelcontextprotocol/clientCapabilities": {},
                        }
                    elif isinstance(info, dict) and isinstance(info.get("name"), str):
                        initialized = await client.post(
                            "/mcp",
                            headers=headers,
                            json={
                                "jsonrpc": "2.0",
                                "id": f"init-{index}",
                                "method": "initialize",
                                "params": {
                                    "protocolVersion": "2025-06-18",
                                    "capabilities": {},
                                    "clientInfo": info,
                                },
                            },
                        )
                        assert initialized.status_code == 200
                        headers["mcp-session-id"] = initialized.headers[
                            "mcp-session-id"
                        ]
                    else:
                        headers["mcp-session-id"] = "malformed-token"
                    for invalid in (False, True):
                        response = await client.post(
                            "/mcp",
                            headers=headers,
                            json={
                                "jsonrpc": "2.0",
                                "id": f"call-{index}-{invalid}",
                                "method": "tools/call",
                                "params": {
                                    "name": "echo",
                                    "arguments": {
                                        "intent": "Check attribution",
                                        **({} if invalid else {"value": PRIVATE}),
                                    },
                                    "_meta": meta,
                                },
                            },
                        )
                        assert response.status_code == 200, response.text
                        assert bool(response.json()["result"].get("isError")) == invalid

    asyncio.run(check())
    spans = _spans(otel_provider)
    assert len(spans) == 6
    for index, span in enumerate(spans):
        attrs = span.attributes
        expected = [
            ("Claude Code", "2.1.0"),
            ("Cursor", "3.0"),
            (None, None),
        ][index // 2]
        assert attrs.get("airbyte.mcp.client_name") == expected[0]
        assert attrs.get("airbyte.mcp.client_version") == expected[1]
        metadata = json.loads(attrs["_dd.ml_obs.metadata"])
        assert metadata["auth_method"] == "none"
        assert attrs["airbyte.mcp.auth_method"] == "none"
        if modern:
            assert "session_id" not in metadata
            assert "airbyte.mcp.session_id" not in attrs
        else:
            session = attrs["gen_ai.conversation.id"]
            assert len(session) == 64
            assert metadata["session_id"] == attrs["airbyte.mcp.session_id"] == session
        if modern or index < 4:
            expected_protocol = "2026-07-28" if modern else "2025-06-18"
            assert metadata["mcp_protocol_version"] == expected_protocol
            assert attrs["airbyte.mcp.mcp_protocol_version"] == expected_protocol
        assert metadata.get("client_name") == expected[0]
        assert metadata.get("client_version") == expected[1]
        assert json.loads(attrs["gen_ai.tool.call.arguments"]) == {
            "intent": "Check attribution"
        }
        if index % 2:
            assert span.status.status_code == StatusCode.ERROR
            assert "ValidationError" in attrs["airbyte.mcp.error_type"]
        else:
            assert span.status.status_code != StatusCode.ERROR
    assert PRIVATE not in _export_text(otel_provider)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (42, None),
        ("   ", None),
        (" bad\nname ", None),
        ("name\u0085", None),
        ("name\u200b", None),
        (" App Name ", "App Name"),
        ("x" * 300, "x" * 256),
    ],
)
def test_client_label_validation_before_dispatch_and_export(
    monkeypatch, client_app, otel_provider, value, expected
):
    monkeypatch.setattr(
        _telemetry,
        "request_properties",
        lambda: {
            "mcp_client_name": value,
            "mcp_client_version": value,
            "other": PRIVATE,
        },
    )
    asyncio.run(_call(client_app, {"value": PRIVATE}))
    attrs = _tool_span(otel_provider).attributes
    assert attrs.get("airbyte.mcp.client_name") == expected
    assert attrs.get("airbyte.mcp.client_version") == expected
    assert PRIVATE not in _export_text(otel_provider)


def test_client_lookup_failure_preserves_execution_and_intent(
    monkeypatch, client_app, otel_provider
):
    def unavailable():
        raise RuntimeError(PRIVATE)

    monkeypatch.setattr(_telemetry, "request_properties", unavailable)
    result = asyncio.run(
        _call(client_app, {"value": PRIVATE, "intent": "Check context"})
    )
    assert not result.is_error
    attrs = _tool_span(otel_provider).attributes
    assert attrs["airbyte.mcp.intent"] == "Check context"
    assert "airbyte.mcp.client_name" not in attrs
    assert PRIVATE not in _export_text(otel_provider)


@pytest.mark.parametrize("backend", ["otel", "datadog-otlp"])
def test_exporter_revalidates_injected_client_fields(
    monkeypatch, client_app, otel_provider, backend
):
    monkeypatch.setenv("AIRBYTE_MCP_TRACING_BACKEND", backend)
    provider, _ = otel_provider
    tracer = provider.get_tracer("client-test")
    for name, kind in [
        ("tools/call echo", SpanKind.SERVER),
        ("GET", SpanKind.CLIENT),
        ("tools/call echo", SpanKind.CLIENT),
        ("tools/call unknown", SpanKind.SERVER),
    ]:
        with tracer.start_as_current_span(
            name,
            kind=kind,
            attributes={
                "airbyte.mcp.client_name": " App ",
                "airbyte.mcp.client_version": "bad\nversion",
                "_dd.ml_obs.metadata": json.dumps({
                    "client_name": PRIVATE,
                    "unapproved": PRIVATE,
                }),
                "gen_ai.tool.call.arguments": PRIVATE,
                "gen_ai.tool.call.result": PRIVATE,
            },
        ):
            pass
    spans = _spans(otel_provider)
    assert len(spans) == 3  # Unknown tools are dropped entirely.
    for span in spans:
        attrs = span.attributes
        expected = "App" if span.kind == SpanKind.SERVER else None
        assert attrs.get("airbyte.mcp.client_name") == expected
        assert "airbyte.mcp.client_version" not in attrs
        assert "gen_ai.tool.call.arguments" not in attrs
        if backend == "datadog-otlp" and expected:
            assert json.loads(attrs["_dd.ml_obs.metadata"]) == {"client_name": expected}
        else:
            assert "_dd.ml_obs.metadata" not in attrs
    assert PRIVATE not in _export_text(otel_provider)


@pytest.mark.parametrize("session", [None, "raw-private-session-SENTINEL", "a" * 64])
def test_request_metadata_omits_missing_or_raw_identifiers(monkeypatch, session):
    monkeypatch.setattr(
        _telemetry,
        "request_properties",
        lambda: {
            "session_id": session,
            "auth_method": "invalid-SENTINEL",
            "mcp_protocol_version": "bad\nprotocol-SENTINEL",
            "workspace_id": "private-workspace-SENTINEL",
            "organization_id": "private-org-SENTINEL",
        },
    )
    attributes = observability._request_trace_attributes()
    assert attributes == (
        {
            "airbyte.mcp.session_id": session,
            "gen_ai.conversation.id": session,
        }
        if session == "a" * 64
        else {}
    )


@pytest.mark.parametrize("wrapped", [False, True])
@pytest.mark.parametrize("raw_session", ["a" * 64, "private-session-SENTINEL"])
def test_http_session_header_is_hashed_once(
    client_app, monkeypatch, otel_provider, wrapped, raw_session
):
    monkeypatch.setenv("AIRBYTE_MCP_TRACING_BACKEND", "datadog-otlp")

    async def run():
        raw = client_app.http_app(path="/mcp", stateless_http=True, json_response=True)
        http_app = observability.SessionIdHeaderDigest(raw) if wrapped else raw
        async with raw.router.lifespan_context(raw):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=http_app),
                base_url="http://testserver",
            ) as client:
                response = await client.post(
                    "/mcp",
                    headers={
                        "accept": "application/json, text/event-stream",
                        "mcp-session-id": raw_session,
                    },
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {"name": "echo", "arguments": {"value": "ok"}},
                    },
                )
                assert response.status_code == 200
                assert not response.json()["result"].get("isError")

    asyncio.run(run())
    attrs = _tool_span(otel_provider).attributes
    digest = hashlib.sha256(raw_session.encode()).hexdigest()
    assert attrs["airbyte.mcp.session_id"] == attrs["gen_ai.conversation.id"] == digest
    assert json.loads(attrs["_dd.ml_obs.metadata"])["session_id"] == digest
    assert raw_session not in _export_text(otel_provider)


@pytest.mark.parametrize("process_credentials", [False, True])
@pytest.mark.parametrize(
    "headers, expected",
    [
        ({}, "none"),
        ({"authorization": "Bearer private-SENTINEL"}, "bearer"),
        (
            {"client-id": "synthetic", "client-secret": "private-SENTINEL"},
            "client_credentials",
        ),
        ({"authorization": "Basic private-SENTINEL"}, "client_credentials"),
    ],
)
def test_manually_hosted_http_auth_matches_request_and_analytics(
    client_app, monkeypatch, otel_provider, process_credentials, headers, expected
):
    from airbyte import constants

    monkeypatch.setattr(constants, "_HOSTED_MCP_MODE_ENABLED", False)
    monkeypatch.setenv("AIRBYTE_MCP_TRACING_BACKEND", "datadog-otlp")
    monkeypatch.delenv("AIRBYTE_CLOUD_CLIENT_ID", raising=False)
    if process_credentials:
        monkeypatch.setenv("AIRBYTE_CLOUD_BEARER_TOKEN", "process-private-SENTINEL")
    else:
        monkeypatch.delenv("AIRBYTE_CLOUD_BEARER_TOKEN", raising=False)
    records = []
    telemetry = ToolCallTelemetryMiddleware(
        extra_properties=_telemetry.request_properties
    )
    monkeypatch.setattr(telemetry._sinks, "emit", records.append)
    client_app.middleware.insert(0, telemetry)

    async def run():
        raw = client_app.http_app(path="/mcp", stateless_http=True, json_response=True)
        async with raw.router.lifespan_context(raw):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=raw), base_url="http://testserver"
            ) as client:
                response = await client.post(
                    "/mcp",
                    headers={
                        "accept": "application/json, text/event-stream",
                        **headers,
                    },
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {"name": "echo", "arguments": {"value": "ok"}},
                    },
                )
                assert response.status_code == 200
                assert not response.json()["result"].get("isError")

    asyncio.run(run())
    attrs = _tool_span(otel_provider).attributes
    assert (
        attrs["airbyte.mcp.auth_method"] == records[-1].extra["auth_method"] == expected
    )
    assert json.loads(attrs["_dd.ml_obs.metadata"])["auth_method"] == expected
    assert "SENTINEL" not in _export_text(otel_provider)
