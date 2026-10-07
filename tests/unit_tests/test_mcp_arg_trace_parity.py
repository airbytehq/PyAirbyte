# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Argument records of the real PyAirbyte MCP app through upstream tracing."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import socket
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastmcp import Client, FastMCP
from fastmcp.server.auth import AccessToken
from fastmcp_extensions import capture_tool_spans
from fastmcp_extensions.otel import middleware as otel_middleware
from fastmcp_extensions.otel.middleware import trace_plan
from opentelemetry import trace
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.trace import ReadableSpan

from airbyte.mcp import _otel as observability
from airbyte.mcp import cloud
from airbyte.mcp.server import app as server_app


FIXTURE = Path(__file__).parent / "fixtures" / "arg_trace_plan.json"
ARG = "airbyte.mcp.arg."
SENTINEL = "SENTINELqzxprivatevalue"
CONNECTOR_ID = "11111111-1111-1111-1111-111111111111"
CALLS: dict[str, dict[str, object]] = {
    "execute_external_api_query": {
        "connector_id": CONNECTOR_ID,
        "entity_type": "contacts",
        "action": "list",
        "api_args": {"filter": SENTINEL},
        "select_fields": [SENTINEL, "id"],
        "exclude_fields": [SENTINEL],
        "page_size": 5,
        "cursor": SENTINEL,
    },
    "execute_external_sql_query": {
        "connector_id": CONNECTOR_ID,
        "sql": f"select '{SENTINEL}' from t",
        "sql_dialect": "snowflake",
        "page_size": 10,
        "cursor": SENTINEL,
    },
    "execute_external_search_query": {
        "connector_id": CONNECTOR_ID,
        "prompt": f"find {SENTINEL}",
        "streams": [{"name": SENTINEL}],
        "limit": 3,
    },
    "list_cloud_connections": {"name_contains": SENTINEL, "limit": 5},
}


def _plan() -> dict[str, dict[str, str]]:
    plan = asyncio.run(trace_plan(server_app))
    return {name: entry["args"] for name, entry in plan.items()}


def test_inventory_matches_fixture() -> None:
    plan = _plan()
    if os.environ.get("UPDATE_ARG_TRACE_FIXTURE"):
        FIXTURE.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n")
    assert plan == json.loads(FIXTURE.read_text())


@pytest.fixture
def traced_app(monkeypatch: pytest.MonkeyPatch) -> Iterator[FastMCP]:
    for name in ("OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    monkeypatch.setenv(
        "AIRBYTE_MCP_TELEMETRY_HMAC_KEY",
        base64.urlsafe_b64encode(bytes(range(0x40, 0x60))).decode().rstrip("="),
    )
    token = AccessToken(
        token="fake-token",
        client_id="client",
        scopes=[],
        claims={"iss": "https://issuer.example.test", "sub": "user-1"},
    )
    monkeypatch.setattr(otel_middleware, "get_access_token", lambda: token)

    def no_cloud(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("Cloud access is stubbed in tests")

    monkeypatch.setattr(cloud, "_get_cloud_workspace", no_cloud)
    connect = socket.socket.connect

    def loopback_only(sock: socket.socket, address: object) -> object:
        if sock.family in {socket.AF_INET, socket.AF_INET6}:
            assert isinstance(address, tuple) and address[0] in {"127.0.0.1", "::1"}
        return connect(sock, address)

    monkeypatch.setattr(socket.socket, "connect", loopback_only)
    middleware, instructions = list(server_app.middleware), server_app.instructions
    observability._reset_for_tests()
    with monkeypatch.context() as patch:
        patch.setattr(trace, "get_tracer_provider", lambda: trace.ProxyTracerProvider())
        patch.setattr(
            RequestsInstrumentor,
            "is_instrumented_by_opentelemetry",
            property(lambda _self: False),
        )
        observability.install(
            server_app, environ={"AIRBYTE_MCP_TRACING_BACKEND": "otel"}
        )
    try:
        yield server_app
    finally:
        server_app.middleware[:] = middleware
        server_app.instructions = instructions
        observability._reset_for_tests()


def _call_all(app: FastMCP) -> dict[str, dict[str, object]]:
    async def run() -> list[ReadableSpan]:
        with capture_tool_spans() as spans:
            async with Client(app) as client:
                for tool, arguments in CALLS.items():
                    await client.call_tool(tool, arguments, raise_on_error=False)
        return spans

    spans = asyncio.run(run())
    return {
        tool: dict(
            next(span for span in spans if span.name == f"tools/call {tool}").attributes
            or {}
        )
        for tool in CALLS
    }


def _expected_keys(mode: str, value: object) -> set[str]:
    if mode.startswith("value"):
        return {"value"}
    if mode.startswith("presence"):
        return {"present"}
    keys = {"digest", "similarity"} if mode.startswith("fingerprint") else {"digest"}
    return keys | {"count"} if isinstance(value, list) else keys


def test_records_match_fixture_and_exclude_raw_values(
    traced_app: FastMCP, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    plan = json.loads(FIXTURE.read_text())
    attributes = _call_all(traced_app)
    for tool, arguments in CALLS.items():
        attrs = attributes[tool]
        assert attrs["airbyte.mcp.arg_hash_status"] == "ok", tool
        for name, value in arguments.items():
            mode = plan[tool][name]
            record = json.loads(str(attrs[ARG + name]))
            assert set(record) == _expected_keys(mode, value), (tool, name, record)
            if mode.startswith("value"):
                assert record["value"] == value
    exported = json.dumps(attributes, default=str)
    assert SENTINEL not in exported
    assert SENTINEL not in caplog.text


def test_prompt_records_carry_similarity_and_sql_stays_equality_only(
    traced_app: FastMCP,
) -> None:
    # As in #1303, `prompt` is fingerprinted while `sql` is equality-only.
    attributes = _call_all(traced_app)
    prompt = json.loads(
        str(attributes["execute_external_search_query"][ARG + "prompt"])
    )
    sql = json.loads(str(attributes["execute_external_sql_query"][ARG + "sql"]))
    assert prompt.keys() == {"digest", "similarity"}
    assert sql.keys() == {"digest"}
