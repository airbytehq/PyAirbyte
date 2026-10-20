# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Argument records of the real PyAirbyte MCP app through upstream tracing."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import socket
from collections.abc import Iterator

import pytest
from fastmcp import Client, FastMCP
from fastmcp.server.auth import AccessToken
from fastmcp_extensions import capture_tool_spans
from fastmcp_extensions.otel import middleware as otel_middleware
from fastmcp_extensions.otel.middleware import trace_plan
from opentelemetry import trace
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.trace import ReadableSpan

from airbyte.mcp import _datadog
from airbyte.mcp import _otel as observability
from airbyte.mcp._arg_trace_rules import ARG_TRACE_RULES
from airbyte.mcp import cloud
from airbyte.mcp.server import app as server_app


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


def test_every_arg_is_classified_by_a_rule(traced_app: FastMCP) -> None:
    plan = asyncio.run(trace_plan(traced_app, include_sources=True))
    unclassified = [
        f"{tool}.{name} uses source {source}; add or adjust a rule in "
        "`airbyte/mcp/arg_trace_rules.yaml` (do not add `TraceArg` markers)."
        for tool, entry in plan.items()
        for name, source in entry["arg_sources"].items()
        if source in {"default", "marker"}
    ]
    assert not unclassified, "\n".join(unclassified)


def test_no_dead_rules(traced_app: FastMCP) -> None:
    plan = asyncio.run(trace_plan(traced_app, include_sources=True))
    used_rules = {
        source.removeprefix("rule:")
        for entry in plan.values()
        for source in entry["arg_sources"].values()
        if source.startswith("rule:")
    }
    unused_rules = set(ARG_TRACE_RULES) - used_rules
    if not any("manifest_yaml_file" in entry["arg_sources"] for entry in plan.values()):
        # The app excludes this argument during import when trusted execution is off.
        unused_rules.discard("manifest_yaml_file")
    assert not unused_rules, (
        "Rules in `airbyte/mcp/arg_trace_rules.yaml` matched no tool arguments: "
        f"{sorted(unused_rules)}"
    )


def test_datadog_and_otel_classification_agree(traced_app: FastMCP) -> None:
    plan = asyncio.run(trace_plan(traced_app))
    tools = asyncio.run(traced_app.list_tools(run_middleware=False))
    assert {tool.name for tool in tools} == set(plan)
    for tool in tools:
        classes = _datadog._ARG_TRACER.classes(
            tool.name,
            getattr(tool, "fn", None),
            plan[tool.name]["args"],
        )
        assert {name: cls.describe() for name, cls in classes.items()} == plan[
            tool.name
        ]["args"], tool.name


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


def test_records_match_plan_and_exclude_raw_values(
    traced_app: FastMCP, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    plan = {
        name: entry["args"]
        for name, entry in asyncio.run(trace_plan(traced_app)).items()
    }
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
