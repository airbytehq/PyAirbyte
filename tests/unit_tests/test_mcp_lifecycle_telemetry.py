# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP session lifecycle telemetry and shared event context."""

from __future__ import annotations

import asyncio
import hashlib
import json

import httpx
import pytest
from fastmcp import Client, FastMCP
from fastmcp_extensions import (
    CapabilityTokenMiddleware,
    TelemetryRecord,
    TelemetrySinks,
    mcp_server,
)
from fastmcp_extensions.capability_tokens import encode_session_token
from starlette.types import Receive, Scope, Send

from airbyte import constants
from airbyte.mcp import _telemetry, server
from airbyte.mcp._otel import SessionIdHeaderDigest
from airbyte.mcp._telemetry import (
    AUTH_FAILED_EVENT,
    SERVER_CONNECTED_EVENT,
    McpRequestTelemetryMiddleware,
    ServerConnectedTelemetryMiddleware,
    request_properties,
)
from airbyte.mcp._tool_utils import ORGANIZATION_ID_CONFIG_ARG, WORKSPACE_ID_CONFIG_ARG


_MCP_HEADERS = {"accept": "application/json, text/event-stream"}
_CONTEXT_KEYS = {
    "is_hosted_mcp",
    "edition",
    "transport",
    "auth_method",
    "session_id",
    "mcp_client_name",
    "mcp_client_version",
    "mcp_protocol_version",
    "organization_id",
    "workspace_id",
}


@pytest.fixture
def records(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[TelemetrySinks, list[TelemetryRecord]]:
    """Sinks that capture records instead of sending them."""
    sinks = TelemetrySinks(package_name="airbyte")
    captured: list[TelemetryRecord] = []
    monkeypatch.setattr(sinks, "emit", captured.append)
    return sinks, captured


@pytest.fixture
def hosted(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resolve telemetry as the hosted HTTP server does."""
    monkeypatch.setattr(constants, "_HOSTED_MCP_MODE_ENABLED", True)
    monkeypatch.delenv(constants.CLOUD_API_ROOT_ENV_VAR, raising=False)


def _probe_app(sinks: TelemetrySinks) -> FastMCP:
    app = mcp_server(
        name="probe",
        server_config_args=[WORKSPACE_ID_CONFIG_ARG, ORGANIZATION_ID_CONFIG_ARG],
        telemetry=False,
    )
    app.add_middleware(ServerConnectedTelemetryMiddleware(sinks))

    @app.tool()
    def probe() -> str:
        return json.dumps(request_properties())

    return app


async def _stateless_session(app: FastMCP, requests: list[tuple[str, dict, dict]]):
    """Send JSON-RPC requests through the hosted HTTP middleware stack."""
    raw = app.http_app(path="/mcp", stateless_http=True, json_response=True)
    sinks = next(
        m._sinks
        for m in app.middleware
        if isinstance(m, ServerConnectedTelemetryMiddleware)
    )
    wrapped = CapabilityTokenMiddleware(
        McpRequestTelemetryMiddleware(
            SessionIdHeaderDigest(raw), sinks=sinks, mcp_path="/mcp"
        )
    )
    responses = []
    async with raw.router.lifespan_context(raw):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=wrapped), base_url="http://testserver"
        ) as client:
            for index, (method, params, headers) in enumerate(requests):
                responses.append(
                    await client.post(
                        "/mcp",
                        json={
                            "jsonrpc": "2.0",
                            "id": index,
                            "method": method,
                            "params": params,
                        },
                        headers=_MCP_HEADERS | headers,
                    )
                )
    return responses


def test_hosted_session_context_reaches_every_event(records, hosted) -> None:
    """Initialize emits `ServerConnected`; later stateless calls recover client info."""
    sinks, captured = records
    app = _probe_app(sinks)
    config_headers = {
        constants.MCP_ORGANIZATION_ID_HEADER: "org-123",
        constants.MCP_WORKSPACE_ID_HEADER: "ws-456",
    }
    (init,) = asyncio.run(
        _stateless_session(
            app,
            [
                (
                    "initialize",
                    {
                        "protocolVersion": "2025-06-18",
                        "capabilities": {},
                        "clientInfo": {"name": "Claude Code", "version": "2.1.0"},
                    },
                    config_headers,
                )
            ],
        )
    )
    assert init.status_code == 200, init.text
    token = init.headers["mcp-session-id"]

    (connected,) = captured
    assert connected.invocation_type == SERVER_CONNECTED_EVENT
    assert connected.name == "initialize"
    assert connected.success is True
    assert _CONTEXT_KEYS <= set(connected.to_dict())
    assert connected.extra["mcp_client_name"] == "Claude Code"
    assert connected.extra["mcp_client_version"] == "2.1.0"
    assert connected.extra["mcp_protocol_version"] == "2025-06-18"
    assert connected.extra["transport"] == "streamable-http"
    assert connected.extra["edition"] == "cloud"
    assert connected.extra["organization_id"] == "org-123"
    assert connected.extra["workspace_id"] == "ws-456"
    assert connected.extra["session_id"] == hashlib.sha256(token.encode()).hexdigest()

    (call,) = asyncio.run(
        _stateless_session(
            app,
            [
                (
                    "tools/call",
                    {"name": "probe", "arguments": {}},
                    config_headers
                    | {
                        "mcp-session-id": token,
                        "mcp-protocol-version": "2025-06-18",
                        "authorization": "Bearer not-verified-here",
                    },
                )
            ],
        )
    )
    assert call.status_code == 200, call.text
    properties = json.loads(call.json()["result"]["content"][0]["text"])
    assert set(properties) == _CONTEXT_KEYS
    assert properties["session_id"] == hashlib.sha256(token.encode()).hexdigest()
    assert properties["mcp_client_name"] == "Claude Code"
    assert properties["mcp_client_version"] == "2.1.0"
    assert properties["mcp_protocol_version"] == "2025-06-18"
    assert properties["auth_method"] == "bearer"
    assert properties["organization_id"] == "org-123"
    assert properties["workspace_id"] == "ws-456"
    assert len(captured) == 1, "tool calls must not emit ServerConnected"


def test_stdio_context_uses_process_session(records, monkeypatch) -> None:
    """Over stdio, the process is the session and client info comes from initialize."""
    sinks, captured = records
    monkeypatch.setattr(constants, "_HOSTED_MCP_MODE_ENABLED", False)
    monkeypatch.setenv(constants.CLOUD_CLIENT_ID_ENV_VAR, "client-id")
    monkeypatch.delenv(constants.CLOUD_BEARER_TOKEN_ENV_VAR, raising=False)
    monkeypatch.setenv(
        constants.CLOUD_API_ROOT_ENV_VAR, "http://localhost:8000/api/public/v1"
    )
    app = _probe_app(sinks)

    async def call_probe() -> dict[str, object]:
        async with Client(app) as client:
            result = await client.call_tool("probe", {})
        return json.loads(result.content[0].text)

    properties = asyncio.run(call_probe())

    assert properties["transport"] == "stdio"
    assert properties["session_id"] == _telemetry._STDIO_SESSION_ID
    assert properties["auth_method"] == "client_credentials"
    assert properties["edition"] == "oss"
    assert properties["mcp_client_name"]
    assert captured
    assert {record.invocation_type for record in captured} == {SERVER_CONNECTED_EVENT}


def _status_app(status: int, www_authenticate: str | None = None):
    async def app(scope: Scope, receive: Receive, send: Send) -> None:  # noqa: ARG001
        headers = [(b"content-type", b"text/plain")]
        if www_authenticate is not None:
            headers.append((b"www-authenticate", www_authenticate.encode()))
        await send({
            "type": "http.response.start",
            "status": status,
            "headers": headers,
        })
        await send({"type": "http.response.body", "body": b""})

    return app


@pytest.mark.parametrize(
    ("path", "status", "headers", "www_authenticate", "expected_reason"),
    [
        pytest.param(
            "/mcp", 401, {}, 'Bearer error="invalid_token"', None, id="missing-token"
        ),
        pytest.param(
            "/mcp",
            401,
            {"authorization": "Bearer expired"},
            'Bearer error="invalid_token"',
            "invalid_token",
            id="invalid-token",
        ),
        pytest.param(
            "/mcp",
            401,
            {"authorization": "Basic Y2xpZW50OnNlY3JldA=="},
            None,
            "invalid_client_credentials",
            id="client-credentials",
        ),
        pytest.param(
            "/mcp",
            403,
            {"authorization": "Bearer narrow"},
            'Bearer error="insufficient_scope"',
            "insufficient_scope",
            id="insufficient-scope",
        ),
        pytest.param(
            "/auth/callback", 400, {}, None, "oauth_callback_error", id="oauth-callback"
        ),
        pytest.param(
            "/mcp", 200, {"authorization": "Bearer ok"}, None, None, id="success"
        ),
        pytest.param(
            "/register",
            401,
            {"authorization": "Bearer x"},
            None,
            None,
            id="untracked-path",
        ),
    ],
)
def test_auth_failures_are_classified(
    records,
    hosted,
    path: str,
    status: int,
    headers: dict[str, str],
    www_authenticate: str | None,
    expected_reason: str | None,
) -> None:
    """Only rejected credentials emit `AuthFailed`; OAuth discovery 401s do not."""
    sinks, captured = records
    token = encode_session_token(client_name="Cursor", client_version="3.0")
    app = McpRequestTelemetryMiddleware(
        _status_app(status, www_authenticate), sinks=sinks, mcp_path="/mcp"
    )

    async def send_request() -> httpx.Response:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            return await client.post(
                path,
                headers=headers
                | {
                    "mcp-session-id": token,
                    constants.MCP_ORGANIZATION_ID_HEADER: "org-123",
                },
            )

    response = asyncio.run(send_request())

    assert response.status_code == status
    if expected_reason is None:
        assert captured == []
        return
    (record,) = captured
    assert record.invocation_type == AUTH_FAILED_EVENT
    assert record.success is False
    assert record.error_type == expected_reason
    assert _CONTEXT_KEYS <= set(record.to_dict())
    assert record.extra["reason"] == expected_reason
    assert record.extra["http_status"] == status
    assert record.extra["mcp_client_name"] == "Cursor"
    assert record.extra["session_id"] == hashlib.sha256(token.encode()).hexdigest()
    assert record.extra["organization_id"] == "org-123"


def test_auth_failures_fall_back_to_env_scope(
    records, hosted, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`AuthFailed` resolves scope from env when headers are absent, like tool calls."""
    sinks, captured = records
    monkeypatch.setenv(constants.CLOUD_ORGANIZATION_ID_ENV_VAR, "org-env")
    monkeypatch.setenv(constants.CLOUD_WORKSPACE_ID_ENV_VAR, "ws-env")
    app = McpRequestTelemetryMiddleware(
        _status_app(401, None), sinks=sinks, mcp_path="/mcp"
    )

    async def send_request() -> httpx.Response:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            return await client.post(
                "/mcp",
                headers={
                    "authorization": "Bearer x",
                    constants.MCP_WORKSPACE_ID_HEADER: "ws-header",
                },
            )

    assert asyncio.run(send_request()).status_code == 401
    (record,) = captured
    assert record.extra["organization_id"] == "org-env"
    assert record.extra["workspace_id"] == "ws-header"


def test_server_registers_lifecycle_telemetry() -> None:
    """The shared app emits `ServerConnected` and resolves tool-call context per call."""
    lifecycle = [
        m
        for m in server.app.middleware
        if isinstance(m, ServerConnectedTelemetryMiddleware)
    ]
    assert len(lifecycle) == 1
    assert lifecycle[0]._sinks is server.lifecycle_telemetry_sinks
