# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP session lifecycle telemetry and shared event context."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterator
from contextvars import ContextVar
from unittest.mock import MagicMock

import httpx
import pytest
from fastmcp import Client, FastMCP
from fastmcp.server.auth import AccessToken
from fastmcp_extensions import (
    CapabilityTokenMiddleware,
    TelemetryConfig,
    TelemetryRecord,
    TelemetrySinks,
    ToolCallTelemetryMiddleware,
    mcp_server,
)
from fastmcp_extensions.capability_tokens import encode_session_token
from mcp.types import TextContent
from starlette.types import Receive, Scope, Send

from airbyte import constants
from airbyte._util import api_util
from airbyte.mcp import _telemetry, _user_identity, server
from airbyte.mcp._otel import SessionIdHeaderDigest
from airbyte.mcp._user_identity import AirbyteUserMiddleware
from airbyte.mcp._telemetry import (
    AUTH_FAILED_EVENT,
    SERVER_CONNECTED_EVENT,
    McpRequestTelemetryMiddleware,
    ServerConnectedTelemetryMiddleware,
    request_properties,
)
from airbyte.mcp._tool_utils import (
    API_URL_CONFIG_ARG,
    CONFIG_API_URL_CONFIG_ARG,
    ORGANIZATION_ID_CONFIG_ARG,
    WORKSPACE_ID_CONFIG_ARG,
)


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
_SERVER_CONNECTED_KEYS = _CONTEXT_KEYS | {"airbyte_user_id", "scope_source"}

_AUTH_USER_ID = "keycloak-user"
_AIRBYTE_USER_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_DEFAULT_WORKSPACE_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
_ORGANIZATION_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
_OLD_WORKSPACE_ID = "dddddddd-dddd-dddd-dddd-dddddddddddd"
_NEW_WORKSPACE_ID = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
_request_auth_user: ContextVar[str | None] = ContextVar(
    "lifecycle_request_auth_user", default=_AUTH_USER_ID
)


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


@pytest.fixture
def hosted_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[list[str], list[str]]]:
    """Stub the verified access token and identity/scope lookups."""
    user_lookups: list[str] = []
    organization_lookups: list[str] = []

    def get_access_token() -> AccessToken:
        return AccessToken(token="verified-access-token", client_id="client", scopes=[])

    monkeypatch.setattr(_telemetry, "get_access_token", get_access_token)
    monkeypatch.setattr(_user_identity, "get_access_token", get_access_token)
    monkeypatch.setattr(
        api_util, "get_user_id_from_bearer_token", lambda _: _request_auth_user.get()
    )

    def get_user_by_auth_id(auth_user_id: str, **_: object) -> dict[str, str]:
        user_lookups.append(auth_user_id)
        return {
            "userId": _AIRBYTE_USER_ID,
            "defaultWorkspaceId": _DEFAULT_WORKSPACE_ID,
        }

    def get_workspace_organization_info(
        workspace_id: str, **_: object
    ) -> dict[str, str]:
        organization_lookups.append(workspace_id)
        return {"organizationId": _ORGANIZATION_ID}

    monkeypatch.setattr(api_util, "get_user_by_auth_id", get_user_by_auth_id)
    monkeypatch.setattr(
        api_util, "get_workspace_organization_info", get_workspace_organization_info
    )
    _user_identity._user_id_cache.clear()
    _user_identity._workspace_organization_id_cache.clear()
    yield user_lookups, organization_lookups
    _user_identity._user_id_cache.clear()
    _user_identity._workspace_organization_id_cache.clear()


def _probe_app(sinks: TelemetrySinks, *, tool_telemetry: bool = False) -> FastMCP:
    telemetry: TelemetryConfig | bool = False
    if tool_telemetry:
        telemetry = TelemetryConfig(
            package_name="airbyte",
            segment_user_id=lambda: (
                _user_identity.current_airbyte_user_id() or server.SEGMENT_USER_ID
            ),
            extra_properties=lambda: {
                **request_properties(),
                **_user_identity.airbyte_user_properties(),
            },
        )
    app = mcp_server(
        name="probe",
        server_config_args=[
            WORKSPACE_ID_CONFIG_ARG,
            ORGANIZATION_ID_CONFIG_ARG,
            API_URL_CONFIG_ARG,
            CONFIG_API_URL_CONFIG_ARG,
        ],
        telemetry=telemetry,
    )
    if tool_telemetry:
        app.middleware.insert(0, AirbyteUserMiddleware())
    app.add_middleware(ServerConnectedTelemetryMiddleware(sinks))

    @app.tool()
    def probe() -> str:
        return json.dumps(request_properties())

    return app


def _initialize_request(
    headers: dict[str, str] | None = None,
) -> tuple[str, dict[str, object], dict[str, str]]:
    return (
        "initialize",
        {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "telemetry-test", "version": "1.0.0"},
        },
        {"authorization": "Bearer verified-access-token"} | (headers or {}),
    )


async def _stateless_session(
    app: FastMCP,
    requests: list[tuple[str, dict[str, object], dict[str, str]]],
) -> list[httpx.Response]:
    """Send JSON-RPC requests through the hosted HTTP middleware stack."""
    raw = app.http_app(path="/mcp", stateless_http=True, json_response=True)
    sinks = next(
        m._sinks
        for m in app.middleware
        if isinstance(m, ServerConnectedTelemetryMiddleware)
    )
    wrapped = McpRequestTelemetryMiddleware(
        CapabilityTokenMiddleware(SessionIdHeaderDigest(raw)),
        sinks=sinks,
        mcp_path="/mcp",
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


def _capture_lifecycle_segment(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[TelemetrySinks, list[TelemetryRecord], MagicMock]:
    sinks = server.lifecycle_telemetry_sinks
    captured: list[TelemetryRecord] = []
    track = MagicMock()
    original_emit = sinks.emit

    def emit(record: TelemetryRecord) -> None:
        captured.append(record)
        original_emit(record)

    monkeypatch.setattr(sinks, "emit", emit)
    monkeypatch.setattr(sinks, "segment_enabled", True)
    monkeypatch.setattr(sinks, "sentry_enabled", False)
    monkeypatch.setattr("fastmcp_extensions._telemetry._segment_analytics.track", track)
    return sinks, captured, track


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


def test_hosted_initialize_resolves_default_scope_and_session_identity(
    hosted, hosted_identity, monkeypatch: pytest.MonkeyPatch
) -> None:
    sinks, captured, track = _capture_lifecycle_segment(monkeypatch)
    app = _probe_app(sinks, tool_telemetry=True)
    tool_telemetry = next(
        middleware
        for middleware in app.middleware
        if isinstance(middleware, ToolCallTelemetryMiddleware)
    )
    monkeypatch.setattr(tool_telemetry._sinks, "segment_enabled", True)
    monkeypatch.setattr(tool_telemetry._sinks, "sentry_enabled", False)
    (init,) = asyncio.run(_stateless_session(app, [_initialize_request()]))
    call_headers = {
        "authorization": "Bearer verified-access-token",
        "mcp-session-id": init.headers["mcp-session-id"],
        "mcp-protocol-version": "2025-06-18",
    }
    (call,) = asyncio.run(
        _stateless_session(
            app,
            [("tools/call", {"name": "probe", "arguments": {}}, call_headers)],
        )
    )

    assert init.status_code == 200, init.text
    assert call.status_code == 200, call.text
    (connected,) = captured
    assert connected.invocation_type == SERVER_CONNECTED_EVENT
    assert _SERVER_CONNECTED_KEYS <= set(connected.to_dict())
    assert connected.extra["airbyte_user_id"] == _AIRBYTE_USER_ID
    assert connected.extra["workspace_id"] == _DEFAULT_WORKSPACE_ID
    assert connected.extra["organization_id"] == _ORGANIZATION_ID
    assert connected.extra["scope_source"] == "default"
    session_digest = hashlib.sha256(
        init.headers["mcp-session-id"].encode("latin-1")
    ).hexdigest()
    assert connected.extra["session_id"] == session_digest
    call_properties = json.loads(call.json()["result"]["content"][0]["text"])
    assert call_properties["session_id"] == session_digest
    server_connected_track = next(
        call for call in track.call_args_list if call.args[1] == SERVER_CONNECTED_EVENT
    )
    tool_call_track = next(
        call for call in track.call_args_list if call.args[1] == "mcp_tool_call"
    )
    assert server_connected_track.args[0] == _AIRBYTE_USER_ID
    assert tool_call_track.args[0] == _AIRBYTE_USER_ID
    assert tool_call_track.args[2]["session_id"] == session_digest
    assert hosted_identity == ([_AUTH_USER_ID], [_DEFAULT_WORKSPACE_ID])


def test_hosted_server_discover_resolves_default_scope(
    records, hosted, hosted_identity
):
    sinks, captured = records
    (response,) = asyncio.run(
        _stateless_session(
            _probe_app(sinks),
            [
                (
                    "server/discover",
                    {},
                    {"authorization": "Bearer verified-access-token"},
                )
            ],
        )
    )

    assert response.status_code == 200, response.text
    assert response.headers.get("mcp-session-id") is None
    assert len(captured) == 1
    connected = captured[0]
    assert connected.invocation_type == SERVER_CONNECTED_EVENT
    assert connected.name == "server/discover"
    assert connected.extra["session_id"] is None
    assert connected.extra["airbyte_user_id"] == _AIRBYTE_USER_ID
    assert connected.extra["workspace_id"] == _DEFAULT_WORKSPACE_ID
    assert connected.extra["organization_id"] == _ORGANIZATION_ID
    assert connected.extra["scope_source"] == "default"
    assert hosted_identity == ([_AUTH_USER_ID], [_DEFAULT_WORKSPACE_ID])


@pytest.mark.parametrize(
    ("organization_header", "expected_organization", "expected_lookups"),
    [
        (None, _ORGANIZATION_ID, ["header-workspace"]),
        ("header-organization", "header-organization", []),
    ],
)
def test_hosted_initialize_prefers_header_workspace_and_organization(
    records,
    hosted,
    hosted_identity,
    organization_header: str | None,
    expected_organization: str,
    expected_lookups: list[str],
) -> None:
    sinks, captured = records
    headers = {constants.MCP_WORKSPACE_ID_HEADER: "header-workspace"}
    if organization_header is not None:
        headers[constants.MCP_ORGANIZATION_ID_HEADER] = organization_header
    (response,) = asyncio.run(
        _stateless_session(_probe_app(sinks), [_initialize_request(headers)])
    )

    assert response.status_code == 200, response.text
    (connected,) = captured
    assert connected.extra["workspace_id"] == "header-workspace"
    assert connected.extra["organization_id"] == expected_organization
    assert connected.extra["scope_source"] == "header"
    assert hosted_identity[1] == expected_lookups


def test_hosted_initialize_survives_user_lookup_failure(
    records, hosted, hosted_identity, monkeypatch: pytest.MonkeyPatch
) -> None:
    sinks, captured = records

    def raise_lookup(*_: object, **__: object) -> None:
        raise RuntimeError("user lookup failed")

    monkeypatch.setattr(api_util, "get_user_by_auth_id", raise_lookup)
    (response,) = asyncio.run(
        _stateless_session(_probe_app(sinks), [_initialize_request()])
    )

    assert response.status_code == 200, response.text
    assert len(captured) == 1
    assert captured[0].extra["airbyte_user_id"] is None
    assert captured[0].extra["workspace_id"] is None
    assert captured[0].extra["organization_id"] is None


def test_hosted_initialize_survives_organization_lookup_failure(
    records, hosted, hosted_identity, monkeypatch: pytest.MonkeyPatch
) -> None:
    sinks, captured = records

    def raise_lookup(*_: object, **__: object) -> None:
        raise RuntimeError("organization lookup failed")

    monkeypatch.setattr(api_util, "get_workspace_organization_info", raise_lookup)
    (response,) = asyncio.run(
        _stateless_session(_probe_app(sinks), [_initialize_request()])
    )

    assert response.status_code == 200, response.text
    assert len(captured) == 1
    assert captured[0].extra["airbyte_user_id"] == _AIRBYTE_USER_ID
    assert captured[0].extra["workspace_id"] == _DEFAULT_WORKSPACE_ID
    assert captured[0].extra["organization_id"] is None
    assert captured[0].extra["scope_source"] == "default"


def test_hosted_initialize_retries_failed_organization_lookup(
    records, hosted, hosted_identity, monkeypatch: pytest.MonkeyPatch
) -> None:
    sinks, captured = records
    lookups: list[str] = []

    def get_workspace_organization_info(
        workspace_id: str, **_: object
    ) -> dict[str, str]:
        lookups.append(workspace_id)
        if len(lookups) == 1:
            raise RuntimeError("organization lookup failed")
        return {"organizationId": _ORGANIZATION_ID}

    monkeypatch.setattr(
        api_util, "get_workspace_organization_info", get_workspace_organization_info
    )
    responses = asyncio.run(
        _stateless_session(
            _probe_app(sinks),
            [_initialize_request(), _initialize_request()],
        )
    )

    assert [response.status_code for response in responses] == [200, 200]
    assert len(captured) == 2
    assert captured[0].extra["organization_id"] is None
    assert captured[1].extra["organization_id"] == _ORGANIZATION_ID
    assert lookups == [_DEFAULT_WORKSPACE_ID, _DEFAULT_WORKSPACE_ID]
    assert hosted_identity[0] == [_AUTH_USER_ID]


def test_hosted_initialize_identity_lookups_are_cached(
    records, hosted, hosted_identity
) -> None:
    sinks, captured = records
    responses = asyncio.run(
        _stateless_session(
            _probe_app(sinks),
            [_initialize_request(), _initialize_request()],
        )
    )

    assert [response.status_code for response in responses] == [200, 200]
    assert len(captured) == 2
    assert hosted_identity == ([_AUTH_USER_ID], [_DEFAULT_WORKSPACE_ID])


def test_hosted_initialize_refreshes_stale_default_workspace(
    records, hosted, hosted_identity, monkeypatch: pytest.MonkeyPatch
) -> None:
    sinks, captured = records
    monkeypatch.setattr(_telemetry, "DEFAULT_WORKSPACE_CACHE_TTL_SECONDS", 0.0)
    user_lookups: list[str] = []

    def get_user_by_auth_id(auth_user_id: str, **_: object) -> dict[str, str]:
        user_lookups.append(auth_user_id)
        return {
            "userId": _AIRBYTE_USER_ID,
            "defaultWorkspaceId": _NEW_WORKSPACE_ID,
        }

    monkeypatch.setattr(api_util, "get_user_by_auth_id", get_user_by_auth_id)
    _user_identity._user_id_cache.set(
        _AUTH_USER_ID,
        _user_identity._CachedAirbyteUser(
            user=_user_identity.AirbyteUser(
                user_id=_AIRBYTE_USER_ID,
                default_workspace_id=_OLD_WORKSPACE_ID,
            ),
            fetched_at=_user_identity.time.monotonic() - 1,
        ),
    )

    (response,) = asyncio.run(
        _stateless_session(_probe_app(sinks), [_initialize_request()])
    )

    assert response.status_code == 200, response.text
    assert len(captured) == 1
    assert captured[0].extra["airbyte_user_id"] == _AIRBYTE_USER_ID
    assert captured[0].extra["workspace_id"] == _NEW_WORKSPACE_ID
    assert captured[0].extra["organization_id"] == _ORGANIZATION_ID
    assert captured[0].extra["scope_source"] == "default"
    assert user_lookups == [_AUTH_USER_ID]
    assert hosted_identity[1] == [_NEW_WORKSPACE_ID]


def test_hosted_initialize_uses_stale_workspace_when_refresh_fails(
    records, hosted, hosted_identity, monkeypatch: pytest.MonkeyPatch
) -> None:
    sinks, captured = records
    monkeypatch.setattr(_telemetry, "DEFAULT_WORKSPACE_CACHE_TTL_SECONDS", 0.0)

    def raise_lookup(*_: object, **__: object) -> None:
        raise RuntimeError("user lookup failed")

    monkeypatch.setattr(api_util, "get_user_by_auth_id", raise_lookup)
    _user_identity._user_id_cache.set(
        _AUTH_USER_ID,
        _user_identity._CachedAirbyteUser(
            user=_user_identity.AirbyteUser(
                user_id=_AIRBYTE_USER_ID,
                default_workspace_id=_OLD_WORKSPACE_ID,
            ),
            fetched_at=_user_identity.time.monotonic() - 1,
        ),
    )

    (response,) = asyncio.run(
        _stateless_session(_probe_app(sinks), [_initialize_request()])
    )

    assert response.status_code == 200, response.text
    assert len(captured) == 1
    assert captured[0].extra["airbyte_user_id"] == _AIRBYTE_USER_ID
    assert captured[0].extra["workspace_id"] == _OLD_WORKSPACE_ID
    assert captured[0].extra["organization_id"] == _ORGANIZATION_ID
    assert captured[0].extra["scope_source"] == "default"
    assert hosted_identity[1] == [_OLD_WORKSPACE_ID]


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
        content = result.content[0]
        assert isinstance(content, TextContent)
        return json.loads(content.text)

    properties = asyncio.run(call_probe())

    assert properties["transport"] == "stdio"
    assert properties["session_id"] == _telemetry._STDIO_SESSION_ID
    assert properties["auth_method"] == "client_credentials"
    assert properties["edition"] == "oss"
    assert properties["mcp_client_name"]
    assert captured
    assert {record.invocation_type for record in captured} == {SERVER_CONNECTED_EVENT}
    (connected,) = captured
    assert connected.extra["airbyte_user_id"] is None
    assert connected.extra["scope_source"] is None


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


def test_auth_failed_keeps_server_segment_identity_without_user_lookups(
    hosted, monkeypatch: pytest.MonkeyPatch
) -> None:
    sinks, captured, track = _capture_lifecycle_segment(monkeypatch)
    get_user_id = MagicMock(
        side_effect=AssertionError("identity lookup should not run")
    )
    get_user = MagicMock(side_effect=AssertionError("identity lookup should not run"))
    get_organization = MagicMock(
        side_effect=AssertionError("organization lookup should not run")
    )
    monkeypatch.setattr(api_util, "get_user_id_from_bearer_token", get_user_id)
    monkeypatch.setattr(api_util, "get_user_by_auth_id", get_user)
    monkeypatch.setattr(api_util, "get_workspace_organization_info", get_organization)
    app = McpRequestTelemetryMiddleware(
        _status_app(401, 'Bearer error="invalid_token"'),
        sinks=sinks,
        mcp_path="/mcp",
    )

    async def send_request() -> httpx.Response:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            return await client.post(
                "/mcp", headers={"authorization": "Bearer invalid"}
            )

    response = asyncio.run(send_request())

    assert response.status_code == 401
    assert len(captured) == 1
    assert captured[0].invocation_type == AUTH_FAILED_EVENT
    assert "airbyte_user_id" not in captured[0].extra
    assert track.call_args.args[:2] == (server.SEGMENT_USER_ID, AUTH_FAILED_EVENT)
    get_user_id.assert_not_called()
    get_user.assert_not_called()
    get_organization.assert_not_called()


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
