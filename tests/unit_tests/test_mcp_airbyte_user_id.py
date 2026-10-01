# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tool-call telemetry is attributed to the caller's canonical Airbyte user."""

from __future__ import annotations

import asyncio
import base64
import json
import time
from collections.abc import Iterator
from contextvars import ContextVar
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastmcp import Client
from fastmcp.server.auth import AccessToken
from fastmcp_extensions import ToolCallTelemetryMiddleware

from airbyte._util import api_util
from airbyte.mcp import _user_identity, guidance, server


TOOL = "get_api_docs_urls"
USER_A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
USER_B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
AUTH_USERS = {"keycloak-a": USER_A, "keycloak-b": USER_B}

_request_auth_user: ContextVar[str | None] = ContextVar(
    "request_auth_user", default=None
)


def _jwt(claims: dict[str, Any]) -> str:
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"header.{payload}.signature"


def _verified_token() -> AccessToken | None:
    auth_user = _request_auth_user.get()
    if auth_user is None:
        return None
    return AccessToken(token=_jwt({"sub": auth_user}), client_id="client", scopes=[])


@pytest.fixture
def segment(monkeypatch: pytest.MonkeyPatch) -> Iterator[MagicMock]:
    """Capture Segment `track` calls for every tool-call event."""
    telemetry = next(
        middleware
        for middleware in server.app.middleware
        if isinstance(middleware, ToolCallTelemetryMiddleware)
    )
    track = MagicMock()
    monkeypatch.setattr("fastmcp_extensions._telemetry._segment_analytics.track", track)
    monkeypatch.setattr(telemetry._sinks, "segment_enabled", True)
    monkeypatch.setattr(telemetry._sinks, "sentry_enabled", False)
    monkeypatch.setattr(_user_identity, "get_access_token", _verified_token)
    monkeypatch.setattr(guidance, "get_connector_api_docs_urls", lambda _: [])
    _user_identity._user_id_cache.clear()
    yield track
    _user_identity._user_id_cache.clear()


@pytest.fixture
def lookups(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Stub `/users/get_by_auth_id`, recording each auth user looked up."""
    calls: list[str] = []

    def get_user_by_auth_id(auth_user_id: str, **_: Any) -> dict[str, Any]:
        calls.append(auth_user_id)
        return {"userId": AUTH_USERS[auth_user_id]}

    monkeypatch.setattr(api_util, "get_user_by_auth_id", get_user_by_auth_id)
    return calls


async def _call_as(auth_user: str | None) -> None:
    _request_auth_user.set(auth_user)
    async with Client(server.app) as client:
        await client.call_tool(TOOL, {"connector_name": "source-faker"})


def _call(*auth_users: str | None) -> None:
    async def run() -> None:
        await asyncio.gather(*(_call_as(auth_user) for auth_user in auth_users))

    asyncio.run(run())


def _identities(track: MagicMock) -> list[tuple[str, object]]:
    """Return `(segment_user_id, airbyte_user_id)` for each tracked tool-call event."""
    return [
        (call.args[0], call.args[2]["airbyte_user_id"])
        for call in track.call_args_list
        if call.args[1] == "mcp_tool_call"
    ]


def test_tool_call_is_attributed_to_airbyte_user(
    segment: MagicMock, lookups: list[str]
) -> None:
    _call("keycloak-a")

    assert _identities(segment) == [(USER_A, USER_A)]
    assert lookups == ["keycloak-a"]


def test_lookup_is_cached_per_auth_user(segment: MagicMock, lookups: list[str]) -> None:
    _call("keycloak-a")
    _call("keycloak-a")
    _call("keycloak-b")

    assert _identities(segment) == [
        (USER_A, USER_A),
        (USER_A, USER_A),
        (USER_B, USER_B),
    ]
    assert lookups == ["keycloak-a", "keycloak-b"]


def test_forget_cached_airbyte_user_removes_entry_and_next_resolve_looks_up(
    segment: MagicMock, lookups: list[str]
) -> None:
    _user_identity._user_id_cache.set(
        "keycloak-a",
        _user_identity.AirbyteUser(
            user_id=USER_A,
            default_workspace_id="old-workspace",
        ),
    )
    _request_auth_user.set("keycloak-a")

    _user_identity.forget_cached_airbyte_user()

    assert _user_identity._user_id_cache.get("keycloak-a") is None
    _user_identity.forget_cached_airbyte_user()
    _call("keycloak-a")
    assert lookups == ["keycloak-a"]
    assert _identities(segment) == [(USER_A, USER_A)]


def test_forget_cached_airbyte_user_without_access_token_is_noop(
    segment: MagicMock,
) -> None:
    cached_user = _user_identity.AirbyteUser(
        user_id=USER_A,
        default_workspace_id="workspace-a",
    )
    _user_identity._user_id_cache.set("keycloak-a", cached_user)
    _request_auth_user.set(None)

    _user_identity.forget_cached_airbyte_user()

    assert _user_identity._user_id_cache.get("keycloak-a") is cached_user


def test_forget_cached_airbyte_user_preserves_other_users(
    segment: MagicMock,
) -> None:
    _user_identity._user_id_cache.set(
        "keycloak-a",
        _user_identity.AirbyteUser(
            user_id=USER_A,
            default_workspace_id="workspace-a",
        ),
    )
    other_user = _user_identity.AirbyteUser(
        user_id=USER_B,
        default_workspace_id="workspace-b",
    )
    _user_identity._user_id_cache.set("keycloak-b", other_user)
    _request_auth_user.set("keycloak-a")

    _user_identity.forget_cached_airbyte_user()

    assert _user_identity._user_id_cache.get("keycloak-a") is None
    assert _user_identity._user_id_cache.get("keycloak-b") is other_user


def test_concurrent_calls_keep_their_own_user(
    segment: MagicMock, lookups: list[str]
) -> None:
    _call("keycloak-a", "keycloak-b", None, "keycloak-a")

    assert sorted(_identities(segment), key=str) == sorted(
        [
            (USER_A, USER_A),
            (USER_B, USER_B),
            (server.SEGMENT_USER_ID, None),
            (USER_A, USER_A),
        ],
        key=str,
    )


def test_unauthenticated_call_falls_back(
    segment: MagicMock, lookups: list[str]
) -> None:
    _call(None)

    assert _identities(segment) == [(server.SEGMENT_USER_ID, None)]
    assert lookups == []


def test_token_without_user_claim_falls_back(
    monkeypatch: pytest.MonkeyPatch, segment: MagicMock, lookups: list[str]
) -> None:
    monkeypatch.setattr(
        _user_identity,
        "get_access_token",
        lambda: AccessToken(token="opaque-token", client_id="client", scopes=[]),
    )
    _call(None)

    assert _identities(segment) == [(server.SEGMENT_USER_ID, None)]
    assert lookups == []


def test_failed_lookup_does_not_break_the_call_and_is_retried(
    monkeypatch: pytest.MonkeyPatch, segment: MagicMock
) -> None:
    calls: list[str] = []

    def get_user_by_auth_id(auth_user_id: str, **_: Any) -> dict[str, Any]:
        calls.append(auth_user_id)
        raise api_util.AirbyteError(message="boom")

    monkeypatch.setattr(api_util, "get_user_by_auth_id", get_user_by_auth_id)
    _call("keycloak-a")
    _call("keycloak-a")

    assert _identities(segment) == [(server.SEGMENT_USER_ID, None)] * 2
    assert calls == ["keycloak-a", "keycloak-a"]


def test_slow_lookup_times_out_without_blocking_the_call(
    monkeypatch: pytest.MonkeyPatch, segment: MagicMock
) -> None:
    monkeypatch.setattr(_user_identity, "USER_ID_LOOKUP_TIMEOUT_SECONDS", 0.1)

    def get_user_by_auth_id(auth_user_id: str, **_: Any) -> dict[str, Any]:
        time.sleep(2)
        return {"userId": AUTH_USERS[auth_user_id]}

    monkeypatch.setattr(api_util, "get_user_by_auth_id", get_user_by_auth_id)

    async def timed_call() -> float:
        started = time.monotonic()
        await _call_as("keycloak-a")
        return time.monotonic() - started

    assert asyncio.run(timed_call()) < 1.5
    assert _identities(segment) == [(server.SEGMENT_USER_ID, None)]


def test_user_id_is_not_left_in_context_after_the_call(
    segment: MagicMock, lookups: list[str]
) -> None:
    _call("keycloak-a")

    assert _user_identity.current_airbyte_user_id() is None


def test_lookup_request_has_a_finite_http_timeout(
    monkeypatch: pytest.MonkeyPatch, segment: MagicMock
) -> None:
    """A hung Config API can't pin the lookup's worker thread indefinitely."""
    request = MagicMock()
    request.return_value.status_code = 200
    request.return_value.json.return_value = {"userId": USER_A}
    monkeypatch.setattr(api_util.requests, "request", request)

    _call("keycloak-a")

    timeout = request.call_args.kwargs["timeout"]
    assert timeout == (
        _user_identity.USER_ID_LOOKUP_TIMEOUT_SECONDS,
        _user_identity.USER_ID_LOOKUP_TIMEOUT_SECONDS,
    )
    assert _identities(segment) == [(USER_A, USER_A)]
