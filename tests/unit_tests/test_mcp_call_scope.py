# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tool-call telemetry carries the workspace and organization each call acted on."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any

import pytest
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.auth import AccessToken
from fastmcp.server.middleware import Middleware
from fastmcp_extensions import TelemetryRecord, ToolCallTelemetryMiddleware

from airbyte._util import api_util
from airbyte.cloud.client import CloudClient
from airbyte.mcp import _scope, _user_identity, server


WORKSPACE = "11111111-1111-1111-1111-111111111111"
OTHER_WORKSPACE = "22222222-2222-2222-2222-222222222222"
ORGANIZATION = "33333333-3333-3333-3333-333333333333"
USER = "44444444-4444-4444-4444-444444444444"


class _FakeWorkspace:
    workspace_id = WORKSPACE

    def list_custom_source_definitions(self, **_: Any) -> list[Any]:
        return []


@pytest.fixture
def events(monkeypatch: pytest.MonkeyPatch) -> list[TelemetryRecord]:
    """Capture every tool-call telemetry record."""
    captured: list[TelemetryRecord] = []
    telemetry = next(
        middleware
        for middleware in server.app.middleware
        if isinstance(middleware, ToolCallTelemetryMiddleware)
    )
    monkeypatch.setattr(telemetry._sinks, "emit", captured.append)
    for name in (
        "AIRBYTE_CLOUD_WORKSPACE_ID",
        "AIRBYTE_CLOUD_ORGANIZATION_ID",
        "AIRBYTE_CLOUD_CLIENT_ID",
        "AIRBYTE_CLOUD_CLIENT_SECRET",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AIRBYTE_CLOUD_BEARER_TOKEN", "test-token")
    return captured


def _call(tool: str, arguments: dict[str, Any]) -> None:
    async def run() -> None:
        async with Client(server.app) as client:
            try:
                await client.call_tool(tool, arguments)
            except ToolError:
                pass

    asyncio.run(run())


def _scope_of(event: TelemetryRecord) -> tuple[object, object, object]:
    return (
        event.extra["workspace_id"],
        event.extra["organization_id"],
        event.extra["scope_source"],
    )


def _stub_workspace(
    monkeypatch: pytest.MonkeyPatch, *, default: str | None
) -> list[str]:
    resolved: list[str] = []
    monkeypatch.setattr(
        CloudClient, "resolve_default_workspace_id", lambda self: default
    )

    def get_workspace(self: CloudClient, workspace_id: str) -> _FakeWorkspace:
        resolved.append(workspace_id)
        return _FakeWorkspace()

    monkeypatch.setattr(CloudClient, "get_workspace", get_workspace)
    return resolved


def test_workspace_argument_is_recorded(
    monkeypatch: pytest.MonkeyPatch, events: list[TelemetryRecord]
) -> None:
    _stub_workspace(monkeypatch, default=OTHER_WORKSPACE)
    _call("list_custom_source_definitions", {"workspace_id": WORKSPACE.upper()})
    assert _scope_of(events[-1]) == (WORKSPACE, None, "arg")


def test_workspace_header_is_recorded(
    monkeypatch: pytest.MonkeyPatch, events: list[TelemetryRecord]
) -> None:
    _stub_workspace(monkeypatch, default=OTHER_WORKSPACE)
    monkeypatch.setenv("AIRBYTE_CLOUD_WORKSPACE_ID", WORKSPACE)
    _call("list_custom_source_definitions", {})
    assert _scope_of(events[-1]) == (WORKSPACE, None, "header")


def test_default_workspace_used_by_the_tool_is_recorded(
    monkeypatch: pytest.MonkeyPatch, events: list[TelemetryRecord]
) -> None:
    resolved = _stub_workspace(monkeypatch, default=WORKSPACE)
    _call("list_custom_source_definitions", {})
    assert resolved == [WORKSPACE]
    assert _scope_of(events[-1]) == (WORKSPACE, None, "default")


def test_argument_is_recorded_when_the_call_fails(
    monkeypatch: pytest.MonkeyPatch, events: list[TelemetryRecord]
) -> None:
    def fail(self: CloudClient, workspace_id: str) -> None:
        raise PermissionError(workspace_id)

    monkeypatch.setattr(CloudClient, "get_workspace", fail)
    _call("list_custom_source_definitions", {"workspace_id": WORKSPACE})
    assert events[-1].success is False
    assert _scope_of(events[-1]) == (WORKSPACE, None, "arg")


def test_organization_argument_is_recorded_without_a_workspace(
    monkeypatch: pytest.MonkeyPatch, events: list[TelemetryRecord]
) -> None:
    monkeypatch.setattr(CloudClient, "list_workspaces", lambda self, **_: [])
    _call("list_cloud_workspaces", {"organization_id": ORGANIZATION})
    assert _scope_of(events[-1]) == (None, ORGANIZATION, "arg")


def test_organization_header_is_recorded_alongside_the_workspace(
    monkeypatch: pytest.MonkeyPatch, events: list[TelemetryRecord]
) -> None:
    _stub_workspace(monkeypatch, default=None)
    monkeypatch.setenv("AIRBYTE_CLOUD_ORGANIZATION_ID", ORGANIZATION)
    _call("list_custom_source_definitions", {"workspace_id": WORKSPACE})
    assert _scope_of(events[-1]) == (WORKSPACE, ORGANIZATION, "arg")


def test_non_uuid_argument_is_not_recorded_or_replaced_by_the_header(
    monkeypatch: pytest.MonkeyPatch, events: list[TelemetryRecord]
) -> None:
    _stub_workspace(monkeypatch, default=OTHER_WORKSPACE)
    monkeypatch.setenv("AIRBYTE_CLOUD_WORKSPACE_ID", OTHER_WORKSPACE)
    _call("list_custom_source_definitions", {"workspace_id": "not-a-workspace"})
    assert _scope_of(events[-1]) == (None, None, None)


def test_tools_without_cloud_scope_record_nulls(events: list[TelemetryRecord]) -> None:
    _call("get_connector_info", {"connector_name": "source-faker"})
    assert _scope_of(events[-1]) == (None, None, None)


def test_scope_does_not_leak_between_calls(
    monkeypatch: pytest.MonkeyPatch, events: list[TelemetryRecord]
) -> None:
    _stub_workspace(monkeypatch, default=WORKSPACE)
    _call("list_custom_source_definitions", {})
    _call("get_connector_info", {"connector_name": "source-faker"})
    assert [_scope_of(event) for event in events[-2:]] == [
        (WORKSPACE, None, "default"),
        (None, None, None),
    ]
    assert _scope.current_call_scope() is None


def test_default_workspace_recorded_from_a_sync_tool_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sync tools run off the event loop; the recorded scope must still reach telemetry."""
    app = FastMCP("scope-test")
    seen: list[dict[str, str | None]] = []

    class _Capture(Middleware):
        async def on_call_tool(self, context, call_next):  # noqa: ANN001, ANN202
            try:
                return await call_next(context)
            finally:
                seen.append(_scope.call_scope_properties())

    app.add_middleware(_scope.CallScopeMiddleware())
    app.add_middleware(_Capture())

    @app.tool
    def sync_tool() -> str:
        _scope.record_default_workspace(WORKSPACE)
        _scope.record_default_workspace(OTHER_WORKSPACE)
        return "ok"

    async def run() -> None:
        async with Client(app) as client:
            await client.call_tool("sync_tool", {})

    asyncio.run(run())
    assert seen == [
        {"workspace_id": WORKSPACE, "organization_id": None, "scope_source": "default"}
    ]


@pytest.fixture
def verified_user(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """Authenticate calls as a user whose default workspace is in `ORGANIZATION`.

    Returns the workspaces whose organization was looked up.
    """
    organization_lookups: list[str] = []
    monkeypatch.setattr(
        _user_identity,
        "get_access_token",
        lambda: AccessToken(token="verified-token", client_id="client", scopes=[]),
    )
    monkeypatch.setattr(
        api_util, "get_user_id_from_bearer_token", lambda _: "keycloak-user"
    )
    monkeypatch.setattr(
        api_util,
        "get_user_by_auth_id",
        lambda *_, **__: {"userId": USER, "defaultWorkspaceId": OTHER_WORKSPACE},
    )

    def get_workspace_organization_info(workspace_id: str, **_: Any) -> dict[str, str]:
        organization_lookups.append(workspace_id)
        return {"organizationId": ORGANIZATION}

    monkeypatch.setattr(
        api_util, "get_workspace_organization_info", get_workspace_organization_info
    )
    _user_identity._user_id_cache.clear()
    _user_identity._workspace_organization_id_cache.clear()
    yield organization_lookups
    _user_identity._user_id_cache.clear()
    _user_identity._workspace_organization_id_cache.clear()


def test_unscoped_call_falls_back_to_the_users_default_organization(
    events: list[TelemetryRecord], verified_user: list[str]
) -> None:
    _call("get_connector_info", {"connector_name": "source-faker"})
    _call("get_connector_info", {"connector_name": "source-faker"})
    assert [_scope_of(event) for event in events[-2:]] == [
        (None, ORGANIZATION, "user_default"),
        (None, ORGANIZATION, "user_default"),
    ]
    assert verified_user == [OTHER_WORKSPACE]


def test_scoped_call_does_not_look_up_the_users_default_organization(
    monkeypatch: pytest.MonkeyPatch,
    events: list[TelemetryRecord],
    verified_user: list[str],
) -> None:
    _stub_workspace(monkeypatch, default=OTHER_WORKSPACE)
    _call("list_custom_source_definitions", {"workspace_id": WORKSPACE})
    assert _scope_of(events[-1]) == (WORKSPACE, None, "arg")
    assert verified_user == []


def test_default_workspace_used_by_the_tool_wins_over_the_users_default_organization(
    monkeypatch: pytest.MonkeyPatch,
    events: list[TelemetryRecord],
    verified_user: list[str],
) -> None:
    _stub_workspace(monkeypatch, default=WORKSPACE)
    _call("list_custom_source_definitions", {})
    assert _scope_of(events[-1]) == (WORKSPACE, None, "default")


def test_unscoped_call_without_a_default_workspace_records_nulls(
    monkeypatch: pytest.MonkeyPatch,
    events: list[TelemetryRecord],
    verified_user: list[str],
) -> None:
    monkeypatch.setattr(
        api_util, "get_user_by_auth_id", lambda *_, **__: {"userId": USER}
    )
    _call("get_connector_info", {"connector_name": "source-faker"})
    assert _scope_of(events[-1]) == (None, None, None)
    assert verified_user == []
