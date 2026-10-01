# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tool-call telemetry carries the workspace and organization each call acted on."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Iterator
from contextvars import ContextVar
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.auth import AccessToken
from fastmcp.server.middleware import Middleware
from fastmcp_extensions import TelemetryRecord, ToolCallTelemetryMiddleware

from airbyte._util import api_util
from airbyte.cloud.client import CloudClient
from airbyte.mcp import _scope, _user_identity, server
from airbyte.secrets.base import SecretString


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
    lookup = AsyncMock()
    monkeypatch.setattr(_scope, "resolve_call_workspace_organization_id", lookup)

    class _Capture(Middleware):
        async def on_call_tool(self, context, call_next):  # noqa: ANN001, ANN202
            try:
                return await call_next(context)
            finally:
                await _scope.enrich_call_scope(None)
                seen.append(_scope.call_scope_properties())

    app.add_middleware(_scope.CallScopeMiddleware())
    app.add_middleware(_Capture())

    @app.tool
    def sync_tool() -> str:
        _scope.record_default_workspace(WORKSPACE)
        _scope.record_default_workspace(OTHER_WORKSPACE)
        _scope.record_resolved_organization(ORGANIZATION, workspace_id=WORKSPACE)
        return "ok"

    async def run() -> None:
        async with Client(app) as client:
            await client.call_tool("sync_tool", {})

    asyncio.run(run())
    assert seen == [
        {
            "workspace_id": WORKSPACE,
            "organization_id": ORGANIZATION,
            "scope_source": "default",
        }
    ]
    lookup.assert_not_called()


@pytest.mark.parametrize(
    "workspace,organization,existing",
    [
        (OTHER_WORKSPACE, ORGANIZATION, None),
        (None, ORGANIZATION, None),
        ("invalid", ORGANIZATION, None),
        (WORKSPACE, "invalid", None),
        (WORKSPACE, ORGANIZATION, USER),
    ],
)
def test_resolved_organization_cannot_replace_or_mismatch_scope(
    workspace, organization, existing
):
    scope = _scope.CallScope(workspace_id=WORKSPACE, organization_id=existing)
    token = _scope._CALL_SCOPE.set(scope)
    try:
        _scope.record_resolved_organization(organization, workspace_id=workspace)
        assert scope.organization_id == existing
    finally:
        _scope._CALL_SCOPE.reset(token)


@pytest.mark.parametrize(
    "tool",
    [
        "describe_cloud_workspace",
        "describe_cloud_organization",
        "get_cloud_organization_billing_status",
    ],
)
def test_cloud_tool_records_resolved_organization(monkeypatch, events, tool):
    from airbyte.mcp import cloud

    org = SimpleNamespace(
        organization_id=ORGANIZATION,
        organization_name="Example",
        email=None,
        enabled_features=[],
        get_billing_status=lambda: SimpleNamespace(
            payment_status="okay",
            subscription_status="subscribed",
            is_account_locked=False,
        ),
    )
    monkeypatch.setattr(CloudClient, "get_organization", lambda *_, **__: org)
    monkeypatch.setattr(
        CloudClient,
        "get_workspace",
        lambda *_, **__: SimpleNamespace(
            workspace_id=WORKSPACE,
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=None,
            workspace_url=None,
            get_organization=lambda **_: org,
        ),
    )
    monkeypatch.setattr(
        cloud.api_util,
        "get_workspace",
        lambda **_: SimpleNamespace(workspace_id=WORKSPACE, name="Example"),
    )
    workspace_tool = tool == "describe_cloud_workspace"
    _call(
        tool,
        {"workspace_id": WORKSPACE}
        if workspace_tool
        else {"organization_name": "Example"},
    )
    assert events[-1].success
    assert _scope_of(events[-1]) == (
        WORKSPACE if workspace_tool else None,
        ORGANIZATION,
        "arg" if workspace_tool else "resolved",
    )


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
    _user_identity._default_organization_lookup_failed_at.clear()
    yield organization_lookups
    _user_identity._user_id_cache.clear()
    _user_identity._workspace_organization_id_cache.clear()
    _user_identity._default_organization_lookup_failed_at.clear()


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


def test_name_scoped_call_does_not_fall_back_to_the_users_default_organization(
    monkeypatch: pytest.MonkeyPatch,
    events: list[TelemetryRecord],
    verified_user: list[str],
) -> None:
    monkeypatch.setattr(CloudClient, "list_workspaces", lambda self, **_: [])
    _call("list_cloud_workspaces", {"organization_name": "Other Org"})
    assert _scope_of(events[-1]) == (None, None, None)
    assert verified_user == []


def test_failed_default_organization_lookup_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
    events: list[TelemetryRecord],
    verified_user: list[str],
) -> None:
    def fail(workspace_id: str, **_: Any) -> dict[str, str]:
        verified_user.append(workspace_id)
        raise ConnectionError(workspace_id)

    monkeypatch.setattr(api_util, "get_workspace_organization_info", fail)
    _call("get_connector_info", {"connector_name": "source-faker"})
    _call("get_connector_info", {"connector_name": "source-faker"})
    assert [_scope_of(event) for event in events[-2:]] == [(None, None, None)] * 2
    assert verified_user == [OTHER_WORKSPACE]


def test_slow_default_organization_lookup_does_not_block_the_tool(
    monkeypatch: pytest.MonkeyPatch,
    events: list[TelemetryRecord],
    verified_user: list[str],
) -> None:
    monkeypatch.setattr(_user_identity, "USER_DEFAULT_ORGANIZATION_WAIT_SECONDS", 0.05)
    release = threading.Event()

    def slow(workspace_id: str, **_: Any) -> dict[str, str]:
        verified_user.append(workspace_id)
        release.wait(timeout=10)
        return {"organizationId": ORGANIZATION}

    monkeypatch.setattr(api_util, "get_workspace_organization_info", slow)

    async def run() -> None:
        async with Client(server.app) as client:
            arguments = {"connector_name": "source-faker"}
            await asyncio.wait_for(client.call_tool("get_connector_info", arguments), 5)
            release.set()
            while _user_identity._pending_default_organization_lookups:
                await asyncio.sleep(0.01)
            await client.call_tool("get_connector_info", arguments)

    try:
        asyncio.run(run())
    finally:
        release.set()
    assert [_scope_of(event) for event in events[-2:]] == [
        (None, None, None),
        (None, ORGANIZATION, "user_default"),
    ]
    assert verified_user == [OTHER_WORKSPACE]


def test_explicit_org_does_not_trigger_redundant_lookup(monkeypatch):
    from unittest.mock import AsyncMock

    unexpected = AsyncMock()
    monkeypatch.setattr(_scope, "resolve_call_workspace_organization_id", unexpected)
    scope = _scope.CallScope(workspace_id=WORKSPACE, organization_id=ORGANIZATION)
    token = _scope._CALL_SCOPE.set(scope)
    try:
        asyncio.run(_scope.enrich_call_scope(None))
        assert scope.organization_id == ORGANIZATION
        unexpected.assert_not_awaited()
    finally:
        _scope._CALL_SCOPE.reset(token)


def test_concurrent_workspace_lookups_share_work_and_survive_waiter_cancellation(
    monkeypatch, verified_user
):
    resolve = _user_identity.resolve_workspace_organization_id

    async def run():
        started = asyncio.Event()
        release = asyncio.Event()
        started_workspaces = []

        async def delayed(workspace, **kwargs):
            started_workspaces.append(workspace)
            if len(started_workspaces) == 2:
                started.set()
            await release.wait()
            return await resolve(workspace, **kwargs)

        monkeypatch.setattr(
            _user_identity, "resolve_workspace_organization_id", delayed
        )
        tasks = [
            asyncio.create_task(
                _user_identity.resolve_call_workspace_organization_id(workspace, None)
            )
            for workspace in [WORKSPACE] * 5 + [OTHER_WORKSPACE] * 5
        ]
        try:
            await asyncio.wait_for(started.wait(), 5)
            tasks[0].cancel()
            with pytest.raises(asyncio.CancelledError):
                await tasks[0]
            release.set()
            assert await asyncio.gather(*tasks[1:]) == [ORGANIZATION] * 9
            assert sorted(started_workspaces) == sorted([WORKSPACE, OTHER_WORKSPACE])
            assert (
                await _user_identity.resolve_call_workspace_organization_id(
                    WORKSPACE, None
                )
                == ORGANIZATION
            )
            assert not _user_identity._pending_default_organization_lookups
        finally:
            release.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(run())
    assert sorted(verified_user) == sorted([WORKSPACE, OTHER_WORKSPACE])


def test_later_call_joins_pending_lookup_after_first_waiter_times_out(
    monkeypatch, verified_user
):
    resolve = _user_identity.resolve_workspace_organization_id
    monkeypatch.setattr(_user_identity, "USER_DEFAULT_ORGANIZATION_WAIT_SECONDS", 0.01)

    async def run():
        release = asyncio.Event()
        lookup_count = 0

        async def delayed(*args, **kwargs):
            nonlocal lookup_count
            lookup_count += 1
            await release.wait()
            return await resolve(*args, **kwargs)

        monkeypatch.setattr(
            _user_identity, "resolve_workspace_organization_id", delayed
        )
        assert (
            await _user_identity.resolve_call_workspace_organization_id(WORKSPACE, None)
            is None
        )
        key = next(iter(_user_identity._pending_default_organization_lookups))
        assert _user_identity._default_organization_lookup_failed_at.get(key)
        pending = _user_identity._pending_default_organization_lookups[key]
        monkeypatch.setattr(_user_identity, "USER_DEFAULT_ORGANIZATION_WAIT_SECONDS", 1)
        later = asyncio.create_task(
            _user_identity.resolve_call_workspace_organization_id(WORKSPACE, None)
        )
        try:
            # Let the later caller join while the cache is still cold and the
            # timeout marker is present, then complete the original API lookup.
            await asyncio.sleep(0)
            release.set()
            assert await later == ORGANIZATION
            assert lookup_count == 1
            assert not _user_identity._pending_default_organization_lookups
        finally:
            release.set()
            await asyncio.gather(pending, later, return_exceptions=True)

    asyncio.run(run())
    assert verified_user == [WORKSPACE]


def test_distinct_workspace_lookups_are_bounded_until_blocking_io_finishes(
    monkeypatch, verified_user
):
    monkeypatch.setattr(_user_identity, "MAX_PENDING_ORGANIZATION_LOOKUPS", 2)
    monkeypatch.setattr(_user_identity, "USER_DEFAULT_ORGANIZATION_WAIT_SECONDS", 0.01)
    release = threading.Event()
    workspaces = [f"{index:08x}-1111-1111-1111-111111111111" for index in range(8)]

    def slow(workspace_id, **_):
        verified_user.append(workspace_id)
        assert release.wait(5)
        return {"organizationId": ORGANIZATION}

    monkeypatch.setattr(api_util, "get_workspace_organization_info", slow)

    async def run():
        try:
            results = await asyncio.gather(*[
                _user_identity.resolve_call_workspace_organization_id(workspace, None)
                for workspace in workspaces
            ])
            assert results == [None] * len(workspaces)
            # Callers timing out must not free capacity while worker threads remain busy.
            await asyncio.sleep(0.03)
            assert len(_user_identity._pending_default_organization_lookups) == 2
            assert set(verified_user) == set(workspaces[:2])
            assert (
                await _user_identity.resolve_call_workspace_organization_id(
                    workspaces[-1], None
                )
                is None
            )
        finally:
            pending = list(
                _user_identity._pending_default_organization_lookups.values()
            )
            release.set()
            await asyncio.gather(*pending, return_exceptions=True)
        assert not _user_identity._pending_default_organization_lookups
        monkeypatch.setattr(_user_identity, "USER_DEFAULT_ORGANIZATION_WAIT_SECONDS", 1)
        # Admission rejection is not a lookup failure: retry immediately once capacity frees.
        assert (
            await _user_identity.resolve_call_workspace_organization_id(
                workspaces[-1], None
            )
            == ORGANIZATION
        )

    try:
        asyncio.run(run())
    finally:
        release.set()
    assert set(verified_user) == {workspaces[0], workspaces[1], workspaces[-1]}


@pytest.mark.parametrize("changed_part", ["token", "api_root", "config_api_root"])
def test_pending_failures_and_cache_are_isolated_by_credentials_and_host(
    monkeypatch, verified_user, changed_part
):
    denied = (SecretString("denied-secret"), "https://first.invalid", None)
    allowed = {
        "token": (SecretString("allowed-secret"), denied[1], denied[2]),
        "api_root": (denied[0], "https://second.invalid", denied[2]),
        "config_api_root": (denied[0], denied[1], "https://config.invalid"),
    }[changed_part]
    credentials = ContextVar("test_scope_credentials", default=denied)
    monkeypatch.setattr(
        _user_identity, "_request_credentials", lambda _: credentials.get()
    )
    started = threading.Event()
    release = threading.Event()
    calls = []

    def lookup(workspace_id, *, bearer_token, api_root, config_api_root, **_):
        identity = (bearer_token, api_root, config_api_root)
        calls.append(identity)
        if identity == denied:
            started.set()
            assert release.wait(5)
            raise PermissionError("synthetic unauthorized caller")
        assert identity == allowed
        return {"organizationId": ORGANIZATION}

    monkeypatch.setattr(api_util, "get_workspace_organization_info", lookup)

    async def call(identity):
        token = credentials.set(identity)
        scope = _scope.CallScope(
            workspace_id=WORKSPACE,
            workspace_source="arg",
            user_default_organization_id="99999999-9999-9999-9999-999999999999",
        )
        scope_token = _scope._CALL_SCOPE.set(scope)
        try:
            await _scope.enrich_call_scope(None)
            assert scope.workspace_id == WORKSPACE
            return scope.resolved().organization_id
        finally:
            _scope._CALL_SCOPE.reset(scope_token)
            credentials.reset(token)

    async def run():
        first = asyncio.create_task(call(denied))
        try:
            assert await asyncio.to_thread(started.wait, 2)
            # This caller must not join the other caller's unauthorized request.
            assert await call(allowed) == ORGANIZATION
        finally:
            release.set()
            await first
        assert await call(denied) is None  # Its own failure remains suppressed.
        assert calls.count(denied) == 1
        _user_identity._workspace_organization_id_cache.clear()
        assert await call(allowed) == ORGANIZATION  # Other callers remain eligible.
        assert calls.count(allowed) == 2
        assert "secret" not in repr(
            _user_identity._default_organization_lookup_failed_at._entries
        )

    try:
        asyncio.run(run())
    finally:
        release.set()
