# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Test the advertised MCP scheduling contract and serialized tool calls."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from unittest.mock import MagicMock
from urllib.parse import urlsplit

import pytest
import requests
import responses
from airbyte_api import models
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from airbyte._util import api_util
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.models import StreamConfigOverride
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.constants import (
    MCP_LOCAL_DEMO_MODE_ENV_VAR,
    MCP_READONLY_MODE_ENV_VAR,
)
from airbyte.exceptions import (
    AirbyteLibInputError,
    AirbyteUnsafeStreamConfigChangeError,
)
from airbyte.mcp import _tool_utils
from airbyte.mcp import cloud as cloud_mcp
from airbyte_protocol.models import SyncMode


def _connection_response() -> models.ConnectionResponse:
    """Create a complete SDK response for the network boundary."""
    return models.ConnectionResponse(
        connection_id="12345678-1234-1234-1234-1234567890ab",
        created_at=0,
        destination_id="destination-id",
        name="name",
        source_id="source-id",
        status=models.ConnectionStatusEnum.INACTIVE,
        workspace_id="workspace-id",
        configurations=models.StreamConfigurations(streams=[]),
        schedule=models.ConnectionScheduleResponse(
            schedule_type=models.ScheduleTypeWithBasicEnum.MANUAL,
        ),
        tags=[],
    )


@pytest.fixture(params=[False, True], ids=["without-telemetry", "with-telemetry"])
def schedule_backend(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> Iterator[tuple[FastMCP, responses.RequestsMock, MagicMock, MagicMock]]:
    """Keep scheduling boundaries real, with optional unrelated telemetry traffic."""
    server = FastMCP("schedule-tests")
    cloud_mcp.register_cloud_tools(server)
    workspace = CloudWorkspace(
        workspace_id="workspace-id",
        bearer_token="token",
        api_root="https://public.example/custom",
        config_api_root="https://config.example/custom",
    )
    lookup = MagicMock(return_value=workspace)
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", lookup)
    monkeypatch.setattr(
        api_util, "get_connection", MagicMock(return_value=_connection_response())
    )
    patch = MagicMock(return_value=_connection_response())
    monkeypatch.setattr(api_util, "patch_connection", patch)
    monkeypatch.delenv(MCP_LOCAL_DEMO_MODE_ENV_VAR, raising=False)
    with responses.RequestsMock(assert_all_requests_are_fired=False) as http:
        http.post(
            "https://config.example/custom/web_backend/connections/update",
            json={
                "connectionId": "12345678-1234-1234-1234-1234567890ab",
                "scheduleType": "basic",
            },
        )
        http.post("https://api.segment.io/v1/batch", json={"success": True})
        if request.param:
            requests.post(
                "https://api.segment.io/v1/batch", json={"batch": []}, timeout=1
            ).raise_for_status()
        yield server, http, lookup, patch


def _connection_api_calls(http: responses.RequestsMock) -> list[responses.Call]:
    """Exclude only Segment telemetry, retaining auth and unexpected API requests."""
    return [
        call
        for call in http.calls
        if urlsplit(call.request.url or "").hostname != "api.segment.io"
    ]


async def _update(server: FastMCP, **arguments: object) -> str:
    """Call the registered tool through the serialized MCP boundary."""
    async with Client(server) as client:
        result = await client.call_tool(
            "update_connection",
            {"connection_id": "12345678-1234-1234-1234-1234567890ab", **arguments},
        )
    return str(result)


async def _unsafe_update(
    server: FastMCP,
    **arguments: object,
) -> str:
    async with Client(server) as client:
        result = await client.call_tool(
            "update_cloud_connection_unsafe",
            {"connection_id": "12345678-1234-1234-1234-1234567890ab", **arguments},
        )
    return str(result)


def test_interval_schema() -> None:
    """Advertise an optional positive whole-hour interval for the update tool."""
    server = FastMCP("schedule-schema-tests")
    cloud_mcp.register_cloud_tools(server)

    async def inspect_tools() -> None:
        async with Client(server) as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
        update = tools["update_connection"]
        interval = update.input_schema["properties"]["interval_hours"]
        integer_schema = next(
            option for option in interval["anyOf"] if option["type"] == "integer"
        )
        assert integer_schema["exclusiveMinimum"] == 0
        assert interval["default"] is None
        assert "interval_hours" not in update.input_schema.get("required", [])
        assert update.annotations is not None
        assert update.annotations.destructive_hint is False
        assert update.annotations.read_only_hint is False
        unsafe = tools["update_cloud_connection_unsafe"]
        assert unsafe.annotations is not None
        assert unsafe.annotations.destructive_hint is True
        assert "set_cloud_connection_stream_sync_mode" not in tools

    asyncio.run(inspect_tools())


def test_create_manual_default_description() -> None:
    """Explain the manual default and how to enable automatic syncs after creation."""
    server = FastMCP("create-schema-tests")
    cloud_mcp.register_cloud_tools(server)

    async def inspect_tools() -> None:
        async with Client(server) as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
        create = tools["create_connection"]
        description = (create.description or "").lower()
        assert "manual" in description
        assert "default" in description
        assert "automatic syncs" in description
        assert "update_connection" in description

    asyncio.run(inspect_tools())


@pytest.mark.parametrize("interval_hours", [0, -1, True, False, 1.0, 1.5, "24"])
def test_direct_update_rejects_invalid_intervals_before_status_change(
    schedule_backend: tuple[FastMCP, responses.RequestsMock, MagicMock, MagicMock],
    interval_hours: object,
) -> None:
    """Python calls bypassing schema validation still reject invalid intervals first."""
    _server, http, lookup, patch = schedule_backend

    with pytest.raises(AirbyteLibInputError, match="positive whole number"):
        cloud_mcp.update_connection(
            ctx=None,
            connection_id="12345678-1234-1234-1234-1234567890ab",
            enabled=True,
            interval_hours=interval_hours,
            cron_expression=None,
            manual_schedule=None,
            override_stream_configs=None,
            workspace_id=None,
        )

    lookup.assert_not_called()
    patch.assert_not_called()
    assert _connection_api_calls(http) == []


@pytest.mark.parametrize("enabled", [None, True, False])
def test_update_interval_schedule_serialized(
    schedule_backend: tuple[FastMCP, responses.RequestsMock, MagicMock, MagicMock],
    enabled: bool | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A serialized interval call reaches the real basic scheduler, with optional status."""
    server, http, _lookup, patch = schedule_backend
    if enabled is False:
        current = _connection_response()
        current.status = models.ConnectionStatusEnum.ACTIVE
        monkeypatch.setattr(api_util, "get_connection", MagicMock(return_value=current))

    result = asyncio.run(
        _update(server, interval_hours=24, enabled=enabled, manual_schedule=False)
    )

    assert "Successfully updated" in result
    assert "every 24 hours" in result
    calls = _connection_api_calls(http)
    assert len(calls) == 1
    assert calls[0].request.method == "POST"
    assert calls[0].request.url == (
        "https://config.example/custom/web_backend/connections/update"
    )
    assert json.loads(calls[0].request.body) == {
        "connectionId": "12345678-1234-1234-1234-1234567890ab",
        "scheduleType": "basic",
        "scheduleData": {"basicSchedule": {"timeUnit": "hours", "units": 24}},
        "skipReset": True,
    }
    if enabled is not None:
        patch.assert_called_once()
        assert patch.call_args.kwargs["status"] == ("active" if enabled else "inactive")
        assert ("enabled" if enabled else "disabled") in result
    else:
        patch.assert_not_called()


@pytest.mark.parametrize("interval_hours", [0, -1, True, False, 1.0, 1.5, "24"])
def test_update_rejects_invalid_intervals_before_status_change(
    schedule_backend: tuple[FastMCP, responses.RequestsMock, MagicMock, MagicMock],
    interval_hours: object,
) -> None:
    """Reject invalid JSON intervals before fetching or changing a connection."""
    server, http, lookup, patch = schedule_backend

    with pytest.raises(ToolError, match="interval_hours"):
        asyncio.run(_update(server, interval_hours=interval_hours, enabled=True))

    lookup.assert_not_called()
    patch.assert_not_called()
    assert _connection_api_calls(http) == []


@pytest.mark.parametrize(
    "schedule_arguments",
    [
        {"interval_hours": 24, "cron_expression": "0 0 0 * * ?"},
        {"interval_hours": 24, "manual_schedule": True},
        {"cron_expression": "0 0 0 * * ?", "manual_schedule": True},
        {
            "interval_hours": 24,
            "cron_expression": "0 0 0 * * ?",
            "manual_schedule": True,
        },
    ],
)
def test_update_rejects_schedule_conflicts_before_status_change(
    schedule_backend: tuple[FastMCP, responses.RequestsMock, MagicMock, MagicMock],
    schedule_arguments: dict[str, object],
) -> None:
    """Conflicting schedules are rejected before an enabled-status mutation."""
    server, http, lookup, patch = schedule_backend

    with pytest.raises(ToolError, match="Cannot specify"):
        asyncio.run(_update(server, enabled=True, **schedule_arguments))

    lookup.assert_not_called()
    patch.assert_not_called()
    assert _connection_api_calls(http) == []


@pytest.mark.parametrize(
    "arguments",
    [{"cron_expression": "0 0 0 * * ?"}, {"manual_schedule": True}, {"enabled": True}],
)
def test_update_existing_settings_remain_supported(
    schedule_backend: tuple[FastMCP, responses.RequestsMock, MagicMock, MagicMock],
    arguments: dict[str, object],
) -> None:
    """Existing serialized schedule and status updates continue using the public API."""
    server, http, _lookup, patch = schedule_backend

    assert "Successfully updated" in asyncio.run(_update(server, **arguments))

    patch.assert_called_once()
    kwargs = patch.call_args.kwargs
    if "cron_expression" in arguments:
        assert kwargs["schedule"].schedule_type == models.ScheduleTypeEnum.CRON
        assert kwargs["schedule"].cron_expression == "0 0 0 * * ?"
    elif "manual_schedule" in arguments:
        assert kwargs["schedule"].schedule_type == models.ScheduleTypeEnum.MANUAL
    else:
        assert kwargs["status"] == "active"
    assert _connection_api_calls(http) == []


def test_update_interval_obeys_local_demo_mode(
    schedule_backend: tuple[FastMCP, responses.RequestsMock, MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Local demo mode blocks interval updates to connections outside the current session."""
    server, http, lookup, patch = schedule_backend
    monkeypatch.setenv(MCP_LOCAL_DEMO_MODE_ENV_VAR, "1")
    monkeypatch.setattr(_tool_utils, "is_hosted_mcp_mode", lambda: False)
    monkeypatch.setattr(_tool_utils, "_GUIDS_CREATED_IN_SESSION", set())

    with pytest.raises(ToolError, match="local demo mode only allows changes"):
        asyncio.run(_update(server, interval_hours=24))

    lookup.assert_not_called()
    patch.assert_not_called()
    assert _connection_api_calls(http) == []


def test_update_remains_hidden_in_readonly_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The schedule update retains its existing read-only-mode filter protection."""
    server = FastMCP("readonly-schedule-tests")
    cloud_mcp.register_cloud_tools(server)
    monkeypatch.setenv(MCP_READONLY_MODE_ENV_VAR, "1")
    tool = asyncio.run(server.get_tool("update_connection"))

    assert not _tool_utils.airbyte_readonly_mode_filter(tool.to_mcp_tool(), server)


def test_update_interval_api_failure_is_reported(
    schedule_backend: tuple[FastMCP, responses.RequestsMock, MagicMock, MagicMock],
) -> None:
    """The tool reports a failed schedule request instead of returning a success message."""
    server, http, _lookup, _patch = schedule_backend
    http.replace(
        responses.POST,
        "https://config.example/custom/web_backend/connections/update",
        json={"message": "schedule rejected"},
        status=400,
    )

    with pytest.raises(ToolError, match="HTTP 400"):
        asyncio.run(_update(server, interval_hours=24))


def test_unsafe_update_always_requests_catalog_reset(
    schedule_backend: tuple[FastMCP, responses.RequestsMock, MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server, _http, _lookup, _patch = schedule_backend
    monkeypatch.setattr(
        api_util,
        "get_connection_catalog",
        MagicMock(
            return_value={
                "syncCatalog": {
                    "streams": [
                        {
                            "stream": {
                                "name": "users",
                                "namespace": "public",
                                "jsonSchema": {"type": "object"},
                                "supportedSyncModes": ["full_refresh", "incremental"],
                            },
                            "config": {
                                "syncMode": "full_refresh",
                                "destinationSyncMode": "append",
                            },
                        },
                        {
                            "stream": {
                                "name": "orders",
                                "namespace": "sales",
                                "jsonSchema": {"type": "object"},
                                "supportedSyncModes": ["full_refresh", "incremental"],
                            },
                            "config": {
                                "syncMode": "full_refresh",
                                "destinationSyncMode": "append",
                            },
                        },
                    ]
                }
            }
        ),
    )
    monkeypatch.setattr(
        api_util,
        "get_connection_state",
        MagicMock(
            return_value={
                "stateType": "stream",
                "connectionId": "12345678-1234-1234-1234-1234567890ab",
                "streamState": [
                    {
                        "streamDescriptor": {"name": "users", "namespace": "public"},
                        "streamState": {"cursor": "saved"},
                    }
                ],
            }
        ),
    )
    replace_catalog = MagicMock(return_value={})
    monkeypatch.setattr(api_util, "replace_connection_catalog", replace_catalog)

    result = asyncio.run(
        _unsafe_update(
            server,
            override_stream_configs=[
                {
                    "stream_name": "users",
                    "stream_namespace": "public",
                    "sync_mode": "incremental",
                    "cursor_field": "updated_at",
                },
                {
                    "stream_name": "orders",
                    "stream_namespace": "sales",
                    "destination_sync_mode": "overwrite",
                },
            ],
        )
    )

    assert replace_catalog.call_args.kwargs["skip_reset"] is False
    assert "users" in result
    assert "the platform will refresh or reset changed streams" in result
    assert "unsafe streams ['public.users']" in result
    assert "changed streams ['public.users', 'sales.orders']" in result
    assert "otherwise a reset clears that stream's destination data" in result


@pytest.mark.parametrize(
    ("enabled", "expected_guidance"),
    [
        (False, "Airbyte UI"),
        (True, "update_cloud_connection_unsafe"),
    ],
)
def test_unsafe_change_guidance_uses_unsafe_tool_enabled(
    schedule_backend: tuple[FastMCP, responses.RequestsMock, MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
    enabled: bool,
    expected_guidance: str,
) -> None:
    set_enabled = MagicMock()
    set_schedule = MagicMock()
    replace_catalog = MagicMock()
    monkeypatch.setattr(
        CloudConnection,
        "update_stream_configs",
        MagicMock(
            side_effect=AirbyteUnsafeStreamConfigChangeError(
                message="unsafe",
                context={"unsafe_streams": [{"name": "users"}]},
            )
        ),
    )
    monkeypatch.setattr(CloudConnection, "set_enabled", set_enabled)
    monkeypatch.setattr(CloudConnection, "set_schedule", set_schedule)
    monkeypatch.setattr(api_util, "replace_connection_catalog", replace_catalog)
    monkeypatch.setattr(
        cloud_mcp,
        "_is_unsafe_connection_update_tool_enabled",
        lambda _ctx: enabled,
    )

    with pytest.raises(AirbyteUnsafeStreamConfigChangeError) as exc_info:
        cloud_mcp.update_cloud_connection(
            ctx=None,
            connection_id="12345678-1234-1234-1234-1234567890ab",
            enabled=True,
            cron_expression=None,
            interval_hours=24,
            manual_schedule=None,
            override_stream_configs=[
                StreamConfigOverride(
                    stream_name="users",
                    sync_mode=SyncMode.incremental,
                    cursor_field="updated_at",
                )
            ],
            workspace_id=None,
        )

    assert expected_guidance in (exc_info.value.guidance or "")
    if not enabled:
        assert "update_cloud_connection_unsafe" not in (exc_info.value.guidance or "")
        assert "Connection URL:" in (exc_info.value.guidance or "")
    set_enabled.assert_not_called()
    set_schedule.assert_not_called()
    replace_catalog.assert_not_called()
