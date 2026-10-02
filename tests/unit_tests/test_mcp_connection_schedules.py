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
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.constants import MCP_CONFIG_READONLY_MODE
from airbyte.exceptions import PyAirbyteInputError
from airbyte.mcp import _tool_utils
from airbyte.mcp import cloud as cloud_mcp


def _connection_response() -> models.ConnectionResponse:
    """Create a complete SDK response for the network boundary."""
    return models.ConnectionResponse(
        connection_id="connection-id",
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
    monkeypatch.setattr(_tool_utils, "AIRBYTE_CLOUD_MCP_SAFE_MODE", False)
    with responses.RequestsMock(assert_all_requests_are_fired=False) as http:
        http.post(
            "https://config.example/custom/web_backend/connections/update",
            json={"connectionId": "connection-id", "scheduleType": "basic"},
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
            "update_cloud_connection", {"connection_id": "connection-id", **arguments}
        )
    return str(result)


def test_interval_schema() -> None:
    """Advertise an optional positive whole-hour interval for the update tool."""
    server = FastMCP("schedule-schema-tests")
    cloud_mcp.register_cloud_tools(server)

    async def inspect_tools() -> None:
        async with Client(server) as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
        update = tools["update_cloud_connection"]
        interval = update.input_schema["properties"]["interval_hours"]
        integer_schema = next(
            option for option in interval["anyOf"] if option["type"] == "integer"
        )
        assert integer_schema["exclusiveMinimum"] == 0
        assert interval["default"] is None
        assert "interval_hours" not in update.input_schema.get("required", [])
        assert update.annotations is not None
        assert update.annotations.destructive_hint is True
        assert update.annotations.read_only_hint is False

    asyncio.run(inspect_tools())


def test_create_manual_default_description() -> None:
    """Explain the manual default and how to enable automatic syncs after creation."""
    server = FastMCP("create-schema-tests")
    cloud_mcp.register_cloud_tools(server)

    async def inspect_tools() -> None:
        async with Client(server) as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
        create = tools["create_connection_on_cloud"]
        description = (create.description or "").lower()
        assert "manual" in description
        assert "default" in description
        assert "automatic syncs" in description
        assert "update_cloud_connection" in description

    asyncio.run(inspect_tools())


@pytest.mark.parametrize("interval_hours", [0, -1, True, False, 1.0, 1.5, "24"])
def test_direct_update_rejects_invalid_intervals_before_status_change(
    schedule_backend: tuple[FastMCP, responses.RequestsMock, MagicMock, MagicMock],
    interval_hours: object,
) -> None:
    """Python calls bypassing schema validation still reject invalid intervals first."""
    _server, http, lookup, patch = schedule_backend

    with pytest.raises(PyAirbyteInputError, match="positive whole number"):
        cloud_mcp.update_cloud_connection(
            ctx=None,
            connection_id="connection-id",
            enabled=True,
            interval_hours=interval_hours,
            cron_expression=None,
            manual_schedule=None,
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
        "connectionId": "connection-id",
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


def test_update_interval_obeys_safe_mode(
    schedule_backend: tuple[FastMCP, responses.RequestsMock, MagicMock, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Safe mode blocks interval updates to connections outside the current session."""
    server, http, lookup, patch = schedule_backend
    monkeypatch.setattr(_tool_utils, "AIRBYTE_CLOUD_MCP_SAFE_MODE", True)
    monkeypatch.setattr(_tool_utils, "_GUIDS_CREATED_IN_SESSION", set())

    with pytest.raises(ToolError, match="not created in this session"):
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
    monkeypatch.setattr(
        _tool_utils,
        "get_mcp_config",
        lambda _app, key: "1" if key == MCP_CONFIG_READONLY_MODE else None,
    )
    tool = asyncio.run(server.get_tool("update_cloud_connection"))

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

    with pytest.raises(ToolError, match="status 400"):
        asyncio.run(_update(server, interval_hours=24))
