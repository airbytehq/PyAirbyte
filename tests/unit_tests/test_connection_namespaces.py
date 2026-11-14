# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Destination namespace settings across MCP, Cloud, and public API boundaries."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Literal
from unittest.mock import MagicMock

import pytest
from airbyte_api import models, utils

from airbyte._util import api_util
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteLibInputError
from airbyte.mcp import cloud as cloud_mcp


@pytest.fixture
def backend(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Use real workspace and connection objects with a mocked public API SDK."""
    response = models.ConnectionResponse(
        connection_id="12345678-1234-1234-1234-1234567890ab",
        created_at=0,
        destination_id="destination-id",
        name="name",
        source_id="source-id",
        status=models.ConnectionStatusEnum.ACTIVE,
        workspace_id="workspace-id",
        configurations=models.StreamConfigurations(streams=[]),
        schedule=models.ConnectionScheduleResponse(
            schedule_type=models.ScheduleTypeWithBasicEnum.MANUAL,
        ),
        tags=[],
    )
    connections = MagicMock()
    connections.create_connection.return_value = SimpleNamespace(
        status_code=200, connection_response=response
    )
    connections.patch_connection.return_value = SimpleNamespace(
        status_code=200, connection_response=response
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        MagicMock(return_value=SimpleNamespace(connections=connections)),
    )
    monkeypatch.setattr(
        api_util,
        "build_stream_configurations",
        MagicMock(return_value=models.StreamConfigurationsInput(streams=[])),
    )
    monkeypatch.setattr(api_util, "get_connection", MagicMock(return_value=response))
    workspace = CloudWorkspace(workspace_id="workspace-id", bearer_token="token")
    monkeypatch.setattr(workspace, "get_source", MagicMock())
    monkeypatch.setattr(workspace, "get_destination", MagicMock())
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace_for_resource",
        lambda *_args, **_kwargs: workspace,
    )
    monkeypatch.setattr(cloud_mcp, "check_guid_created_in_session", lambda *_args: None)
    monkeypatch.setattr(
        cloud_mcp, "register_guid_created_in_session", lambda *_args: None
    )
    return connections


@pytest.mark.parametrize("operation", ["create", "update", "unsafe"])
@pytest.mark.parametrize(
    ("definition", "template"),
    [
        (None, None),
        ("source", None),
        ("destination", None),
        ("custom_format", "${SOURCE_NAMESPACE}"),
        ("custom_format", "fixed_schema"),
    ],
)
def test_namespace_payload(
    backend: MagicMock,
    operation: str,
    definition: Literal["source", "destination", "custom_format"] | None,
    template: str | None,
) -> None:
    """Serialize only requested namespace settings, independently of table prefixes."""
    if operation == "create":
        cloud_mcp.create_connection(
            ctx=None,
            connection_name="name",
            source_id="source-id",
            destination_id="destination-id",
            workspace_id=None,
            selected_streams=["users"],
            table_prefix="prefix_",
            namespace_definition=definition,
            namespace_format=template,
        )
        request = backend.create_connection.call_args.args[0]
        payload = json.loads(
            utils.marshal_json(request, models.ConnectionCreateRequest)
        )
        assert payload["prefix"] == "prefix_"
    else:
        tool = (
            cloud_mcp.update_connection
            if operation == "update"
            else cloud_mcp.update_connection_unsafe
        )
        tool(
            ctx=None,
            connection_id="12345678-1234-1234-1234-1234567890ab",
            enabled=False if definition is None else None,
            cron_expression=None,
            interval_hours=None,
            manual_schedule=None,
            override_stream_configs=None,
            apply_discover_request_id=None,
            workspace_id=None,
            namespace_definition=definition,
            namespace_format=template,
        )
        request = backend.patch_connection.call_args.args[0].connection_patch_request
        payload = json.loads(utils.marshal_json(request, models.ConnectionPatchRequest))
        assert "prefix" not in payload
    if definition is None:
        assert "namespaceDefinition" not in payload
    else:
        assert payload["namespaceDefinition"] == definition
    if template is None:
        assert "namespaceFormat" not in payload
    else:
        assert payload["namespaceFormat"] == template


@pytest.mark.parametrize("operation", ["create", "update", "unsafe"])
@pytest.mark.parametrize(
    ("definition", "template"),
    [
        (None, "schema"),
        ("source", "schema"),
        ("destination", "schema"),
        ("custom_format", ""),
        ("custom_format", "  "),
    ],
)
def test_invalid_namespace_settings_do_not_write(
    backend: MagicMock,
    operation: str,
    definition: Literal["source", "destination", "custom_format"] | None,
    template: str,
) -> None:
    """Reject incompatible and empty templates before any connection writes."""
    with pytest.raises(AirbyteLibInputError, match="namespace_format"):
        if operation == "create":
            cloud_mcp.create_connection(
                ctx=None,
                connection_name="name",
                source_id="source-id",
                destination_id="destination-id",
                workspace_id=None,
                selected_streams=["users"],
                table_prefix=None,
                namespace_definition=definition,
                namespace_format=template,
            )
        else:
            tool = (
                cloud_mcp.update_connection
                if operation == "update"
                else cloud_mcp.update_connection_unsafe
            )
            tool(
                ctx=None,
                connection_id="12345678-1234-1234-1234-1234567890ab",
                enabled=True,
                cron_expression=None,
                interval_hours=None,
                manual_schedule=None,
                override_stream_configs=None,
                apply_discover_request_id=None,
                workspace_id=None,
                namespace_definition=definition,
                namespace_format=template,
            )
    backend.create_connection.assert_not_called()
    backend.patch_connection.assert_not_called()
