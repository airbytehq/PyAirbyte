# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Destination namespace settings across MCP, Cloud, and public API boundaries."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Literal
from unittest.mock import MagicMock

import pytest
from airbyte_api import models, utils
from fastmcp import Client, FastMCP

from airbyte._util import api_util
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteLibInputError
from airbyte.mcp import cloud as cloud_mcp


def test_namespace_tool_schemas() -> None:
    """Shared argument aliases retain schemas, descriptions, and optional defaults."""
    server = FastMCP("namespace-schema-tests")
    cloud_mcp.register_cloud_tools(server)

    async def inspect_tools() -> None:
        async with Client(server) as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
        for name in (
            "create_connection",
            "update_connection",
            "update_connection_unsafe",
        ):
            tool = tools[name]
            properties = tool.input_schema["properties"]
            definition = properties["namespace_definition"]
            pattern = properties["custom_namespace_pattern"]
            choices = next(
                option["enum"] for option in definition["anyOf"] if "enum" in option
            )
            assert choices == ["source", "destination", "custom"]
            assert definition["default"] is None
            assert pattern["default"] is None
            assert "namespace_definition" not in tool.input_schema.get("required", [])
            assert "custom_namespace_pattern" not in tool.input_schema.get(
                "required", []
            )
            assert "namespace_format" not in properties
            assert "BigQuery" in definition["description"]
            assert "stream_namespace" in definition["description"]
            assert "${SOURCE_NAMESPACE}_raw" in pattern["description"]
            assert "if any" in pattern["description"]
            assert "including `${SOURCE_NAMESPACE}`" in (tool.description or "")
            assert "`custom_namespace_pattern`" in (tool.description or "")

    asyncio.run(inspect_tools())


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
        ("custom", "${SOURCE_NAMESPACE}"),
        ("custom", "fixed_schema"),
        ("custom", "${SOURCE_NAMESPACE}_raw"),
    ],
)
def test_namespace_payload(
    backend: MagicMock,
    operation: str,
    definition: Literal["source", "destination", "custom"] | None,
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
            custom_namespace_pattern=template,
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
            custom_namespace_pattern=template,
        )
        request = backend.patch_connection.call_args.args[0].connection_patch_request
        payload = json.loads(utils.marshal_json(request, models.ConnectionPatchRequest))
        assert "prefix" not in payload
    if definition is None:
        assert "namespaceDefinition" not in payload
    else:
        assert payload["namespaceDefinition"] == (
            "custom_format" if definition == "custom" else definition
        )
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
        ("custom", ""),
        ("custom", "  "),
    ],
)
def test_invalid_namespace_settings_do_not_write(
    backend: MagicMock,
    operation: str,
    definition: Literal["source", "destination", "custom"] | None,
    template: str,
) -> None:
    """Reject incompatible and empty templates before any connection writes."""
    with pytest.raises(AirbyteLibInputError, match="custom_namespace_pattern"):
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
                custom_namespace_pattern=template,
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
                custom_namespace_pattern=template,
            )
    backend.create_connection.assert_not_called()
    backend.patch_connection.assert_not_called()
