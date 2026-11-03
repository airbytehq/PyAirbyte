# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for resolving Cloud workspaces from resource IDs."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

import pytest
from airbyte._util import api_util
from airbyte.cloud.client import CloudClient
from airbyte.cloud.connectors import ConnectorType
from airbyte.exceptions import AirbyteMissingResourceError
from airbyte.mcp import _scope
from airbyte.mcp import cloud as cloud_mcp
from fastmcp import Context


RESOURCE_WORKSPACE = "11111111-1111-1111-1111-111111111111"
CONFIGURED_WORKSPACE = "22222222-2222-2222-2222-222222222222"


def _context() -> Context:
    return cast(Context, object())


def _client(*, default_workspace_id: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        default_workspace_id=default_workspace_id,
        public_api_root="https://api.example.test/v1",
        client_id="client-id",
        client_secret="client-secret",
        bearer_token="bearer-token",
    )


def test_explicit_workspace_skips_resource_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _context()
    workspace = object()
    get_workspace = MagicMock(return_value=workspace)
    get_connection = MagicMock()
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", get_workspace)
    monkeypatch.setattr(api_util, "get_connection", get_connection)

    result = cloud_mcp._get_cloud_workspace_for_resource(
        ctx, "explicit-workspace", connection_id="connection-id"
    )

    assert result is workspace
    get_workspace.assert_called_once_with(ctx, "explicit-workspace")
    get_connection.assert_not_called()


def test_configured_workspace_skips_resource_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _context()
    workspace = object()
    get_workspace = MagicMock(return_value=workspace)
    get_connection = MagicMock()
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_client",
        lambda _ctx: _client(default_workspace_id=CONFIGURED_WORKSPACE),
    )
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", get_workspace)
    monkeypatch.setattr(api_util, "get_connection", get_connection)

    result = cloud_mcp._get_cloud_workspace_for_resource(
        ctx, None, connection_id="connection-id"
    )

    assert result is workspace
    get_workspace.assert_called_once_with(ctx)
    get_connection.assert_not_called()


def test_connection_lookup_resolves_workspace_and_records_resource_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    ctx = _context()
    get_default_workspace = MagicMock(
        side_effect=AssertionError("unexpected default lookup")
    )
    get_workspace = MagicMock(return_value=object())
    get_connection = MagicMock(
        return_value=SimpleNamespace(workspace_id=RESOURCE_WORKSPACE)
    )
    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _ctx: client)
    monkeypatch.setattr(
        CloudClient, "resolve_default_workspace_id", get_default_workspace
    )
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", get_workspace)
    monkeypatch.setattr(api_util, "get_connection", get_connection)

    scope = _scope.CallScope()
    token = _scope._CALL_SCOPE.set(scope)
    try:
        cloud_mcp._get_cloud_workspace_for_resource(
            ctx, None, connection_id="connection-id"
        )
    finally:
        _scope._CALL_SCOPE.reset(token)

    get_connection.assert_called_once_with(
        connection_id="connection-id",
        workspace_id=None,
        api_root=client.public_api_root,
        client_id=client.client_id,
        client_secret=client.client_secret,
        bearer_token=client.bearer_token,
    )
    get_workspace.assert_called_once_with(ctx, RESOURCE_WORKSPACE)
    get_default_workspace.assert_not_called()
    assert scope.workspace_id == RESOURCE_WORKSPACE
    assert scope.workspace_source == "resource"


@pytest.mark.parametrize(
    ("connector_type", "lookup_name", "expected_type"),
    [
        pytest.param(None, "get_connector", ConnectorType.SOURCE, id="untyped"),
        pytest.param(
            ConnectorType.SOURCE, "get_source", ConnectorType.SOURCE, id="source"
        ),
        pytest.param(
            ConnectorType.DESTINATION,
            "get_destination",
            ConnectorType.DESTINATION,
            id="destination",
        ),
    ],
)
def test_connector_lookup_routes_by_type(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: ConnectorType | None,
    lookup_name: str,
    expected_type: ConnectorType,
) -> None:
    connector = SimpleNamespace(workspace_id=RESOURCE_WORKSPACE)
    lookup = MagicMock(
        return_value=(expected_type, connector)
        if lookup_name == "get_connector"
        else connector
    )
    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _ctx: _client())
    for method in ("get_connector", "get_source", "get_destination"):
        monkeypatch.setattr(api_util, method, MagicMock())
    monkeypatch.setattr(api_util, lookup_name, lookup)

    result = cloud_mcp._resolve_resource_workspace_id(
        _context(), connector_id="connector-id", connector_type=connector_type
    )

    assert result == RESOURCE_WORKSPACE
    if lookup_name == "get_connector":
        lookup.assert_called_once_with(
            connector_id="connector-id",
            api_root="https://api.example.test/v1",
            client_id="client-id",
            client_secret="client-secret",
            bearer_token="bearer-token",
        )
    else:
        id_arg = (
            "source_id" if connector_type == ConnectorType.SOURCE else "destination_id"
        )
        lookup.assert_called_once_with(**{
            id_arg: "connector-id",
            "api_root": "https://api.example.test/v1",
            "client_id": "client-id",
            "client_secret": "client-secret",
            "bearer_token": "bearer-token",
        })


def test_resource_lookup_error_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    error = AirbyteMissingResourceError(
        resource_type="connection",
        resource_name_or_id="missing-connection",
    )
    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _ctx: _client())
    monkeypatch.setattr(api_util, "get_connection", MagicMock(side_effect=error))

    with pytest.raises(AirbyteMissingResourceError) as exc_info:
        cloud_mcp._get_cloud_workspace_for_resource(
            _context(), None, connection_id="missing-connection"
        )

    assert exc_info.value is error


def test_resource_workspace_resolution_supports_connector_and_connection_tools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    ctx = _context()
    resolve_default_workspace_id = MagicMock(return_value=None)
    workspace = SimpleNamespace(
        get_connector=MagicMock(return_value=object()),
        get_connection=MagicMock(
            return_value=SimpleNamespace(
                get_previous_sync_logs=MagicMock(return_value=[])
            )
        ),
    )
    get_workspace = MagicMock(return_value=workspace)
    get_connector = MagicMock(
        return_value=(
            ConnectorType.SOURCE,
            SimpleNamespace(workspace_id=RESOURCE_WORKSPACE),
        )
    )
    get_connection = MagicMock(
        return_value=SimpleNamespace(workspace_id=RESOURCE_WORKSPACE)
    )
    details = object()
    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _ctx: client)
    monkeypatch.setattr(
        CloudClient, "resolve_default_workspace_id", resolve_default_workspace_id
    )
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", get_workspace)
    monkeypatch.setattr(api_util, "get_connector", get_connector)
    monkeypatch.setattr(api_util, "get_connection", get_connection)
    monkeypatch.setattr(
        cloud_mcp, "_describe_cloud_connector", MagicMock(return_value=details)
    )

    connector_result = cloud_mcp.describe_connector(
        ctx,
        connector_id="connector-id",
        connector_type=None,
        workspace_id=None,
        with_config=False,
        with_replication_details=False,
        with_direct_access_guidance=False,
        with_data_replication_docs=False,
    )
    jobs_result = cloud_mcp.list_connection_sync_jobs(
        ctx,
        connection_id="connection-id",
        workspace_id=None,
        max_jobs=20,
        from_tail=None,
        job_type=None,
    )

    assert connector_result is details
    assert jobs_result.jobs == []
    assert get_workspace.call_args_list == [
        ((ctx, RESOURCE_WORKSPACE), {}),
        ((ctx, RESOURCE_WORKSPACE), {}),
    ]
    workspace.get_connector.assert_called_once_with(connector_id="connector-id")
    workspace.get_connection.assert_called_once_with(connection_id="connection-id")
    resolve_default_workspace_id.assert_not_called()
