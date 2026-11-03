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
from airbyte.cloud.models import CloudSourceCatalogDiscoverResult
from airbyte.exceptions import AirbyteMissingResourceError
from airbyte.mcp import _scope
from airbyte.mcp import cloud as cloud_mcp
from fastmcp import Context


RESOURCE_WORKSPACE = "11111111-1111-1111-1111-111111111111"
CONFIGURED_WORKSPACE = "22222222-2222-2222-2222-222222222222"


def _context() -> Context:
    return cast(Context, object())


def _client(*, default_workspace_id: str | None = None) -> CloudClient:
    return CloudClient(
        bearer_token="bearer-token",
        public_api_root="https://api.example.test/v1",
        workspace_id=default_workspace_id,
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
    get_connection_workspace_id = MagicMock()
    client = _client(default_workspace_id=CONFIGURED_WORKSPACE)
    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _ctx: client)
    monkeypatch.setattr(
        client, "get_connection_workspace_id", get_connection_workspace_id
    )
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", get_workspace)
    monkeypatch.setattr(api_util, "get_connection", get_connection)

    result = cloud_mcp._get_cloud_workspace_for_resource(
        ctx, None, connection_id="connection-id"
    )

    assert result is workspace
    get_workspace.assert_called_once_with(ctx)
    get_connection_workspace_id.assert_not_called()
    get_connection.assert_not_called()


def test_connection_lookup_resolves_workspace_and_records_resource_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    get_resource_workspace = MagicMock(return_value=RESOURCE_WORKSPACE)
    ctx = _context()
    get_default_workspace = MagicMock(
        side_effect=AssertionError("unexpected default lookup")
    )
    get_workspace = MagicMock(return_value=object())
    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _ctx: client)
    monkeypatch.setattr(client, "get_connection_workspace_id", get_resource_workspace)
    monkeypatch.setattr(
        CloudClient, "resolve_default_workspace_id", get_default_workspace
    )
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", get_workspace)

    scope = _scope.CallScope()
    token = _scope._CALL_SCOPE.set(scope)
    try:
        cloud_mcp._get_cloud_workspace_for_resource(
            ctx, None, connection_id="connection-id"
        )
    finally:
        _scope._CALL_SCOPE.reset(token)

    get_resource_workspace.assert_called_once_with("connection-id")
    get_workspace.assert_called_once_with(ctx, RESOURCE_WORKSPACE)
    get_default_workspace.assert_not_called()
    assert scope.workspace_id == RESOURCE_WORKSPACE
    assert scope.workspace_source == "resource"


@pytest.mark.parametrize(
    "connector_type",
    [
        pytest.param(None, id="untyped"),
        pytest.param(ConnectorType.SOURCE, id="source"),
        pytest.param(ConnectorType.DESTINATION, id="destination"),
    ],
)
def test_connector_workspace_resolution_passes_type_to_client(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: ConnectorType | None,
) -> None:
    client = _client()
    lookup = MagicMock(return_value=RESOURCE_WORKSPACE)
    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _ctx: client)
    monkeypatch.setattr(client, "get_connector_workspace_id", lookup)

    result = cloud_mcp._resolve_resource_workspace_id(
        _context(), connector_id="connector-id", connector_type=connector_type
    )

    assert result == RESOURCE_WORKSPACE
    lookup.assert_called_once_with(
        "connector-id",
        connector_type.value if connector_type is not None else None,
    )


def test_resource_lookup_error_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    error = AirbyteMissingResourceError(
        resource_type="connection",
        resource_name_or_id="missing-connection",
    )
    client = _client()
    lookup = MagicMock(side_effect=error)
    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _ctx: client)
    monkeypatch.setattr(client, "get_connection_workspace_id", lookup)

    with pytest.raises(AirbyteMissingResourceError) as exc_info:
        cloud_mcp._get_cloud_workspace_for_resource(
            _context(), None, connection_id="missing-connection"
        )

    assert exc_info.value is error
    lookup.assert_called_once_with("missing-connection")


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
    get_connector_workspace_id = MagicMock(return_value=RESOURCE_WORKSPACE)
    get_connection_workspace_id = MagicMock(return_value=RESOURCE_WORKSPACE)
    details = object()
    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _ctx: client)
    monkeypatch.setattr(
        client, "get_connector_workspace_id", get_connector_workspace_id
    )
    monkeypatch.setattr(
        client, "get_connection_workspace_id", get_connection_workspace_id
    )
    monkeypatch.setattr(
        CloudClient, "resolve_default_workspace_id", resolve_default_workspace_id
    )
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", get_workspace)
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
    get_connector_workspace_id.assert_called_once_with("connector-id", None)
    get_connection_workspace_id.assert_called_once_with("connection-id")
    resolve_default_workspace_id.assert_not_called()


def test_discover_source_catalog_can_poll_without_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _client()
    ctx = _context()
    request_id = "discover-request-id"
    pending_result = CloudSourceCatalogDiscoverResult(
        source_id="source-id",
        discover_request_id=request_id,
        request_status="pending",
    )
    start_discover = MagicMock(return_value=pending_result)
    source = SimpleNamespace(start_catalog_discover=start_discover)
    workspace = SimpleNamespace(get_source=MagicMock(return_value=source))
    get_workspace = MagicMock(return_value=workspace)
    get_source = MagicMock(return_value=SimpleNamespace(workspace_id="ws-b"))
    get_command_status = MagicMock(return_value={"status": "running"})
    get_discover_command_output = MagicMock()
    resolve_default_workspace_id = MagicMock(return_value=None)
    monkeypatch.setattr(cloud_mcp, "_get_cloud_client", lambda _ctx: client)
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", get_workspace)
    monkeypatch.setattr(api_util, "get_source", get_source)
    monkeypatch.setattr(api_util, "get_command_status", get_command_status)
    monkeypatch.setattr(
        api_util,
        "get_discover_command_output",
        get_discover_command_output,
    )
    monkeypatch.setattr(
        CloudClient, "resolve_default_workspace_id", resolve_default_workspace_id
    )

    start_result = cloud_mcp.discover_source_catalog(ctx, source_id="source-id")
    poll_result = cloud_mcp.discover_source_catalog(
        ctx,
        discover_request_id=start_result.discover_request_id,
    )

    assert client.default_workspace_id is None
    assert start_result.discover_request_id == request_id
    assert poll_result.discover_request_id == request_id
    assert poll_result.request_status == "running"
    get_source.assert_called_once_with(
        source_id="source-id",
        api_root=client.public_api_root,
        client_id=client.client_id,
        client_secret=client.client_secret,
        bearer_token=client.bearer_token,
    )
    get_workspace.assert_called_once_with(ctx, "ws-b")
    workspace.get_source.assert_called_once_with(source_id="source-id")
    start_discover.assert_called_once_with()
    get_command_status.assert_called_once_with(
        request_id,
        api_root=client.public_api_root,
        config_api_root=client.config_api_root,
        client_id=client.client_id,
        client_secret=client.client_secret,
        bearer_token=client.bearer_token,
    )
    get_discover_command_output.assert_not_called()
    resolve_default_workspace_id.assert_not_called()
