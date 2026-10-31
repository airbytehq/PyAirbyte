# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for asynchronous Cloud source catalog discovery."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from airbyte._util import api_util
from airbyte.cloud import connectors
from airbyte.cloud.connectors import CloudSource
from airbyte.cloud.models import (
    CloudSourceCatalogDiffStreamSummary,
    CloudSourceCatalogDiscoverResult,
)
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteCloudError, AirbyteLibInputError
from airbyte.mcp import cloud as cloud_mcp

_SOURCE_ID = "source-id"
_REQUEST_ID = "request-id"
_API_ROOT = "https://api.example/v1"
_CONFIG_API_ROOT = "https://config.example/v1"


def _workspace() -> CloudWorkspace:
    return CloudWorkspace(
        workspace_id="workspace-id",
        bearer_token="token",
        api_root=_API_ROOT,
        config_api_root=_CONFIG_API_ROOT,
    )


def _source(source_id: str = _SOURCE_ID) -> CloudSource:
    return CloudSource(workspace=_workspace(), connector_id=source_id)


def _server() -> FastMCP:
    server = FastMCP("source-catalog-discovery-tests")
    cloud_mcp.register_cloud_tools(server)
    return server


async def _call_tool(server: FastMCP, arguments: dict[str, Any]) -> Any:
    async with Client(server) as client:
        return await client.call_tool("discover_cloud_source_catalog", arguments)


def test_start_catalog_discover_uses_a_generated_request_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    discover_request_id = UUID("12345678-1234-1234-1234-1234567890ab")
    monkeypatch.setattr(connectors, "uuid4", lambda: discover_request_id)
    run_discover = MagicMock(return_value={"id": str(discover_request_id)})
    monkeypatch.setattr(api_util, "run_source_discover_command", run_discover)

    result = _source().start_catalog_discover()

    assert result == CloudSourceCatalogDiscoverResult(
        source_id=_SOURCE_ID,
        discover_request_id=str(discover_request_id),
        request_status="pending",
    )
    run_discover.assert_called_once_with(
        source_id=_SOURCE_ID,
        discover_request_id=str(discover_request_id),
        api_root=_API_ROOT,
        config_api_root=_CONFIG_API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token="token",
    )


@pytest.mark.parametrize("request_status", ["pending", "running", "cancelled"])
def test_get_catalog_discover_result_returns_noncompleted_status(
    monkeypatch: pytest.MonkeyPatch,
    request_status: str,
) -> None:
    status = MagicMock(return_value={"status": request_status})
    output = MagicMock()
    monkeypatch.setattr(api_util, "get_command_status", status)
    monkeypatch.setattr(api_util, "get_discover_command_output", output)

    result = _source().get_catalog_discover_result(_REQUEST_ID)

    assert result.source_id == _SOURCE_ID
    assert result.discover_request_id == _REQUEST_ID
    assert result.request_status == request_status
    assert result.outcome is None
    output.assert_not_called()


def test_get_catalog_discover_result_returns_success_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        api_util,
        "get_command_status",
        MagicMock(return_value={"status": "completed"}),
    )
    monkeypatch.setattr(
        api_util,
        "get_discover_command_output",
        MagicMock(
            return_value={
                "status": "succeeded",
                "catalogId": "catalog-id",
                "catalog": {"streams": [{}, {}, {}]},
            }
        ),
    )

    result = _source().get_catalog_discover_result(_REQUEST_ID)

    assert result.request_status == "completed"
    assert result.outcome == "succeeded"
    assert result.source_catalog_id == "catalog-id"
    assert result.stream_count == 3
    assert result.failure_reason is None
    assert "catalog" not in result.model_dump()


def test_get_catalog_discover_result_exposes_external_failure_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        api_util,
        "get_command_status",
        MagicMock(return_value={"status": "completed"}),
    )
    monkeypatch.setattr(
        api_util,
        "get_discover_command_output",
        MagicMock(
            return_value={
                "status": "failed",
                "failureReason": {
                    "externalMessage": "The source could not be read.",
                    "internalMessage": "internal diagnostic",
                },
            }
        ),
    )

    result = _source().get_catalog_discover_result(_REQUEST_ID)

    assert result.outcome == "failed"
    assert result.failure_reason == "The source could not be read."
    assert "internal" not in result.model_dump_json()


def test_get_catalog_discover_result_propagates_unknown_request_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        api_util,
        "get_command_status",
        MagicMock(side_effect=AirbyteCloudError(message="Unknown request ID.")),
    )

    with pytest.raises(AirbyteCloudError, match="Unknown request ID"):
        _source().get_catalog_discover_result(_REQUEST_ID)


def test_get_catalog_discover_result_rejects_empty_request_id() -> None:
    with pytest.raises(AirbyteLibInputError, match="non-empty string"):
        _source().get_catalog_discover_result(" ")


def test_get_latest_saved_catalog_returns_timestamp_and_stream_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_catalog = MagicMock(
        return_value={
            "updatedAt": 1_700_000_000,
            "catalog": {"streams": [{}, {}]},
        }
    )
    monkeypatch.setattr(api_util, "get_most_recent_source_catalog", get_catalog)

    result = _source().get_latest_saved_catalog()

    assert result.source_id == _SOURCE_ID
    assert result.updated_at == datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)
    assert result.stream_count == 2
    get_catalog.assert_called_once_with(
        source_id=_SOURCE_ID,
        api_root=_API_ROOT,
        config_api_root=_CONFIG_API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token="token",
    )


def test_get_latest_saved_catalog_returns_none_when_catalog_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        api_util,
        "get_most_recent_source_catalog",
        MagicMock(return_value={"updatedAt": 1_700_000_000, "catalog": {}}),
    )

    result = _source().get_latest_saved_catalog()

    assert result.updated_at is None
    assert result.stream_count is None


def test_compare_source_catalogs_summarizes_diff_without_exposing_catalogs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = {
        "catalog_diff": {
            "transforms": [
                {
                    "transformType": "update_stream",
                    "streamDescriptor": {"name": "users", "namespace": "public"},
                    "updateStream": {
                        "fieldTransforms": [
                            {
                                "transformType": "add_field",
                                "fieldName": ["email"],
                                "breaking": False,
                            },
                            {
                                "transformType": "remove_field",
                                "fieldName": ["name"],
                                "breaking": True,
                            },
                        ],
                        "streamAttributeTransforms": [
                            {"transformType": "update_sync_mode", "breaking": True}
                        ],
                    },
                }
            ]
        },
        "schema_change": "breaking",
        "merged_catalog": {"streams": [{"json_schema": {"secret": "not returned"}}]},
    }
    get_diff = MagicMock(return_value=response)
    monkeypatch.setattr(api_util, "get_catalog_diff", get_diff)

    result = _workspace().compare_source_catalogs(
        "new-catalog-id",
        compare_to_source_catalog_id="old-catalog-id",
    )

    assert result.source_catalog_id == "new-catalog-id"
    assert result.schema_change == "breaking"
    assert result.catalog_diff_summary == [
        CloudSourceCatalogDiffStreamSummary(
            stream_name="users",
            stream_namespace="public",
            transform_type="update_stream",
            breaking_flags=[False, True, True],
            field_additions_count=1,
            field_removals_count=1,
        )
    ]
    assert "merged_catalog" not in result.model_dump()
    assert "json_schema" not in result.model_dump_json()
    get_diff.assert_called_once_with(
        current_catalog_id="old-catalog-id",
        new_catalog_id="new-catalog-id",
        connection_id=None,
        api_root=_API_ROOT,
        config_api_root=_CONFIG_API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token="token",
    )


def test_compare_source_catalogs_uses_connection_catalog_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_connection = MagicMock(return_value={"catalogId": "connection-catalog-id"})
    get_diff = MagicMock(
        return_value={
            "catalog_diff": {"transforms": []},
            "schema_change": "non_breaking",
            "merged_catalog": {"streams": []},
        }
    )
    monkeypatch.setattr(api_util, "get_connection_catalog", get_connection)
    monkeypatch.setattr(api_util, "get_catalog_diff", get_diff)

    result = _workspace().compare_source_catalogs(
        "new-catalog-id",
        compare_to_connection_id="connection-id",
    )

    assert result.schema_change == "non_breaking"
    assert result.catalog_diff_summary == []
    get_connection.assert_called_once_with(
        connection_id="connection-id",
        api_root=_API_ROOT,
        config_api_root=_CONFIG_API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token="token",
    )
    get_diff.assert_called_once_with(
        current_catalog_id="connection-catalog-id",
        new_catalog_id="new-catalog-id",
        connection_id="connection-id",
        api_root=_API_ROOT,
        config_api_root=_CONFIG_API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token="token",
    )


def test_compare_source_catalogs_requires_connection_catalog_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        api_util,
        "get_connection_catalog",
        MagicMock(return_value={"catalogId": None}),
    )
    get_diff = MagicMock()
    monkeypatch.setattr(api_util, "get_catalog_diff", get_diff)

    with pytest.raises(AirbyteLibInputError, match="no saved source catalog"):
        _workspace().compare_source_catalogs(
            "new-catalog-id",
            compare_to_connection_id="connection-id",
        )

    get_diff.assert_not_called()


def test_source_catalog_summary_without_comparison_is_not_supported() -> None:
    with pytest.raises(AirbyteLibInputError, match="not supported yet"):
        _workspace().compare_source_catalogs("catalog-id")


def test_discover_tool_starts_new_discovery_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = MagicMock()
    source.start_catalog_discover.return_value = CloudSourceCatalogDiscoverResult(
        source_id=_SOURCE_ID,
        discover_request_id=_REQUEST_ID,
        request_status="pending",
    )
    workspace = MagicMock()
    workspace.get_source.return_value = source
    lookup = MagicMock(return_value=workspace)
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", lookup)

    result = asyncio.run(_call_tool(_server(), {"source_id": _SOURCE_ID}))

    assert result.structured_content["request_status"] == "pending"
    assert "catalog" not in result.structured_content
    workspace.get_source.assert_called_once_with(source_id=_SOURCE_ID)
    source.start_catalog_discover.assert_called_once_with()
    source.get_latest_saved_catalog.assert_not_called()


def test_discover_tool_returns_latest_saved_catalog_when_no_refresh_is_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = MagicMock()
    source.get_latest_saved_catalog.return_value = CloudSourceCatalogDiscoverResult(
        source_id=_SOURCE_ID,
        stream_count=2,
    )
    workspace = MagicMock()
    workspace.get_source.return_value = source
    monkeypatch.setattr(
        cloud_mcp, "_get_cloud_workspace", MagicMock(return_value=workspace)
    )

    result = asyncio.run(
        _call_tool(_server(), {"source_id": _SOURCE_ID, "no_refresh": True})
    )

    assert result.structured_content["stream_count"] == 2
    assert "catalog" not in result.structured_content
    source.get_latest_saved_catalog.assert_called_once_with()
    source.start_catalog_discover.assert_not_called()


def test_discover_tool_retrieves_result_by_request_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = MagicMock()
    workspace.get_catalog_discover_result.return_value = (
        CloudSourceCatalogDiscoverResult(
            discover_request_id=_REQUEST_ID,
            request_status="running",
        )
    )
    lookup = MagicMock(return_value=workspace)
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", lookup)

    result = asyncio.run(_call_tool(_server(), {"discover_request_id": _REQUEST_ID}))

    assert result.structured_content["request_status"] == "running"
    workspace.get_catalog_discover_result.assert_called_once_with(_REQUEST_ID)
    workspace.get_source.assert_not_called()


@pytest.mark.parametrize(
    ("arguments", "expected_call"),
    [
        pytest.param(
            {
                "source_catalog_id": "new-catalog-id",
                "compare_to_source_catalog_id": "old-catalog-id",
            },
            {
                "source_catalog_id": "new-catalog-id",
                "compare_to_source_catalog_id": "old-catalog-id",
                "compare_to_connection_id": None,
            },
            id="compare-by-source-catalog-id",
        ),
        pytest.param(
            {
                "source_catalog_id": "new-catalog-id",
                "compare_to_connection_id": "connection-id",
            },
            {
                "source_catalog_id": "new-catalog-id",
                "compare_to_source_catalog_id": None,
                "compare_to_connection_id": "connection-id",
            },
            id="compare-by-connection-id",
        ),
    ],
)
def test_discover_tool_compares_catalogs(
    monkeypatch: pytest.MonkeyPatch,
    arguments: dict[str, Any],
    expected_call: dict[str, Any],
) -> None:
    workspace = MagicMock()
    workspace.compare_source_catalogs.return_value = CloudSourceCatalogDiscoverResult(
        source_catalog_id="new-catalog-id",
        catalog_diff_summary=[],
        schema_change="no_change",
    )
    monkeypatch.setattr(
        cloud_mcp, "_get_cloud_workspace", MagicMock(return_value=workspace)
    )

    result = asyncio.run(_call_tool(_server(), arguments))

    assert result.structured_content["source_catalog_id"] == "new-catalog-id"
    assert result.structured_content["catalog_diff_summary"] == []
    assert result.structured_content["schema_change"] == "no_change"
    workspace.compare_source_catalogs.assert_called_once_with(**expected_call)


@pytest.mark.parametrize(
    ("arguments", "error_message"),
    [
        pytest.param(
            {"source_id": _SOURCE_ID, "discover_request_id": _REQUEST_ID},
            "not both",
            id="source-and-request",
        ),
        pytest.param(
            {"discover_request_id": _REQUEST_ID, "no_refresh": True},
            "cannot be used",
            id="no-refresh-and-request",
        ),
        pytest.param(
            {"source_catalog_id": "catalog-id"},
            "not supported yet",
            id="source-catalog-without-comparison",
        ),
        pytest.param(
            {"source_catalog_id": "catalog-id", "source_id": _SOURCE_ID},
            "cannot be combined",
            id="catalog-and-source",
        ),
        pytest.param(
            {"source_catalog_id": "catalog-id", "discover_request_id": _REQUEST_ID},
            "cannot be combined",
            id="catalog-and-request",
        ),
        pytest.param(
            {"source_catalog_id": "catalog-id", "no_refresh": True},
            "cannot be combined",
            id="catalog-and-no-refresh",
        ),
        pytest.param(
            {"compare_to_source_catalog_id": "old-catalog-id"},
            "require `source_catalog_id`",
            id="compare-without-source-catalog",
        ),
        pytest.param(
            {
                "source_catalog_id": "catalog-id",
                "compare_to_source_catalog_id": "old-catalog-id",
                "compare_to_connection_id": "connection-id",
            },
            "not both",
            id="both-comparison-targets",
        ),
        pytest.param({}, "Provide `source_id` or `discover_request_id`", id="neither"),
    ],
)
def test_discover_tool_rejects_invalid_argument_combinations(
    monkeypatch: pytest.MonkeyPatch,
    arguments: dict[str, Any],
    error_message: str,
) -> None:
    workspace = MagicMock()
    workspace.compare_source_catalogs.side_effect = AirbyteLibInputError(
        message="not supported yet"
    )
    lookup = MagicMock(return_value=workspace)
    monkeypatch.setattr(cloud_mcp, "_get_cloud_workspace", lookup)

    with pytest.raises(ToolError, match=error_message):
        asyncio.run(_call_tool(_server(), arguments))

    if error_message == "not supported yet":
        lookup.assert_called_once()
        workspace.compare_source_catalogs.assert_called_once_with(
            source_catalog_id="catalog-id",
            compare_to_source_catalog_id=None,
            compare_to_connection_id=None,
        )
    else:
        lookup.assert_not_called()


def test_discover_tool_is_advertised_as_nondestructive() -> None:
    server = _server()
    tool = asyncio.run(server.get_tool("discover_cloud_source_catalog"))

    assert tool.annotations.destructive_hint is False
