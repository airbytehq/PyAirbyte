# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for compact SQL schemas and destination ID recovery."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from airbyte._direct_connectors.models import (
    ExternalApiExecuteResult,
    _SQL_PASSTHROUGH_DESTINATION_DIALECTS,
)
from airbyte.cloud.models import ConnectorType
from airbyte.cloud.sql import get_sql_schema, list_sql_destinations
from airbyte.exceptions import (
    AirbyteCloudApiError,
    AirbyteCloudError,
    AirbyteLibInputError,
    AirbyteMissingResourceError,
)
from airbyte.mcp import cloud
from airbyte.mcp._error_handling import format_user_facing_error


def _connector(dialect: str = "bigquery") -> MagicMock:
    connector = MagicMock()
    connector.connector_id = "destination-1"
    connector.as_cloud_destination.return_value = connector
    connector.connector_type = ConnectorType.DESTINATION
    connector.definition_id = next(
        key
        for key, value in _SQL_PASSTHROUGH_DESTINATION_DIALECTS.items()
        if value == dialect
    )
    connector.configuration = {
        "project_id": "project",
        "dataset_id": "dataset",
        "database": "project",
        "schema": "dataset",
    }
    connector.workspace.list_connections.return_value = []
    return connector


@pytest.mark.parametrize("dialect", ["bigquery", "snowflake"])
def test_schema_columns_and_meanings(dialect: str) -> None:
    connector = _connector(dialect)
    connection = SimpleNamespace(
        destination_id="destination-1",
        table_prefix="",
        namespace_definition="destination",
        namespace_format=None,
        dump_raw_catalog=lambda: {
            "streams": [
                {
                    "stream": {
                        "name": "users",
                        "json_schema": {
                            "properties": {
                                "created_at": {"description": "Creation\ntime"}
                            }
                        },
                    }
                }
            ]
        },
    )
    connector.workspace.list_connections.return_value = [connection]
    row = (
        {"table_catalog": "project", "table_schema": "dataset", "table_name": "users"}
        if dialect == "bigquery"
        else {"database_name": "project", "schema_name": "dataset", "name": "users"}
    )
    connector.execute_sql_query.side_effect = [
        ExternalApiExecuteResult(status="success", result=[row]),
        ExternalApiExecuteResult(
            status="success",
            connector_metadata={
                "columns": [
                    {"name": "created_at", "type": "TIMESTAMP"},
                    {"name": "payload", "type": "JSON"},
                ]
            },
        ),
    ]
    result = get_sql_schema(connector)
    table = (
        "`project.dataset.users`"
        if dialect == "bigquery"
        else '"project"."dataset"."users"'
    )
    assert result["tables"] == [
        {
            "table": table,
            "columns": ["created_at TIMESTAMP - Creation time", "payload JSON - "],
        }
    ]
    assert result["truncated"] is False
    connector.execute_sql_query.assert_called_with(f"DESCRIBE TABLE {table}")


def test_empty_schema() -> None:
    connector = _connector()
    connector.execute_sql_query.return_value = ExternalApiExecuteResult(
        status="success", result=[]
    )
    assert get_sql_schema(connector)["tables"] == []


@pytest.mark.parametrize("has_next_page", [False, True])
def test_schema_reports_discovery_pagination(has_next_page: bool) -> None:
    connector = _connector()
    connector.execute_sql_query.return_value = ExternalApiExecuteResult(
        status="success", result=[], connector_metadata={"has_next_page": has_next_page}
    )
    assert get_sql_schema(connector)["truncated"] is has_next_page


def test_schema_filter_and_cap() -> None:
    connector = _connector()
    connector.execute_sql_query.side_effect = [
        ExternalApiExecuteResult(
            status="success",
            result=[
                {"table_catalog": "p", "table_schema": "d", "table_name": name}
                for name in ("other", "users", "users_old")
            ],
        ),
        ExternalApiExecuteResult(status="success", connector_metadata={"columns": []}),
    ]
    result = get_sql_schema(connector, table_name_contains="USERS", max_tables=1)
    assert [table["table"] for table in result["tables"]] == ["`p.d.users`"]
    assert result["truncated"] is True


@pytest.mark.parametrize("cap", [0, 101])
def test_invalid_cap(cap: int) -> None:
    with pytest.raises(AirbyteLibInputError, match="max_tables"):
        get_sql_schema(_connector(), max_tables=cap)


def test_metadata_failure_propagates() -> None:
    connector = _connector()
    connector.execute_sql_query.side_effect = AirbyteCloudError(
        message="Warehouse unavailable"
    )
    with pytest.raises(AirbyteCloudError, match="Warehouse unavailable"):
        get_sql_schema(connector)


@pytest.mark.parametrize(
    "tool", [cloud.execute_external_sql_query, cloud.get_external_sql_schema]
)
@pytest.mark.parametrize(
    "invalid", ["missing", "source", "unsupported", "lazy_missing"]
)
def test_invalid_destination_ids(
    monkeypatch: pytest.MonkeyPatch, tool: object, invalid: str
) -> None:
    connector = _connector()
    workspace = MagicMock()
    workspace.list_destinations.return_value = [connector]
    connector.name = "Warehouse"
    connector.workspace = workspace
    workspace.get_connector.return_value = connector
    monkeypatch.setattr(cloud, "_get_cloud_workspace", lambda *_: workspace)
    monkeypatch.setattr(
        cloud, "_get_cloud_workspace_for_resource", lambda *_, **__: workspace
    )
    if invalid == "missing":
        workspace.get_connector.side_effect = AirbyteMissingResourceError(
            message="Not found"
        )
    elif invalid == "source":
        workspace.get_connector.return_value = SimpleNamespace(
            connector_id="bad",
            connector_type=ConnectorType.SOURCE,
            definition_id=connector.definition_id,
        )
    elif invalid == "unsupported":
        workspace.get_connector.return_value = SimpleNamespace(
            connector_id="bad",
            connector_type=ConnectorType.DESTINATION,
            definition_id="unsupported",
        )
    else:
        connector.execute_sql_query.side_effect = AirbyteMissingResourceError(
            message="Not found"
        )
    args = {"sql": "SELECT 1"} if tool is cloud.execute_external_sql_query else {}
    with pytest.raises(
        AirbyteLibInputError, match="destination-1, Warehouse, bigquery"
    ):
        tool(None, connector_id="bad", **args)


def test_destination_list_filter() -> None:
    workspace = MagicMock()
    workspace.list_destinations.return_value = [
        _connector(),
        SimpleNamespace(definition_id="other"),
    ]
    assert len(list_sql_destinations(workspace)) == 1


def test_listing_failure_preserves_original(monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = MagicMock()
    workspace.get_connector.side_effect = AirbyteMissingResourceError(
        message="Original not found"
    )
    workspace.list_destinations.side_effect = AirbyteCloudError(
        message="Listing unavailable"
    )
    monkeypatch.setattr(cloud, "_get_cloud_workspace", lambda *_: workspace)
    monkeypatch.setattr(
        cloud, "_get_cloud_workspace_for_resource", lambda *_, **__: workspace
    )
    with pytest.raises(AirbyteLibInputError, match="Could not list valid") as error:
        cloud.get_external_sql_schema(None, connector_id="bad")
    assert "Original not found" in str(error.value.__cause__)


def test_destination_guidance_survives_cloud_error_formatting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _connector()
    connector.name = "Warehouse"
    workspace = MagicMock()
    workspace.list_destinations.return_value = [connector]
    missing = AirbyteMissingResourceError(message="Not found")
    missing.__cause__ = AirbyteCloudApiError(
        status_code=404,
        message="Unknown connector",
        context={"response_text": '{"detail":"Unknown connector"}'},
    )
    workspace.get_connector.side_effect = missing
    monkeypatch.setattr(cloud, "_get_cloud_workspace", lambda *_: workspace)
    monkeypatch.setattr(
        cloud, "_get_cloud_workspace_for_resource", lambda *_, **__: workspace
    )
    with pytest.raises(AirbyteLibInputError) as caught:
        cloud.get_external_sql_schema(None, connector_id="bad")
    text = format_user_facing_error(caught.value)
    assert "Unknown connector" in text
    assert "destination-1, Warehouse, bigquery" in text


@pytest.mark.parametrize("failure_stage", ["lookup", "validation", "execution"])
def test_destination_ids_use_resolved_workspace(
    monkeypatch: pytest.MonkeyPatch, failure_stage: str
) -> None:
    default = MagicMock()
    default.list_destinations.return_value = []
    resolved = MagicMock()
    connector = _connector()
    connector.name = "Resolved warehouse"
    connector.workspace = resolved
    resolved.list_destinations.return_value = [connector]
    resolved.get_connector.return_value = connector
    if failure_stage == "lookup":
        resolved.get_connector.side_effect = AirbyteMissingResourceError(
            message="Not found"
        )
    elif failure_stage == "validation":
        resolved.get_connector.return_value = SimpleNamespace(
            connector_id="bad",
            definition_id="unsupported",
            connector_type=ConnectorType.DESTINATION,
        )
    else:
        connector.execute_sql_query.side_effect = AirbyteMissingResourceError(
            message="Not found"
        )
    monkeypatch.setattr(cloud, "_get_cloud_workspace", lambda *_: default)
    monkeypatch.setattr(
        cloud, "_get_cloud_workspace_for_resource", lambda *_, **__: resolved
    )
    with pytest.raises(AirbyteLibInputError) as caught:
        cloud.get_external_sql_schema(None, connector_id="bad")
    assert "Resolved warehouse" in format_user_facing_error(caught.value)
    default.list_destinations.assert_not_called()
