# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for compact SQL schemas and destination ID recovery."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from airbyte_api.errors import SDKError
from fastmcp.exceptions import ToolError

from airbyte._util import api_util
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
from airbyte.mcp._error_handling import agent_error_text, format_user_facing_error


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


def _sql_result(
    rows: list[dict[str, str]],
    columns: list[dict[str, str]] | None = None,
    *,
    dialect: str = "bigquery",
    truncated: bool = False,
    next_cursor: str | None = None,
) -> ExternalApiExecuteResult:
    return ExternalApiExecuteResult(
        status="success",
        result={
            "data": rows,
            "meta": {
                "engine": dialect,
                "sql_dialect": dialect,
                "row_count": len(rows),
                "columns": (
                    columns
                    if columns is not None
                    else [
                        {"name": name, "type": "STRING"}
                        for name in (rows[0] if rows else {})
                    ]
                ),
                "truncated": truncated,
                "next_cursor": next_cursor,
                "took_ms": 1,
            },
        },
    )


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
                    "selected": True,
                    "stream": {
                        "name": "users",
                        "json_schema": {
                            "properties": {
                                "created_at": {"description": "Creation\ntime"}
                            }
                        },
                    },
                },
                {
                    "selected": False,
                    "stream": {
                        "name": "users",
                        "json_schema": {
                            "properties": {"created_at": {"description": "Not synced"}}
                        },
                    },
                },
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
        _sql_result([row], dialect=dialect),
        _sql_result(
            [],
            [
                {"name": "created_at", "type": "TIMESTAMP"},
                {"name": "payload", "type": "JSON"},
            ],
            dialect=dialect,
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
    connector.execute_sql_query.return_value = _sql_result([])
    assert get_sql_schema(connector)["tables"] == []


@pytest.mark.parametrize("dialect", ["bigquery", "snowflake"])
def test_schema_scopes_discovery_before_filter_and_cap(dialect: str) -> None:
    connector = _connector(dialect)
    scoped_sql = (
        "SHOW TABLES IN SCHEMA `project`.`dataset`"
        if dialect == "bigquery"
        else 'SHOW TABLES IN SCHEMA "PROJECT"."DATASET"'
    )
    other_tables = [
        {
            "table_catalog": "project",
            "table_schema": "other",
            "table_name": f"other_{i}",
            "table_type": "BASE TABLE",
        }
        for i in range(1000)
    ]
    own_tables = [
        {
            "table_catalog": "project",
            "table_schema": "dataset",
            "table_name": name,
            "table_type": "BASE TABLE",
        }
        for name in ("other", "linear_issues", "linear_teams")
    ]

    def execute(sql: str, **kwargs: object) -> ExternalApiExecuteResult:
        if sql == "SHOW TABLES":
            return _sql_result(other_tables, truncated=True, next_cursor="project-page")
        if sql == scoped_sql:
            return _sql_result(own_tables, dialect=dialect)
        assert sql.startswith("DESCRIBE TABLE ")
        return _sql_result([], [{"name": "id", "type": "STRING"}], dialect=dialect)

    connector.execute_sql_query.side_effect = execute
    result = get_sql_schema(connector, table_name_contains="linear", max_tables=2)
    assert [table["table"] for table in result["tables"]] == (
        ["`project.dataset.linear_issues`", "`project.dataset.linear_teams`"]
        if dialect == "bigquery"
        else [
            '"project"."dataset"."linear_issues"',
            '"project"."dataset"."linear_teams"',
        ]
    )
    assert result["truncated"] is False
    assert result["warnings"] == []
    connector.execute_sql_query.assert_any_call(scoped_sql, page_size=1000)


@pytest.mark.parametrize("dialect", ["bigquery", "snowflake"])
@pytest.mark.parametrize(
    "definition,namespace_format,expected",
    [
        ("source", None, "source_a"),
        ("custom_format", "raw_${SOURCE_NAMESPACE}", "raw_source_a"),
        ("destination", None, "dataset"),
    ],
)
def test_schema_uses_selected_connection_namespaces(
    dialect: str, definition: str, namespace_format: str | None, expected: str
) -> None:
    connector = _connector(dialect)
    connection = SimpleNamespace(
        destination_id="destination-1",
        table_prefix="linear_",
        namespace_definition=definition,
        namespace_format=namespace_format,
        dump_raw_catalog=lambda: {
            "streams": [
                {
                    "selected": True,
                    "stream": {"name": "issues", "namespace": "source_a"},
                },
                {
                    "selected": True,
                    "stream": {"name": "teams", "namespace": "source_a"},
                },
                {
                    "selected": False,
                    "stream": {"name": "ignored", "namespace": "other"},
                },
            ]
        },
    )
    connector.workspace.list_connections.return_value = [connection]
    connector.execute_sql_query.return_value = _sql_result([], dialect=dialect)
    assert get_sql_schema(connector)["tables"] == []
    quote = "`" if dialect == "bigquery" else '"'
    database = "project" if dialect == "bigquery" else "PROJECT"
    expected = expected if dialect == "bigquery" else expected.upper()
    connector.execute_sql_query.assert_called_once_with(
        f"SHOW TABLES IN SCHEMA {quote}{database}{quote}.{quote}{expected}{quote}",
        page_size=1000,
    )


def test_schema_reads_scoped_pages_before_filtering_and_capping() -> None:
    connector = _connector()
    connector.execute_sql_query.side_effect = [
        _sql_result(
            [
                {
                    "table_catalog": "project",
                    "table_schema": "dataset",
                    "table_name": "other",
                    "table_type": "BASE TABLE",
                }
            ],
            truncated=True,
            next_cursor="next-page",
        ),
        _sql_result([
            {
                "table_catalog": "project",
                "table_schema": "dataset",
                "table_name": "linear_issues",
                "table_type": "BASE TABLE",
            }
        ]),
        _sql_result([], [{"name": "id", "type": "STRING"}]),
    ]
    result = get_sql_schema(connector, table_name_contains="linear", max_tables=1)
    assert result["tables"] == [
        {"table": "`project.dataset.linear_issues`", "columns": ["id STRING - "]}
    ]
    assert result["truncated"] is False
    connector.execute_sql_query.assert_any_call(
        None, page_size=1000, cursor="next-page"
    )


@pytest.mark.parametrize(
    "namespace_definition", ["source", "custom_format", "destination"]
)
def test_snowflake_discovery_normalizes_namespace_casing(
    namespace_definition: str,
) -> None:
    connector = _connector("snowflake")
    connector.configuration["database"] = "analytics"
    connector.configuration["schema"] = "public"
    connector.workspace.list_connections.return_value = (
        []
        if namespace_definition == "destination"
        else [
            SimpleNamespace(
                destination_id="destination-1",
                table_prefix="",
                namespace_definition=namespace_definition,
                namespace_format="${SOURCE_NAMESPACE}",
                dump_raw_catalog=lambda: {
                    "streams": [
                        {
                            "selected": True,
                            "stream": {"name": "linear_issues", "namespace": "public"},
                        }
                    ]
                },
            )
        ]
    )
    connector.execute_sql_query.side_effect = [
        _sql_result(
            [
                {
                    "database_name": "ANALYTICS",
                    "schema_name": "PUBLIC",
                    "name": "LINEAR_ISSUES",
                }
            ],
            dialect="snowflake",
        ),
        _sql_result([], [{"name": "ID", "type": "NUMBER"}], dialect="snowflake"),
    ]
    result = get_sql_schema(connector, table_name_contains="linear", max_tables=1)
    connector.execute_sql_query.assert_any_call(
        'SHOW TABLES IN SCHEMA "ANALYTICS"."PUBLIC"', page_size=1000
    )
    assert result["tables"] == [
        {"table": '"ANALYTICS"."PUBLIC"."LINEAR_ISSUES"', "columns": ["ID NUMBER - "]}
    ]
    assert result["truncated"] is False


def test_schema_without_known_namespace_does_not_scan_project() -> None:
    connector = _connector()
    connector.configuration = {"project_id": "project"}
    with pytest.raises(AirbyteCloudError, match="dataset or schema"):
        get_sql_schema(connector)
    connector.execute_sql_query.assert_not_called()


def test_schema_connection_failure_falls_back_to_configured_dataset() -> None:
    connector = _connector()
    connector.workspace.list_connections.side_effect = AirbyteCloudError(
        message="Unavailable"
    )
    connector.execute_sql_query.return_value = _sql_result([])
    result = get_sql_schema(connector)
    assert result["tables"] == []
    assert result["warnings"] == [
        "Connection namespaces and source field descriptions are unavailable."
    ]
    connector.execute_sql_query.assert_called_once_with(
        "SHOW TABLES IN SCHEMA `project`.`dataset`", page_size=1000
    )


def test_schema_combines_connection_schemas_before_applying_cap() -> None:
    connector = _connector()
    connector.workspace.list_connections.return_value = [
        SimpleNamespace(
            destination_id="destination-1",
            table_prefix="linear_",
            namespace_definition="source",
            namespace_format=None,
            dump_raw_catalog=lambda: {
                "streams": [
                    {"selected": True, "stream": {"name": "issues", "namespace": "a"}},
                    {"selected": True, "stream": {"name": "teams", "namespace": "b"}},
                ]
            },
        )
    ]
    connector.execute_sql_query.side_effect = [
        _sql_result([
            {
                "table_catalog": "project",
                "table_schema": "a",
                "table_name": "linear_issues",
                "table_type": "BASE TABLE",
            }
        ]),
        _sql_result([
            {
                "table_catalog": "project",
                "table_schema": "b",
                "table_name": "linear_teams",
                "table_type": "BASE TABLE",
            }
        ]),
        _sql_result([]),
    ]
    result = get_sql_schema(connector, table_name_contains="linear", max_tables=1)
    assert result["tables"] == [{"table": "`project.a.linear_issues`", "columns": []}]
    assert result["truncated"] is True
    assert result["warnings"] == [
        "Schema inventory is incomplete. Narrow `table_name_contains` or increase "
        "`max_tables` (up to 100)."
    ]
    connector.execute_sql_query.assert_any_call(
        "SHOW TABLES IN SCHEMA `project`.`a`", page_size=1000
    )
    connector.execute_sql_query.assert_any_call(
        "SHOW TABLES IN SCHEMA `project`.`b`", page_size=1000
    )


@pytest.mark.parametrize(
    "last_description", ["First meaning", "Second meaning", "Third meaning"]
)
def test_conflicting_field_meanings_stay_blank(last_description: str) -> None:
    connector = _connector()
    connector.workspace.list_connections.return_value = [
        SimpleNamespace(
            destination_id="destination-1",
            table_prefix="",
            namespace_definition="destination",
            dump_raw_catalog=lambda: {
                "streams": [
                    {
                        "selected": True,
                        "stream": {
                            "name": "users",
                            "json_schema": {
                                "properties": {"id": {"description": description}}
                            },
                        },
                    }
                    for description in (
                        "First meaning",
                        "Second meaning",
                        last_description,
                    )
                ]
            },
        )
    ]
    connector.execute_sql_query.side_effect = [
        _sql_result(
            [
                {
                    "table_catalog": "project",
                    "table_schema": "dataset",
                    "table_name": "users",
                }
            ],
        ),
        _sql_result([], [{"name": "id", "type": "INT64"}]),
    ]
    assert get_sql_schema(connector)["tables"] == [
        {"table": "`project.dataset.users`", "columns": ["id INT64 - "]}
    ]


@pytest.mark.parametrize(
    "truncated,next_cursor", [(False, None), (True, None), (False, "cursor")]
)
def test_schema_reports_discovery_pagination(
    truncated: bool, next_cursor: str | None
) -> None:
    connector = _connector()
    connector.execute_sql_query.return_value = _sql_result(
        [], truncated=truncated, next_cursor=next_cursor
    )
    assert get_sql_schema(connector)["truncated"] is (
        truncated or next_cursor is not None
    )


def test_schema_filter_and_cap() -> None:
    connector = _connector()
    connector.execute_sql_query.side_effect = [
        _sql_result(
            [
                {"table_catalog": "p", "table_schema": "d", "table_name": name}
                for name in ("other", "users", "users_old")
            ],
        ),
        _sql_result([]),
    ]
    result = get_sql_schema(connector, table_name_contains="USERS", max_tables=1)
    assert [table["table"] for table in result["tables"]] == ["`p.d.users`"]
    assert result["truncated"] is True


def test_schema_reports_outer_discovery_pagination() -> None:
    connector = _connector()
    discovery = _sql_result([])
    discovery.connector_metadata.has_next_page = True
    connector.execute_sql_query.return_value = discovery
    assert get_sql_schema(connector)["truncated"] is True


@pytest.mark.parametrize("prefix_part", ["table_catalog", "table_schema"])
def test_schema_filter_ignores_database_and_schema(prefix_part: str) -> None:
    connector = _connector()
    other = {"table_catalog": "p", "table_schema": "d", "table_name": "other"}
    other[prefix_part] = "users"
    connector.execute_sql_query.side_effect = [
        _sql_result(
            [
                other,
                {"table_catalog": "p", "table_schema": "d", "table_name": "users"},
            ],
        ),
        _sql_result([]),
    ]
    result = get_sql_schema(connector, table_name_contains="USERS", max_tables=1)
    assert result["tables"] == [{"table": "`p.d.users`", "columns": []}]
    assert result["truncated"] is False


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


@pytest.mark.parametrize("stage", ["tables", "columns"])
def test_missing_sql_payload_metadata(stage: str) -> None:
    connector = _connector()
    malformed = ExternalApiExecuteResult(status="success", result={"data": []})
    connector.execute_sql_query.side_effect = (
        [malformed]
        if stage == "tables"
        else [
            _sql_result([
                {"table_catalog": "p", "table_schema": "d", "table_name": "users"}
            ]),
            malformed,
        ]
    )
    with pytest.raises(AirbyteCloudError, match="metadata"):
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


@pytest.mark.parametrize(
    "tool", [cloud.execute_external_sql_query, cloud.get_external_sql_schema]
)
@pytest.mark.parametrize("stage", ["workspace", "connector", "validation"])
@pytest.mark.parametrize("status", [400, 401, 403, 500])
@pytest.mark.parametrize("sdk_wrapped", [False, True])
def test_destination_lookup_cloud_errors(
    monkeypatch: pytest.MonkeyPatch,
    tool: object,
    stage: str,
    status: int,
    sdk_wrapped: bool,
) -> None:
    workspace = MagicMock()
    connector = _connector()
    connector.name = "Warehouse"
    workspace.list_destinations.return_value = [connector]
    workspace.get_connector.return_value = connector
    failure = AirbyteCloudApiError(
        status_code=status,
        message="Cloud lookup failed",
        context={"response_text": '{"detail":"Cloud lookup failed"}'},
    )
    if sdk_wrapped:
        sdk_error = SDKError(
            "Cloud lookup failed", status, '{"detail":"Cloud lookup failed"}', None
        )
        failure = api_util._wrap_sdk_error(sdk_error)
        failure.__cause__ = sdk_error
    resolve = MagicMock(return_value=workspace)
    monkeypatch.setattr(cloud, "_get_cloud_workspace", lambda *_: workspace)
    monkeypatch.setattr(cloud, "_get_cloud_workspace_for_resource", resolve)
    if stage == "workspace":
        resolve.side_effect = failure
    elif stage == "connector":
        workspace.get_connector.side_effect = failure
    else:
        monkeypatch.setattr(
            cloud, "require_sql_destination", MagicMock(side_effect=failure)
        )
    args = {"sql": "SELECT 1"} if tool is cloud.execute_external_sql_query else {}
    expected = AirbyteLibInputError if status == 400 else type(failure)
    with pytest.raises(expected) as caught:
        tool(None, connector_id="destination-id-placeholder", **args)
    if status == 400:
        text = format_user_facing_error(caught.value)
        assert "Cloud lookup failed" in text
        assert "destination-1, Warehouse, bigquery" in text
        wrapped = ToolError("Tool failed")
        wrapped.__cause__ = caught.value
        assert "destination-1, Warehouse, bigquery" in agent_error_text(wrapped)
        connector.execute_sql_query.assert_not_called()
    else:
        assert caught.value is failure
        workspace.list_destinations.assert_not_called()


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
    wrapped = ToolError("Tool failed")
    wrapped.__cause__ = caught.value
    assert "destination-1, Warehouse, bigquery" in agent_error_text(wrapped)


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
