# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Compact warehouse metadata for SQL passthrough destinations."""

from typing import Any

import requests

from airbyte._direct_connectors.connector_docs import _destination_load_context
from airbyte._direct_connectors.models import _SQL_PASSTHROUGH_DESTINATION_DIALECTS
from airbyte.cloud.connectors import CloudConnector
from airbyte.cloud.models import ConnectorType
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteCloudError, AirbyteLibInputError


_MAX_SCHEMA_TABLES = 100


def list_sql_destinations(workspace: CloudWorkspace) -> list[dict[str, str]]:
    """List deployed SQL passthrough destinations, without probing external access."""
    return [
        {"id": destination.connector_id, "name": str(destination.name), "dialect": dialect}
        for destination in workspace.list_destinations()
        if (dialect := _SQL_PASSTHROUGH_DESTINATION_DIALECTS.get(destination.definition_id))
    ]


def require_sql_destination(connector: CloudConnector) -> str:
    """Return the registered dialect or reject a non-SQL destination."""
    dialect = _SQL_PASSTHROUGH_DESTINATION_DIALECTS.get(connector.definition_id)
    if connector.connector_type != ConnectorType.DESTINATION or dialect is None:
        raise AirbyteLibInputError(
            message=f"Connector {connector.connector_id!r} is not a SQL-passthrough destination.",
            guidance="Use a deployed BigQuery or Snowflake destination ID.",
        )
    return dialect


def _destination_metadata(
    connector: CloudConnector,
) -> tuple[dict[tuple[str, str, str, str], str], set[tuple[str, str]]]:
    meanings: dict[tuple[str, str, str, str], str] = {}
    namespaces: set[tuple[str, str]] = set()
    context = _destination_load_context(connector.as_cloud_destination())
    for connection in connector.workspace.list_connections():
        if connection.destination_id != connector.connector_id:
            continue
        catalog = connection.dump_raw_catalog() or {}
        for entry in catalog.get("streams", []):
            if not entry.get("selected", True):
                continue
            stream = entry["stream"]
            table = connection.table_prefix + stream["name"]
            namespace = stream.get("namespace")
            if connection.namespace_definition == "custom_format":
                schema = (connection.namespace_format or "").replace(
                    "${SOURCE_NAMESPACE}", namespace or ""
                )
            elif connection.namespace_definition == "source":
                schema = namespace or context.schema_name
            else:
                schema = context.schema_name
            if not context.database_name or not schema:
                continue
            namespaces.add((context.database_name, schema))
            for name, field in stream.get("json_schema", {}).get("properties", {}).items():
                description = field.get("description", "")
                if isinstance(description, str) and description:
                    key = (
                        context.database_name.casefold(),
                        schema.casefold(),
                        table.casefold(),
                        name.casefold(),
                    )
                    meaning = " ".join(description.split())
                    if key in meanings and meanings[key] != meaning:
                        meanings[key] = ""
                    else:
                        meanings[key] = meaning
    return meanings, namespaces


def _discovery_queries(namespaces: set[tuple[str, str]], dialect: str) -> list[str]:
    queries = []
    for database, schema in sorted(namespaces):
        quote = "`" if dialect == "bigquery" else '"'
        scope = ".".join(
            quote
            + (name.upper() if dialect == "snowflake" else name).replace(quote, quote * 2)
            + quote
            for name in (database, schema)
        )
        queries.append(f"SHOW TABLES IN SCHEMA {scope}")
    return queries


def _discover_tables(
    connector: CloudConnector,
    queries: list[str],
    table_name_contains: str | None,
    max_tables: int,
) -> tuple[list[list[str]], bool]:
    tables: list[list[str]] = []
    incomplete = False
    for sql in queries:
        cursor: str | None = None
        while True:
            discovery = (
                connector.execute_sql_query(None, page_size=1000, cursor=cursor)
                if cursor
                else connector.execute_sql_query(sql, page_size=1000)
            )
            if discovery.errors or discovery.status != "success":
                raise AirbyteCloudError(
                    message="SQL table discovery failed.", context={"result": discovery}
                )
            payload = discovery.result
            if (
                not isinstance(payload, dict)
                or not isinstance(payload.get("data"), list)
                or not isinstance(payload.get("meta"), dict)
            ):
                raise AirbyteCloudError(message="SQL table discovery returned no row metadata.")
            metadata = payload["meta"]
            for entity in payload["data"]:
                row = {key.lower(): value for key, value in entity.items()}
                parts = [
                    row.get("table_catalog", row.get("database_name")),
                    row.get("table_schema", row.get("schema_name")),
                    row.get("table_name", row.get("name")),
                ]
                if not all(isinstance(part, str) and part for part in parts):
                    raise AirbyteCloudError(
                        message="SQL discovery returned an unqualified table name."
                    )
                parts = [str(part) for part in parts]
                if (
                    table_name_contains
                    and table_name_contains.casefold() not in parts[-1].casefold()
                ):
                    continue
                if parts not in tables:
                    tables.append(parts)
            next_cursor = metadata.get("next_cursor") or discovery.connector_metadata.end_cursor
            if len(tables) > max_tables:
                break
            if not next_cursor or next_cursor == cursor:
                incomplete |= bool(
                    metadata.get("truncated")
                    or next_cursor
                    or discovery.connector_metadata.has_next_page
                )
                break
            cursor = next_cursor
        if len(tables) > max_tables:
            break
    return tables, incomplete


def get_sql_schema(
    connector: CloudConnector, *, table_name_contains: str | None = None, max_tables: int = 20
) -> dict[str, Any]:
    """Return qualified tables and one-line columns using metadata-only SQL macros.

    Warehouse types are authoritative; descriptions come from synced stream catalogs
    when available. JSON, ARRAY, OBJECT and STRUCT columns require dialect-specific
    extraction functions before scalar comparisons.
    """
    if not 1 <= max_tables <= _MAX_SCHEMA_TABLES:
        raise AirbyteLibInputError(message="`max_tables` must be between 1 and 100.")
    dialect = require_sql_destination(connector)
    warnings = []
    try:
        meanings, namespaces = _destination_metadata(connector)
    except (AirbyteCloudError, requests.RequestException):
        meanings = {}
        namespaces = set()
        warnings.append("Connection namespaces and source field descriptions are unavailable.")
    if not namespaces:
        context = _destination_load_context(connector.as_cloud_destination())
        if not context.database_name or not context.schema_name:
            raise AirbyteCloudError(
                message="SQL discovery requires a destination database and dataset or schema."
            )
        namespaces.add((context.database_name, context.schema_name))
    tables, incomplete = _discover_tables(
        connector, _discovery_queries(namespaces, dialect), table_name_contains, max_tables
    )
    result = []
    for parts in tables[:max_tables]:
        qualified = (
            "`" + ".".join(part.replace("`", "\\`") for part in parts) + "`"
            if dialect == "bigquery"
            else ".".join('"' + part.replace('"', '""') + '"' for part in parts)
        )
        described = connector.execute_sql_query(f"DESCRIBE TABLE {qualified}")
        if described.errors or described.status != "success":
            raise AirbyteCloudError(message=f"SQL column discovery failed for {qualified}.")
        columns = described.result.get("meta") if isinstance(described.result, dict) else None
        if not isinstance(columns, dict) or not isinstance(columns.get("columns"), list):
            raise AirbyteCloudError(
                message=f"SQL column discovery returned no metadata for {qualified}."
            )
        result.append(
            {
                "table": qualified,
                "columns": [
                    f"{column['name']} {column['type']} - "
                    + meanings.get(
                        (
                            parts[0].casefold(),
                            parts[1].casefold(),
                            parts[2].casefold(),
                            column["name"].casefold(),
                        ),
                        "",
                    )
                    for column in columns.get("columns", [])
                ],
            }
        )
    if len(tables) > max_tables or incomplete:
        warnings.append(
            "Schema inventory is incomplete. Narrow `table_name_contains` or increase "
            "`max_tables` (up to 100)."
        )
    return {
        "dialect": dialect,
        "tables": result,
        "truncated": (len(tables) > max_tables or incomplete),
        "warnings": warnings,
    }
