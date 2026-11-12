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


def _field_meanings(connector: CloudConnector) -> dict[tuple[str, str, str, str], str]:
    meanings: dict[tuple[str, str, str, str], str] = {}
    for connection in connector.workspace.list_connections():
        if connection.destination_id != connector.connector_id:
            continue
        catalog = connection.dump_raw_catalog() or {}
        context = _destination_load_context(connector.as_cloud_destination())
        for entry in catalog.get("streams", []):
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
    return meanings


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
    discovery = connector.execute_sql_query("SHOW TABLES", page_size=1000)
    if discovery.errors or discovery.status != "success":
        raise AirbyteCloudError(
            message="SQL table discovery failed.", context={"result": discovery}
        )
    tables = []
    for entity in discovery.entities:
        row = {key.lower(): value for key, value in entity.items()}
        parts = [
            row.get("table_catalog", row.get("database_name")),
            row.get("table_schema", row.get("schema_name")),
            row.get("table_name", row.get("name")),
        ]
        if not all(isinstance(part, str) and part for part in parts):
            raise AirbyteCloudError(message="SQL discovery returned an unqualified table name.")
        names = [str(part) for part in parts]
        if table_name_contains and table_name_contains.casefold() not in ".".join(names).casefold():
            continue
        tables.append(names)
    warnings = []
    try:
        meanings = _field_meanings(connector)
    except (AirbyteCloudError, requests.RequestException):
        meanings = {}
        warnings.append("Source field descriptions are unavailable.")
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
        columns = described.connector_metadata.model_extra or {}
        if "columns" not in columns:
            raise AirbyteCloudError(
                message=f"SQL column discovery returned no metadata for {qualified}."
            )
        result.append(
            {
                "table": qualified,
                "columns": [
                    f"{column['name']} {column['type']} - "
                    + meanings.get(
                        (*[part.casefold() for part in parts], column["name"].casefold()), ""
                    )
                    for column in columns.get("columns", [])
                ],
            }
        )
    metadata = discovery.connector_metadata.model_extra or {}
    return {
        "dialect": dialect,
        "tables": result,
        "truncated": len(tables) > max_tables or bool(metadata.get("truncated")),
        "warnings": warnings,
    }
