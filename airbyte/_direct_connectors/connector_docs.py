# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Docs helpers for SQL passthrough destinations (Snowflake, BigQuery).

Three paths serve destination docs:

- `CloudWorkspace.get_agent_skill_docs` / `CloudConnector.read_agent_skill_docs`
  (MCP tool `get_agent_skill_docs`) read destination skill docs straight from
  the Agents API, with no local enrichment.
- `CloudConnector.get_direct_access_guidance` (MCP tool `describe_cloud_connector`
  with `with_direct_access_guidance=True`) prepends a short local intro --
  location, table naming, and dialect notes -- to the server-served destination
  docs overview.
- The same method falls back to a single structure-only doc -- location, table
  naming, connections, and streams -- when the destination is not enrolled for
  direct access (403/404) or the deployment has no Context layer API. Fallback
  docs never reference Airbyte SQL tools.

This module also builds `_DirectConnectorInspectResult` payloads locally from the
Cloud workspace objects and summarizes the connections touching a connector for
the `describe_cloud_*` MCP tools.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, NamedTuple

from airbyte._direct_connectors.models import (
    _BIGQUERY_DESTINATION_DEFINITION_ID,
    _SNOWFLAKE_DESTINATION_DEFINITION_ID,
    _SQL_PASSTHROUGH_DESTINATION_DIALECTS,
    _SQL_PASSTHROUGH_DESTINATION_NAMES,
    CloudConnectorConnectionInfo,
    DirectAccessGuidance,
    DirectAccessGuidanceIndexEntry,
    _ConnectionLike,
    _ConnectorLike,
    _DestinationLike,
    _DirectConnectorInspectResult,
    _WorkspaceLike,
)


if TYPE_CHECKING:
    from collections.abc import Mapping

_DESTINATION_LOAD_CONTEXT_KEYS: Mapping[str, tuple[str, str]] = {
    _SNOWFLAKE_DESTINATION_DEFINITION_ID: ("database", "schema"),
    _BIGQUERY_DESTINATION_DEFINITION_ID: ("project_id", "dataset_id"),
}
"""Destination definition ID -> (database, schema) configuration keys locating tables."""


DESTINATION_SKILL_PREFIX = "connector-destination:"
SOURCE_SKILL_PREFIX = "connector-source:"

SQL_PASSTHROUGH_NOT_ENABLED_NOTICE = (
    "Airbyte SQL passthrough is not enabled for this destination, so it cannot be "
    "queried through Airbyte tools."
)
"""Notice prepended to SQL guidance when the destination is not enrolled."""

SQL_PASSTHROUGH_UNAVAILABLE_NOTICE = (
    "Airbyte SQL passthrough is not available in this deployment, so it cannot be "
    "queried through Airbyte tools."
)
"""Notice prepended to SQL guidance when the deployment has no Context layer API."""

_ENGINE_NAMES: Mapping[str, str] = {
    "snowflake": "Snowflake",
    "bigquery": "BigQuery",
}

_NAMESPACE_NOUNS: Mapping[str, str] = {
    "snowflake": "database and schema",
    "bigquery": "dataset",
}
"""How each engine names the namespace an unqualified table reference resolves to."""

_LOCATION_LABELS: Mapping[str, tuple[str, str]] = {
    "snowflake": ("database", "schema"),
    "bigquery": ("project", "dataset"),
}
"""Per-engine labels for the database/schema-like location keys."""

_DIALECT_NOTES: Mapping[str, list[str]] = {
    "snowflake": [
        (
            "Unquoted identifiers are upper-cased and case-insensitive. With the default "
            "destination settings Airbyte writes upper-cased table AND column names, so stream "
            "`users` is table `USERS` and field `primaryUserId` is column `PRIMARYUSERID`; "
            "connections using the legacy case-preserving raw-table mode keep the original "
            "case."
        ),
        (
            "Prefer unquoted identifiers: double-quoting makes them case-sensitive, so "
            '`SELECT "id"` fails with `invalid identifier` against column `ID`. Quote a name '
            "only when it is mixed or lower case (legacy raw-table mode), contains spaces or "
            "special characters, or is a reserved word."
        ),
        "Qualify tables in another schema as `<database>.<schema>.<table>`.",
        (
            "Avoid `INFORMATION_SCHEMA` scans on large accounts; they can exceed the query "
            "time budget."
        ),
    ],
    "bigquery": [
        "Table names are case-sensitive and match the stream name.",
        "Qualify tables in another dataset as `` `<project>.<dataset>.<table>` `` (backticked).",
        "Use Standard SQL; legacy SQL is not accepted.",
    ],
}
"""Per-engine naming and dialect rules, written without any tool references."""

_AIRBYTE_METADATA_COLUMNS = (
    "Every Airbyte-written table also carries `_airbyte_raw_id`, `_airbyte_extracted_at` "
    "(when the record was extracted from the source; useful for freshness checks), "
    "`_airbyte_meta` (per-row sync errors), and "
    "`_airbyte_generation_id`. Raw records live in the `airbyte_internal` namespace."
)


def connector_id_from_skill_id(skill_id: str) -> str:
    """Strip a `connector-destination:`/`connector-source:` prefix; otherwise return as-is."""
    for prefix in (DESTINATION_SKILL_PREFIX, SOURCE_SKILL_PREFIX):
        if skill_id.startswith(prefix):
            return skill_id[len(prefix) :]
    return skill_id


def destination_skill_id(connector_id: str) -> str:
    """Return the docs skill ID for a SQL passthrough destination."""
    return f"{DESTINATION_SKILL_PREFIX}{connector_id}"


def source_skill_id(connector_id: str) -> str:
    """Return the docs skill ID for a source connector."""
    return f"{SOURCE_SKILL_PREFIX}{connector_id}"


def build_destination_connector_details(
    destination: _DestinationLike,
) -> _DirectConnectorInspectResult:
    """Build `_DirectConnectorInspectResult` for a destination the API does not know."""
    workspace: _WorkspaceLike = destination.workspace
    return _DirectConnectorInspectResult(
        connector_id=destination.connector_id,
        name=destination.name,
        workspace_id=workspace.workspace_id,
        docs_skill_id=destination_skill_id(destination.connector_id),
        integration_name=_SQL_PASSTHROUGH_DESTINATION_NAMES.get(destination.definition_id),
        warnings=[],
    )


class _DestinationLoadContext(NamedTuple):
    """Where a destination writes synced tables.

    `database_name` and `schema_name` come from the destination configuration (Snowflake
    database/schema, BigQuery project/dataset). When built for a specific connection,
    `schema_name` reflects that connection's namespace setting and `table_prefix` is the
    connection's prefix; without a connection, `table_prefix` is `None`. Fields are `None`
    when the destination is not a known SQL passthrough type or the value is unknown.
    """

    database_name: str | None = None
    schema_name: str | None = None
    table_prefix: str | None = None


def _destination_load_context(
    destination: _DestinationLike,
    connection: _ConnectionLike | None = None,
) -> _DestinationLoadContext:
    """Return where a destination writes synced tables for a connection, if given."""
    keys = _DESTINATION_LOAD_CONTEXT_KEYS.get(destination.definition_id)
    if keys is None:
        return _DestinationLoadContext()
    config = dict(destination.configuration or {})
    database_key, schema_key = keys
    database_name = config.get(database_key)
    schema_name = config.get(schema_key)

    table_prefix: str | None = None
    if connection is not None:
        table_prefix = connection.table_prefix or None
        if connection.namespace_definition == "custom_format":
            schema_name = connection.namespace_format or None
        elif connection.namespace_definition == "source":
            schema_name = None

    return _DestinationLoadContext(
        database_name=(database_name if isinstance(database_name, str) and database_name else None),
        schema_name=(schema_name if isinstance(schema_name, str) and schema_name else None),
        table_prefix=table_prefix,
    )


def _destination_connections(destination: _DestinationLike) -> list[_ConnectionLike]:
    workspace: _WorkspaceLike = destination.workspace
    return [
        connection
        for connection in workspace.list_connections()
        if connection.destination_id == destination.connector_id
    ]


def build_direct_access_sql_guidance(
    destination: _DestinationLike,
    *,
    sql_passthrough_notice: str | None = None,
) -> DirectAccessGuidance:
    """Build a structure-only `DirectAccessGuidance` for a SQL passthrough destination.

    Used when the destination's skill docs cannot be read from the Agents API
    (not enrolled, or no Context layer API). The doc covers table layout, naming,
    and the connections syncing in; it never references Airbyte SQL tools. When
    `sql_passthrough_notice` is set, it leads the content and is returned in
    `warnings`.
    """
    dialect = _SQL_PASSTHROUGH_DESTINATION_DIALECTS[destination.definition_id]
    skill_id = destination_skill_id(destination.connector_id)
    metadata = DirectAccessGuidanceIndexEntry(
        id=skill_id,
        kind="connector_destination",
        title=f"{destination.name} (SQL passthrough destination)",
        summary=f"Table layout and naming for the `{destination.name}` destination.",
        tags=["destination"],
    )
    connections = _destination_connections(destination)
    content: list[dict[str, Any]] = []
    if sql_passthrough_notice:
        content.append({"type": "paragraph", "text": sql_passthrough_notice})
    content += _destination_layout_blocks(
        destination=destination,
        dialect=dialect,
        load_context=_destination_load_context(destination),
    )
    content += [
        {"type": "paragraph", "text": "Read rows from a table:"},
        {
            "type": "code",
            "language": "sql",
            "code": _qualified_table_example(destination),
        },
    ]
    content += _connections_section(
        destination=destination,
        connections=connections,
    )
    content += _streams_section(
        connections=connections,
        destination=destination,
        dialect=dialect,
    )
    return DirectAccessGuidance(
        metadata=metadata,
        outline=[],
        section_id=None,
        content=content,
        warnings=[sql_passthrough_notice] if sql_passthrough_notice else [],
    )


def merge_destination_skill_docs(
    server_docs: DirectAccessGuidance,
    destination: _DestinationLike,
) -> DirectAccessGuidance:
    """Prepend PyAirbyte's local table-layout intro to the server's overview content.

    Only the default (no-section) or `overview` read is augmented; the server
    outline and all other section reads pass through unchanged.
    """
    if server_docs.section_id not in {None, "overview"}:
        return server_docs
    dialect = _SQL_PASSTHROUGH_DESTINATION_DIALECTS[destination.definition_id]
    content = (
        _destination_layout_blocks(
            destination=destination,
            dialect=dialect,
            load_context=_destination_load_context(destination),
        )
        + server_docs.content
    )
    return server_docs.model_copy(update={"content": content})


def _location_sentence(dialect: str, load_context: _DestinationLoadContext) -> str:
    """Describe where a destination's tables land, without a trailing period."""
    if load_context.database_name is not None or load_context.schema_name is not None:
        database_label, schema_label = _LOCATION_LABELS[dialect]
        location_text = ", ".join(
            f"{label} `{value}`"
            for label, value in (
                (database_label, load_context.database_name),
                (schema_label, load_context.schema_name),
            )
            if value is not None
        )
        return (
            f"Tables land in {location_text} unless a connection overrides the "
            "namespace; unqualified table names resolve there"
        )
    return (
        f"Tables land in the destination's configured {_NAMESPACE_NOUNS[dialect]}, "
        "which is also where unqualified table names resolve"
    )


def _table_naming_sentence(dialect: str) -> str:
    sentence = "Tables are named `<table_prefix><stream_name>`"
    if dialect == "snowflake":
        return (
            sentence + ", upper-cased unless the connection uses the legacy case-preserving "
            "raw-table mode."
        )
    if dialect == "bigquery":
        return sentence + "; names are case-sensitive."
    return sentence + "."


def _destination_layout_blocks(
    destination: _DestinationLike,
    dialect: str,
    load_context: _DestinationLoadContext,
) -> list[dict[str, Any]]:
    """Intro paragraph plus dialect notes describing the destination's table layout."""
    engine = _ENGINE_NAMES[dialect]
    return [
        {
            "type": "paragraph",
            "text": (
                f"`{destination.name}` is an Airbyte Cloud {engine} destination. "
                f"{_location_sentence(dialect, load_context)}. "
                f"{_table_naming_sentence(dialect)}"
            ),
        },
        {
            "type": "list",
            "items": [*_DIALECT_NOTES[dialect], _AIRBYTE_METADATA_COLUMNS],
        },
    ]


def _qualified_table_example(destination: _DestinationLike) -> str:
    """Return an example `SELECT` qualifying the table with the destination's namespace."""
    namespace = _destination_load_context(destination).schema_name
    if namespace is None:
        return "SELECT * FROM <table> LIMIT 10"
    if destination.definition_id == _BIGQUERY_DESTINATION_DEFINITION_ID:
        return f"SELECT * FROM `{namespace}.<table>` LIMIT 10"
    return f"SELECT * FROM {namespace}.<table> LIMIT 10"


def _connections_section(
    destination: _DestinationLike,
    connections: list[_ConnectionLike],
) -> list[dict[str, Any]]:
    if not connections:
        return [
            {
                "type": "paragraph",
                "text": "No connections sync into this destination.",
            }
        ]
    workspace: _WorkspaceLike = destination.workspace
    source_names = {source.connector_id: source.name for source in workspace.list_sources()}
    items = []
    for connection in connections:
        source_name = source_names.get(connection.source_id, connection.source_id)
        item = (
            f"{connection.name} (connection_id={connection.connection_id}) "
            f"syncs from source {source_name} (source_id={connection.source_id})"
        )
        if connection.table_prefix:
            item += f", with table prefix {connection.table_prefix!r}"
        items.append(item)
    return [
        {"type": "heading", "level": 2, "text": "Connections syncing into this destination"},
        {"type": "list", "items": items},
    ]


def _table_name(dialect: str, table_prefix: str, stream_name: str) -> str:
    name = f"{table_prefix}{stream_name}"
    return name.upper() if dialect == "snowflake" else name


def _connection_namespace_note(
    connection: _ConnectionLike,
    load_context: _DestinationLoadContext,
) -> str:
    """Describe where a connection's tables land: namespace choice plus table prefix."""
    prefix_clause = (
        f", with table prefix {load_context.table_prefix!r}."
        if load_context.table_prefix
        else ", with no table prefix."
    )
    namespace_definition = connection.namespace_definition
    if namespace_definition == "source":
        note = "Streams land in a namespace mirroring the source's own namespace (e.g. its schema)"
    elif namespace_definition == "custom_format":
        note = f"Streams land in namespace format `{connection.namespace_format}`"
    else:
        note = "Streams land in the destination's default namespace"
        if load_context.schema_name is not None:
            note += f", schema `{load_context.schema_name}`"
    return note + prefix_clause


def _streams_section(
    connections: list[_ConnectionLike],
    destination: _DestinationLike,
    dialect: str,
) -> list[dict[str, Any]]:
    if not connections:
        return [
            {
                "type": "paragraph",
                "text": "No connections sync into this destination, so no streams land here.",
            }
        ]
    blocks: list[dict[str, Any]] = [
        {"type": "heading", "level": 2, "text": "Streams enabled per connection"},
        {
            "type": "paragraph",
            "text": (
                "Each enabled stream is written to a table named `<table_prefix><stream_name>` "
                f"in the destination's configured {_NAMESPACE_NOUNS[dialect]}. Names below "
                "assume the destination's default identifier casing and namespace."
            ),
        },
    ]
    for connection in connections:
        blocks.append({"type": "heading", "level": 3, "text": str(connection.name)})
        blocks.extend(
            [
                {
                    "type": "paragraph",
                    "text": _connection_namespace_note(
                        connection=connection,
                        load_context=_destination_load_context(
                            destination,
                            connection,
                        ),
                    ),
                },
                {
                    "type": "table",
                    "headers": ["Stream", "Table"],
                    "rows": [
                        [
                            stream_name,
                            f"`{_table_name(dialect, connection.table_prefix, stream_name)}`",
                        ]
                        for stream_name in connection.stream_names
                    ],
                },
            ]
        )
    return blocks


def build_connection_details(connector: _ConnectorLike) -> list[CloudConnectorConnectionInfo]:
    """Summarize each connection that reads from or writes to `connector`.

    Counterpart connector names are resolved with one `list_sources()` and one
    `list_destinations()` call, and the destination's database/schema location is read
    from its configuration.
    """
    workspace: _WorkspaceLike = connector.workspace
    if connector.connector_type.value == "source":
        connections = [
            connection
            for connection in workspace.list_connections()
            if connection.source_id == connector.connector_id
        ]
    else:
        connections = [
            connection
            for connection in workspace.list_connections()
            if connection.destination_id == connector.connector_id
        ]

    source_names = {source.connector_id: source.name for source in workspace.list_sources()}
    destinations = {
        destination.connector_id: destination for destination in workspace.list_destinations()
    }

    infos: list[CloudConnectorConnectionInfo] = []
    for connection in connections:
        destination = destinations.get(connection.destination_id)
        ctx = (
            _destination_load_context(
                destination,
                connection,
            )
            if destination is not None
            else _DestinationLoadContext()
        )
        infos.append(
            CloudConnectorConnectionInfo(
                connection_id=connection.connection_id,
                name=str(connection.name),
                source_id=connection.source_id,
                source_name=str(source_names.get(connection.source_id, connection.source_id)),
                destination_id=connection.destination_id,
                destination_name=str(
                    destination.name if destination is not None else connection.destination_id
                ),
                schedule=connection.schedule.friendly_description
                if connection.schedule is not None
                else None,
                stream_names=list(connection.stream_names),
                namespace_definition=connection.namespace_definition,
                namespace_format=connection.namespace_format,
                table_prefix=connection.table_prefix,
                destination_database=ctx.database_name,
                destination_schema=ctx.schema_name,
            )
        )
    return infos
