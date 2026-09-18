# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Built-in docs for SQL passthrough destinations.

The Agents API only knows source connectors, so connector IDs that address Cloud
destinations (the targets of `sql_select`) 404 on `inspect` and skill docs reads. This
module builds the equivalent `AgentConnectorDetails`/`AgentSkillDocs` payloads locally
from the Cloud workspace objects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airbyte.agents.models import (
    AgentConnectorDetails,
    AgentSkillDocs,
    AgentSkillInfo,
    AgentSkillSection,
)
from airbyte.exceptions import PyAirbyteInputError


if TYPE_CHECKING:
    from collections.abc import Mapping

    from airbyte.cloud.connections import CloudConnection
    from airbyte.cloud.connectors import CloudDestination

SNOWFLAKE_DESTINATION_DEFINITION_ID = "424892c4-daac-4491-b35d-c6688ba547ba"
BIGQUERY_DESTINATION_DEFINITION_ID = "22f6c74f-5699-40ff-833c-4a879ea40133"

SQL_PASSTHROUGH_DESTINATION_DIALECTS: Mapping[str, str] = {
    SNOWFLAKE_DESTINATION_DEFINITION_ID: "snowflake",
    BIGQUERY_DESTINATION_DEFINITION_ID: "bigquery",
}
"""Destination definition ID -> `sql_dialect` value accepted by the `sql_select` action."""

SQL_PASSTHROUGH_DESTINATION_NAMES: Mapping[str, str] = {
    SNOWFLAKE_DESTINATION_DEFINITION_ID: "Snowflake",
    BIGQUERY_DESTINATION_DEFINITION_ID: "BigQuery",
}
"""Destination definition ID -> display name of the destination integration."""

_DESTINATION_LOCATION_KEYS: Mapping[str, tuple[tuple[str, str], tuple[str, str]]] = {
    SNOWFLAKE_DESTINATION_DEFINITION_ID: (("database", "database"), ("schema", "schema")),
    BIGQUERY_DESTINATION_DEFINITION_ID: (("project", "project_id"), ("dataset", "dataset_id")),
}
"""Destination definition ID -> (label, configuration key) pairs locating synced tables."""

_NAMESPACE_LABELS = frozenset({"schema", "dataset"})

SQL_PASSTHROUGH_DESTINATION_DEFINITION_IDS = frozenset(SQL_PASSTHROUGH_DESTINATION_DIALECTS)

DESTINATION_SKILL_PREFIX = "connector-destination:"
SOURCE_SKILL_PREFIX = "connector-source:"

SECTION_SQL_PASSTHROUGH = "sql-passthrough"
SECTION_CONNECTIONS = "connections"
SECTION_STREAMS = "streams"

_SECTION_TITLES: Mapping[str, str] = {
    SECTION_SQL_PASSTHROUGH: "Query the destination with sql_select",
    SECTION_CONNECTIONS: "Connections syncing into this destination",
    SECTION_STREAMS: "Streams enabled per connection",
}

_ENGINE_NAMES: Mapping[str, str] = {
    "snowflake": "Snowflake",
    "bigquery": "BigQuery",
}

_NAMESPACE_NOUNS: Mapping[str, str] = {
    "snowflake": "database and schema",
    "bigquery": "dataset",
}
"""How each engine names the namespace an unqualified table reference resolves to."""

_DIALECT_NOTES: Mapping[str, list[str]] = {
    "snowflake": [
        (
            "Unquoted identifiers are upper-cased and case-insensitive. With the default "
            "destination settings Airbyte writes upper-cased table names, so stream `users` "
            "is table `USERS`; connections using the legacy case-preserving raw-table mode "
            "keep the original case. Confirm names with `SHOW TABLES`."
        ),
        "Qualify tables in another schema as `<database>.<schema>.<table>`.",
        (
            "Avoid `INFORMATION_SCHEMA` scans: on large accounts they can exceed the query "
            "time budget. `SHOW TABLES` is served from metadata and returns quickly."
        ),
    ],
    "bigquery": [
        "Table names are case-sensitive and match the stream name.",
        "Qualify tables in another dataset as `` `<project>.<dataset>.<table>` `` (backticked).",
        "Use Standard SQL; legacy SQL is not accepted.",
    ],
}

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


def build_destination_connector_details(destination: CloudDestination) -> AgentConnectorDetails:
    """Build `AgentConnectorDetails` for a Cloud destination the Agents API does not know."""
    return AgentConnectorDetails(
        connector_id=destination.connector_id,
        name=destination.name,
        workspace_id=destination.workspace.workspace_id,
        docs_skill_id=destination_skill_id(destination.connector_id),
        integration_name=SQL_PASSTHROUGH_DESTINATION_NAMES.get(destination.definition_id),
        warnings=[],
    )


def _destination_location(destination: CloudDestination) -> list[tuple[str, str]]:
    """Return (label, value) pairs locating synced tables (for example database/schema)."""
    configuration = destination.configuration or {}
    return [
        (label, value)
        for label, key in _DESTINATION_LOCATION_KEYS.get(destination.definition_id, ())
        if isinstance(value := configuration.get(key), str) and value
    ]


def _destination_connections(destination: CloudDestination) -> list[Any]:
    return [
        connection
        for connection in destination.workspace.list_connections()
        if connection.destination_id == destination.connector_id
    ]


def build_destination_skill_docs(
    destination: CloudDestination,
    *,
    section: str | None = None,
) -> AgentSkillDocs:
    """Build `AgentSkillDocs` for a SQL passthrough destination."""
    dialect = SQL_PASSTHROUGH_DESTINATION_DIALECTS[destination.definition_id]
    skill_id = destination_skill_id(destination.connector_id)
    metadata = AgentSkillInfo(
        id=skill_id,
        kind="connector_destination",
        title=f"{destination.name} (SQL passthrough destination)",
        summary=(
            f"Docs for querying the `{destination.name}` destination with "
            "`execute_agent_connector_ro` and action `sql_select`."
        ),
        tags=["destination", "sql_select"],
    )
    outline = [
        AgentSkillSection(id=section_id, title=title, available=True)
        for section_id, title in _SECTION_TITLES.items()
    ]

    if section is None:
        connections = _destination_connections(destination)
        location = _destination_location(destination)
        content = _overview(destination, dialect, location)
        content += _connections_section(destination, connections)
        content += _streams_section(connections, location, dialect)
        return AgentSkillDocs(metadata=metadata, outline=outline, section_id=None, content=content)

    if section == SECTION_SQL_PASSTHROUGH:
        content = _sql_passthrough_section(destination, dialect)
    elif section == SECTION_CONNECTIONS:
        content = _connections_section(destination, _destination_connections(destination))
    elif section == SECTION_STREAMS:
        content = _streams_section(
            _destination_connections(destination), _destination_location(destination), dialect
        )
    else:
        raise PyAirbyteInputError(
            message=f"Unknown section {section!r} for skill {skill_id!r}.",
            guidance=f"Valid sections: {', '.join(_SECTION_TITLES)}.",
        )
    return AgentSkillDocs(metadata=metadata, outline=outline, section_id=section, content=content)


def _sql_select_call(destination: CloudDestination, dialect: str, sql: str) -> dict[str, Any]:
    return {
        "type": "code",
        "language": "python",
        "code": (
            "execute_agent_connector_ro(\n"
            f'    connector_id="{destination.connector_id}",\n'
            '    entity_type="tables",\n'
            '    action="sql_select",\n'
            f'    api_args={{"sql": "{sql}", "sql_dialect": "{dialect}"}},\n'
            ")"
        ),
    }


def _overview(
    destination: CloudDestination,
    dialect: str,
    location: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    """Self-contained summary shown by `inspect_agent_connector`, without any Cloud lookups."""
    engine = _ENGINE_NAMES[dialect]
    if location:
        location_text = ", ".join(f"{label} `{value}`" for label, value in location)
        location_sentence = (
            f"Tables land in {location_text} unless a connection overrides the namespace; "
            "unqualified table names resolve there."
        )
    else:
        location_sentence = (
            f"Tables land in the destination's configured {_NAMESPACE_NOUNS[dialect]}, which "
            "is also where unqualified table names resolve."
        )
    return [
        {
            "type": "paragraph",
            "text": (
                f"`{destination.name}` is an Airbyte Cloud {engine} destination. Query it with "
                '`execute_agent_connector_ro` (`action="sql_select"`): one read-only `SELECT` '
                f'(or `WITH`) statement per call, `"sql_dialect": "{dialect}"` in `api_args`, '
                "rows returned as JSON. Start by listing its tables:"
            ),
        },
        _sql_select_call(destination, dialect, "SHOW TABLES"),
        {
            "type": "paragraph",
            "text": (
                "`SHOW TABLES` is the only non-`SELECT` statement accepted; it lists the "
                f"tables in the destination. {location_sentence} Tables are named after the "
                "streams of the connections syncing here (`<table_prefix><stream_name>`); "
                "read the `streams` section of these docs for the exact list per connection. "
                "The connections and enabled streams below describe what data lands here; "
                "re-read the `connections` or `streams` section to refresh them."
            ),
        },
        {
            "type": "list",
            "items": [
                *_DIALECT_NOTES[dialect],
                (
                    "Discover columns without reading rows: send `SELECT * FROM <table> "
                    'LIMIT 1` with `"dry_run": true` in `api_args`; only the column list is '
                    "returned."
                ),
                (
                    "Results are capped by the server; when a response includes `end_cursor`, "
                    "pass it back as the top-level `cursor` argument to fetch the next page. "
                    "Always add a `LIMIT` clause to `SELECT` queries (`SHOW TABLES` takes no "
                    "`LIMIT`)."
                ),
                (
                    "Statements are cancelled after a fixed time budget (about a minute) and "
                    "return an error; narrow the query rather than retrying it unchanged."
                ),
                _AIRBYTE_METADATA_COLUMNS,
            ],
        },
    ]


def _sql_passthrough_section(destination: CloudDestination, dialect: str) -> list[dict[str, Any]]:
    engine = _ENGINE_NAMES[dialect]
    return [
        {"type": "heading", "level": 2, "text": "Query the destination with sql_select"},
        {
            "type": "paragraph",
            "text": (
                "This destination accepts one read-only SQL statement per call via "
                '`execute_agent_connector_ro` with `action="sql_select"` and `api_args` '
                f'containing `"sql"` and `"sql_dialect": "{dialect}"` (`entity_type` is '
                "ignored; any value works). Only `SELECT`/`WITH` statements and the literal "
                f"`SHOW TABLES` are accepted; anything else is rejected before reaching {engine}. "
                "Results are returned as JSON rows."
            ),
        },
        {"type": "paragraph", "text": "List tables in the destination:"},
        _sql_select_call(destination, dialect, "SHOW TABLES"),
        {
            "type": "paragraph",
            "text": (
                "`SHOW TABLES` lists the tables in the destination's configured "
                f"{_NAMESPACE_NOUNS[dialect]}. Unqualified table names in your own SQL resolve "
                "to that same namespace."
            ),
        },
        {"type": "paragraph", "text": "Discover columns without reading rows (`dry_run`):"},
        {
            "type": "code",
            "language": "python",
            "code": (
                "execute_agent_connector_ro(\n"
                f'    connector_id="{destination.connector_id}",\n'
                '    entity_type="tables",\n'
                '    action="sql_select",\n'
                "    api_args={\n"
                '        "sql": "SELECT * FROM <table> LIMIT 1",\n'
                f'        "sql_dialect": "{dialect}",\n'
                '        "dry_run": True,\n'
                "    },\n"
                ")"
            ),
        },
        {"type": "paragraph", "text": "Read rows from a table:"},
        {
            "type": "code",
            "language": "sql",
            "code": _qualified_table_example(destination),
        },
        {"type": "heading", "level": 3, "text": f"{engine} specifics"},
        {"type": "list", "items": list(_DIALECT_NOTES[dialect])},
        {"type": "heading", "level": 3, "text": "Limits and pagination"},
        {
            "type": "list",
            "items": [
                (
                    "Row count and response size are capped by the server. When a response "
                    "includes `end_cursor`, pass it back as the top-level `cursor` argument "
                    "to fetch the next page (`dry_run` cannot be combined with `cursor`)."
                ),
                (
                    "Always add a `LIMIT` clause to `SELECT` queries and select only the "
                    "columns you need (`SHOW TABLES` takes no `LIMIT`)."
                ),
                (
                    "Statements are cancelled after a fixed time budget (about a minute) and "
                    "return an error; narrow the query rather than retrying it unchanged."
                ),
            ],
        },
        {"type": "heading", "level": 3, "text": "Airbyte metadata columns"},
        {"type": "paragraph", "text": _AIRBYTE_METADATA_COLUMNS},
    ]


def _namespace_entry(location: list[tuple[str, str]]) -> tuple[str, str] | None:
    """Return the schema-level (label, value) entry of a destination location, if any."""
    return next((entry for entry in location if entry[0] in _NAMESPACE_LABELS), None)


def _qualified_table_example(destination: CloudDestination) -> str:
    """Return an example `SELECT` qualifying the table with the destination's namespace."""
    namespace_entry = _namespace_entry(_destination_location(destination))
    if namespace_entry is None:
        return "SELECT * FROM <table> LIMIT 10"
    namespace = namespace_entry[1]
    if destination.definition_id == BIGQUERY_DESTINATION_DEFINITION_ID:
        return f"SELECT * FROM `{namespace}.<table>` LIMIT 10"
    return f"SELECT * FROM {namespace}.<table> LIMIT 10"


def _connections_section(
    destination: CloudDestination,
    connections: list[Any],
) -> list[dict[str, Any]]:
    if not connections:
        return [
            {
                "type": "paragraph",
                "text": "No connections sync into this destination.",
            }
        ]
    source_names = {
        source.connector_id: source.name for source in destination.workspace.list_sources()
    }
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
    connection: CloudConnection,
    location: list[tuple[str, str]],
) -> str:
    """Describe where a connection's tables land: namespace choice plus table prefix."""
    prefix_clause = (
        f", with table prefix {connection.table_prefix!r}."
        if connection.table_prefix
        else ", with no table prefix."
    )
    namespace_definition = connection.namespace_definition
    if namespace_definition == "source":
        note = "Streams land in a namespace mirroring the source's own namespace (e.g. its schema)"
    elif namespace_definition == "custom_format":
        note = f"Streams land in namespace format `{connection.namespace_format}`"
    else:
        note = "Streams land in the destination's default namespace"
        if namespace_entry := _namespace_entry(location):
            label, value = namespace_entry
            note += f", {label} `{value}`"
    return note + prefix_clause


def _streams_section(
    connections: list[Any],
    location: list[tuple[str, str]],
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
                f"in the destination's configured {_NAMESPACE_NOUNS[dialect]}. Query these "
                "names directly (unqualified) with `sql_select`. Names below assume the "
                "destination's default identifier casing and namespace; confirm with "
                "`SHOW TABLES`."
            ),
        },
    ]
    for connection in connections:
        blocks.append({"type": "heading", "level": 3, "text": str(connection.name)})
        blocks.extend(
            [
                {
                    "type": "paragraph",
                    "text": _connection_namespace_note(connection, location),
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
