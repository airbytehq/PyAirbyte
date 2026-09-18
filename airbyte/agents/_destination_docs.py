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

    from airbyte.cloud.connectors import CloudDestination

SNOWFLAKE_DESTINATION_DEFINITION_ID = "424892c4-daac-4491-b35d-c6688ba547ba"
BIGQUERY_DESTINATION_DEFINITION_ID = "22f6c74f-5699-40ff-833c-4a879ea40133"

SQL_PASSTHROUGH_DESTINATION_DIALECTS: Mapping[str, str] = {
    SNOWFLAKE_DESTINATION_DEFINITION_ID: "snowflake",
    BIGQUERY_DESTINATION_DEFINITION_ID: "bigquery",
}
"""Destination definition ID -> `sql_dialect` value accepted by the `sql_select` action."""

SQL_PASSTHROUGH_DESTINATION_DEFINITION_IDS = frozenset(SQL_PASSTHROUGH_DESTINATION_DIALECTS)

DESTINATION_SKILL_PREFIX = "connector-destination:"
SOURCE_SKILL_PREFIX = "connector-source:"

SECTION_SQL_PASSTHROUGH = "sql-passthrough"
SECTION_CONNECTIONS = "connections"
SECTION_STREAMS = "streams"

_SECTION_TITLES: Mapping[str, str] = {
    SECTION_SQL_PASSTHROUGH: "Query the destination with sql_select",
    SECTION_CONNECTIONS: "Connections syncing into this destination",
    SECTION_STREAMS: "Tables written by each connection",
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
        "Unquoted identifiers are upper-cased and case-insensitive; Airbyte writes tables in "
        "upper case, so stream `users` is table `USERS`.",
        "Qualify tables in another schema as `<database>.<schema>.<table>`.",
        "Avoid `INFORMATION_SCHEMA` scans: on large accounts they can exceed the query time "
        "budget. `SHOW TABLES` is served from metadata and returns quickly.",
    ],
    "bigquery": [
        "Table names are case-sensitive and match the stream name.",
        "Qualify tables in another dataset as `` `<project>.<dataset>.<table>` `` (backticked).",
        "Use Standard SQL; legacy SQL is not accepted.",
    ],
}

_AIRBYTE_METADATA_COLUMNS = (
    "Every Airbyte-written table also carries `_airbyte_raw_id`, `_airbyte_extracted_at` "
    "(sync time, useful for freshness checks), `_airbyte_meta` (per-row sync errors), and "
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
        warnings=[],
    )


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
        content = _overview(destination, dialect)
        return AgentSkillDocs(metadata=metadata, outline=outline, section_id=None, content=content)

    if section == SECTION_SQL_PASSTHROUGH:
        content = _sql_passthrough_section(destination, dialect)
    elif section == SECTION_CONNECTIONS:
        content = _connections_section(destination)
    elif section == SECTION_STREAMS:
        content = _streams_section(destination, dialect)
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


def _overview(destination: CloudDestination, dialect: str) -> list[dict[str, Any]]:
    """Self-contained summary shown by `inspect_agent_connector`, without any Cloud lookups."""
    engine = _ENGINE_NAMES[dialect]
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
                "`SHOW TABLES` is the only non-`SELECT` statement accepted; it lists the tables "
                f"in the destination's configured {_NAMESPACE_NOUNS[dialect]}, which is also "
                "where unqualified table names resolve. Tables are named after the streams of the "
                "connections syncing here (`<table_prefix><stream_name>`); read the `streams` "
                "section of these docs for the exact list per connection."
            ),
        },
        {
            "type": "list",
            "items": [
                *_DIALECT_NOTES[dialect],
                "Discover columns without reading rows: send `SELECT * FROM <table> LIMIT 1` with "
                '`"dry_run": true` in `api_args`; only the column list is returned.',
                "Results are capped by the server; when a response includes `end_cursor`, pass it "
                "back as the top-level `cursor` argument to fetch the next page. Always add a "
                "`LIMIT` clause.",
                "Statements are cancelled after a fixed time budget (about a minute) and return "
                "an error; narrow the query rather than retrying it unchanged.",
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
            "code": "SELECT * FROM <table> ORDER BY _airbyte_extracted_at DESC LIMIT 10",
        },
        {"type": "heading", "level": 3, "text": f"{engine} specifics"},
        {"type": "list", "items": list(_DIALECT_NOTES[dialect])},
        {"type": "heading", "level": 3, "text": "Limits and pagination"},
        {
            "type": "list",
            "items": [
                "Row count and response size are capped by the server. When a response includes "
                "`end_cursor`, pass it back as the top-level `cursor` argument to fetch the next "
                "page (`dry_run` cannot be combined with `cursor`).",
                "Always add a `LIMIT` clause and select only the columns you need.",
                "Statements are cancelled after a fixed time budget (about a minute) and return "
                "an error; narrow the query rather than retrying it unchanged.",
            ],
        },
        {"type": "heading", "level": 3, "text": "Airbyte metadata columns"},
        {"type": "paragraph", "text": _AIRBYTE_METADATA_COLUMNS},
    ]


def _connections_section(destination: CloudDestination) -> list[dict[str, Any]]:
    connections = _destination_connections(destination)
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


def _streams_section(destination: CloudDestination, dialect: str) -> list[dict[str, Any]]:
    connections = _destination_connections(destination)
    if not connections:
        return [
            {
                "type": "paragraph",
                "text": "No connections sync into this destination, so no streams land here.",
            }
        ]
    blocks: list[dict[str, Any]] = [
        {"type": "heading", "level": 2, "text": "Tables written by each connection"},
        {
            "type": "paragraph",
            "text": (
                "Each enabled stream is written to a table named `<table_prefix><stream_name>` "
                f"in the destination's configured {_NAMESPACE_NOUNS[dialect]}. Query these "
                "names directly (unqualified) with `sql_select`."
            ),
        },
    ]
    for connection in connections:
        blocks.append({"type": "heading", "level": 3, "text": str(connection.name)})
        prefix_note = (
            f"Table prefix {connection.table_prefix!r}."
            if connection.table_prefix
            else "No table prefix."
        )
        blocks.extend(
            [
                {"type": "paragraph", "text": prefix_note},
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
