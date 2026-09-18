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
from airbyte.cloud.models import (
    BIGQUERY_DESTINATION_DEFINITION_ID,
    SNOWFLAKE_DESTINATION_DEFINITION_ID,
    SQL_PASSTHROUGH_DESTINATION_DIALECTS,
    SQL_PASSTHROUGH_DESTINATION_NAMES,
)
from airbyte.exceptions import PyAirbyteInputError


if TYPE_CHECKING:
    from collections.abc import Mapping

    from airbyte.cloud.connections import CloudConnection
    from airbyte.cloud.connectors import CloudDestination

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
        text = (
            f"`{destination.name}` is an Airbyte Cloud destination, reachable via "
            '`execute_agent_connector_ro` with `action="sql_select"` and '
            f'`"sql_dialect": "{dialect}"` in `api_args`. '
        )
        if location:
            location_text = ", ".join(f"{label} `{value}`" for label, value in location)
            text += (
                f"Tables land in {location_text} unless a connection overrides the " "namespace. "
            )
        text += (
            "Read the `sql-passthrough` section for query syntax. The connections and "
            "enabled streams below describe what data lands here; re-read the "
            "`connections` or `streams` section to refresh them."
        )
        content: list[dict[str, Any]] = [{"type": "paragraph", "text": text}]
        content += _connections_section(destination, connections)
        content += _streams_section(connections, location)
        return AgentSkillDocs(metadata=metadata, outline=outline, section_id=None, content=content)

    if section == SECTION_SQL_PASSTHROUGH:
        content = _sql_passthrough_section(destination, dialect)
    elif section == SECTION_CONNECTIONS:
        content = _connections_section(destination, _destination_connections(destination))
    elif section == SECTION_STREAMS:
        content = _streams_section(
            _destination_connections(destination), _destination_location(destination)
        )
    else:
        raise PyAirbyteInputError(
            message=f"Unknown section {section!r} for skill {skill_id!r}.",
            guidance=f"Valid sections: {', '.join(_SECTION_TITLES)}.",
        )
    return AgentSkillDocs(metadata=metadata, outline=outline, section_id=section, content=content)


def _sql_passthrough_section(destination: CloudDestination, dialect: str) -> list[dict[str, Any]]:
    connector_id = destination.connector_id
    return [
        {"type": "heading", "level": 2, "text": "Query the destination with sql_select"},
        {
            "type": "paragraph",
            "text": (
                "This destination accepts one read-only SQL statement per call via "
                '`execute_agent_connector_ro` with `entity_type="tables"`, '
                '`action="sql_select"`, and `api_args` containing `"sql"` and '
                f'`"sql_dialect": "{dialect}"`. Results are returned as JSON rows.'
            ),
        },
        {
            "type": "paragraph",
            "text": "List tables in the destination:",
        },
        {
            "type": "code",
            "language": "python",
            "code": (
                "execute_agent_connector_ro(\n"
                f'    connector_id="{connector_id}",\n'
                '    entity_type="tables",\n'
                '    action="sql_select",\n'
                f'    api_args={{"sql": "SHOW TABLES", "sql_dialect": "{dialect}"}},\n'
                ")"
            ),
        },
        {
            "type": "paragraph",
            "text": "Read rows from a table:",
        },
        {
            "type": "code",
            "language": "sql",
            "code": _qualified_table_example(destination),
        },
        {
            "type": "paragraph",
            "text": (
                "Send a single read-only statement per call and add a `LIMIT` clause to keep "
                "result payloads small."
            ),
        },
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
    ]
    for connection in connections:
        blocks.append({"type": "heading", "level": 3, "text": str(connection.name)})
        blocks.extend(
            [
                {
                    "type": "paragraph",
                    "text": _connection_namespace_note(connection, location),
                },
                {"type": "list", "items": list(connection.stream_names)},
            ]
        )
    return blocks
