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
from airbyte.cloud.models import SQL_PASSTHROUGH_DESTINATION_DIALECTS
from airbyte.exceptions import PyAirbyteInputError


if TYPE_CHECKING:
    from collections.abc import Mapping

    from airbyte.cloud.connectors import CloudDestination


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
        content: list[dict[str, Any]] = [
            {
                "type": "paragraph",
                "text": (
                    f"`{destination.name}` is an Airbyte Cloud destination, reachable via "
                    '`execute_agent_connector_ro` with `action="sql_select"` and '
                    f'`"sql_dialect": "{dialect}"` in `api_args`. Read the `sql-passthrough` '
                    "section first; `connections` and `streams` describe what data lands here."
                ),
            }
        ]
        return AgentSkillDocs(metadata=metadata, outline=outline, section_id=None, content=content)

    if section == SECTION_SQL_PASSTHROUGH:
        content = _sql_passthrough_section(destination, dialect)
    elif section == SECTION_CONNECTIONS:
        content = _connections_section(destination)
    elif section == SECTION_STREAMS:
        content = _streams_section(destination)
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
            "code": "SELECT * FROM <table> LIMIT 10",
        },
        {
            "type": "paragraph",
            "text": (
                "Send a single read-only statement per call and add a `LIMIT` clause to keep "
                "result payloads small."
            ),
        },
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


def _streams_section(destination: CloudDestination) -> list[dict[str, Any]]:
    connections = _destination_connections(destination)
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
        prefix_note = (
            f"Sync writes these streams with table prefix {connection.table_prefix!r}."
            if connection.table_prefix
            else "Sync writes these streams with no table prefix."
        )
        blocks.extend(
            [
                {"type": "paragraph", "text": prefix_note},
                {"type": "list", "items": list(connection.stream_names)},
            ]
        )
    return blocks
