# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Built-in docs for SQL passthrough destinations.

The Agents API may not know destination skills, so connector IDs that address Cloud
destinations (the targets of `sql_select`) can 404 on `inspect` and skill docs reads.
This module builds the equivalent `CloudContextLayerConnectorDetails`/`DirectAccessGuidance`
payloads locally from the Cloud workspace objects, merges them into server-served
destination docs so PyAirbyte's SQL guidance is not lost once the API serves them,
and summarizes the connections touching a connector for `CloudConnector.describe()`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, NamedTuple

from airbyte._direct_connectors.models import (
    _BIGQUERY_DESTINATION_DEFINITION_ID,
    _SNOWFLAKE_DESTINATION_DEFINITION_ID,
    _SQL_PASSTHROUGH_DESTINATION_DIALECTS,
    _SQL_PASSTHROUGH_DESTINATION_NAMES,
    CloudConnectorConnectionInfo,
    CloudContextLayerConnectorDetails,
    DirectAccessGuidance,
    DirectAccessGuidanceInfo,
    DirectAccessGuidanceSection,
)
from airbyte.exceptions import PyAirbyteInputError


if TYPE_CHECKING:
    from collections.abc import Mapping

    from airbyte.cloud.connections import CloudConnection
    from airbyte.cloud.connectors import CloudConnector, CloudDestination

_DESTINATION_LOCATION_KEYS: Mapping[str, tuple[tuple[str, str], tuple[str, str]]] = {
    _SNOWFLAKE_DESTINATION_DEFINITION_ID: (("database", "database"), ("schema", "schema")),
    _BIGQUERY_DESTINATION_DEFINITION_ID: (("project", "project_id"), ("dataset", "dataset_id")),
}
"""Destination definition ID -> (label, configuration key) pairs locating synced tables."""


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

LOCAL_DESTINATION_SECTION_IDS = frozenset(_SECTION_TITLES)
"""Section IDs PyAirbyte builds locally; these never hit the Agents API."""

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
            "destination settings Airbyte writes upper-cased table AND column names, so stream "
            "`users` is table `USERS` and field `primaryUserId` is column `PRIMARYUSERID`; "
            "connections using the legacy case-preserving raw-table mode keep the original "
            "case. Confirm names with `SHOW TABLES`."
        ),
        (
            "Prefer unquoted identifiers: double-quoting makes them case-sensitive, so "
            '`SELECT "id"` fails with `invalid identifier` against column `ID`. Quote a name '
            "exactly as returned by `SHOW TABLES` or the dry run only when it is mixed or lower "
            "case (legacy raw-table mode), contains spaces or special characters, or is a "
            "reserved word."
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


def build_destination_connector_details(
    destination: CloudDestination,
) -> CloudContextLayerConnectorDetails:
    """Build `CloudContextLayerConnectorDetails` for a destination the API does not know."""
    return CloudContextLayerConnectorDetails(
        connector_id=destination.connector_id,
        name=destination.name,
        workspace_id=destination.workspace.workspace_id,
        docs_skill_id=destination_skill_id(destination.connector_id),
        integration_name=_SQL_PASSTHROUGH_DESTINATION_NAMES.get(destination.definition_id),
        warnings=[],
    )


class _LocationPart(NamedTuple):
    """One engine-specific component of where a destination writes tables."""

    label: str
    """Engine term for this component, e.g. `database`, `schema`, `project`, `dataset`."""
    value: str
    """The configured name, e.g. `ANALYTICS`."""


class _DestinationLocation(NamedTuple):
    """Where a destination writes synced tables, read from its connector configuration.

    `container` is the top-level grouping (Snowflake database, BigQuery project) and
    `namespace` is the schema-level grouping that unqualified table names resolve to
    (Snowflake schema, BigQuery dataset). Either is `None` when the destination type is
    not a known SQL passthrough or the configuration omits it.
    """

    container: _LocationPart | None = None
    namespace: _LocationPart | None = None

    @property
    def parts(self) -> list[_LocationPart]:
        """The present components in container-then-namespace order, for rendering."""
        return [part for part in (self.container, self.namespace) if part is not None]


def _location_part(label: str, value: object) -> _LocationPart | None:
    """Return a `_LocationPart` for a non-empty string configuration value, else `None`."""
    return _LocationPart(label, value) if isinstance(value, str) and value else None


def _destination_location(
    definition_id: str,
    configuration: Mapping[str, Any] | None,
) -> _DestinationLocation:
    """Return where the destination writes synced tables, read from its configuration."""
    keys = _DESTINATION_LOCATION_KEYS.get(definition_id)
    if keys is None:
        return _DestinationLocation()
    config = dict(configuration or {})
    (container_label, container_key), (namespace_label, namespace_key) = keys
    return _DestinationLocation(
        container=_location_part(container_label, config.get(container_key)),
        namespace=_location_part(namespace_label, config.get(namespace_key)),
    )


def _connector_location(connector: CloudDestination) -> _DestinationLocation:
    """Return the table location for a deployed destination connector."""
    return _destination_location(connector.definition_id, connector.configuration)


def _destination_connections(destination: CloudDestination) -> list[Any]:
    return [
        connection
        for connection in destination.workspace.list_connections()
        if connection.destination_id == destination.connector_id
    ]


def build_direct_access_sql_guidance(
    destination: CloudDestination,
    *,
    section: str | None = None,
) -> DirectAccessGuidance:
    """Build `DirectAccessGuidance` for a SQL passthrough destination."""
    dialect = _SQL_PASSTHROUGH_DESTINATION_DIALECTS[destination.definition_id]
    skill_id = destination_skill_id(destination.connector_id)
    metadata = DirectAccessGuidanceInfo(
        id=skill_id,
        kind="connector_destination",
        title=f"{destination.name} (SQL passthrough destination)",
        summary=(
            f"Docs for querying the `{destination.name}` destination with "
            "`execute_agent_connector_ro` and action `sql_select`."
        ),
        tags=["destination", "sql_select"],
    )
    outline = _local_outline()

    if section is None:
        connections = _destination_connections(destination)
        location = _connector_location(destination)
        content = _overview(
            destination=destination,
            dialect=dialect,
            location=location,
        )
        content += _connections_section(
            destination=destination,
            connections=connections,
        )
        content += _streams_section(
            connections=connections,
            location=location,
            dialect=dialect,
        )
        return DirectAccessGuidance(
            metadata=metadata,
            outline=outline,
            section_id=None,
            content=content,
        )

    if section == SECTION_SQL_PASSTHROUGH:
        content = _sql_passthrough_section(
            destination=destination,
            dialect=dialect,
        )
    elif section == SECTION_CONNECTIONS:
        content = _connections_section(
            destination=destination,
            connections=_destination_connections(destination),
        )
    elif section == SECTION_STREAMS:
        content = _streams_section(
            connections=_destination_connections(destination),
            location=_connector_location(destination),
            dialect=dialect,
        )
    else:
        raise PyAirbyteInputError(
            message=f"Unknown section {section!r} for skill {skill_id!r}.",
            guidance=f"Valid sections: {', '.join(_SECTION_TITLES)}.",
        )
    return DirectAccessGuidance(
        metadata=metadata,
        outline=outline,
        section_id=section,
        content=content,
    )


def _local_outline() -> list[DirectAccessGuidanceSection]:
    """Return the outline entries for the locally built destination sections."""
    return [
        DirectAccessGuidanceSection(
            id=section_id,
            title=title,
            available=True,
        )
        for section_id, title in _SECTION_TITLES.items()
    ]


def merge_destination_skill_docs(
    server_docs: DirectAccessGuidance,
    destination: CloudDestination,
) -> DirectAccessGuidance:
    """Augment server-provided destination docs with PyAirbyte's SQL guidance.

    Local sections are appended to the outline (skipping ids the server already
    provides), and the local overview is prepended to the default (no-section) or
    `overview` content.
    """
    dialect = _SQL_PASSTHROUGH_DESTINATION_DIALECTS[destination.definition_id]
    server_section_ids = {section.id for section in server_docs.outline}
    outline = [
        *server_docs.outline,
        *(section for section in _local_outline() if section.id not in server_section_ids),
    ]
    content = server_docs.content
    if server_docs.section_id in {None, "overview"}:
        content = (
            _overview(
                destination=destination,
                dialect=dialect,
                location=_connector_location(destination),
            )
            + content
        )
    return server_docs.model_copy(update={"outline": outline, "content": content})


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
    location: _DestinationLocation,
) -> list[dict[str, Any]]:
    """Self-contained summary shown by `inspect_agent_connector`, without any Cloud lookups."""
    engine = _ENGINE_NAMES[dialect]
    if location.parts:
        location_text = ", ".join(f"{part.label} `{part.value}`" for part in location.parts)
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
                    "returned. Do not guess column names: run the dry-run step first and "
                    "select only columns it returns."
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
        {
            "type": "paragraph",
            "text": (
                "Discover columns without reading rows (`dry_run`). Do not guess column names: "
                "run the dry-run step first and select only columns it returns:"
            ),
        },
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


def _qualified_table_example(destination: CloudDestination) -> str:
    """Return an example `SELECT` qualifying the table with the destination's namespace."""
    namespace_part = _connector_location(destination).namespace
    if namespace_part is None:
        return "SELECT * FROM <table> LIMIT 10"
    namespace = namespace_part.value
    if destination.definition_id == _BIGQUERY_DESTINATION_DEFINITION_ID:
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


def _stream_rows(connection: CloudConnection, dialect: str) -> list[list[str]]:
    """Return (stream name, table name) rows for a connection's enabled streams."""
    return [
        [
            stream_name,
            f"`{_table_name(
                dialect=dialect,
                table_prefix=connection.table_prefix,
                stream_name=stream_name,
            )}`",
        ]
        for stream_name in connection.stream_names
    ]


def _connection_namespace_note(
    connection: CloudConnection,
    location: _DestinationLocation,
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
        if location.namespace is not None:
            note += f", {location.namespace.label} `{location.namespace.value}`"
    return note + prefix_clause


def _streams_section(
    connections: list[Any],
    location: _DestinationLocation,
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
                    "text": _connection_namespace_note(
                        connection=connection,
                        location=location,
                    ),
                },
                {
                    "type": "table",
                    "headers": ["Stream", "Table"],
                    "rows": _stream_rows(connection=connection, dialect=dialect),
                },
            ]
        )
    return blocks


def build_connection_infos(connector: CloudConnector) -> list[CloudConnectorConnectionInfo]:
    """Summarize each connection that reads from or writes to `connector`.

    Counterpart connector names are resolved with one `list_sources()` and one
    `list_destinations()` call, and the destination's database/schema location is read
    from its configuration.
    """
    workspace = connector.workspace
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
        location = (
            _destination_location(destination.definition_id, destination.configuration)
            if destination is not None
            else _DestinationLocation()
        )
        database = location.container.value if location.container else None
        namespace_part = location.namespace
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
                schedule=connection.schedule_description,
                stream_names=list(connection.stream_names),
                namespace_definition=connection.namespace_definition,
                namespace_format=connection.namespace_format,
                table_prefix=connection.table_prefix,
                destination_database=database,
                destination_schema=namespace_part.value if namespace_part else None,
            )
        )
    return infos
