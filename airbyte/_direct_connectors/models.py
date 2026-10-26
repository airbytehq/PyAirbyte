# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Response models for the Airbyte Agents API.

> ## ⚠️ Experimental Interface
>
> **The Airbyte Agents Python interfaces are experimental.** Class names, method signatures,
> and result models may change or be removed without notice between minor versions of
> PyAirbyte. Pin an exact PyAirbyte version if you depend on them.

All models allow extra fields, because the Agents API returns rich connector-specific
payloads that PyAirbyte deliberately does not attempt to model exhaustively.
"""

from __future__ import annotations

from datetime import datetime  # noqa: TC003  # Needed at runtime for Pydantic field types.
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from airbyte._util.compat import StrEnum
from airbyte.exceptions import AirbyteLibInputError


if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence


class CloudContextStoreEntity(BaseModel):
    """An entity that a connector supports caching in the Airbyte Context Store."""

    model_config = ConfigDict(extra="allow")

    entity: str
    """The entity name, for example `issues`."""

    suggested: bool | None = None
    """Whether Airbyte suggests caching this entity."""


class CloudContextStoreReadiness(BaseModel):
    """Context Store readiness information for a connector."""

    model_config = ConfigDict(extra="allow")

    supported_context_store_entities: list[CloudContextStoreEntity] = Field(default_factory=list)
    """The entities this connector can cache in the Context Store."""

    configured_cache_entities: list[dict[str, Any]] = Field(default_factory=list)
    """The entities currently configured for caching, with their sync status."""


class DirectAccessGuidanceIndexEntry(BaseModel):
    """One entry in the direct-access guidance index.

    The catalog record advertising a guidance doc (`id`, `kind`, `title`, `summary`,
    `tags`) so a caller can pick one and read it by `id`. The same record is returned as
    `DirectAccessGuidance.metadata`.
    """

    model_config = ConfigDict(extra="allow")

    id: str
    """The skill ID. Pass it to `CloudWorkspace.get_agent_skill_docs` to read it."""

    kind: str | None = None
    """The guidance category, for example `static` or `connector_source`."""

    title: str | None = None
    """The human-readable guidance title."""

    summary: str | None = None
    """A short summary of what the guidance documents."""

    tags: list[str] = Field(default_factory=list)
    """Search and categorization tags for the guidance."""

    warnings: list[Any] = Field(default_factory=list)
    """Non-fatal issues reported while building or reading the guidance's docs."""


class DirectAccessGuidanceSection(BaseModel):
    """A section of direct-access guidance, as listed in the guidance outline."""

    model_config = ConfigDict(extra="allow")

    id: str
    """The section ID. Pass it as `section` to read this section."""

    title: str | None = None
    """The human-readable section title."""

    summary: str | None = None
    """A short summary of the section content."""

    available: bool = True
    """Whether this section can currently be read."""


class DirectAccessGuidance(BaseModel):
    """Direct-access guidance for a connector: static docs plus dynamic context."""

    model_config = ConfigDict(extra="allow")

    metadata: DirectAccessGuidanceIndexEntry
    """The index entry for the requested guidance."""

    outline: list[DirectAccessGuidanceSection] = Field(default_factory=list)
    """The sections available for this guidance."""

    section_id: str | None = None
    """The requested section ID, or `None` for the default docs response."""

    content: list[dict[str, Any]] = Field(default_factory=list)
    """Rendered docs content blocks, such as headings, paragraphs, and code blocks."""


class _DirectConnectorInspectResult(BaseModel):
    """Context layer details for a direct connector, built from its docs skill."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    connector_id: str
    """The connector ID."""

    name: str | None = None
    """The connector name."""

    workspace_id: str | None = None
    """The ID of the workspace the connector belongs to."""

    organization_id: str | None = None
    """The ID of the organization the connector belongs to."""

    source_definition_id: str | None = None
    """The ID of the underlying Airbyte source definition."""

    integration_name: str | None = Field(default=None, alias="source_definition_name")
    """Name of the underlying integration, for example `GitHub` or `Snowflake`."""

    docs_skill_id: str | None = None
    """Skill ID to pass to `CloudWorkspace.get_agent_skill_docs(...)` for this
    connector's usage docs."""

    context_store_readiness: CloudContextStoreReadiness | None = None
    """Context Store readiness information, when reported."""

    warnings: list[Any] = Field(default_factory=list)
    """Warnings reported by the Agents API, for example degraded capabilities."""

    @property
    def context_store_entities(self) -> list[str]:
        """The entity names this connector can cache in the Context Store.

        Note that this lists Context Store-supported entities specifically. The Agents API
        does not publish an exhaustive list of executable entity and action pairs, so an
        entity may be executable via `AgentConnector.execute()` without appearing here.
        """
        if self.context_store_readiness is None:
            return []
        return [
            entity.entity
            for entity in self.context_store_readiness.supported_context_store_entities
        ]


class ExternalApiReadOnlyAction(StrEnum):
    """Read actions accepted by `CloudConnector.execute_api_query`."""

    LIST = "list"
    GET = "get"
    SEARCH = "search"


class ExternalApiWriteAction(StrEnum):
    """Write actions accepted by `CloudConnector.execute_api_action`."""

    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


class ExternalApiExecutionMetadata(BaseModel):
    """Metadata describing how an Agents connector action was executed."""

    model_config = ConfigDict(extra="allow")

    connector_instance_id: str | None = None
    """The connector instance that served the request."""

    execution_time_ms: int | None = None
    """The server-side execution time, in milliseconds."""


class ExternalApiConnectorMetadata(BaseModel):
    """Connector-reported metadata about a single action's result, including pagination."""

    model_config = ConfigDict(extra="allow")

    has_next_page: bool | None = None
    """Whether more entities are available after this page, when the connector reports it."""

    end_cursor: str | None = None
    """The cursor for the next page, when one is available. Pass it as `cursor` for Context Store
    `search`, or as the connector's own cursor argument in `api_args` for direct connector
    actions."""


class ExternalApiExecuteResult(BaseModel):
    """The result of executing a single action against an Airbyte Agents connector."""

    model_config = ConfigDict(extra="allow")

    status: str
    """The execution status reported by the Agents API, for example `success`."""

    result: Any = None
    """The action's payload. Entity-returning actions put a list of entities here."""

    connector_metadata: ExternalApiConnectorMetadata = Field(
        default_factory=ExternalApiConnectorMetadata
    )
    """Connector-reported metadata about the result, including pagination cursors."""

    execution_metadata: ExternalApiExecutionMetadata = Field(
        default_factory=ExternalApiExecutionMetadata
    )
    """Metadata describing how the action was executed."""

    warning: dict[str, Any] | None = None
    """A warning reported alongside an otherwise successful result."""

    errors: list[str] = Field(default_factory=list)
    """Agent-facing error messages; when non-empty, show them to the user."""

    @field_validator("connector_metadata", "execution_metadata", mode="before")
    @classmethod
    def _none_to_empty(cls, value: object) -> object:
        return {} if value is None else value

    @property
    def entities(self) -> list[dict[str, Any]]:
        """The result as a list of entities.

        Raises `AirbyteLibInputError` if the action did not return a list of entities. Use
        `result` for actions whose payload is not a list of entities.
        """
        if not isinstance(self.result, list):
            raise AirbyteLibInputError(
                message="This action did not return a list of entities.",
                guidance="Use the `result` attribute to read non-entity result payloads.",
                context={"result_type": type(self.result).__name__},
            )

        invalid_types = sorted(
            {type(entity).__name__ for entity in self.result if not isinstance(entity, dict)}
        )
        if invalid_types:
            raise AirbyteLibInputError(
                message="This action returned a list that is not a list of entities.",
                guidance="Use the `result` attribute to read non-entity result payloads.",
                context={"unexpected_item_types": invalid_types},
            )
        return self.result

    @property
    def has_next_page(self) -> bool:
        """Whether the connector reported more entities after this page."""
        return bool(self.connector_metadata.has_next_page)

    @property
    def end_cursor(self) -> str | None:
        """The cursor for the next page, or `None` when there is no next page."""
        return self.connector_metadata.end_cursor


class ExternalSearchType(StrEnum):
    """Search types accepted by `CloudConnector.execute_search_query`."""

    KEYWORD = "keyword"
    SEMANTIC = "semantic"
    HYBRID = "hybrid"


class ExternalSearchStreamFilter(BaseModel):
    """A stream to search, with an optional projection of the returned entity fields."""

    model_config = ConfigDict(extra="allow")

    stream_name: str
    """The stream to search."""

    source_id: str | None = None
    """The source the stream belongs to.

    Required on a destination search. May be omitted on a source search, where it must
    match the searched source if given.
    """

    namespace: str | None = None
    """The stream namespace, when the stream has one."""

    fields: list[str] | None = None
    """Fields of `entity_data` to return. An output projection, not a match filter."""


class ExternalSearchHit(BaseModel):
    """A single entity matched by a search."""

    model_config = ConfigDict(extra="allow")

    source_id: str
    """The source the matched entity was synced from."""

    stream_name: str
    """The stream the matched entity belongs to."""

    entity_id: str
    """The matched entity's ID."""

    entity_data: dict[str, Any] = Field(default_factory=dict)
    """The matched entity's data, projected to the requested `fields` when given.

    Null values are not stored, so an absent key means null or unset. A keyword-only hit
    also omits any field longer than 4096 characters or split across keyword chunks; its
    matched text is in `context`.
    """

    score: float
    """The match score. Higher is a better match."""

    context: str | None = None
    """The matched text: the semantic match, or for a keyword-only hit the matched chunk
    centered on the first query term, with `…` where text was left out. Truncated to
    `max_context_chars` when requested."""

    @field_validator("entity_id", mode="before")
    @classmethod
    def _numeric_entity_id_to_str(cls, value: object) -> object:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
        return value


class ExternalSearchIndexMetadata(BaseModel):
    """Per-index details about how a search ran."""

    model_config = ConfigDict(extra="allow")

    source_id: str | None = None
    """The source whose index was searched."""

    destination_id: str | None = None
    """The destination the searched data syncs to."""

    namespace: str | None = None
    """The searched stream's namespace, when it has one."""

    stream_name: str | None = None
    """The searched stream."""

    index_name: str | None = None
    """The searched index."""

    error: str | None = None
    """The error that stopped this index from being searched, if any."""

    search_time_ms: int | None = None
    """The time spent searching this index, in milliseconds."""


class ExternalSearchResult(BaseModel):
    """The result of searching a connector's indexed data."""

    model_config = ConfigDict(extra="allow")

    hits: list[ExternalSearchHit]
    """The matched entities, best match first."""

    metadata: list[ExternalSearchIndexMetadata]
    """Per-index details about how the search ran."""

    response_time_ms: int | None = None
    """The server-side response time, in milliseconds."""

    warnings: list[str] = Field(default_factory=list)
    """Server warnings, plus per-index search errors collected from `metadata` so partial
    failures stand out, and a notice when no index was searched at all."""

    errors: list[str] = Field(default_factory=list)
    """Errors that stopped the search, for example search not being enabled."""

    @field_validator("warnings", mode="before")
    @classmethod
    def _none_to_empty(cls, value: object) -> object:
        return [] if value is None else value

    @model_validator(mode="after")
    def _collect_index_errors(self) -> ExternalSearchResult:
        collected = [
            f"Search of stream {entry.stream_name!r} (index {entry.index_name!r}) "
            f"failed: {entry.error}"
            for entry in self.metadata
            if entry.error
        ]
        if not self.metadata and not self.errors:
            collected.append(
                "No index matched this search type and stream selection, so empty results "
                "do not mean there are no matches. Call `get_cloud_search_status` to see "
                "which streams have which index types."
            )
        self.warnings = [
            *self.warnings,
            *(warning for warning in collected if warning not in self.warnings),
        ]
        return self


class ExternalSearchBackfillStatus(BaseModel):
    """Progress of the latest backfill run for one stream."""

    model_config = ConfigDict(extra="allow")

    status: str
    """The backfill status: `starting` until a backfiller picks the run up, then `running`
    and `finalizing`, and finally `complete`, `failed`, or `cancelled`."""

    backfill_start_time: datetime | None = None
    """The run's start time: rows whose cursor field is earlier were skipped. `None`
    backfills the whole history."""

    expected_total_records: int | None = None
    """The number of records the backfill expects to collect."""

    collected_records: int | None = None
    """The number of records collected so far."""

    indexed_records: int | None = None
    """The number of records indexed so far."""

    started_at: datetime | None = None
    """When the backfill started."""

    completed_at: datetime | None = None
    """When the backfill completed."""

    failed_at: datetime | None = None
    """When the backfill failed."""

    cancelled_at: datetime | None = None
    """When the backfill was cancelled."""


class ExternalSearchIndexStatus(BaseModel):
    """The status of one search index for a stream."""

    model_config = ConfigDict(extra="allow")

    type: str
    """The index type, for example `keyword` or `semantic`."""

    status: str
    """The index status, for example `updating` or `idle`."""

    name: str | None = None
    """The index name."""

    indexed_records: int | None = None
    """The number of records indexed."""

    physical_rows: int | None = None
    """The number of physical rows stored in the index."""

    optimized_physical_rows: int | None = None
    """The number of physical rows already optimized."""

    updated_at: datetime | None = None
    """When the index was last updated."""

    storage_size_mb: float | None = None
    """The index storage size, in megabytes."""


class ExternalSearchStreamStatus(BaseModel):
    """Search indexing status for one stream."""

    model_config = ConfigDict(extra="allow")

    name: str
    """The stream name."""

    namespace: str | None = None
    """The stream namespace, when it has one."""

    backfill: ExternalSearchBackfillStatus | None = None
    """The initial indexing backfill, when one is tracked."""

    indexes: list[ExternalSearchIndexStatus] = Field(default_factory=list)
    """The stream's search indexes."""


class ExternalSearchSourceStatus(BaseModel):
    """Search indexing status for one indexed source."""

    model_config = ConfigDict(extra="allow")

    source_id: str
    """The indexed source."""

    destination_id: str | None = None
    """The destination the indexed data syncs to."""

    connection_id: str | None = None
    """The connection syncing the indexed data."""

    streams: list[ExternalSearchStreamStatus] = Field(default_factory=list)
    """The source's indexed streams."""


class ExternalSearchStatusResult(BaseModel):
    """Search indexing status for a connector."""

    model_config = ConfigDict(extra="allow")

    sources: list[ExternalSearchSourceStatus]
    """The indexed sources: the connector itself for a source, or every indexed source
    synced to a destination."""

    warnings: list[str] = Field(default_factory=list)
    """Non-fatal issues, for example a stream filter that matched no indexed stream."""

    errors: list[str] = Field(default_factory=list)
    """Errors that stopped the status check, for example search not being enabled."""

    @field_validator("warnings", mode="before")
    @classmethod
    def _none_to_empty(cls, value: object) -> object:
        return [] if value is None else value


class ExternalSearchBackfillRun(BaseModel):
    """One stream's backfill run, as reported by a backfill request."""

    model_config = ConfigDict(extra="allow")

    id: str
    """The stream's backfill ID, kept across runs."""

    stream_name: str
    """The stream, or empty for the run that stands for every stream."""

    source_id: str
    """The connection's source."""

    workspace_id: str
    """The connection's workspace."""

    connection_id: str | None = None
    """The connection being backfilled."""

    destination_id: str | None = None
    """The destination the backfill reads from."""

    requested_at: datetime | None = None
    """When the run was requested."""

    started_at: datetime | None = None
    """When a backfiller started the run."""

    completed_at: datetime | None = None
    """When the run completed."""

    failed_at: datetime | None = None
    """When the run failed."""

    cancel_requested_at: datetime | None = None
    """When cancelling the run was requested."""

    cancelled_at: datetime | None = None
    """When the run was cancelled."""

    expected_total_rows: int | None = None
    """The number of rows the run expects to read, once counted."""

    error: str | None = None
    """The failure code of a failed run, for example `enqueue_failed`."""


class ExternalSearchBackfillRequested(BaseModel):
    """The outcome of a backfill request for one stream."""

    model_config = ConfigDict(extra="allow")

    stream_name: str
    """The stream. Empty when the request named no streams: that one run stands for every
    stream selected in the connection, and is split into per-stream runs when it starts."""

    outcome: str
    """`requested` when a new run was queued, `running` when the stream's current run is
    still queued or running and was kept, or `failed` when the run could not be queued."""

    run_id: str | None = None
    """The run's ID."""

    backfill: ExternalSearchBackfillRun
    """The stream's backfill run."""


class ExternalSearchBackfillRequestResult(BaseModel):
    """The result of requesting a search backfill for a connection."""

    model_config = ConfigDict(extra="allow")

    streams: list[ExternalSearchBackfillRequested]
    """Each requested stream's outcome."""


class ConnectorEnablement(BaseModel):
    """The Fusion features enabled for a deployed Cloud source or destination."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    enable_agent_access: bool
    """Whether AI agents can access the connector through the Context layer."""

    enable_indexing: bool
    """Whether Airbyte indexes the connector's data for search. Requires agent access."""

    organization_id: str | None = Field(default=None, alias="organizationId")
    """The ID of the organization the connector belongs to."""

    workspace_id: str | None = Field(default=None, alias="workspaceId")
    """The ID of the workspace the connector belongs to."""

    actor_id: str | None = Field(default=None, alias="actorId")
    """The connector ID."""

    actor_type: str | None = Field(default=None, alias="actorType")
    """The connector kind, `source` or `destination`."""

    enable_backfill: bool | None = None
    """Destinations only: whether indexing backfills previously synced data."""

    backfill_start_time: datetime | None = None
    """Destinations only: the start of the backfilled time range, when set."""


class CloudConnectorConnectionInfo(BaseModel):
    """Summary of a single connection touching a connector."""

    model_config = ConfigDict(extra="allow")

    connection_id: str
    """The connection ID."""

    name: str
    """The connection name."""

    source_id: str
    """The source connector ID."""

    source_name: str
    """The source connector name."""

    destination_id: str
    """The destination connector ID."""

    destination_name: str
    """The destination connector name."""

    schedule: str | None = None
    """The sync schedule: `manual`, a cron expression, or `every <units> <time_unit>`."""

    stream_names: list[str] = Field(default_factory=list)
    """The streams enabled on the connection."""

    namespace_definition: str | None = None
    """How destination namespaces are chosen: `source`, `destination`, or `custom_format`."""

    namespace_format: str | None = None
    """The namespace format template, when `namespace_definition` is `custom_format`."""

    table_prefix: str = ""
    """The destination table prefix."""

    destination_database: str | None = None
    """The database-level location tables land in (Snowflake database, BigQuery project)."""

    destination_schema: str | None = None
    """The schema-level location tables land in, resolved from the destination config and
    the connection's namespace setting (Snowflake schema, BigQuery dataset)."""


_SNOWFLAKE_DESTINATION_DEFINITION_ID = "424892c4-daac-4491-b35d-c6688ba547ba"
_BIGQUERY_DESTINATION_DEFINITION_ID = "22f6c74f-5699-40ff-833c-4a879ea40133"

_SQL_PASSTHROUGH_DESTINATION_DIALECTS: Mapping[str, str] = {
    _SNOWFLAKE_DESTINATION_DEFINITION_ID: "snowflake",
    _BIGQUERY_DESTINATION_DEFINITION_ID: "bigquery",
}
"""Destination definition ID -> `sql_dialect` value accepted by the `sql_select` action."""

_SQL_PASSTHROUGH_DESTINATION_NAMES: Mapping[str, str] = {
    _SNOWFLAKE_DESTINATION_DEFINITION_ID: "Snowflake",
    _BIGQUERY_DESTINATION_DEFINITION_ID: "BigQuery",
}
"""Destination definition ID -> display name of the destination integration."""

_SQL_PASSTHROUGH_DESTINATION_DEFINITION_IDS = frozenset(_SQL_PASSTHROUGH_DESTINATION_DIALECTS)

# Referenced here so unused-global linters (CodeQL) don't flag these package-private
# constants, which are consumed from sibling modules.
_ = (
    _SQL_PASSTHROUGH_DESTINATION_DIALECTS,
    _SQL_PASSTHROUGH_DESTINATION_NAMES,
    _SQL_PASSTHROUGH_DESTINATION_DEFINITION_IDS,
)
"""Destination definitions AI agents can query through SQL passthrough."""


# Structural typing helpers. The `airbyte.cloud` classes conform to these protocols,
# letting this package type its helpers without importing `airbyte.cloud.*` (which
# would create an import cycle, as `airbyte.cloud` imports this package).


class _EnumValueLike(Protocol):
    """Any enum member exposing a string `value`."""

    @property
    def value(self) -> str:
        """The enum member's string value."""
        raise NotImplementedError


class _ScheduleLike(Protocol):
    """The schedule object `connector_docs` reads off a connection."""

    @property
    def friendly_description(self) -> str:
        """A human-readable schedule description."""
        raise NotImplementedError


class _ConnectionLike(Protocol):
    """Attributes `connector_docs` reads off `airbyte.cloud.connections.CloudConnection`."""

    connection_id: str

    @property
    def name(self) -> str | None:
        """The connection's display name."""
        raise NotImplementedError

    @property
    def source_id(self) -> str:
        """The source connector ID."""
        raise NotImplementedError

    @property
    def destination_id(self) -> str:
        """The destination connector ID."""
        raise NotImplementedError

    @property
    def stream_names(self) -> list[str]:
        """The enabled stream names."""
        raise NotImplementedError

    @property
    def table_prefix(self) -> str:
        """The prefix applied to synced table names."""
        raise NotImplementedError

    @property
    def namespace_definition(self) -> str | None:
        """The connection's namespace definition mode."""
        raise NotImplementedError

    @property
    def namespace_format(self) -> str | None:
        """The connection's custom namespace format."""
        raise NotImplementedError

    @property
    def schedule(self) -> _ScheduleLike | None:
        """The connection's sync schedule, when known."""
        raise NotImplementedError


class _ConnectorLike(Protocol):
    """Attributes `connector_docs` reads off `airbyte.cloud.connectors.CloudConnector`."""

    connector_id: str
    # Typed `Any` here: `CloudConnector.workspace` is a read-write attribute, so an
    # invariance check would reject the `CloudWorkspace`/`_WorkspaceLike` pairing.
    # Functions in `connector_docs` re-narrow it to `_WorkspaceLike` locally.
    workspace: Any

    @property
    def name(self) -> str | None:
        """The connector's display name."""
        raise NotImplementedError

    @property
    def connector_type(self) -> _EnumValueLike:
        """The connector type (`source` or `destination`)."""
        raise NotImplementedError


class _DestinationLike(_ConnectorLike, Protocol):
    """`airbyte.cloud.connectors.CloudDestination` additions used by `connector_docs`."""

    @property
    def definition_id(self) -> str:
        """The connector definition ID."""
        raise NotImplementedError

    @property
    def configuration(self) -> dict[str, Any] | None:
        """The destination configuration, secrets redacted."""
        raise NotImplementedError


class _WorkspaceLike(Protocol):
    """Attributes `connector_docs` reads off `airbyte.cloud.workspaces.CloudWorkspace`."""

    workspace_id: str

    def list_connections(
        self,
        name: str | None = None,
        *,
        name_filter: Callable | None = None,
        limit: int | None = None,
    ) -> Sequence[_ConnectionLike]:
        """List the workspace's connections."""
        raise NotImplementedError

    def list_sources(
        self,
        name: str | None = None,
        *,
        name_filter: Callable | None = None,
        limit: int | None = None,
    ) -> Sequence[_ConnectorLike]:
        """List the workspace's source connectors."""
        raise NotImplementedError

    def list_destinations(
        self,
        name: str | None = None,
        *,
        name_filter: Callable | None = None,
        limit: int | None = None,
    ) -> Sequence[_DestinationLike]:
        """List the workspace's destination connectors."""
        raise NotImplementedError


# Referenced here so unused-global linters don't flag `_WorkspaceLike`, which is
# consumed from `connector_docs`.
_ = _WorkspaceLike
