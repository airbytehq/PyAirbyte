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

from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from airbyte.exceptions import PyAirbyteInputError
from airbyte.registry import (
    ApiDocsUrl,  # noqa: TC001  # Needed at runtime for Pydantic field types.
)


if TYPE_CHECKING:
    from collections.abc import Mapping


class AgentWorkspaceInfo(BaseModel):
    """Summary information about a workspace, as returned by the Agents API."""

    model_config = ConfigDict(extra="allow")

    id: str
    """The workspace ID."""

    name: str | None = None
    """The workspace name."""

    organization_id: str | None = None
    """The ID of the organization the workspace belongs to."""

    status: str | None = None
    """The workspace status, for example `active`."""


class CloudDirectConnectorInfo(BaseModel):
    """Summary information about a connector, as returned by the Agents API."""

    model_config = ConfigDict(extra="allow")

    id: str
    """The connector ID."""

    name: str | None = None
    """The connector name, for example `GitHub - <workspace_id>`."""


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


class DirectAccessGuidanceInfo(BaseModel):
    """Summary of one entry of direct-access guidance, as listed by the Agents API."""

    model_config = ConfigDict(extra="allow")

    id: str
    """The skill ID. Pass it to `CloudWorkspace._read_guidance` to read the guidance."""

    kind: str | None = None
    """The skill category, for example `static` or `connector_source`."""

    title: str | None = None
    """The human-readable skill title."""

    summary: str | None = None
    """A short summary of what the skill documents."""

    tags: list[str] = Field(default_factory=list)
    """Search and categorization tags for the skill."""

    warnings: list[Any] = Field(default_factory=list)
    """Non-fatal issues reported while building or reading the skill's docs."""


class DirectAccessGuidanceList(BaseModel):
    """A page of direct-access guidance entries, as returned by the Agents API."""

    model_config = ConfigDict(extra="allow")

    data: list[DirectAccessGuidanceInfo]
    """The skills on this page."""

    next_cursor: str | None = None
    """The cursor to pass as `cursor` to fetch the next page, when one is available."""


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

    metadata: DirectAccessGuidanceInfo
    """Metadata for the requested skill."""

    outline: list[DirectAccessGuidanceSection] = Field(default_factory=list)
    """The sections available for this skill."""

    section_id: str | None = None
    """The requested section ID, or `None` for the default docs response."""

    content: list[dict[str, Any]] = Field(default_factory=list)
    """Rendered docs content blocks, such as headings, paragraphs, and code blocks."""


class CloudContextLayerConnectorDetails(BaseModel):
    """Connector metadata returned by the Agents API `inspect` endpoint."""

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
    """Skill ID to pass to `AgentWorkspace.get_skill(...).read_docs()` (MCP:
    `read_agent_skill_docs`) for this connector's usage docs."""

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

    @field_validator("connector_metadata", "execution_metadata", mode="before")
    @classmethod
    def _none_to_empty(cls, value: object) -> object:
        return {} if value is None else value

    @property
    def entities(self) -> list[dict[str, Any]]:
        """The result as a list of entities.

        Raises `PyAirbyteInputError` if the action did not return a list of entities. Use
        `result` for actions whose payload is not a list of entities.
        """
        if not isinstance(self.result, list):
            raise PyAirbyteInputError(
                message="This action did not return a list of entities.",
                guidance="Use the `result` attribute to read non-entity result payloads.",
                context={"result_type": type(self.result).__name__},
            )

        invalid_types = sorted(
            {type(entity).__name__ for entity in self.result if not isinstance(entity, dict)}
        )
        if invalid_types:
            raise PyAirbyteInputError(
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
    """The schema-level location tables land in (Snowflake schema, BigQuery dataset)."""


class CloudConnectorDocs(BaseModel):
    """Connector docs rendered for agent consumption."""

    model_config = ConfigDict(extra="allow")

    skill_id: str | None = None
    """The docs skill ID, for example `connector-source:<id>`."""

    title: str | None = None
    """The human-readable docs title."""

    content: str
    """The docs body, rendered as Markdown."""

    outline: list[DirectAccessGuidanceSection] = Field(default_factory=list)
    """The sections available in the docs."""

    section_id: str | None = None
    """The requested section ID, or `None` for the default docs response."""

    warnings: list[str] = Field(default_factory=list)
    """Non-fatal issues reported while reading or rendering the docs."""


class CloudConnectorDetails(BaseModel):
    """A description of a deployed Cloud connector, as returned by `describe()`."""

    model_config = ConfigDict(extra="allow")

    connector_id: str
    """The connector ID."""

    connector_type: Literal["source", "destination"]
    """Whether the connector is a source or a destination."""

    connector_name: str
    """The connector's display name."""

    connector_url: str
    """The connector's web URL."""

    connector_definition_id: str
    """The connector definition ID (for example, the ID for `source-postgres`)."""

    integration_name: str | None = None
    """Name of the underlying integration, for example `GitHub` or `Snowflake`."""

    external_access_enabled: bool
    """Whether AI agents can use this connector through the Airbyte Context layer."""

    search_indexing_enabled: bool
    """Whether Airbyte indexes this connector's data for fast search."""

    context_store_readiness: CloudContextStoreReadiness | None = None
    """Context Store readiness information, when reported."""

    docs_skill_id: str | None = None
    """Skill ID for this connector's direct-access docs."""

    connector_definition_name: str | None = None
    """The connector definition's display name, populated only by `with_config`."""

    config: dict[str, Any] | None = None
    """The connector configuration, populated only by `with_config`.

    Secret values are redacted by the Cloud API. Always `None` for sources, which the
    API does not expose configuration for."""

    replication_details: list[CloudConnectorConnectionInfo] | None = None
    """Connections touching this connector, populated only by `with_replication_details`."""

    direct_access_guidance: CloudConnectorDocs | None = None
    """Direct-access docs rendered as Markdown, populated only by `with_direct_access_guidance`."""

    data_replication_docs: list[ApiDocsUrl] | None = None
    """Upstream API documentation links, populated only by `with_data_replication_docs`."""

    warnings: list[str] = Field(default_factory=list)
    """Non-fatal issues encountered while describing the connector."""

    errors: list[str] = Field(default_factory=list)
    """Fatal issues encountered while describing optional connector details."""


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
