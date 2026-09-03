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

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from airbyte.exceptions import PyAirbyteInputError


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


class AgentConnectorInfo(BaseModel):
    """Summary information about a connector, as returned by the Agents API."""

    model_config = ConfigDict(extra="allow")

    id: str
    """The connector ID."""

    name: str | None = None
    """The connector name, for example `GitHub - <workspace_id>`."""


class AgentContextStoreEntity(BaseModel):
    """An entity that a connector supports caching in the Airbyte Context Store."""

    model_config = ConfigDict(extra="allow")

    entity: str
    """The entity name, for example `issues`."""

    suggested: bool | None = None
    """Whether Airbyte suggests caching this entity."""


class AgentContextStoreReadiness(BaseModel):
    """Context Store readiness information for a connector."""

    model_config = ConfigDict(extra="allow")

    supported_context_store_entities: list[AgentContextStoreEntity] = Field(default_factory=list)
    """The entities this connector can cache in the Context Store."""

    configured_cache_entities: list[dict[str, Any]] = Field(default_factory=list)
    """The entities currently configured for caching, with their sync status."""


class AgentConnectorDetails(BaseModel):
    """Connector metadata returned by the Agents API `inspect` endpoint."""

    model_config = ConfigDict(extra="allow")

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

    source_definition_name: str | None = None
    """The name of the underlying Airbyte source definition, for example `GitHub`."""

    docs_skill_id: str | None = None
    """Skill ID to pass to `read_skill_docs` for this connector's usage docs."""

    context_store_readiness: AgentContextStoreReadiness | None = None
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


class AgentExecutionMetadata(BaseModel):
    """Metadata describing how an Agents connector action was executed."""

    model_config = ConfigDict(extra="allow")

    connector_instance_id: str | None = None
    """The connector instance that served the request."""

    execution_time_ms: int | None = None
    """The server-side execution time, in milliseconds."""


class AgentConnectorMetadata(BaseModel):
    """Connector-reported metadata about a single action's result, including pagination."""

    model_config = ConfigDict(extra="allow")

    has_next_page: bool | None = None
    """Whether more entities are available after this page, when the connector reports it."""

    end_cursor: str | None = None
    """The cursor to pass as `cursor` to fetch the next page, when one is available."""


class AgentExecuteResult(BaseModel):
    """The result of executing a single action against an Airbyte Agents connector."""

    model_config = ConfigDict(extra="allow")

    status: str
    """The execution status reported by the Agents API, for example `success`."""

    result: Any = None
    """The action's payload. Entity-returning actions put a list of entities here."""

    connector_metadata: AgentConnectorMetadata = Field(default_factory=AgentConnectorMetadata)
    """Connector-reported metadata about the result, including pagination cursors."""

    execution_metadata: AgentExecutionMetadata = Field(default_factory=AgentExecutionMetadata)
    """Metadata describing how the action was executed."""

    warning: dict[str, Any] | None = None
    """A warning reported alongside an otherwise successful result."""

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
