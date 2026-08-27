# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""PyAirbyte classes and methods for executing Airbyte Agents connector actions.

Airbyte Agents connectors run single actions on demand (for example, listing GitHub
repositories) instead of running a full sync. Actions are served by the Airbyte Agents API,
which accepts the same Airbyte Cloud credentials used elsewhere in the `airbyte.cloud`
module, as long as the organization has an Airbyte Agents subscription.

## Usage Example

```python
from airbyte import cloud

workspace = cloud.CloudWorkspace.from_env()

connector = workspace.get_agent_connector(name="GitHub")
print(connector.inspect().entities)

result = connector.execute(
    entity="repositories",
    action="list",
    params={"per_page": 10},
    select_fields=["full_name"],
)
for record in result.records:
    print(record)
```
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from airbyte._util import agents_api_util
from airbyte.exceptions import (
    AirbyteMissingResourceError,
    AirbyteMultipleResourcesError,
    PyAirbyteInputError,
)


if TYPE_CHECKING:
    from airbyte.cloud._credentials import _AirbyteCredentials


class AgentConnectorInfo(BaseModel):
    """Summary information about a connector configured in an Airbyte Agents workspace."""

    model_config = ConfigDict(extra="ignore")

    connector_id: str
    """The connector ID, as configured in the workspace."""

    name: str | None = None
    """The connector's display name, as configured in the workspace."""

    connector_type: str | None = None
    """The connector type (for example, `github`)."""

    definition_id: str | None = None
    """The connector definition ID (also known as the actor definition ID)."""

    @classmethod
    def from_api_response(cls, connector: dict[str, Any]) -> AgentConnectorInfo:
        """Create a public model from an Agents API connector list entry."""
        template = connector.get("summarized_source_template") or {}
        return cls(
            connector_id=str(connector["id"]),
            name=connector.get("name"),
            connector_type=template.get("connector_name"),
            definition_id=template.get("actor_definition_id"),
        )


class AgentEntityInfo(BaseModel):
    """An entity supported by an Airbyte Agents connector."""

    model_config = ConfigDict(extra="ignore")

    entity: str
    """The entity name, as accepted by the `entity` argument of an execute call."""

    suggested: bool = False
    """Whether Airbyte suggests this entity for Context Store caching."""


class AgentConnectorDetails(BaseModel):
    """Detailed metadata for a connector configured in an Airbyte Agents workspace."""

    model_config = ConfigDict(extra="ignore")

    connector_id: str
    """The connector ID."""

    name: str | None = None
    """The connector's display name."""

    workspace_id: str | None = None
    """The workspace the connector belongs to."""

    organization_id: str | None = None
    """The organization the connector belongs to."""

    definition_name: str | None = None
    """The connector definition name (for example, `GitHub`)."""

    entities: list[AgentEntityInfo] = Field(default_factory=list)
    """Entities the connector supports."""

    @classmethod
    def from_api_response(cls, payload: dict[str, Any]) -> AgentConnectorDetails:
        """Create a public model from an Agents API `inspect` response."""
        readiness = payload.get("context_store_readiness") or {}
        supported = readiness.get("supported_context_store_entities") or []
        return cls(
            connector_id=str(payload["connector_id"]),
            name=payload.get("name"),
            workspace_id=payload.get("workspace_id"),
            organization_id=payload.get("organization_id"),
            definition_name=payload.get("source_definition_name"),
            entities=[AgentEntityInfo.model_validate(entry) for entry in supported],
        )


class AgentExecutionMetadata(BaseModel):
    """Metadata describing how an Airbyte Agents action was executed."""

    model_config = ConfigDict(extra="ignore")

    connector_instance_id: str | None = None
    """The connector instance that served the action."""

    execution_time_ms: int | None = None
    """Wall-clock execution time, in milliseconds."""


class AgentConnectorMetadata(BaseModel):
    """Connector-reported pagination metadata for an Airbyte Agents action."""

    model_config = ConfigDict(extra="ignore")

    has_next_page: bool | None = None
    """Whether the connector reports more pages of results."""

    end_cursor: str | None = None
    """The cursor to pass back to the connector to fetch the next page, if any."""


class AgentExecuteResult(BaseModel):
    """The result of executing an action on an Airbyte Agents connector."""

    model_config = ConfigDict(extra="ignore")

    status: str
    """The execution status reported by Airbyte, for example `success`."""

    result: Any = None
    """The action's result payload.

    The shape is connector- and action-specific: `list` actions return a list of records,
    while other actions may return a single object or a scalar.
    """

    connector_metadata: AgentConnectorMetadata = Field(default_factory=AgentConnectorMetadata)
    """Connector-reported pagination metadata."""

    execution_metadata: AgentExecutionMetadata = Field(default_factory=AgentExecutionMetadata)
    """Metadata describing how the action was executed."""

    warning: dict[str, Any] | None = None
    """A warning returned by Airbyte, if any."""

    @classmethod
    def from_api_response(cls, payload: dict[str, Any]) -> AgentExecuteResult:
        """Create a public model from an Agents API `execute` response."""
        return cls(
            status=str(payload.get("status", "")),
            result=payload.get("result"),
            connector_metadata=AgentConnectorMetadata.model_validate(
                payload.get("connector_metadata") or {}
            ),
            execution_metadata=AgentExecutionMetadata.model_validate(
                payload.get("execution_metadata") or {}
            ),
            warning=payload.get("warning"),
        )

    @property
    def records(self) -> list[dict[str, Any]]:
        """The result payload as a list of records.

        Raises:
            PyAirbyteInputError: If the action did not return a list of records.
        """
        if isinstance(self.result, list):
            return [record for record in self.result if isinstance(record, dict)]

        raise PyAirbyteInputError(
            message="This action did not return a list of records.",
            guidance="Use the `result` attribute to read non-record result payloads.",
            context={"result_type": type(self.result).__name__},
        )

    @property
    def has_next_page(self) -> bool:
        """Whether the connector reports more pages of results."""
        return bool(self.connector_metadata.has_next_page)

    @property
    def end_cursor(self) -> str | None:
        """The cursor to pass back to the connector to fetch the next page, if any."""
        return self.connector_metadata.end_cursor


class AgentConnector:
    """A connector configured in an Airbyte Agents workspace.

    Get an `AgentConnector` from `airbyte.cloud.CloudWorkspace.get_agent_connector()` or
    `airbyte.cloud.CloudWorkspace.list_agent_connectors()`, then call `execute()` to run a
    single connector action.
    """

    def __init__(
        self,
        *,
        connector_id: str,
        credentials: _AirbyteCredentials,
        organization_id: str | None = None,
        name: str | None = None,
    ) -> None:
        """Initialize an `AgentConnector`.

        This does not fetch anything from the API. Connector metadata is fetched lazily by
        `inspect()`.
        """
        self.connector_id = connector_id
        """The connector ID."""

        self.name = name
        """The connector's display name, if known."""

        self._credentials = credentials
        self._organization_id = organization_id or credentials.organization_id

    def __repr__(self) -> str:
        """Return a string representation of the connector."""
        return f"AgentConnector(connector_id={self.connector_id!r}, name={self.name!r})"

    def inspect(self) -> AgentConnectorDetails:
        """Fetch connector metadata, including the entities the connector supports."""
        payload = agents_api_util.inspect_agent_connector(
            connector_id=self.connector_id,
            client_id=self._credentials.client_id,
            client_secret=self._credentials.client_secret,
            bearer_token=self._credentials.bearer_token,
            organization_id=self._organization_id,
            agents_api_root=self._credentials.agents_api_root,
            public_api_root=self._credentials.public_api_root,
        )
        return AgentConnectorDetails.from_api_response(payload)

    def execute(
        self,
        entity: str,
        action: str,
        params: dict[str, Any] | None = None,
        *,
        select_fields: list[str] | None = None,
        exclude_fields: list[str] | None = None,
        skip_truncation: bool = True,
        intent: str | None = None,
    ) -> AgentExecuteResult:
        """Execute a single action on this connector.

        Args:
            entity: The entity to act on, for example `repositories`. Call `inspect()` to
                list the entities a connector supports.
            action: The action to run, for example `list`, `get`, or `create`.
            params: Action-specific parameters, for example `{"per_page": 10}`.
            select_fields: If set, only these fields are returned in each record.
            exclude_fields: If set, these fields are omitted from each record.
            skip_truncation: Whether to return full field values instead of letting Airbyte
                truncate long values for LLM consumption.
            intent: An optional description of why the action is being run, which Airbyte
                records alongside the execution.

        Returns:
            An `AgentExecuteResult` with the action's result payload and metadata.
        """
        payload = agents_api_util.execute_agent_connector_action(
            connector_id=self.connector_id,
            entity=entity,
            action=action,
            params=params,
            select_fields=select_fields,
            exclude_fields=exclude_fields,
            skip_truncation=skip_truncation,
            intent=intent,
            client_id=self._credentials.client_id,
            client_secret=self._credentials.client_secret,
            bearer_token=self._credentials.bearer_token,
            organization_id=self._organization_id,
            agents_api_root=self._credentials.agents_api_root,
            public_api_root=self._credentials.public_api_root,
        )
        return AgentExecuteResult.from_api_response(payload)


def list_agent_connectors(
    *,
    workspace_id: str,
    credentials: _AirbyteCredentials,
    organization_id: str | None = None,
) -> list[AgentConnector]:
    """List the connectors configured in an Airbyte Agents workspace."""
    records = agents_api_util.list_agent_connectors(
        workspace_id=workspace_id,
        client_id=credentials.client_id,
        client_secret=credentials.client_secret,
        bearer_token=credentials.bearer_token,
        organization_id=organization_id or credentials.organization_id,
        agents_api_root=credentials.agents_api_root,
        public_api_root=credentials.public_api_root,
    )
    return [
        AgentConnector(
            connector_id=str(record["id"]),
            name=record.get("name"),
            credentials=credentials,
            organization_id=organization_id,
        )
        for record in records
    ]


def get_agent_connector(
    *,
    workspace_id: str,
    credentials: _AirbyteCredentials,
    connector_id: str | None = None,
    name: str | None = None,
    organization_id: str | None = None,
) -> AgentConnector:
    """Get a single connector configured in an Airbyte Agents workspace.

    Provide either `connector_id` (no API call is made) or `name` (the workspace's
    connectors are listed to resolve the name).

    Raises:
        PyAirbyteInputError: If neither or both of `connector_id` and `name` are provided.
        AirbyteMissingResourceError: If no connector matches the given name.
        AirbyteMultipleResourcesError: If more than one connector matches the given name.
    """
    if bool(connector_id) == bool(name):
        raise PyAirbyteInputError(
            message="Provide exactly one of `connector_id` or `name`.",
            guidance="Pass a connector ID to skip lookup, or a name to resolve it by name.",
        )

    if connector_id:
        return AgentConnector(
            connector_id=connector_id,
            credentials=credentials,
            organization_id=organization_id,
        )

    matches = [
        connector
        for connector in list_agent_connectors(
            workspace_id=workspace_id,
            credentials=credentials,
            organization_id=organization_id,
        )
        if connector.name == name
    ]
    if not matches:
        raise AirbyteMissingResourceError(
            resource_type="Agents connector",
            resource_name_or_id=name,
            context={"workspace_id": workspace_id},
        )
    if len(matches) > 1:
        raise AirbyteMultipleResourcesError(
            resource_type="Agents connector",
            resource_name_or_id=name,
            context={
                "workspace_id": workspace_id,
                "connector_ids": [connector.connector_id for connector in matches],
            },
        )
    return matches[0]
