# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Cloud connectors module for working with Cloud sources and destinations.

This module provides classes for working with Cloud sources and destinations. Rather
than creating `CloudConnector` objects directly, it is recommended to use the
`airbyte.cloud.workspaces` module to create and manage cloud connector objects.

Classes:
  - `CloudConnector`: A cloud connector object.
  - `CloudSource`: A cloud source object.
  - `CloudDestination`: A cloud destination object.

## Usage Examples

Obtain a cloud source object and run a `check` on it:

```python
from airbyte.cloud import CloudWorkspace

workspace = CloudWorkspace(
    workspace_id="...",
    client_id="...",
    client_secret="...",
)

# Get the cloud source object
cloud_source = workspace.get_source("...")

# Check the source configuration and credentials
check_result = cloud_source.check()
if check_result:
    # Truthy if the check was successful
    print("Check successful")
else:
    # Stringify the check result to get the error message
    print(f"Check failed: {check_result}")
```
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from enum import Enum
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal

import yaml

from airbyte import exceptions as exc
from airbyte._util import api_util, text_util
from airbyte.agents import _api_util as agents_api_util
from airbyte.agents._actions import (
    UNSUPPORTED_ACTIONS,
    AgentReadAction,
    AgentWriteAction,
    _build_params,
)
from airbyte.agents.models import AgentExecuteResult
from airbyte.cloud.models import (
    SQL_PASSTHROUGH_DESTINATION_DIALECTS,
    SQL_PASSTHROUGH_DESTINATION_NAMES,
    CloudCustomSourceDefinitionInfo,
    CloudDestinationInfo,
    CloudSourceInfo,
    _DeclarativeSourceDefinitionResponseLike,
    _DestinationResponseLike,
    _SourceResponseLike,
)


if TYPE_CHECKING:
    from collections.abc import Iterator

    from airbyte.agents._actions import AgentAction
    from airbyte.cloud.workspaces import CloudWorkspace


@dataclass
class CheckResult:
    """A cloud check result object."""

    success: bool
    """Whether the check result is valid."""

    error_message: str | None = None
    """None if the check was successful. Otherwise the failure message from the check result."""

    internal_error: str | None = None
    """None if the check was able to be run. Otherwise, this will describe the internal failure."""

    def __bool__(self) -> bool:
        """Truthy when check was successful."""
        return self.success

    def __str__(self) -> str:
        """Get a string representation of the check result."""
        return "Success" if self.success else f"Failed: {self.error_message}"

    def __repr__(self) -> str:
        """Get a string representation of the check result."""
        return (
            f"CheckResult(success={self.success}, "
            f"error_message={self.error_message or self.internal_error})"
        )


class ConnectorType(str, Enum):
    """The kind of a deployed Cloud connector."""

    SOURCE = "source"
    DESTINATION = "destination"


class ConnectorFeature(str, Enum):
    """Optional capabilities a deployed Cloud connector may have enabled."""

    EXTERNAL_ACCESS = "external_access"
    """The connector can be used by AI agents through the Airbyte Context layer."""

    SEARCH_INDEXING = "search_indexing"
    """Airbyte indexes the connector's data for fast search.

    Distinct from any native search the connector itself may offer as a passthrough
    operation.
    """


class CloudConnector(abc.ABC):
    """A cloud connector is a deployed source or destination on Airbyte Cloud.

    You can use a connector object to manage the connector.
    """

    connector_type: ClassVar[Literal["source", "destination"]]
    """The type of the connector."""

    def __init__(
        self,
        workspace: CloudWorkspace,
        connector_id: str,
    ) -> None:
        """Initialize a cloud connector object."""
        self.workspace = workspace
        """The workspace that the connector belongs to."""
        self.connector_id = connector_id
        """The ID of the connector."""

        self._connector_info: CloudSourceInfo | CloudDestinationInfo | None = None
        """The connection info object. (Cached.)"""

        self._enabled_features: frozenset[ConnectorFeature] | None = None
        """Features enabled for this connector. (Cached; `None` until resolved.)"""

    def _get_enabled_features(self) -> frozenset[ConnectorFeature]:
        """Return the enabled features, resolving them through the workspace on first use."""
        if self._enabled_features is None:
            self._enabled_features = self.workspace._get_connector_features(self)  # noqa: SLF001

        return self._enabled_features

    @property
    def external_access_enabled(self) -> bool:
        """Whether AI agents can use this connector through the Airbyte Context layer.

        Always `False` when the workspace's API root has no Context layer (for example,
        self-managed deployments). Otherwise this may make API calls on first access.
        """
        return ConnectorFeature.EXTERNAL_ACCESS in self._get_enabled_features()

    @property
    def search_indexing_enabled(self) -> bool:
        """Whether Airbyte indexes this connector's data for fast search.

        Always `False` for destinations and when the workspace's API root has no Context
        layer. Otherwise this may make API calls on first access.
        """
        if self.connector_type == ConnectorType.DESTINATION:
            return False

        return ConnectorFeature.SEARCH_INDEXING in self._get_enabled_features()

    @property
    def name(self) -> str | None:
        """Get the display name of the connector, if available.

        E.g. "My Postgres Source", not the canonical connector name ("source-postgres").
        """
        if not self._connector_info:
            self._connector_info = self._fetch_connector_info()

        return self._connector_info.name

    @property
    def definition_id(self) -> str:
        """Get the connector definition ID.

        E.g. the definition ID for `source-postgres`, not the ID of this deployed connector.
        """
        if not self._connector_info:
            self._connector_info = self._fetch_connector_info()

        return self._connector_info.definition_id

    @abc.abstractmethod
    def _fetch_connector_info(self) -> CloudSourceInfo | CloudDestinationInfo:
        """Populate the connector with data from the API."""
        ...

    @property
    def connector_url(self) -> str:
        """Get the web URL of the source connector."""
        return f"{self.workspace.workspace_url}/{self.connector_type}/{self.connector_id}"

    def __repr__(self) -> str:
        """String representation of the connector."""
        return (
            f"CloudConnector(type={self.connector_type!s}, "
            f"workspace_id={self.workspace.workspace_id}, "
            f"connector_id={self.connector_id}, "
            f"connector_url={self.connector_url})"
        )

    def permanently_delete(self) -> None:
        """Permanently delete the connector."""
        if self.connector_type == "source":
            self.workspace.permanently_delete_source(self.connector_id)
        else:
            self.workspace.permanently_delete_destination(self.connector_id)

    def check(
        self,
        *,
        raise_on_error: bool = True,
    ) -> CheckResult:
        """Check the connector.

        Returns:
            A `CheckResult` object containing the result. The object is truthy if the check was
            successful and falsy otherwise. The error message is available in the `error_message`
            or by converting the object to a string.
        """
        result = api_util.check_connector(
            workspace_id=self.workspace.workspace_id,
            connector_type=self.connector_type,
            actor_id=self.connector_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )
        check_result = CheckResult(
            success=result[0],
            error_message=result[1],
        )
        if raise_on_error and not check_result:
            raise ValueError(f"Check failed: {check_result}")

        return check_result

    def _execute_direct_action(  # noqa: PLR0913  # Explicit args mirror `AgentConnector.execute`.
        self,
        *,
        entity_type: str,
        action: AgentAction | str,
        api_args: dict[str, Any] | None = None,
        select_fields: list[str] | None = None,
        exclude_fields: list[str] | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
        skip_truncation: bool = True,
        intent: str | None = None,
    ) -> AgentExecuteResult:
        """Execute a single entity/action operation through the Agents API.

        Raises `AirbyteExternalAccessNotEnabledError` without any network call when the
        workspace's API roots have no Context layer API, and when the Agents API reports
        the connector as forbidden or not found. Other errors propagate unchanged.
        """
        connector_name = self._connector_info.name if self._connector_info else None
        if not self.workspace._has_context_layer_api():  # noqa: SLF001
            raise exc.AirbyteExternalAccessNotEnabledError(
                connector_name=connector_name,
                connector_id=self.connector_id,
            )

        if action in UNSUPPORTED_ACTIONS:
            raise exc.PyAirbyteInputError(
                message=f"The {action!r} action is not supported by PyAirbyte.",
                guidance=(
                    "This action returns a binary stream instead of JSON, and PyAirbyte does "
                    "not yet support streaming responses."
                ),
                context={"entity_type": entity_type, "action": action},
            )

        if action not in {*AgentReadAction, *AgentWriteAction}:
            action_names = ", ".join(
                member.value for member in (*AgentReadAction, *AgentWriteAction)
            )
            raise exc.PyAirbyteInputError(
                message=f"The {action!r} action is not a valid action name for `execute`.",
                guidance=f"Use one of: {action_names}.",
                context={"entity_type": entity_type, "action": action},
            )

        action_value = action.value if isinstance(action, Enum) else action
        params = _build_params(api_args=api_args, page_size=page_size, cursor=cursor)
        if (
            action_value == AgentReadAction.SQL_SELECT.value
            and self.workspace.workspace_id is not None
            and params.get("workspace_id") is None
            and params.get("workspace_name") is None
        ):
            params.pop("workspace_name", None)
            params["workspace_id"] = self.workspace.workspace_id
        request_body: dict[str, Any] = {
            "entity": entity_type,
            "action": action_value,
            "params": params,
            "skip_truncation": skip_truncation,
        }
        if select_fields is not None:
            request_body["select_fields"] = select_fields
        if exclude_fields is not None:
            request_body["exclude_fields"] = exclude_fields
        if intent is not None:
            request_body["intent"] = intent

        try:
            response = agents_api_util.execute_agent_connector_action(
                connector_id=self.connector_id,
                request_body=request_body,
                credentials=self.workspace._credentials,  # noqa: SLF001
                organization_id=self.workspace._resolve_agents_organization_id(),  # noqa: SLF001
            )
        except exc.AirbyteError as error:
            status_code = (error.context or {}).get("status_code")
            if status_code in {HTTPStatus.FORBIDDEN, HTTPStatus.NOT_FOUND}:
                raise exc.AirbyteExternalAccessNotEnabledError(
                    connector_name=connector_name,
                    connector_id=self.connector_id,
                ) from error

            raise

        return AgentExecuteResult.model_validate(response)


class CloudSource(CloudConnector):
    """A cloud source is a source that is deployed on Airbyte Cloud."""

    connector_type: ClassVar[Literal["source", "destination"]] = "source"
    """The type of the connector."""

    @property
    def source_id(self) -> str:
        """Get the ID of the source.

        This is an alias for `connector_id`.
        """
        return self.connector_id

    def _fetch_connector_info(self) -> CloudSourceInfo:
        """Populate the source with data from the API."""
        return CloudSourceInfo.from_api_response(
            api_util.get_source(
                source_id=self.connector_id,
                api_root=self.workspace.api_root,
                client_id=self.workspace.client_id,
                client_secret=self.workspace.client_secret,
                bearer_token=self.workspace.bearer_token,
            )
        )

    def rename(self, name: str) -> CloudSource:
        """Rename the source.

        Args:
            name: New name for the source

        Returns:
            Updated CloudSource object with refreshed info
        """
        updated_response = api_util.patch_source(
            source_id=self.connector_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            name=name,
        )
        self._connector_info = CloudSourceInfo.from_api_response(updated_response)
        return self

    def update_config(self, config: dict[str, Any]) -> CloudSource:
        """Update the source configuration.

        This is a destructive operation that can break existing connections if the
        configuration is changed incorrectly. Use with caution.

        Args:
            config: New configuration for the source

        Returns:
            Updated CloudSource object with refreshed info
        """
        updated_response = api_util.patch_source(
            source_id=self.connector_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config=config,
        )
        self._connector_info = CloudSourceInfo.from_api_response(updated_response)
        return self

    @classmethod
    def _from_source_response(
        cls,
        workspace: CloudWorkspace,
        source_response: _SourceResponseLike,
    ) -> CloudSource:
        """Internal factory method.

        Creates a CloudSource object from a REST API source response object.
        """
        source_info = CloudSourceInfo.from_api_response(source_response)
        result = cls(
            workspace=workspace,
            connector_id=source_info.source_id,
        )
        result._connector_info = source_info  # noqa: SLF001  # Accessing Non-Public API
        return result

    def execute(  # noqa: PLR0913  # Explicit args are the point of this public API.
        self,
        entity_type: str,
        action: AgentAction | str,
        api_args: dict[str, Any] | None = None,
        *,
        select_fields: list[str] | None = None,
        exclude_fields: list[str] | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
        skip_truncation: bool = True,
        intent: str | None = None,
    ) -> AgentExecuteResult:
        """Execute a single action against one entity type on this source.

        `entity_type` and `action` are connector-specific, for example `issues` and `list`.
        Direct actions require external access to be enabled for this source in its
        organization's Context Layer settings.

        `api_args` holds connector-specific arguments passed through to the connector, for
        example `{"repository": "airbytehq/PyAirbyte"}`. All other arguments are interpreted
        by PyAirbyte or by the Agents API itself:

        - `select_fields` and `exclude_fields` prune fields from returned entities.
        - `page_size` and `cursor` are merged into `api_args` as pagination arguments.
        - `skip_truncation` disables the Agents API's default truncation of large payloads.
        - `intent` is a free-text description of why the action is being run, which some
          connectors use to refine results.

        The `download` action is rejected, because it returns a binary stream and PyAirbyte
        does not yet support streaming responses.
        """
        return self._execute_direct_action(
            entity_type=entity_type,
            action=action,
            api_args=api_args,
            select_fields=select_fields,
            exclude_fields=exclude_fields,
            page_size=page_size,
            cursor=cursor,
            skip_truncation=skip_truncation,
            intent=intent,
        )

    def list_entities(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `list` action, which returns a page of entities of `entity_type`."""
        return self.execute(entity_type, "list", api_args, **kwargs)

    def iter_entities(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        *,
        limit: int | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `list_entities()`.
    ) -> Iterator[dict[str, Any]]:
        """Yield entities of `entity_type`, following the connector's pagination cursor.

        This is the pagination-free way to read entities: each page is fetched lazily as
        the caller iterates, so no cursor bookkeeping is needed.

        ```python
        for issue in source.iter_entities("issues", {"repository": "airbytehq/PyAirbyte"}):
            print(issue["title"])
        ```

        `limit` caps how many entities are yielded in total, which matters for entity types
        with no natural end. Pass `page_size` to control how many are fetched per request.

        Iteration stops early if the connector reports another page without advancing its
        cursor, rather than requesting the same page forever.

        Use `list_entities()` instead when a single page is enough, or when the result's
        `status`, `warning`, or `execution_metadata` are needed.
        """
        cursor: str | None = kwargs.pop("cursor", None)
        seen_cursors: set[str] = set()
        yielded = 0

        while True:
            result = self.list_entities(entity_type, api_args, cursor=cursor, **kwargs)
            for agent_entity in result.entities:
                yield agent_entity
                yielded += 1
                if limit is not None and yielded >= limit:
                    return

            cursor = result.end_cursor
            if not result.has_next_page or cursor is None or cursor in seen_cursors:
                return
            seen_cursors.add(cursor)

    def search_entities(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `search` action, which returns matching entities of `entity_type`."""
        return self.execute(entity_type, "search", api_args, **kwargs)

    def get_entity(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `get` action, which returns a single entity of `entity_type`."""
        return self.execute(entity_type, "get", api_args, **kwargs)

    def create_entity(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `create` action, which creates an entity of `entity_type`."""
        return self.execute(entity_type, "create", api_args, **kwargs)

    def update_entity(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `update` action, which updates an entity of `entity_type`."""
        return self.execute(entity_type, "update", api_args, **kwargs)

    def delete_entity(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `delete` action, which deletes an entity of `entity_type`."""
        return self.execute(entity_type, "delete", api_args, **kwargs)


class CloudDestination(CloudConnector):
    """A cloud destination is a destination that is deployed on Airbyte Cloud."""

    connector_type: ClassVar[Literal["source", "destination"]] = "destination"
    """The type of the connector."""

    def __init__(
        self,
        workspace: CloudWorkspace,
        connector_id: str,
    ) -> None:
        """Initialize a cloud destination object."""
        super().__init__(workspace=workspace, connector_id=connector_id)
        self._configuration: dict[str, Any] | None = None
        """The destination configuration. (Cached.)"""

    @property
    def destination_id(self) -> str:
        """Get the ID of the destination.

        This is an alias for `connector_id`.
        """
        return self.connector_id

    @property
    def configuration(self) -> dict[str, Any] | None:
        """The destination configuration as returned by the API.

        Secret values are redacted by the API. `list_destinations` responses do not
        carry a reliably typed configuration, so this is always fetched via
        `get_destination` on first access.
        """
        if self._configuration is None:
            info = self._fetch_connector_info()
            self._configuration = info.configuration
            self._connector_info = info

        return self._configuration

    def _fetch_connector_info(self) -> CloudDestinationInfo:
        """Populate the destination with data from the API."""
        return CloudDestinationInfo.from_api_response(
            api_util.get_destination(
                destination_id=self.connector_id,
                api_root=self.workspace.api_root,
                client_id=self.workspace.client_id,
                client_secret=self.workspace.client_secret,
                bearer_token=self.workspace.bearer_token,
            )
        )

    def rename(self, name: str) -> CloudDestination:
        """Rename the destination.

        Args:
            name: New name for the destination

        Returns:
            Updated CloudDestination object with refreshed info
        """
        updated_response = api_util.patch_destination(
            destination_id=self.connector_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            name=name,
        )
        self._connector_info = CloudDestinationInfo.from_api_response(updated_response)
        return self

    def update_config(self, config: dict[str, Any]) -> CloudDestination:
        """Update the destination configuration.

        This is a destructive operation that can break existing connections if the
        configuration is changed incorrectly. Use with caution.

        Args:
            config: New configuration for the destination

        Returns:
            Updated CloudDestination object with refreshed info
        """
        updated_response = api_util.patch_destination(
            destination_id=self.connector_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config=config,
        )
        self._connector_info = CloudDestinationInfo.from_api_response(updated_response)
        return self

    @classmethod
    def _from_destination_response(
        cls,
        workspace: CloudWorkspace,
        destination_response: _DestinationResponseLike,
    ) -> CloudDestination:
        """Internal factory method.

        Creates a CloudDestination object from a REST API destination response object.
        """
        destination_info = CloudDestinationInfo.from_api_response(destination_response)
        result = cls(
            workspace=workspace,
            connector_id=destination_info.destination_id,
        )
        result._connector_info = destination_info  # noqa: SLF001  # Accessing Non-Public API
        return result

    def sql_select(
        self,
        sql: str,
        *,
        sql_dialect: str | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
    ) -> AgentExecuteResult:
        """Run a read-only SQL `SELECT` (or `SHOW TABLES`) through SQL passthrough.

        The statement runs on the query engine behind this destination, via the Agents
        API `sql_select` action. `sql_dialect` defaults to the dialect registered for this
        destination's connector definition; it is required for destination definitions
        that do not support SQL passthrough.

        Direct actions require external access to be enabled for this destination in its
        organization's Context Layer settings.
        """
        if sql_dialect is None:
            sql_dialect = SQL_PASSTHROUGH_DESTINATION_DIALECTS.get(self.definition_id)
        if sql_dialect is None:
            supported = ", ".join(
                f"{name} ({definition_id})"
                for definition_id, name in SQL_PASSTHROUGH_DESTINATION_NAMES.items()
            )
            raise exc.PyAirbyteInputError(
                message=(
                    f"Destination {self.name!r} does not support SQL passthrough, so "
                    "`sql_dialect` is required."
                ),
                guidance=(
                    "Pass `sql_dialect` explicitly, or use a destination that supports SQL "
                    f"passthrough: {supported}."
                ),
                context={
                    "connector_id": self.connector_id,
                    "definition_id": self.definition_id,
                },
            )

        return self._execute_direct_action(
            entity_type="sql",
            action=AgentReadAction.SQL_SELECT,
            api_args={"sql": sql, "sql_dialect": sql_dialect},
            page_size=page_size,
            cursor=cursor,
        )


class CustomCloudSourceDefinition:
    """A custom source connector definition in Airbyte Cloud.

    This represents either a YAML (declarative) or Docker-based custom source definition.
    """

    connector_type: ClassVar[Literal["source", "destination"]] = "source"
    """The type of the connector: 'source' or 'destination'."""

    def __init__(
        self,
        workspace: CloudWorkspace,
        definition_id: str,
        definition_type: Literal["yaml", "docker"],
    ) -> None:
        """Initialize a custom source definition object.

        Note: Only YAML connectors are currently supported. Docker connectors
        will raise NotImplementedError.
        """
        self.workspace = workspace
        self.definition_id = definition_id
        self.definition_type: Literal["yaml", "docker"] = definition_type
        self._definition_info: CloudCustomSourceDefinitionInfo | None = None
        self._connector_builder_project_id: str | None = None
        self._connector_builder_project_id_fetched: bool = False
        self._builder_project_workspace_id: str | None = None
        self._builder_project_data: dict[str, Any] | None = None

    def _fetch_definition_info(
        self,
    ) -> CloudCustomSourceDefinitionInfo:
        """Fetch definition info from the API."""
        if self.definition_type == "yaml":
            return CloudCustomSourceDefinitionInfo.from_api_response(
                api_util.get_custom_yaml_source_definition(
                    workspace_id=self.workspace.workspace_id,
                    definition_id=self.definition_id,
                    api_root=self.workspace.api_root,
                    client_id=self.workspace.client_id,
                    client_secret=self.workspace.client_secret,
                    bearer_token=self.workspace.bearer_token,
                )
            )
        raise NotImplementedError(
            "Docker custom source definitions are not yet supported. "
            "Only YAML manifest-based custom sources are currently available."
        )

    @property
    def name(self) -> str:
        """Get the display name of the custom connector definition."""
        if not self._definition_info:
            self._definition_info = self._fetch_definition_info()
        return self._definition_info.name

    @property
    def manifest(self) -> dict[str, Any] | None:
        """Get the Low-code CDK manifest. Only present for YAML connectors."""
        if self.definition_type != "yaml":
            return None
        if not self._definition_info:
            self._definition_info = self._fetch_definition_info()
        return self._definition_info.manifest

    @property
    def version(self) -> str | None:
        """Get the manifest version. Only present for YAML connectors."""
        if self.definition_type != "yaml":
            return None
        if not self._definition_info:
            self._definition_info = self._fetch_definition_info()
        return self._definition_info.version

    @property
    def docker_repository(self) -> str | None:
        """Get the Docker repository. Only present for Docker connectors.

        Note: Docker connectors are not yet supported and will raise NotImplementedError.
        """
        if self.definition_type != "docker":
            return None
        raise NotImplementedError(
            "Docker custom source definitions are not yet supported. "
            "Only YAML manifest-based custom sources are currently available."
        )

    @property
    def docker_image_tag(self) -> str | None:
        """Get the Docker image tag. Only present for Docker connectors.

        Note: Docker connectors are not yet supported and will raise NotImplementedError.
        """
        if self.definition_type != "docker":
            return None
        raise NotImplementedError(
            "Docker custom source definitions are not yet supported. "
            "Only YAML manifest-based custom sources are currently available."
        )

    @property
    def documentation_url(self) -> str | None:
        """Get the documentation URL. Only present for Docker connectors.

        Note: Docker connectors are not yet supported and will raise NotImplementedError.
        """
        if self.definition_type != "docker":
            return None
        raise NotImplementedError(
            "Docker custom source definitions are not yet supported. "
            "Only YAML manifest-based custom sources are currently available."
        )

    @property
    def connector_builder_project_id(self) -> str | None:
        """Get the connector builder project ID. Only present for YAML connectors."""
        if self.definition_type != "yaml":
            return None

        if self._connector_builder_project_id_fetched:
            return self._connector_builder_project_id

        result = api_util.get_connector_builder_project_for_definition_id(
            workspace_id=self.workspace.workspace_id,
            definition_id=self.definition_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )
        self._connector_builder_project_id = result.get("builderProjectId")
        self._connector_builder_project_id_fetched = True
        # The builder project may live in a different workspace than the caller's.
        # We must use the project's owning workspace ID when fetching its data.
        self._builder_project_workspace_id = result.get("workspaceId")

        return self._connector_builder_project_id

    @property
    def connector_builder_project_url(self) -> str | None:
        """Get the connector builder project URL. Only present for YAML connectors."""
        if self.definition_type != "yaml":
            return None

        project_id = self.connector_builder_project_id
        if not project_id:
            return None

        return f"{self.workspace.workspace_url}/connector-builder/edit/{project_id}"

    def get_builder_project_data(
        self,
        *,
        use_cache: bool = True,
    ) -> dict[str, Any]:
        """Fetch the full connector builder project data, including draft manifest if present.

        This calls the `/v1/connector_builder_projects/get_with_manifest` endpoint which returns
        the project metadata and draft manifest (if one exists).

        Args:
            use_cache: If True, return cached data from a previous call if available.
                Set to False to force a fresh API request. Defaults to True.

        Returns:
            A dictionary containing the builder project details. Key fields include:
            - builderProject: The project metadata (name, hasDraft,
              activeDeclarativeManifestVersion, etc.)
            - declarativeManifest: The draft manifest data (if hasDraft is True),
              which contains a 'manifest' field with the actual YAML manifest dict.

        Raises:
            NotImplementedError: If this is not a YAML custom source definition.
            PyAirbyteInputError: If the connector builder project ID cannot be found.
        """
        if self.definition_type != "yaml":
            raise NotImplementedError(
                "Builder project data is only available for YAML custom source definitions. "
                "Docker custom sources are not yet supported."
            )

        if use_cache and self._builder_project_data is not None:
            return self._builder_project_data

        builder_project_id = self.connector_builder_project_id
        if not builder_project_id:
            raise exc.PyAirbyteInputError(
                message="Could not find connector builder project ID for this definition.",
                context={
                    "definition_id": self.definition_id,
                    "workspace_id": self.workspace.workspace_id,
                },
            )

        self._builder_project_data = api_util.get_connector_builder_project(
            workspace_id=self._builder_project_workspace_id or self.workspace.workspace_id,
            builder_project_id=builder_project_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )
        return self._builder_project_data

    @property
    def has_draft(self) -> bool | None:
        """Check whether this definition has an unpublished draft in Connector Builder.

        Returns:
            True if a draft exists, False if no draft exists,
            or None if this is not a YAML connector or the project ID is unavailable.
        """
        if self.definition_type != "yaml":
            return None

        if not self.connector_builder_project_id:
            return None

        project_data = self.get_builder_project_data()
        builder_project = project_data.get("builderProject", {})
        return builder_project.get("hasDraft", False)

    @property
    def draft_manifest(self) -> dict[str, Any] | None:
        """Get the draft (unpublished) manifest from the Connector Builder, if one exists.

        This reads the working draft that has been saved in the Connector Builder UI
        but not yet published. Returns None if no draft exists or if this is not a
        YAML connector.

        Returns:
            The draft manifest as a dictionary, or None if no draft exists.
        """
        if self.definition_type != "yaml":
            return None

        if not self.connector_builder_project_id:
            return None

        project_data = self.get_builder_project_data()
        builder_project = project_data.get("builderProject", {})
        if not builder_project.get("hasDraft", False):
            return None

        declarative_manifest = project_data.get("declarativeManifest", {})
        manifest = declarative_manifest.get("manifest")
        if isinstance(manifest, dict):
            return manifest

        return None

    @property
    def definition_url(self) -> str:
        """Get the web URL of the custom source definition.

        For YAML connectors, this is the connector builder 'edit' URL.
        For Docker connectors, this is the custom connectors page.
        """
        return (
            self.connector_builder_project_url
            or f"{self.workspace.workspace_url}/settings/{self.connector_type}"
        )

    def permanently_delete(
        self,
        *,
        safe_mode: bool = True,
    ) -> None:
        """Permanently delete this custom source definition.

        Args:
            safe_mode: If True, requires the connector name to contain "delete-me" or "deleteme"
                (case insensitive) to prevent accidental deletion. Defaults to True.
        """
        if self.definition_type == "yaml":
            api_util.delete_custom_yaml_source_definition(
                workspace_id=self.workspace.workspace_id,
                definition_id=self.definition_id,
                api_root=self.workspace.api_root,
                client_id=self.workspace.client_id,
                client_secret=self.workspace.client_secret,
                bearer_token=self.workspace.bearer_token,
                safe_mode=safe_mode,
            )
        else:
            raise NotImplementedError(
                "Docker custom source definitions are not yet supported. "
                "Only YAML manifest-based custom sources are currently available."
            )

    def update_definition(
        self,
        *,
        manifest_yaml: dict[str, Any] | Path | str | None = None,
        docker_tag: str | None = None,
        pre_validate: bool = True,
    ) -> CustomCloudSourceDefinition:
        """Update this custom source definition.

        You must specify EXACTLY ONE of manifest_yaml (for YAML connectors) OR
        docker_tag (for Docker connectors), but not both.

        For YAML connectors: updates the manifest
        For Docker connectors: Not yet supported (raises NotImplementedError)

        Args:
            manifest_yaml: New manifest (YAML connectors only)
            docker_tag: New Docker tag (Docker connectors only, not yet supported)
            pre_validate: Whether to validate manifest (YAML only)

        Returns:
            Updated CustomCloudSourceDefinition object

        Raises:
            PyAirbyteInputError: If both or neither parameters are provided
            NotImplementedError: If docker_tag is provided (Docker not yet supported)
        """
        is_yaml = manifest_yaml is not None
        is_docker = docker_tag is not None

        if is_yaml == is_docker:
            raise exc.PyAirbyteInputError(
                message=(
                    "Must specify EXACTLY ONE of manifest_yaml (for YAML) OR "
                    "docker_tag (for Docker), but not both"
                ),
                context={
                    "manifest_yaml_provided": is_yaml,
                    "docker_tag_provided": is_docker,
                },
            )

        if is_yaml:
            manifest_dict: dict[str, Any]
            if isinstance(manifest_yaml, Path):
                manifest_dict = yaml.safe_load(manifest_yaml.read_text())
            elif isinstance(manifest_yaml, str):
                manifest_dict = yaml.safe_load(manifest_yaml)
            else:
                manifest_dict = manifest_yaml  # type: ignore[assignment]

            if pre_validate:
                api_util.validate_yaml_manifest(manifest_dict, raise_on_error=True)

            result = api_util.update_custom_yaml_source_definition(
                workspace_id=self.workspace.workspace_id,
                definition_id=self.definition_id,
                manifest=manifest_dict,
                api_root=self.workspace.api_root,
                client_id=self.workspace.client_id,
                client_secret=self.workspace.client_secret,
                bearer_token=self.workspace.bearer_token,
            )
            return CustomCloudSourceDefinition._from_yaml_response(self.workspace, result)

        raise NotImplementedError(
            "Docker custom source definitions are not yet supported. "
            "Only YAML manifest-based custom sources are currently available."
        )

    def rename(
        self,
        new_name: str,  # noqa: ARG002
    ) -> CustomCloudSourceDefinition:
        """Rename this custom source definition.

        Note: Only Docker custom sources can be renamed. YAML custom sources
        cannot be renamed as their names are derived from the manifest.

        Args:
            new_name: New display name for the connector

        Returns:
            Updated CustomCloudSourceDefinition object

        Raises:
            PyAirbyteInputError: If attempting to rename a YAML connector
            NotImplementedError: If attempting to rename a Docker connector (not yet supported)
        """
        if self.definition_type == "yaml":
            raise exc.PyAirbyteInputError(
                message="Cannot rename YAML custom source definitions",
                context={"definition_id": self.definition_id},
            )

        raise NotImplementedError(
            "Docker custom source definitions are not yet supported. "
            "Only YAML manifest-based custom sources are currently available."
        )

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"CustomCloudSourceDefinition(definition_id={self.definition_id}, "
            f"name={self.name}, definition_type={self.definition_type})"
        )

    @classmethod
    def _from_yaml_response(
        cls,
        workspace: CloudWorkspace,
        response: _DeclarativeSourceDefinitionResponseLike,
    ) -> CustomCloudSourceDefinition:
        """Internal factory method for YAML connectors."""
        definition_info = CloudCustomSourceDefinitionInfo.from_api_response(response)
        result = cls(
            workspace=workspace,
            definition_id=definition_info.definition_id,
            definition_type="yaml",
        )
        result._definition_info = definition_info  # noqa: SLF001
        return result

    def deploy_source(
        self,
        name: str,
        config: dict[str, Any],
        *,
        unique: bool = True,
        random_name_suffix: bool = False,
    ) -> CloudSource:
        """Deploy a new cloud source using this custom source definition.

        Args:
            name: The name for the new source.
            config: A dictionary containing the connection configuration for the new source.
            unique: If True, raises an error if a source with the same name already exists
                in the workspace. Default is True.
            random_name_suffix: If True, appends a random suffix to the name to ensure uniqueness.
                Default is False.

        Returns:
            A `CloudSource` object representing the newly created source.
        """
        if self.definition_type != "yaml":
            raise NotImplementedError(
                "Only YAML custom source definitions can be used to deploy new sources. "
                "Docker custom sources are not yet supported."
            )

        if random_name_suffix:
            name += f" (ID: {text_util.generate_random_suffix()})"

        if unique:
            existing = self.workspace.list_sources(name=name)
            if existing:
                raise exc.AirbyteDuplicateResourcesError(
                    resource_type="source",
                    resource_name=name,
                )

        result = api_util.create_source(
            name=name,
            definition_id=self.definition_id,
            workspace_id=self.workspace.workspace_id,
            config=config,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
        )
        return CloudSource._from_source_response(  # noqa: SLF001  # Accessing Non-Public API
            workspace=self.workspace,
            source_response=result,
        )

    def set_testing_values(
        self,
        testing_values: dict[str, Any],
    ) -> CustomCloudSourceDefinition:
        """Set the testing values for this custom source definition's connector builder project.

        Testing values are the input configuration values used when testing the connector
        in the Connector Builder UI. Setting these values allows users to immediately
        run test read operations after deploying a custom source to the Builder UI.

        This method replaces any existing testing values with the provided dictionary.
        Pass the full set of values you want to persist, not just the fields you're changing.

        Args:
            testing_values: A dictionary containing the configuration values to use for testing.
                This should match the connector's spec schema. Replaces any existing values.

        Returns:
            This `CustomCloudSourceDefinition` object (for method chaining).

        Raises:
            NotImplementedError: If this is not a YAML custom source definition.
            PyAirbyteInputError: If the connector builder project ID cannot be found.
        """
        if self.definition_type != "yaml":
            raise NotImplementedError(
                "Testing values can only be set for YAML custom source definitions. "
                "Docker custom sources are not yet supported."
            )

        builder_project_id = self.connector_builder_project_id
        if not builder_project_id:
            raise exc.PyAirbyteInputError(
                message="Could not find connector builder project ID for this definition.",
                context={
                    "definition_id": self.definition_id,
                    "workspace_id": self.workspace.workspace_id,
                },
            )

        # Get the spec from the definition info
        if not self._definition_info:
            self._definition_info = self._fetch_definition_info()

        # Build the spec object from the manifest, matching the Builder UI pattern
        spec: dict[str, Any] = {}
        if self._definition_info.manifest:
            manifest_spec = self._definition_info.manifest.get("spec", {})
            if manifest_spec:
                spec = {
                    "documentationUrl": manifest_spec.get("documentation_url"),
                    "connectionSpecification": manifest_spec.get("connection_specification", {}),
                    "advancedAuth": manifest_spec.get("advanced_auth"),
                }

        api_util.update_connector_builder_project_testing_values(
            workspace_id=self.workspace.workspace_id,
            builder_project_id=builder_project_id,
            testing_values=testing_values,
            spec=spec,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )

        return self
