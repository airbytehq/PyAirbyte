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
check_result = cloud_source.check(wait=False)
check_result.wait_for_completion()
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
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal

import yaml

from airbyte import exceptions as exc
from airbyte._util import api_util, text_util
from airbyte.cloud.models import (
    CloudCustomSourceDefinitionInfo,
    CloudDestinationInfo,
    CloudSourceInfo,
    _DeclarativeSourceDefinitionResponseLike,
    _DestinationResponseLike,
    _SourceResponseLike,
)


if TYPE_CHECKING:
    from airbyte.cloud.workspaces import CloudWorkspace


DEFAULT_CHECK_TIMEOUT_SECONDS = 300


@dataclass
class CheckResult:
    """A cloud check result object."""

    success: bool = False
    """Whether the check result is valid."""

    error_message: str | None = None
    """None if the check was successful. Otherwise the failure message from the check result."""

    internal_error: str | None = None
    """None if the check was able to be run. Otherwise, this will describe the internal failure."""

    command_id: str | None = None
    """The ID of the asynchronous check command."""

    failure_type: str | None = None
    """The failure type returned by the check command."""

    connector: CloudConnector | None = None
    """The connector being checked."""

    _status: str | None = None
    """The latest command status."""

    def get_status(self) -> str:
        """Return the latest command status."""
        if self._status is not None and self._status not in {"pending", "running"}:
            return self._status
        if self.connector is None or self.command_id is None:
            return self._status or "completed"

        self._status = api_util.get_command_status(
            command_id=self.command_id,
            api_root=self.connector.workspace.api_root,
            config_api_root=self.connector.workspace.config_api_root,
            client_id=self.connector.workspace.client_id,
            client_secret=self.connector.workspace.client_secret,
            bearer_token=self.connector.workspace.bearer_token,
        )
        return self._status

    def is_complete(self) -> bool:
        """Return whether the command has reached a final status."""
        return self.get_status() not in {"pending", "running"}

    def wait_for_completion(
        self,
        *,
        wait_timeout: int = DEFAULT_CHECK_TIMEOUT_SECONDS,
        raise_timeout: bool = True,
        raise_failure: bool = False,
    ) -> CheckResult:
        """Wait for the check command to finish running."""
        start_time = time.time()
        while True:
            if self.is_complete():
                self.refresh()
                if raise_failure and not self:
                    raise ValueError(f"Check failed: {self}")
                return self

            if time.time() - start_time > wait_timeout:
                if raise_timeout:
                    connector_id = self.connector.connector_id if self.connector else None
                    raise exc.AirbyteConnectorCheckTimeoutError(
                        connector_id=connector_id,
                        command_id=self.command_id,
                        timeout=wait_timeout,
                    )
                return self

            time.sleep(api_util.JOB_WAIT_INTERVAL_SECS)

    def cancel(self) -> None:
        """Cancel the check command."""
        if self.connector is None or self.command_id is None:
            raise exc.PyAirbyteInputError(
                message="A connector and command ID are required to cancel a check."
            )
        api_util.cancel_command(
            command_id=self.command_id,
            api_root=self.connector.workspace.api_root,
            config_api_root=self.connector.workspace.config_api_root,
            client_id=self.connector.workspace.client_id,
            client_secret=self.connector.workspace.client_secret,
            bearer_token=self.connector.workspace.bearer_token,
        )

    def get_logs(self) -> list[str]:
        """Return the logs from the check command."""
        if self.connector is None or self.command_id is None or self.get_status() == "cancelled":
            return []
        response = api_util.get_check_command_output(
            command_id=self.command_id,
            with_logs=True,
            api_root=self.connector.workspace.api_root,
            config_api_root=self.connector.workspace.config_api_root,
            client_id=self.connector.workspace.client_id,
            client_secret=self.connector.workspace.client_secret,
            bearer_token=self.connector.workspace.bearer_token,
        )
        logs = response.get("logs") or {}
        return logs.get("logLines") or []

    def refresh(self) -> None:
        """Refresh success and error fields from the command output."""
        if self.connector is None or self.command_id is None:
            return
        status = self.get_status()
        if status in {"pending", "running"}:
            return
        if status == "cancelled":
            self.success = False
            self.error_message = "Check command was cancelled."
            return

        response = api_util.get_check_command_output(
            command_id=self.command_id,
            api_root=self.connector.workspace.api_root,
            config_api_root=self.connector.workspace.config_api_root,
            client_id=self.connector.workspace.client_id,
            client_secret=self.connector.workspace.client_secret,
            bearer_token=self.connector.workspace.bearer_token,
        )
        status = response.get("status")
        failure_reason = response.get("failureReason") or {}
        self.success = status == "succeeded"
        self.error_message = (
            None
            if self.success
            else failure_reason.get("externalMessage") or response.get("message")
        )
        self.failure_type = failure_reason.get("failureType")
        self.internal_error = (
            failure_reason.get("internalMessage")
            if not self.success and not failure_reason.get("externalMessage")
            else None
        )

    def __bool__(self) -> bool:
        """Truthy when check was successful."""
        return self.success

    def __str__(self) -> str:
        """Get a string representation of the check result."""
        if self.success:
            return "Success"
        failure_message = (
            self.error_message or self.internal_error or "No failure message provided."
        )
        return f"Failed: {failure_message}"

    def __repr__(self) -> str:
        """Get a string representation of the check result."""
        result = (
            f"CheckResult(success={self.success}, "
            f"error_message={self.error_message or self.internal_error})"
        )
        if self.command_id:
            result = result[:-1] + f", command_id={self.command_id!r})"
        return result


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
        wait: bool = True,
        wait_timeout: int = DEFAULT_CHECK_TIMEOUT_SECONDS,
        command_id: str | None = None,
    ) -> CheckResult:
        """Check the connector.

        Runs the check asynchronously via the platform command API. With `wait=True` (default) this
        blocks until the check completes. With `wait=False` it returns immediately; use
        `CheckResult.wait_for_completion()` or call `check(command_id=..., wait=False)` to poll.
        Pass `command_id` to attach to an existing check command instead of starting a new one.
        """
        if command_id is None:
            command_id = api_util.run_check_command(
                actor_id=self.connector_id,
                workspace_id=self.workspace.workspace_id,
                api_root=self.workspace.api_root,
                config_api_root=self.workspace.config_api_root,
                client_id=self.workspace.client_id,
                client_secret=self.workspace.client_secret,
                bearer_token=self.workspace.bearer_token,
            )
        check_result = CheckResult(
            command_id=command_id,
            connector=self,
        )
        if wait:
            check_result.wait_for_completion(
                wait_timeout=wait_timeout,
                raise_timeout=True,
                raise_failure=False,
            )
        else:
            check_result.get_status()
            if check_result.is_complete():
                check_result.refresh()

        if raise_on_error and check_result.is_complete() and not check_result:
            raise ValueError(f"Check failed: {check_result}")

        return check_result


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


class CloudDestination(CloudConnector):
    """A cloud destination is a destination that is deployed on Airbyte Cloud."""

    connector_type: ClassVar[Literal["source", "destination"]] = "destination"
    """The type of the connector."""

    @property
    def destination_id(self) -> str:
        """Get the ID of the destination.

        This is an alias for `connector_id`.
        """
        return self.connector_id

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
