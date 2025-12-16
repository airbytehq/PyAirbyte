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
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal

import yaml
from airbyte_api import models as api_models  # noqa: TC002

from airbyte import exceptions as exc
from airbyte._util import api_util, text_util


if TYPE_CHECKING:
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

        self._connector_info: api_models.SourceResponse | api_models.DestinationResponse | None = (
            None
        )
        """The connection info object. (Cached.)"""

    @property
    def name(self) -> str | None:
        """Get the display name of the connector, if available.

        E.g. "My Postgres Source", not the canonical connector name ("source-postgres").
        """
        if not self._connector_info:
            self._connector_info = self._fetch_connector_info()

        return self._connector_info.name

    @abc.abstractmethod
    def _fetch_connector_info(self) -> api_models.SourceResponse | api_models.DestinationResponse:
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
            client_id=self.workspace.client_id,  # type: ignore[arg-type]
            client_secret=self.workspace.client_secret,  # type: ignore[arg-type]
            bearer_token=self.workspace.bearer_token,
        )
        check_result = CheckResult(
            success=result[0],
            error_message=result[1],
        )
        if raise_on_error and not check_result:
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

    def _fetch_connector_info(self) -> api_models.SourceResponse:
        """Populate the source with data from the API."""
        return api_util.get_source(
            source_id=self.connector_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,  # type: ignore[arg-type]
            client_secret=self.workspace.client_secret,  # type: ignore[arg-type]
            bearer_token=self.workspace.bearer_token,
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
            client_id=self.workspace.client_id,  # type: ignore[arg-type]
            client_secret=self.workspace.client_secret,  # type: ignore[arg-type]
            bearer_token=self.workspace.bearer_token,
            name=name,
        )
        self._connector_info = updated_response
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
            client_id=self.workspace.client_id,  # type: ignore[arg-type]
            client_secret=self.workspace.client_secret,  # type: ignore[arg-type]
            bearer_token=self.workspace.bearer_token,
            config=config,
        )
        self._connector_info = updated_response
        return self

    @classmethod
    def _from_source_response(
        cls,
        workspace: CloudWorkspace,
        source_response: api_models.SourceResponse,
    ) -> CloudSource:
        """Internal factory method.

        Creates a CloudSource object from a REST API SourceResponse object.
        """
        result = cls(
            workspace=workspace,
            connector_id=source_response.source_id,
        )
        result._connector_info = source_response  # noqa: SLF001  # Non-public API
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

    def _fetch_connector_info(self) -> api_models.DestinationResponse:
        """Populate the destination with data from the API."""
        return api_util.get_destination(
            destination_id=self.connector_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,  # type: ignore[arg-type]
            client_secret=self.workspace.client_secret,  # type: ignore[arg-type]
            bearer_token=self.workspace.bearer_token,
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
            client_id=self.workspace.client_id,  # type: ignore[arg-type]
            client_secret=self.workspace.client_secret,  # type: ignore[arg-type]
            bearer_token=self.workspace.bearer_token,
            name=name,
        )
        self._connector_info = updated_response
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
            client_id=self.workspace.client_id,  # type: ignore[arg-type]
            client_secret=self.workspace.client_secret,  # type: ignore[arg-type]
            bearer_token=self.workspace.bearer_token,
            config=config,
        )
        self._connector_info = updated_response
        return self

    @classmethod
    def _from_destination_response(
        cls,
        workspace: CloudWorkspace,
        destination_response: api_models.DestinationResponse,
    ) -> CloudDestination:
        """Internal factory method.

        Creates a CloudDestination object from a REST API DestinationResponse object.
        """
        result = cls(
            workspace=workspace,
            connector_id=destination_response.destination_id,
        )
        result._connector_info = destination_response  # noqa: SLF001  # Non-public API
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
        self._definition_info: api_models.DeclarativeSourceDefinitionResponse | None = None
        self._connector_builder_project_id: str | None = None

    def _fetch_definition_info(
        self,
    ) -> api_models.DeclarativeSourceDefinitionResponse:
        """Fetch definition info from the API."""
        if self.definition_type == "yaml":
            return api_util.get_custom_yaml_source_definition(
                workspace_id=self.workspace.workspace_id,
                definition_id=self.definition_id,
                api_root=self.workspace.api_root,
                client_id=self.workspace.client_id,  # type: ignore[arg-type]
                client_secret=self.workspace.client_secret,  # type: ignore[arg-type]
                bearer_token=self.workspace.bearer_token,
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

        if self._connector_builder_project_id is not None:
            return self._connector_builder_project_id

        self._connector_builder_project_id = (
            api_util.get_connector_builder_project_for_definition_id(
                workspace_id=self.workspace.workspace_id,
                definition_id=self.definition_id,
                api_root=self.workspace.api_root,
                client_id=self.workspace.client_id,  # type: ignore[arg-type]
                client_secret=self.workspace.client_secret,  # type: ignore[arg-type]
                bearer_token=self.workspace.bearer_token,
            )
        )

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
                client_id=self.workspace.client_id,  # type: ignore[arg-type]
                client_secret=self.workspace.client_secret,  # type: ignore[arg-type]
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
                client_id=self.workspace.client_id,  # type: ignore[arg-type]
                client_secret=self.workspace.client_secret,  # type: ignore[arg-type]
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
        response: api_models.DeclarativeSourceDefinitionResponse,
    ) -> CustomCloudSourceDefinition:
        """Internal factory method for YAML connectors."""
        result = cls(
            workspace=workspace,
            definition_id=response.id,
            definition_type="yaml",
        )
        result._definition_info = response  # noqa: SLF001  # Non-public API
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
            client_id=self.workspace.client_id,  # type: ignore[arg-type]
            client_secret=self.workspace.client_secret,  # type: ignore[arg-type]
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
        )

        return self
