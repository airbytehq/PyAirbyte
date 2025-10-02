# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte classes and methods for interacting with the Airbyte Cloud API.

By overriding `api_root`, you can use this module to interact with self-managed Airbyte instances,
both OSS and Enterprise.

## Usage Examples

Get a new workspace object and deploy a source to it:

```python
import airbyte as ab
from airbyte import cloud

workspace = cloud.CloudWorkspace(
    workspace_id="...",
    client_id="...",
    client_secret="...",
)

# Deploy a source to the workspace
source = ab.get_source("source-faker", config={"count": 100})
deployed_source = workspace.deploy_source(
    name="test-source",
    source=source,
)

# Run a check on the deployed source and raise an exception if the check fails
check_result = deployed_source.check(raise_on_error=True)

# Permanently delete the newly-created source
workspace.permanently_delete_source(deployed_source)
```
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import yaml

from airbyte import exceptions as exc
from airbyte._util import api_util, text_util
from airbyte._util.api_util import get_web_url_root
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.connectors import (
    CloudCustomDestinationDefinition,
    CloudCustomSourceDefinition,
    CloudDestination,
    CloudSource,
)
from airbyte.destinations.base import Destination
from airbyte.secrets.base import SecretString


if TYPE_CHECKING:
    from collections.abc import Callable

    from airbyte.sources.base import Source


@dataclass  # noqa: PLR0904
class CloudWorkspace:
    """A remote workspace on the Airbyte Cloud.

    By overriding `api_root`, you can use this class to interact with self-managed Airbyte
    instances, both OSS and Enterprise.
    """

    workspace_id: str
    client_id: SecretString
    client_secret: SecretString
    api_root: str = api_util.CLOUD_API_ROOT

    def __post_init__(self) -> None:
        """Ensure that the client ID and secret are handled securely."""
        self.client_id = SecretString(self.client_id)
        self.client_secret = SecretString(self.client_secret)

    @property
    def workspace_url(self) -> str | None:
        """The web URL of the workspace."""
        return f"{get_web_url_root(self.api_root)}/workspaces/{self.workspace_id}"

    # Test connection and creds

    def connect(self) -> None:
        """Check that the workspace is reachable and raise an exception otherwise.

        Note: It is not necessary to call this method before calling other operations. It
              serves primarily as a simple check to ensure that the workspace is reachable
              and credentials are correct.
        """
        _ = api_util.get_workspace(
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        print(f"Successfully connected to workspace: {self.workspace_url}")

    # Get sources, destinations, and connections

    def get_connection(
        self,
        connection_id: str,
    ) -> CloudConnection:
        """Get a connection by ID.

        This method does not fetch data from the API. It returns a `CloudConnection` object,
        which will be loaded lazily as needed.
        """
        return CloudConnection(
            workspace=self,
            connection_id=connection_id,
        )

    def get_source(
        self,
        source_id: str,
    ) -> CloudSource:
        """Get a source by ID.

        This method does not fetch data from the API. It returns a `CloudSource` object,
        which will be loaded lazily as needed.
        """
        return CloudSource(
            workspace=self,
            connector_id=source_id,
        )

    def get_destination(
        self,
        destination_id: str,
    ) -> CloudDestination:
        """Get a destination by ID.

        This method does not fetch data from the API. It returns a `CloudDestination` object,
        which will be loaded lazily as needed.
        """
        return CloudDestination(
            workspace=self,
            connector_id=destination_id,
        )

    # Deploy sources and destinations

    def deploy_source(
        self,
        name: str,
        source: Source,
        *,
        unique: bool = True,
        random_name_suffix: bool = False,
    ) -> CloudSource:
        """Deploy a source to the workspace.

        Returns the newly deployed source.

        Args:
            name: The name to use when deploying.
            source: The source object to deploy.
            unique: Whether to require a unique name. If `True`, duplicate names
                are not allowed. Defaults to `True`.
            random_name_suffix: Whether to append a random suffix to the name.
        """
        source_config_dict = source._hydrated_config.copy()  # noqa: SLF001 (non-public API)
        source_config_dict["sourceType"] = source.name.replace("source-", "")

        if random_name_suffix:
            name += f" (ID: {text_util.generate_random_suffix()})"

        if unique:
            existing = self.list_sources(name=name)
            if existing:
                raise exc.AirbyteDuplicateResourcesError(
                    resource_type="source",
                    resource_name=name,
                )

        deployed_source = api_util.create_source(
            name=name,
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            config=source_config_dict,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return CloudSource(
            workspace=self,
            connector_id=deployed_source.source_id,
        )

    def deploy_destination(
        self,
        name: str,
        destination: Destination | dict[str, Any],
        *,
        unique: bool = True,
        random_name_suffix: bool = False,
    ) -> CloudDestination:
        """Deploy a destination to the workspace.

        Returns the newly deployed destination ID.

        Args:
            name: The name to use when deploying.
            destination: The destination to deploy. Can be a local Airbyte `Destination` object or a
                dictionary of configuration values.
            unique: Whether to require a unique name. If `True`, duplicate names
                are not allowed. Defaults to `True`.
            random_name_suffix: Whether to append a random suffix to the name.
        """
        if isinstance(destination, Destination):
            destination_conf_dict = destination._hydrated_config.copy()  # noqa: SLF001 (non-public API)
            destination_conf_dict["destinationType"] = destination.name.replace("destination-", "")
            # raise ValueError(destination_conf_dict)
        else:
            destination_conf_dict = destination.copy()
            if "destinationType" not in destination_conf_dict:
                raise exc.PyAirbyteInputError(
                    message="Missing `destinationType` in configuration dictionary.",
                )

        if random_name_suffix:
            name += f" (ID: {text_util.generate_random_suffix()})"

        if unique:
            existing = self.list_destinations(name=name)
            if existing:
                raise exc.AirbyteDuplicateResourcesError(
                    resource_type="destination",
                    resource_name=name,
                )

        deployed_destination = api_util.create_destination(
            name=name,
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            config=destination_conf_dict,  # Wants a dataclass but accepts dict
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return CloudDestination(
            workspace=self,
            connector_id=deployed_destination.destination_id,
        )

    def permanently_delete_source(
        self,
        source: str | CloudSource,
    ) -> None:
        """Delete a source from the workspace.

        You can pass either the source ID `str` or a deployed `Source` object.
        """
        if not isinstance(source, (str, CloudSource)):
            raise exc.PyAirbyteInputError(
                message="Invalid source type.",
                input_value=type(source).__name__,
            )

        api_util.delete_source(
            source_id=source.connector_id if isinstance(source, CloudSource) else source,
            api_root=self.api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

    # Deploy and delete destinations

    def permanently_delete_destination(
        self,
        destination: str | CloudDestination,
    ) -> None:
        """Delete a deployed destination from the workspace.

        You can pass either the `Cache` class or the deployed destination ID as a `str`.
        """
        if not isinstance(destination, (str, CloudDestination)):
            raise exc.PyAirbyteInputError(
                message="Invalid destination type.",
                input_value=type(destination).__name__,
            )

        api_util.delete_destination(
            destination_id=(
                destination if isinstance(destination, str) else destination.destination_id
            ),
            api_root=self.api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

    # Deploy and delete connections

    def deploy_connection(
        self,
        connection_name: str,
        *,
        source: CloudSource | str,
        selected_streams: list[str],
        destination: CloudDestination | str,
        table_prefix: str | None = None,
    ) -> CloudConnection:
        """Create a new connection between an already deployed source and destination.

        Returns the newly deployed connection object.

        Args:
            connection_name: The name of the connection.
            source: The deployed source. You can pass a source ID or a CloudSource object.
            destination: The deployed destination. You can pass a destination ID or a
                CloudDestination object.
            table_prefix: Optional. The table prefix to use when syncing to the destination.
            selected_streams: The selected stream names to sync within the connection.
        """
        if not selected_streams:
            raise exc.PyAirbyteInputError(
                guidance="You must provide `selected_streams` when creating a connection."
            )

        source_id: str = source if isinstance(source, str) else source.connector_id
        destination_id: str = (
            destination if isinstance(destination, str) else destination.connector_id
        )

        deployed_connection = api_util.create_connection(
            name=connection_name,
            source_id=source_id,
            destination_id=destination_id,
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            selected_stream_names=selected_streams,
            prefix=table_prefix or "",
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

        return CloudConnection(
            workspace=self,
            connection_id=deployed_connection.connection_id,
            source=deployed_connection.source_id,
            destination=deployed_connection.destination_id,
        )

    def permanently_delete_connection(
        self,
        connection: str | CloudConnection,
        *,
        cascade_delete_source: bool = False,
        cascade_delete_destination: bool = False,
    ) -> None:
        """Delete a deployed connection from the workspace."""
        if connection is None:
            raise ValueError("No connection ID provided.")

        if isinstance(connection, str):
            connection = CloudConnection(
                workspace=self,
                connection_id=connection,
            )

        api_util.delete_connection(
            connection_id=connection.connection_id,
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

        if cascade_delete_source:
            self.permanently_delete_source(source=connection.source_id)
        if cascade_delete_destination:
            self.permanently_delete_destination(destination=connection.destination_id)

    # List sources, destinations, and connections

    def list_connections(
        self,
        name: str | None = None,
        *,
        name_filter: Callable | None = None,
    ) -> list[CloudConnection]:
        """List connections by name in the workspace.

        TODO: Add pagination support
        """
        connections = api_util.list_connections(
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            name=name,
            name_filter=name_filter,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return [
            CloudConnection._from_connection_response(  # noqa: SLF001 (non-public API)
                workspace=self,
                connection_response=connection,
            )
            for connection in connections
            if name is None or connection.name == name
        ]

    def list_sources(
        self,
        name: str | None = None,
        *,
        name_filter: Callable | None = None,
    ) -> list[CloudSource]:
        """List all sources in the workspace.

        TODO: Add pagination support
        """
        sources = api_util.list_sources(
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            name=name,
            name_filter=name_filter,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return [
            CloudSource._from_source_response(  # noqa: SLF001 (non-public API)
                workspace=self,
                source_response=source,
            )
            for source in sources
            if name is None or source.name == name
        ]

    def list_destinations(
        self,
        name: str | None = None,
        *,
        name_filter: Callable | None = None,
    ) -> list[CloudDestination]:
        """List all destinations in the workspace.

        TODO: Add pagination support
        """
        destinations = api_util.list_destinations(
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            name=name,
            name_filter=name_filter,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return [
            CloudDestination._from_destination_response(  # noqa: SLF001 (non-public API)
                workspace=self,
                destination_response=destination,
            )
            for destination in destinations
            if name is None or destination.name == name
        ]

    def publish_custom_source_definition(
        self,
        name: str,
        *,
        manifest_yaml: dict[str, Any] | Path | str | None = None,
        docker_image: str | None = None,
        docker_tag: str | None = None,
        documentation_url: str | None = None,
        unique: bool = True,
        pre_validate: bool = True,
    ) -> CloudCustomSourceDefinition:
        """Publish a custom source connector definition.

        You must specify EITHER manifest_yaml (for YAML connectors) OR both docker_image
        and docker_tag (for Docker connectors), but not both.

        Args:
            name: Display name for the connector definition
            manifest_yaml: Low-code CDK manifest (dict, Path to YAML file, or YAML string)
            docker_image: Docker repository (e.g., 'airbyte/source-custom')
            docker_tag: Docker image tag (e.g., '1.0.0')
            documentation_url: Optional URL to connector documentation (Docker only)
            unique: Whether to enforce name uniqueness
            pre_validate: Whether to validate manifest client-side (YAML only)

        Returns:
            CloudCustomSourceDefinition object representing the created definition

        Raises:
            PyAirbyteInputError: If both or neither of manifest_yaml and docker_image provided
            AirbyteDuplicateResourcesError: If unique=True and name already exists
        """
        is_yaml = manifest_yaml is not None
        is_docker = docker_image is not None

        if is_yaml == is_docker:
            raise exc.PyAirbyteInputError(
                message=(
                    "Must specify EITHER manifest_yaml (for YAML connectors) OR "
                    "docker_image + docker_tag (for Docker connectors), but not both"
                ),
                context={
                    "manifest_yaml_provided": is_yaml,
                    "docker_image_provided": is_docker,
                },
            )

        if is_docker and docker_tag is None:
            raise exc.PyAirbyteInputError(
                message="docker_tag is required when docker_image is specified",
                context={"docker_image": docker_image},
            )

        if unique:
            existing = self.list_custom_source_definitions(
                name=name,
                custom_connector_type="yaml" if is_yaml else "docker",
            )
            if existing:
                raise exc.AirbyteDuplicateResourcesError(
                    resource_type="custom_source_definition",
                    resource_name=name,
                )

        if is_yaml:
            manifest_dict: dict[str, Any]
            if isinstance(manifest_yaml, Path):
                manifest_dict = yaml.safe_load(manifest_yaml.read_text())
            elif isinstance(manifest_yaml, str):
                manifest_dict = yaml.safe_load(manifest_yaml)
            elif manifest_yaml is not None:
                manifest_dict = manifest_yaml
            else:
                raise exc.PyAirbyteInputError(
                    message="manifest_yaml is required for YAML connectors",
                    context={"name": name},
                )

            if pre_validate:
                api_util.validate_yaml_manifest(manifest_dict, raise_on_error=True)

            result = api_util.create_custom_yaml_source_definition(
                name=name,
                workspace_id=self.workspace_id,
                manifest=manifest_dict,
                api_root=self.api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            return CloudCustomSourceDefinition._from_yaml_response(self, result)  # noqa: SLF001
        result = api_util.create_custom_docker_source_definition(
            name=name,
            docker_repository=docker_image,  # type: ignore[arg-type]
            docker_image_tag=docker_tag,  # type: ignore[arg-type]
            workspace_id=self.workspace_id,
            documentation_url=documentation_url,
            api_root=self.api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return CloudCustomSourceDefinition._from_docker_response(self, result)  # noqa: SLF001

    def list_custom_source_definitions(
        self,
        *,
        name: str | None = None,
        custom_connector_type: Literal["yaml", "docker"] | None = None,
    ) -> list[CloudCustomSourceDefinition]:
        """List custom source connector definitions.

        Args:
            name: Filter by exact name match
            custom_connector_type: Filter by connector type ("yaml" or "docker")

        Returns:
            List of CloudCustomSourceDefinition objects
        """
        result: list[CloudCustomSourceDefinition] = []

        if custom_connector_type is None or custom_connector_type == "yaml":
            yaml_definitions = api_util.list_custom_yaml_source_definitions(
                workspace_id=self.workspace_id,
                api_root=self.api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            result.extend(
                CloudCustomSourceDefinition._from_yaml_response(self, d)  # noqa: SLF001
                for d in yaml_definitions
                if name is None or d.name == name
            )

        if custom_connector_type is None or custom_connector_type == "docker":
            docker_definitions = api_util.list_custom_docker_source_definitions(
                workspace_id=self.workspace_id,
                api_root=self.api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            result.extend(
                CloudCustomSourceDefinition._from_docker_response(self, d)  # noqa: SLF001
                for d in docker_definitions
                if name is None or d.name == name
            )

        return result

    def get_custom_source_definition(
        self,
        definition_id: str,
    ) -> CloudCustomSourceDefinition:
        """Get a specific custom source definition by ID.

        This method will attempt to fetch as a YAML definition first, then as a Docker
        definition if not found.

        Args:
            definition_id: The definition ID

        Returns:
            CloudCustomSourceDefinition object
        """
        try:
            result = api_util.get_custom_yaml_source_definition(
                workspace_id=self.workspace_id,
                definition_id=definition_id,
                api_root=self.api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            return CloudCustomSourceDefinition._from_yaml_response(self, result)  # noqa: SLF001
        except Exception:
            result = api_util.get_custom_docker_source_definition(
                workspace_id=self.workspace_id,
                definition_id=definition_id,
                api_root=self.api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            return CloudCustomSourceDefinition._from_docker_response(self, result)  # noqa: SLF001

    def update_custom_source_definition(
        self,
        definition_id: str,
        *,
        name: str | None = None,
        manifest_yaml: dict[str, Any] | Path | str | None = None,
        docker_tag: str | None = None,
        pre_validate: bool = True,
    ) -> CloudCustomSourceDefinition:
        """Update a custom source definition.

        For YAML connectors: can update manifest_yaml
        For Docker connectors: can update name and/or docker_tag

        Args:
            definition_id: The definition ID to update
            name: New display name (Docker connectors only)
            manifest_yaml: New manifest (YAML connectors only)
            docker_tag: New Docker tag (Docker connectors only)
            pre_validate: Whether to validate manifest (YAML only)

        Returns:
            Updated CloudCustomSourceDefinition object
        """
        definition = self.get_custom_source_definition(definition_id)

        if definition.connector_type == "yaml":
            if manifest_yaml is None:
                raise exc.PyAirbyteInputError(
                    message="manifest_yaml is required for updating YAML connectors",
                    context={"definition_id": definition_id},
                )

            manifest_dict: dict[str, Any]
            if isinstance(manifest_yaml, Path):
                manifest_dict = yaml.safe_load(manifest_yaml.read_text())
            elif isinstance(manifest_yaml, str):
                manifest_dict = yaml.safe_load(manifest_yaml)
            else:
                manifest_dict = manifest_yaml

            if pre_validate:
                api_util.validate_yaml_manifest(manifest_dict, raise_on_error=True)

            result = api_util.update_custom_yaml_source_definition(
                workspace_id=self.workspace_id,
                definition_id=definition_id,
                manifest=manifest_dict,
                api_root=self.api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            return CloudCustomSourceDefinition._from_yaml_response(self, result)  # noqa: SLF001
        if name is None or docker_tag is None:
            raise exc.PyAirbyteInputError(
                message="Both name and docker_tag are required for updating Docker connectors",
                context={"definition_id": definition_id},
            )

        result = api_util.update_custom_docker_source_definition(
            workspace_id=self.workspace_id,
            definition_id=definition_id,
            name=name,
            docker_image_tag=docker_tag,
            api_root=self.api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return CloudCustomSourceDefinition._from_docker_response(self, result)  # noqa: SLF001

    def permanently_delete_custom_source_definition(
        self,
        definition_id: str,
    ) -> None:
        """Permanently delete a custom source definition.

        Args:
            definition_id: The definition ID to delete
        """
        definition = self.get_custom_source_definition(definition_id)

        if definition.connector_type == "yaml":
            api_util.delete_custom_yaml_source_definition(
                workspace_id=self.workspace_id,
                definition_id=definition_id,
                api_root=self.api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
        else:
            api_util.delete_custom_docker_source_definition(
                workspace_id=self.workspace_id,
                definition_id=definition_id,
                api_root=self.api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )

    def publish_custom_destination_definition(
        self,
        name: str,
        *,
        docker_image: str,
        docker_tag: str,
        documentation_url: str | None = None,
        unique: bool = True,
    ) -> CloudCustomDestinationDefinition:
        """Publish a custom destination connector definition.

        Currently only Docker-based destinations are supported.

        Args:
            name: Display name for the connector definition
            docker_image: Docker repository (e.g., 'airbyte/destination-custom')
            docker_tag: Docker image tag (e.g., '1.0.0')
            documentation_url: Optional URL to connector documentation
            unique: Whether to enforce name uniqueness

        Returns:
            CloudCustomDestinationDefinition object representing the created definition
        """
        if unique:
            existing = self.list_custom_destination_definitions(name=name)
            if existing:
                raise exc.AirbyteDuplicateResourcesError(
                    resource_type="custom_destination_definition",
                    resource_name=name,
                )

        result = api_util.create_custom_docker_destination_definition(
            name=name,
            docker_repository=docker_image,
            docker_image_tag=docker_tag,
            workspace_id=self.workspace_id,
            documentation_url=documentation_url,
            api_root=self.api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return CloudCustomDestinationDefinition._from_docker_response(self, result)  # noqa: SLF001

    def list_custom_destination_definitions(
        self,
        *,
        name: str | None = None,
    ) -> list[CloudCustomDestinationDefinition]:
        """List custom destination connector definitions.

        Args:
            name: Filter by exact name match

        Returns:
            List of CloudCustomDestinationDefinition objects
        """
        definitions = api_util.list_custom_docker_destination_definitions(
            workspace_id=self.workspace_id,
            api_root=self.api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

        return [
            CloudCustomDestinationDefinition._from_docker_response(self, d)  # noqa: SLF001
            for d in definitions
            if name is None or d.name == name
        ]

    def get_custom_destination_definition(
        self,
        definition_id: str,
    ) -> CloudCustomDestinationDefinition:
        """Get a specific custom destination definition by ID.

        Args:
            definition_id: The definition ID

        Returns:
            CloudCustomDestinationDefinition object
        """
        result = api_util.get_custom_docker_destination_definition(
            workspace_id=self.workspace_id,
            definition_id=definition_id,
            api_root=self.api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return CloudCustomDestinationDefinition._from_docker_response(self, result)  # noqa: SLF001

    def update_custom_destination_definition(
        self,
        definition_id: str,
        *,
        name: str,
        docker_tag: str,
    ) -> CloudCustomDestinationDefinition:
        """Update a custom destination definition.

        Args:
            definition_id: The definition ID to update
            name: New display name
            docker_tag: New Docker tag

        Returns:
            Updated CloudCustomDestinationDefinition object
        """
        result = api_util.update_custom_docker_destination_definition(
            workspace_id=self.workspace_id,
            definition_id=definition_id,
            name=name,
            docker_image_tag=docker_tag,
            api_root=self.api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return CloudCustomDestinationDefinition._from_docker_response(self, result)  # noqa: SLF001

    def permanently_delete_custom_destination_definition(
        self,
        definition_id: str,
    ) -> None:
        """Permanently delete a custom destination definition.

        Args:
            definition_id: The definition ID to delete
        """
        api_util.delete_custom_docker_destination_definition(
            workspace_id=self.workspace_id,
            definition_id=definition_id,
            api_root=self.api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
