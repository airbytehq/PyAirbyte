# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte classes and methods for interacting with the Airbyte Cloud API.

By overriding `api_root`, you can use this module to interact with self-managed Airbyte instances,
both OSS and Enterprise.

## Usage Examples

### Authentication with OAuth2 Client Credentials

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


Alternatively, authenticate using a bearer token directly:

```python
import airbyte as ab
from airbyte import cloud

workspace = cloud.CloudWorkspace(
    workspace_id="...",
    bearer_token="...",
)

sources = workspace.list_sources()
```


Generate a bearer token from client credentials:

```python
workspace = cloud.CloudWorkspace(
    workspace_id="...",
    client_id="...",
    client_secret="...",
)

token = workspace.create_oauth_token()
print(f"Bearer token: {token}")
```
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, overload

import yaml

from airbyte import exceptions as exc
from airbyte._util import api_util, text_util
from airbyte._util.api_util import get_web_url_root
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.connectors import (
    CloudDestination,
    CloudSource,
    CustomCloudSourceDefinition,
)
from airbyte.destinations.base import Destination
from airbyte.exceptions import AirbyteError
from airbyte.secrets.base import SecretString


if TYPE_CHECKING:
    from collections.abc import Callable

    from airbyte.sources.base import Source


@dataclass
class CloudOrganization:
    """Information about an organization in Airbyte Cloud.

    This is a minimal value object returned by CloudWorkspace.get_organization().
    """

    organization_id: str
    """The organization ID."""

    organization_name: str | None = None
    """Display name of the organization."""


@dataclass  # noqa: PLR0904
class CloudWorkspace:
    """A remote workspace on the Airbyte Cloud.

    By overriding `api_root`, you can use this class to interact with self-managed Airbyte
    instances, both OSS and Enterprise.

    Authentication can be done via either OAuth2 client credentials or bearer token.
    Provide either (client_id + client_secret) OR bearer_token, but not both.
    """

    workspace_id: str
    client_id: SecretString | None = None
    client_secret: SecretString | None = None
    bearer_token: SecretString | None = None
    api_root: str = api_util.CLOUD_API_ROOT

    def __post_init__(self) -> None:
        """Ensure that credentials are handled securely and validate auth method."""
        has_client_creds = self.client_id is not None and self.client_secret is not None
        has_bearer = self.bearer_token is not None

        if has_client_creds and has_bearer:
            raise exc.PyAirbyteInputError(
                message="Cannot provide both client credentials and bearer token. "
                "Please use only one authentication method.",
            )

        if not has_client_creds and not has_bearer:
            raise exc.PyAirbyteInputError(
                message="Must provide either client credentials (client_id + client_secret) "
                "or bearer_token for authentication.",
            )

        if self.client_id is not None:
            self.client_id = SecretString(self.client_id)
        if self.client_secret is not None:
            self.client_secret = SecretString(self.client_secret)
        if self.bearer_token is not None:
            self.bearer_token = SecretString(self.bearer_token)

    @property
    def workspace_url(self) -> str | None:
        """The web URL of the workspace."""
        return f"{get_web_url_root(self.api_root)}/workspaces/{self.workspace_id}"

    @cached_property
    def _organization_info(self) -> dict[str, Any]:
        """Fetch and cache organization info for this workspace.

        Uses the Config API endpoint for an efficient O(1) lookup.
        This is an internal method; use get_organization() for public access.
        """
        return api_util.get_workspace_organization_info(
            workspace_id=self.workspace_id,
            api_root=self.api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

    @overload
    def get_organization(self) -> CloudOrganization: ...

    @overload
    def get_organization(
        self,
        *,
        raise_on_error: Literal[True],
    ) -> CloudOrganization: ...

    @overload
    def get_organization(
        self,
        *,
        raise_on_error: Literal[False],
    ) -> CloudOrganization | None: ...

    def get_organization(
        self,
        *,
        raise_on_error: bool = True,
    ) -> CloudOrganization | None:
        """Get the organization this workspace belongs to.

        Fetching organization info requires ORGANIZATION_READER permissions on the organization,
        which may not be available with workspace-scoped credentials.

        Args:
            raise_on_error: If True (default), raises AirbyteError on permission or API errors.
                If False, returns None instead of raising.

        Returns:
            CloudOrganization object with organization_id and organization_name,
            or None if raise_on_error=False and an error occurred.

        Raises:
            AirbyteError: If raise_on_error=True and the organization info cannot be fetched
                (e.g., due to insufficient permissions or missing data).
        """
        try:
            info = self._organization_info
        except AirbyteError:
            if raise_on_error:
                raise
            return None

        organization_id = info.get("organizationId")
        organization_name = info.get("organizationName")

        # Validate that both organization_id and organization_name are non-null and non-empty
        if not organization_id or not organization_name:
            if raise_on_error:
                raise AirbyteError(
                    message="Organization info is incomplete.",
                    context={
                        "organization_id": organization_id,
                        "organization_name": organization_name,
                    },
                )
            return None

        return CloudOrganization(
            organization_id=organization_id,
            organization_name=organization_name,
        )

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
            client_id=self.client_id,  # type: ignore[arg-type]
            client_secret=self.client_secret,  # type: ignore[arg-type]
            bearer_token=self.bearer_token,
        )
        print(f"Successfully connected to workspace: {self.workspace_url}")

    def create_oauth_token(self) -> SecretString:
        """Create and return an OAuth2 bearer token.

        This method generates a new bearer token using the workspace's client credentials.
        The token can be used for subsequent API calls or shared with other applications.

        Returns:
            SecretString containing the bearer token

        Raises:
            PyAirbyteInputError: If the workspace is configured with a bearer token
                instead of client credentials
        """
        if self.bearer_token is not None:
            raise exc.PyAirbyteInputError(
                message=(
                    "Cannot create OAuth token when workspace is configured with a bearer token. "
                    "This method requires client credentials (client_id + client_secret)."
                ),
            )

        return api_util.get_bearer_token(
            client_id=self.client_id,  # type: ignore[arg-type]
            client_secret=self.client_secret,  # type: ignore[arg-type]
            api_root=self.api_root,
        )

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
            client_id=self.client_id,  # type: ignore[arg-type]
            client_secret=self.client_secret,  # type: ignore[arg-type]
            bearer_token=self.bearer_token,
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
            client_id=self.client_id,  # type: ignore[arg-type]
            client_secret=self.client_secret,  # type: ignore[arg-type]
            bearer_token=self.bearer_token,
        )
        return CloudDestination(
            workspace=self,
            connector_id=deployed_destination.destination_id,
        )

    def permanently_delete_source(
        self,
        source: str | CloudSource,
        *,
        safe_mode: bool = True,
    ) -> None:
        """Delete a source from the workspace.

        You can pass either the source ID `str` or a deployed `Source` object.

        Args:
            source: The source ID or CloudSource object to delete
            safe_mode: If True, requires the source name to contain "delete-me" or "deleteme"
                (case insensitive) to prevent accidental deletion. Defaults to True.
        """
        if not isinstance(source, (str, CloudSource)):
            raise exc.PyAirbyteInputError(
                message="Invalid source type.",
                input_value=type(source).__name__,
            )

        api_util.delete_source(
            source_id=source.connector_id if isinstance(source, CloudSource) else source,
            source_name=source.name if isinstance(source, CloudSource) else None,
            api_root=self.api_root,
            client_id=self.client_id,  # type: ignore[arg-type]
            client_secret=self.client_secret,  # type: ignore[arg-type]
            bearer_token=self.bearer_token,
            safe_mode=safe_mode,
        )

    # Deploy and delete destinations

    def permanently_delete_destination(
        self,
        destination: str | CloudDestination,
        *,
        safe_mode: bool = True,
    ) -> None:
        """Delete a deployed destination from the workspace.

        You can pass either the `Cache` class or the deployed destination ID as a `str`.

        Args:
            destination: The destination ID or CloudDestination object to delete
            safe_mode: If True, requires the destination name to contain "delete-me" or "deleteme"
                (case insensitive) to prevent accidental deletion. Defaults to True.
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
            destination_name=(
                destination.name if isinstance(destination, CloudDestination) else None
            ),
            api_root=self.api_root,
            client_id=self.client_id,  # type: ignore[arg-type]
            client_secret=self.client_secret,  # type: ignore[arg-type]
            bearer_token=self.bearer_token,
            safe_mode=safe_mode,
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
            client_id=self.client_id,  # type: ignore[arg-type]
            client_secret=self.client_secret,  # type: ignore[arg-type]
            bearer_token=self.bearer_token,
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
        safe_mode: bool = True,
    ) -> None:
        """Delete a deployed connection from the workspace.

        Args:
            connection: The connection ID or CloudConnection object to delete
            cascade_delete_source: If True, also delete the source after deleting the connection
            cascade_delete_destination: If True, also delete the destination after deleting
                the connection
            safe_mode: If True, requires the connection name to contain "delete-me" or "deleteme"
                (case insensitive) to prevent accidental deletion. Defaults to True. Also applies
                to cascade deletes.
        """
        if connection is None:
            raise ValueError("No connection ID provided.")

        if isinstance(connection, str):
            connection = CloudConnection(
                workspace=self,
                connection_id=connection,
            )

        api_util.delete_connection(
            connection_id=connection.connection_id,
            connection_name=connection.name,
            api_root=self.api_root,
            workspace_id=self.workspace_id,
            client_id=self.client_id,  # type: ignore[arg-type]
            client_secret=self.client_secret,  # type: ignore[arg-type]
            bearer_token=self.bearer_token,
            safe_mode=safe_mode,
        )

        if cascade_delete_source:
            self.permanently_delete_source(
                source=connection.source_id,
                safe_mode=safe_mode,
            )
        if cascade_delete_destination:
            self.permanently_delete_destination(
                destination=connection.destination_id,
                safe_mode=safe_mode,
            )

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
            client_id=self.client_id,  # type: ignore[arg-type]
            client_secret=self.client_secret,  # type: ignore[arg-type]
            bearer_token=self.bearer_token,
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
            client_id=self.client_id,  # type: ignore[arg-type]
            client_secret=self.client_secret,  # type: ignore[arg-type]
            bearer_token=self.bearer_token,
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
            client_id=self.client_id,  # type: ignore[arg-type]
            client_secret=self.client_secret,  # type: ignore[arg-type]
            bearer_token=self.bearer_token,
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
        unique: bool = True,
        pre_validate: bool = True,
        testing_values: dict[str, Any] | None = None,
    ) -> CustomCloudSourceDefinition:
        """Publish a custom source connector definition.

        You must specify EITHER manifest_yaml (for YAML connectors) OR both docker_image
        and docker_tag (for Docker connectors), but not both.

        Args:
            name: Display name for the connector definition
            manifest_yaml: Low-code CDK manifest (dict, Path to YAML file, or YAML string)
            docker_image: Docker repository (e.g., 'airbyte/source-custom')
            docker_tag: Docker image tag (e.g., '1.0.0')
            unique: Whether to enforce name uniqueness
            pre_validate: Whether to validate manifest client-side (YAML only)
            testing_values: Optional configuration values to use for testing in the
                Connector Builder UI. If provided, these values are stored as the complete
                testing values object for the connector builder project (replaces any existing
                values), allowing immediate test read operations.

        Returns:
            CustomCloudSourceDefinition object representing the created definition

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
                definition_type="yaml" if is_yaml else "docker",
            )
            if any(d.name == name for d in existing):
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
                client_id=self.client_id,  # type: ignore[arg-type]
                client_secret=self.client_secret,  # type: ignore[arg-type]
                bearer_token=self.bearer_token,
            )
            custom_definition = CustomCloudSourceDefinition._from_yaml_response(  # noqa: SLF001
                self, result
            )

            # Set testing values if provided
            if testing_values is not None:
                custom_definition.set_testing_values(testing_values)

            return custom_definition

        raise NotImplementedError(
            "Docker custom source definitions are not yet supported. "
            "Only YAML manifest-based custom sources are currently available."
        )

    def list_custom_source_definitions(
        self,
        *,
        definition_type: Literal["yaml", "docker"],
    ) -> list[CustomCloudSourceDefinition]:
        """List custom source connector definitions.

        Args:
            definition_type: Connector type to list ("yaml" or "docker"). Required.

        Returns:
            List of CustomCloudSourceDefinition objects matching the specified type
        """
        if definition_type == "yaml":
            yaml_definitions = api_util.list_custom_yaml_source_definitions(
                workspace_id=self.workspace_id,
                api_root=self.api_root,
                client_id=self.client_id,  # type: ignore[arg-type]
                client_secret=self.client_secret,  # type: ignore[arg-type]
                bearer_token=self.bearer_token,
            )
            return [
                CustomCloudSourceDefinition._from_yaml_response(self, d)  # noqa: SLF001
                for d in yaml_definitions
            ]

        raise NotImplementedError(
            "Docker custom source definitions are not yet supported. "
            "Only YAML manifest-based custom sources are currently available."
        )

    def get_custom_source_definition(
        self,
        definition_id: str,
        *,
        definition_type: Literal["yaml", "docker"],
    ) -> CustomCloudSourceDefinition:
        """Get a specific custom source definition by ID.

        Args:
            definition_id: The definition ID
            definition_type: Connector type ("yaml" or "docker"). Required.

        Returns:
            CustomCloudSourceDefinition object
        """
        if definition_type == "yaml":
            result = api_util.get_custom_yaml_source_definition(
                workspace_id=self.workspace_id,
                definition_id=definition_id,
                api_root=self.api_root,
                client_id=self.client_id,  # type: ignore[arg-type]
                client_secret=self.client_secret,  # type: ignore[arg-type]
                bearer_token=self.bearer_token,
            )
            return CustomCloudSourceDefinition._from_yaml_response(self, result)  # noqa: SLF001

        raise NotImplementedError(
            "Docker custom source definitions are not yet supported. "
            "Only YAML manifest-based custom sources are currently available."
        )
