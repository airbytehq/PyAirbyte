# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte classes and methods for interacting with the Airbyte Cloud API.

By overriding `api_root`, you can use this module to interact with self-managed Airbyte instances,
both OSS and Enterprise.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from airbyte import exceptions as exc
from airbyte._util import api_util, text_util
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.connectors import CloudDestination, CloudSource
from airbyte.cloud.sync_results import SyncResult


if TYPE_CHECKING:
    from collections.abc import Callable

    from airbyte.destinations.base import Destination
    from airbyte.sources.base import Source


@dataclass
class CloudWorkspace:
    """A remote workspace on the Airbyte Cloud.

    By overriding `api_root`, you can use this class to interact with self-managed Airbyte
    instances, both OSS and Enterprise.
    """

    workspace_id: str
    api_key: str
    api_root: str = api_util.CLOUD_API_ROOT

    @property
    def workspace_url(self) -> str | None:
        """The URL of the workspace."""
        return f"{self.api_root}/workspaces/{self.workspace_id}"

    # Test connection and creds

    def connect(self) -> None:
        """Check that the workspace is reachable and raise an exception otherwise.

        Note: It is not necessary to call this method before calling other operations. It
              serves primarily as a simple check to ensure that the workspace is reachable
              and credentials are correct.
        """
        _ = api_util.get_workspace(
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
        )
        print(f"Successfully connected to workspace: {self.workspace_url}")

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
        source_configuration = source.get_config().copy()
        source_configuration["sourceType"] = source.name.replace("source-", "")

        if random_name_suffix:
            name += f" (ID: {text_util.generate_random_suffix()})"

        if unique:
            existing = self.list_sources(name=name)
            if existing:
                raise exc.AirbyteDuplicateResourcesError(
                    resource_type="destination",
                    resource_name=name,
                )

        deployed_source = api_util.create_source(
            name=name,
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
            config=source_configuration,
        )
        return CloudSource(
            workspace=self,
            connector_id=deployed_source.source_id,
        )

    def deploy_destination(
        self,
        name: str,
        destination: Destination,
        *,
        unique: bool = True,
        random_name_suffix: bool = False,
    ) -> CloudDestination:
        """Deploy a destination to the workspace.

        Returns the newly deployed destination ID.

        Args:
            name: The name to use when deploying.
            destination: The destination object to deploy.
            unique: Whether to require a unique name. If `True`, duplicate names
                are not allowed. Defaults to `True`.
            random_name_suffix: Whether to append a random suffix to the name.
        """
        destination_configuration = destination.get_config().copy()
        destination_configuration["destinationType"] = destination.name.replace("destination-", "")

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
            api_key=self.api_key,
            workspace_id=self.workspace_id,
            config=destination_configuration,
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
        if not isinstance(source, str | CloudSource):
            raise exc.PyAirbyteInputError(
                message="Invalid source type.",
                input_value=type(source).__name__,
            )

        api_util.delete_source(
            source_id=source.connector_id if isinstance(source, CloudSource) else source,
            api_root=self.api_root,
            api_key=self.api_key,
        )

    # Deploy and delete destinations

    def permanently_delete_destination(
        self,
        destination: str | CloudDestination,
    ) -> None:
        """Delete a deployed destination from the workspace.

        You can pass either the `Cache` class or the deployed destination ID as a `str`.
        """
        if not isinstance(destination, str | CloudDestination):
            raise exc.PyAirbyteInputError(
                message="Invalid destination type.",
                input_value=type(destination).__name__,
            )

        api_util.delete_destination(
            destination_id=(
                destination if isinstance(destination, str) else destination.destination_id
            ),
            api_root=self.api_root,
            api_key=self.api_key,
        )

    # Deploy and delete connections

    def deploy_connection(
        self,
        connection_name: str,
        *,
        source: CloudSource | str,
        destination: CloudDestination | str,
        table_prefix: str | None = None,
        selected_streams: list[str] | None = None,
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

        # Resolve source ID
        source_id: str
        source_id = source if isinstance(source, str) else source.connector_id

        # destination is already deployed
        destination_id = destination if isinstance(destination, str) else destination.connector_id
        if not selected_streams:
            raise exc.PyAirbyteInputError(
                guidance=(
                    "You must provide `selected_streams` when creating a connection "
                    "from an existing destination."
                )
            )

        deployed_connection = api_util.create_connection(
            name=connection_name,
            source_id=source_id,
            destination_id=destination_id,
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
            selected_stream_names=selected_streams,
            prefix=table_prefix or "",
        )

        return CloudConnection(
            workspace=self,
            connection_id=deployed_connection.connection_id,
            source=deployed_connection.source_id,
            destination=deployed_connection.destination_id,
        )

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

    def permanently_delete_connection(
        self,
        connection: str | CloudConnection,
        *,
        delete_source: bool = False,
        delete_destination: bool = False,
    ) -> None:
        """Delete a deployed connection from the workspace."""
        if connection is None:
            raise ValueError("No connection ID provided.")  # noqa: TRY003

        if isinstance(connection, str):
            connection = CloudConnection(
                workspace=self,
                connection_id=connection,
            )

        api_util.delete_connection(
            connection_id=connection.connection_id,
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
        )
        if delete_source:
            self.permanently_delete_source(source=connection.source_id)

        if delete_destination:
            self.permanently_delete_destination(destination=connection.destination_id)

    # Run syncs

    def run_sync(
        self,
        connection_id: str,
        *,
        wait: bool = True,
        wait_timeout: int = 300,
    ) -> SyncResult:
        """Run a sync on a deployed connection."""
        connection = CloudConnection(
            workspace=self,
            connection_id=connection_id,
        )
        return connection.run_sync(wait=wait, wait_timeout=wait_timeout)

    # Get sync results and previous sync logs

    def get_sync_result(
        self,
        connection_id: str,
        job_id: str | None = None,
    ) -> SyncResult | None:
        """Get the sync result for a connection job.

        If `job_id` is not provided, the most recent sync job will be used.

        Returns `None` if job_id is omitted and no previous jobs are found.
        """
        connection = CloudConnection(
            workspace=self,
            connection_id=connection_id,
        )
        if job_id is None:
            results = self.get_previous_sync_logs(
                connection_id=connection_id,
                limit=1,
            )
            if results:
                return results[0]

            return None
        connection = CloudConnection(
            workspace=self,
            connection_id=connection_id,
        )
        return SyncResult(
            workspace=self,
            connection=connection,
            job_id=job_id,
        )

    def get_previous_sync_logs(
        self,
        connection_id: str,
        *,
        limit: int = 10,
    ) -> list[SyncResult]:
        """Get the previous sync logs for a connection."""
        connection = CloudConnection(
            workspace=self,
            connection_id=connection_id,
        )
        return connection.get_previous_sync_logs(
            limit=limit,
        )

    # List sources, destinations, and connections

    def list_connections(
        self,
        name: str | None = None,
        *,
        name_filter: Callable | None = None,
    ) -> list[CloudConnection]:
        """List connections by name in the workspace."""
        connections = api_util.list_connections(
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
            name=name,
            name_filter=name_filter,
        )
        return [
            CloudConnection(
                workspace=self,
                connection_id=connection.connection_id,
                source=None,
                destination=None,
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
        """List all sources in the workspace."""
        sources = api_util.list_sources(
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
            name=name,
            name_filter=name_filter,
        )
        return [
            CloudSource(
                workspace=self,
                connector_id=source.source_id,
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
        """List all destinations in the workspace."""
        destinations = api_util.list_destinations(
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
            name=name,
            name_filter=name_filter,
        )
        return [
            CloudDestination(
                workspace=self,
                connector_id=destination.destination_id,
            )
            for destination in destinations
            if name is None or destination.name == name
        ]
