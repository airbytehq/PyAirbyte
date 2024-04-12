# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte classes and methods for interacting with the Airbyte Cloud API.

By overriding `api_root`, you can use this module to interact with self-managed Airbyte instances,
both OSS and Enterprise.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from airbyte import exceptions as exc
from airbyte._util.api_util import (
    CLOUD_API_ROOT,
    create_connection,
    create_destination,
    create_source,
    delete_connection,
    delete_destination,
    delete_source,
    get_workspace,
)
from airbyte.cloud._destination_util import get_destination_config_from_cache
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.sync_results import SyncResult
from airbyte.sources.base import Source


if TYPE_CHECKING:
    from airbyte._util.api_imports import DestinationResponse
    from airbyte.caches.base import CacheBase


@dataclass
class CloudWorkspace:
    """A remote workspace on the Airbyte Cloud.

    By overriding `api_root`, you can use this class to interact with self-managed Airbyte
    instances, both OSS and Enterprise.
    """

    workspace_id: str
    api_key: str
    api_root: str = CLOUD_API_ROOT

    @property
    def workspace_url(self) -> str | None:
        return f"{self.api_root}/workspaces/{self.workspace_id}"

    # Test connection and creds

    def connect(self) -> None:
        """Check that the workspace is reachable and raise an exception otherwise.

        Note: It is not necessary to call this method before calling other operations. It
              serves primarily as a simple check to ensure that the workspace is reachable
              and credentials are correct.
        """
        _ = get_workspace(
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
        )
        print(f"Successfully connected to workspace: {self.workspace_url}")

    # Deploy and delete sources

    # TODO: Make this a public API
    def _deploy_source(
        self,
        source: Source,
    ) -> str:
        """Deploy a source to the workspace.

        Returns the newly deployed source ID.
        """
        source_configuration = source.get_config().copy()
        source_configuration["sourceType"] = source.name.replace("source-", "")

        deployed_source = create_source(
            name=f"{source.name.replace('-', ' ').title()} (Deployed by PyAirbyte)",
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
            config=source_configuration,
        )

        # Set the deployment Ids on the source object
        source._deployed_api_root = self.api_root  # noqa: SLF001  # Accessing nn-public API
        source._deployed_workspace_id = self.workspace_id  # noqa: SLF001  # Accessing nn-public API
        source._deployed_source_id = deployed_source.source_id  # noqa: SLF001  # Accessing nn-public API

        return deployed_source.source_id

    def _permanently_delete_source(
        self,
        source: str | Source,
    ) -> None:
        """Delete a source from the workspace.

        You can pass either the source ID `str` or a deployed `Source` object.
        """
        if not isinstance(source, (str, Source)):
            raise ValueError(f"Invalid source type: {type(source)}")  # noqa: TRY004, TRY003

        if isinstance(source, Source):
            if not source._deployed_source_id:  # noqa: SLF001
                raise ValueError("Source has not been deployed.")  # noqa: TRY003

            source_id = source._deployed_source_id  # noqa: SLF001

        elif isinstance(source, str):
            source_id = source

        delete_source(
            source_id=source_id,
            api_root=self.api_root,
            api_key=self.api_key,
        )

    # Deploy and delete destinations

    # TODO: Make this a public API
    def _deploy_cache_as_destination(
        self,
        cache: CacheBase,
    ) -> str:
        """Deploy a cache to the workspace as a new destination.

        Returns the newly deployed destination ID.
        """
        cache_type_name = cache.__class__.__name__.replace("Cache", "")

        deployed_destination: DestinationResponse = create_destination(
            name=f"Destination {cache_type_name} (Deployed by PyAirbyte)",
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
            config=get_destination_config_from_cache(cache),
        )

        # Set the deployment Ids on the source object
        cache._deployed_api_root = self.api_root  # noqa: SLF001  # Accessing nn-public API
        cache._deployed_workspace_id = self.workspace_id  # noqa: SLF001  # Accessing nn-public API
        cache._deployed_destination_id = deployed_destination.destination_id  # noqa: SLF001  # Accessing nn-public API

        return deployed_destination.destination_id

    def _permanently_delete_destination(
        self,
        *,
        destination: str | None = None,
        cache: CacheBase | None = None,
    ) -> None:
        """Delete a deployed destination from the workspace.

        You can pass either the `Cache` class or the deployed destination ID as a `str`.
        """
        if destination is None and cache is None:
            raise ValueError("You must provide either a destination ID or a cache object.")  # noqa: TRY003
        if destination is not None and cache is not None:
            raise ValueError(  # noqa: TRY003
                "You must provide either a destination ID or a cache object, not both."
            )

        if cache:
            if not cache._deployed_destination_id:  # noqa: SLF001
                raise ValueError("Cache has not been deployed.")  # noqa: TRY003

            destination = cache._deployed_destination_id  # noqa: SLF001

        if destination is None:
            raise ValueError("No destination ID provided.")  # noqa: TRY003

        delete_destination(
            destination_id=destination,
            api_root=self.api_root,
            api_key=self.api_key,
        )

    # Deploy and delete connections

    # TODO: Make this a public API
    def _deploy_connection(
        self,
        source: Source | str,
        cache: CacheBase | None = None,
        destination: str | None = None,
        table_prefix: str | None = None,
        selected_streams: list[str] | None = None,
    ) -> CloudConnection:
        """Deploy a source and cache to the workspace as a new connection.

        Returns the newly deployed connection ID as a `str`.

        Args:
            source (Source | str): The source to deploy. You can pass either an already deployed
                source ID `str` or a PyAirbyte `Source` object. If you pass a `Source` object,
                it will be deployed automatically.
            cache (CacheBase, optional): The cache to deploy as a new destination. You can provide
                `cache` or `destination`, but not both.
            destination (str, optional): The destination ID to use. You can provide
                `cache` or `destination`, but not both.
        """
        # Resolve source ID
        source_id: str
        if isinstance(source, Source):
            selected_streams = selected_streams or source.get_selected_streams()
            if source._deployed_source_id:  # noqa: SLF001
                source_id = source._deployed_source_id  # noqa: SLF001
            else:
                source_id = self._deploy_source(source)
        else:
            source_id = source
            if not selected_streams:
                raise exc.PyAirbyteInputError(
                    guidance="You must provide `selected_streams` when deploying a source ID."
                )

        # Resolve destination ID
        destination_id: str
        if destination:
            destination_id = destination
        elif cache:
            table_prefix = table_prefix if table_prefix is not None else (cache.table_prefix or "")
            if not cache._deployed_destination_id:  # noqa: SLF001
                destination_id = self._deploy_cache_as_destination(cache)
            else:
                destination_id = cache._deployed_destination_id  # noqa: SLF001
        else:
            raise exc.PyAirbyteInputError(
                guidance="You must provide either a destination ID or a cache object."
            )

        assert source_id is not None
        assert destination_id is not None

        deployed_connection = create_connection(
            name="Connection (Deployed by PyAirbyte)",
            source_id=source_id,
            destination_id=destination_id,
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
            selected_stream_names=selected_streams,
            prefix=table_prefix or "",
        )

        if isinstance(source, Source):
            source._deployed_api_root = self.api_root  # noqa: SLF001
            source._deployed_workspace_id = self.workspace_id  # noqa: SLF001
            source._deployed_source_id = source_id  # noqa: SLF001
        if cache:
            cache._deployed_api_root = self.api_root  # noqa: SLF001
            cache._deployed_workspace_id = self.workspace_id  # noqa: SLF001
            cache._deployed_destination_id = deployed_connection.destination_id  # noqa: SLF001

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

    def _permanently_delete_connection(
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

        delete_connection(
            connection_id=connection.connection_id,
            api_root=self.api_root,
            api_key=self.api_key,
            workspace_id=self.workspace_id,
        )
        if delete_source:
            self._permanently_delete_source(source=connection.source_id)

        if delete_destination:
            self._permanently_delete_destination(destination=connection.destination_id)

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
