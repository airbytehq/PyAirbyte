# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Cloud Connections."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from airbyte._util import api_util
from airbyte.cloud.connectors import CloudDestination, CloudSource
from airbyte.cloud.sync_results import SyncResult


if TYPE_CHECKING:
    from airbyte_api.models import ConnectionResponse, JobResponse

    from airbyte.cloud.workspaces import CloudWorkspace


class CloudConnection:
    """A connection is an extract-load (EL) pairing of a source and destination in Airbyte Cloud.

    You can use a connection object to run sync jobs, retrieve logs, and manage the connection.
    """

    def __init__(
        self,
        workspace: CloudWorkspace,
        connection_id: str,
        source: str | None = None,
        destination: str | None = None,
    ) -> None:
        """It is not recommended to create a `CloudConnection` object directly.

        Instead, use `CloudWorkspace.get_connection()` to create a connection object.
        """
        self.connection_id = connection_id
        """The ID of the connection."""

        self.workspace = workspace
        """The workspace that the connection belongs to."""

        self._source_id = source
        """The ID of the source."""

        self._destination_id = destination
        """The ID of the destination."""

        self._connection_info: ConnectionResponse | None = None
        """The connection info object. (Cached.)"""

        self._cloud_source_object: CloudSource | None = None
        """The source object. (Cached.)"""

        self._cloud_destination_object: CloudDestination | None = None
        """The destination object. (Cached.)"""

    def _fetch_connection_info(self) -> ConnectionResponse:
        """Populate the connection with data from the API."""
        return api_util.get_connection(
            workspace_id=self.workspace.workspace_id,
            connection_id=self.connection_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
        )

    # Properties

    @property
    def source_id(self) -> str:
        """The ID of the source."""
        if not self._source_id:
            if not self._connection_info:
                self._connection_info = self._fetch_connection_info()

            self._source_id = self._connection_info.source_id

        return cast("str", self._source_id)

    @property
    def source(self) -> CloudSource:
        """Get the source object."""
        if self._cloud_source_object:
            return self._cloud_source_object

        self._cloud_source_object = CloudSource(
            workspace=self.workspace,
            connector_id=self.source_id,
        )
        return self._cloud_source_object

    @property
    def destination_id(self) -> str:
        """The ID of the destination."""
        if not self._destination_id:
            if not self._connection_info:
                self._connection_info = self._fetch_connection_info()

            self._destination_id = self._connection_info.source_id

        return cast("str", self._destination_id)

    @property
    def destination(self) -> CloudDestination:
        """Get the destination object."""
        if self._cloud_destination_object:
            return self._cloud_destination_object

        self._cloud_destination_object = CloudDestination(
            workspace=self.workspace,
            connector_id=self.destination_id,
        )
        return self._cloud_destination_object

    @property
    def stream_names(self) -> list[str]:
        """The stream names."""
        if not self._connection_info:
            self._connection_info = self._fetch_connection_info()

        return [stream.name for stream in self._connection_info.configurations.streams or []]

    @property
    def table_prefix(self) -> str:
        """The table prefix."""
        if not self._connection_info:
            self._connection_info = self._fetch_connection_info()

        return self._connection_info.prefix or ""

    @property
    def connection_url(self) -> str | None:
        """The URL to the connection."""
        return f"{self.workspace.workspace_url}/connections/{self.connection_id}"

    @property
    def job_history_url(self) -> str | None:
        """The URL to the job history for the connection."""
        return f"{self.connection_url}/job-history"

    # Run Sync

    def run_sync(
        self,
        *,
        wait: bool = True,
        wait_timeout: int = 300,
    ) -> SyncResult:
        """Run a sync."""
        connection_response = api_util.run_connection(
            connection_id=self.connection_id,
            api_root=self.workspace.api_root,
            workspace_id=self.workspace.workspace_id,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
        )
        sync_result = SyncResult(
            workspace=self.workspace,
            connection=self,
            job_id=connection_response.job_id,
        )

        if wait:
            sync_result.wait_for_completion(
                wait_timeout=wait_timeout,
                raise_failure=True,
                raise_timeout=True,
            )

        return sync_result

    # Logs

    def get_previous_sync_logs(
        self,
        *,
        limit: int = 10,
    ) -> list[SyncResult]:
        """Get the previous sync logs for a connection."""
        sync_logs: list[JobResponse] = api_util.get_job_logs(
            connection_id=self.connection_id,
            api_root=self.workspace.api_root,
            workspace_id=self.workspace.workspace_id,
            limit=limit,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
        )
        return [
            SyncResult(
                workspace=self.workspace,
                connection=self,
                job_id=sync_log.job_id,
                _latest_job_info=sync_log,
            )
            for sync_log in sync_logs
        ]

    def get_sync_result(
        self,
        job_id: int | None = None,
    ) -> SyncResult | None:
        """Get the sync result for the connection.

        If `job_id` is not provided, the most recent sync job will be used.

        Returns `None` if job_id is omitted and no previous jobs are found.
        """
        if job_id is None:
            # Get the most recent sync job
            results = self.get_previous_sync_logs(
                limit=1,
            )
            if results:
                return results[0]

            return None

        # Get the sync job by ID (lazy loaded)
        return SyncResult(
            workspace=self.workspace,
            connection=self,
            job_id=job_id,
        )

    # Deletions

    def permanently_delete(
        self,
        *,
        cascade_delete_source: bool = False,
        cascade_delete_destination: bool = False,
    ) -> None:
        """Delete the connection.

        Args:
            cascade_delete_source: Whether to also delete the source.
            cascade_delete_destination: Whether to also delete the destination.
        """
        self.workspace.permanently_delete_connection(self)

        if cascade_delete_source:
            self.workspace.permanently_delete_source(self.source_id)

        if cascade_delete_destination:
            self.workspace.permanently_delete_destination(self.destination_id)
