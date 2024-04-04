# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Sync results for Airbyte Cloud workspaces."""

from __future__ import annotations

import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, final

from airbyte_api.models.shared import ConnectionResponse, JobStatusEnum

from airbyte._util import api_util
from airbyte.cloud._destination_util import create_cache_from_destination
from airbyte.datasets import CachedDataset
from airbyte.exceptions import HostedConnectionSyncError, HostedConnectionSyncTimeoutError


DEFAULT_SYNC_TIMEOUT_SECONDS = 30 * 60  # 30 minutes


if TYPE_CHECKING:
    import sqlalchemy

    from airbyte.caches.base import CacheBase
    from airbyte.cloud._workspaces import CloudWorkspace


FINAL_STATUSES = {
    JobStatusEnum.SUCCEEDED,
    JobStatusEnum.FAILED,
    JobStatusEnum.CANCELLED,
}
FAILED_STATUSES = {
    JobStatusEnum.FAILED,
    JobStatusEnum.CANCELLED,
}


@dataclass
class SyncResult:
    """The result of a sync operation."""

    workspace: CloudWorkspace
    connection_id: str
    job_id: str
    _latest_status: JobStatusEnum | None = None
    _connection_response: ConnectionResponse | None = None

    def _get_connection_info(self, *, force_refresh: bool = False) -> ConnectionResponse:
        """TODO"""
        if self._connection_response and not force_refresh:
            return self._connection_response

        self._connection_response = api_util.get_connection(
            workspace_id=self.workspace.workspace_id,
            api_root=self.workspace.api_root,
            api_key=self.workspace.api_key,
            connection_id=self.connection_id,
        )
        return self._connection_response

    def _get_destination_configuration(self, *, force_refresh: bool = False) -> dict[str, Any]:
        connection_info: ConnectionResponse = self._get_connection_info(force_refresh=force_refresh)
        destination_response = api_util.get_destination(
            destination_id=connection_info.destination_id,
            api_root=self.workspace.api_root,
            api_key=self.workspace.api_key,
        )
        return destination_response.configuration

    def is_job_complete(self) -> bool:
        """Check if the sync job is complete."""
        return self.get_job_status() in FINAL_STATUSES

    def get_job_status(self) -> JobStatusEnum:
        """Check if the sync job is still running."""
        if self._latest_status and self._latest_status in FINAL_STATUSES:
            return self._latest_status

        job_info = api_util.get_job_info(
            job_id=self.job_id,
            api_root=self.workspace.api_root,
            api_key=self.workspace.api_key,
        )
        self._latest_status = job_info.status

        return job_info.status

    def raise_failure_status(
        self,
        *,
        refresh_status: bool = False,
    ) -> None:
        """Raise an exception if the sync job failed.

        By default, this method will use the latest status available. If you want to refresh the
        status before checking for failure, set `refresh_status=True`. If the job has failed, this
        method will raise a `HostedConnectionSyncError`.

        Otherwise, do nothing.
        """
        latest_status = self._latest_status
        if refresh_status:
            latest_status = self.get_job_status()

        if latest_status in FAILED_STATUSES:
            raise HostedConnectionSyncError(
                workspace=self.workspace,
                connection_id=self.connection_id,
                job_id=self.job_id,
                job_status=self._latest_status,
            )

    def wait_for_completion(
        self,
        *,
        wait_timeout: int = DEFAULT_SYNC_TIMEOUT_SECONDS,
        raise_timeout: bool = True,
        raise_failure: bool = False,
    ) -> JobStatusEnum:
        """Wait for a job to finish running."""
        start_time = time.time()
        while True:
            latest_status = self.get_job_status()
            if latest_status in FINAL_STATUSES:
                if raise_failure:
                    # No-op if the job succeeded or is still running:
                    self.raise_failure_status()

                return latest_status

            if time.time() - start_time > wait_timeout:
                if raise_timeout:
                    raise HostedConnectionSyncTimeoutError(
                        workspace=self.workspace,
                        connection_id=self.connection_id,
                        job_id=self.job_id,
                        job_status=latest_status,
                        timeout=wait_timeout,
                    )

                return latest_status  # This will be a non-final status

            time.sleep(api_util.JOB_WAIT_INTERVAL_SECS)

    def get_sql_cache(self) -> CacheBase:
        """Return a SQL Cache object for working with the data in a SQL-based destination's."""
        # TODO: Implement
        return create_cache_from_destination(destination_configuration)

    def get_sql_engine(self) -> sqlalchemy.engine.Engine:
        """Return a SQL Engine for querying a SQL-based destination."""
        self.get_sql_cache().get_sql_engine()

    def get_sql_table_name(self, stream_name: str) -> str:
        """Return the SQL table name of the named stream."""
        return self.get_sql_cache().processor.get_sql_table_name(stream_name=stream_name)

    def get_sql_table(
        self,
        stream_name: str,
    ) -> sqlalchemy.Table:
        """Return a SQLAlchemy table object for the named stream."""
        self.get_sql_cache().processor.get_sql_table(stream_name)

    def get_dataset(self, stream_name: str) -> CachedDataset:
        """Return cached dataset."""
        cache = self.get_sql_cache()
        return cache.streams[stream_name]

    def get_sql_database_name(self) -> str:
        """Return the SQL database name."""
        cache = self.get_sql_cache()
        return cache.get_database_name()

    def get_sql_schema_name(self) -> str:
        """Return the SQL schema name."""
        cache = self.get_sql_cache()
        return cache.schema_name

    @property
    def stream_names(self) -> set[str]:
        """TODO"""
        return self.get_sql_cache().processor.expected_streams

    @final
    @property
    def streams(
        self,
    ) -> SyncResultStreams:
        """Return a temporary table name."""
        return self.SyncResultStreams(self)

    class SyncResultStreams(Mapping[str, CachedDataset]):
        """TODO"""

        def __init__(
            self,
            parent: SyncResult,
            /,
        ) -> None:
            self.parent: SyncResult = parent

        def __getitem__(self, key: str) -> CachedDataset:
            return self.parent.get_dataset(stream_name=key)

        def __iter__(self) -> Iterator[str]:
            """TODO"""
            return iter(self.parent.stream_names)

        def __len__(self) -> int:
            return len(self.parent.stream_names)
