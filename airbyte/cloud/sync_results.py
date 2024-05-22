# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Sync results for Airbyte Cloud workspaces.

## Examples

### Run a sync job and wait for completion

To get started, we'll need a `.CloudConnection` object. You can obtain this object by calling
`.CloudWorkspace.get_connection()`.

```python
from airbyte import cloud

# Initialize an Airbyte Cloud workspace object
workspace = cloud.CloudWorkspace(
    workspace_id="123",
    api_key=ab.get_secret("AIRBYTE_CLOUD_API_KEY"),
)

# Get a connection object
connection = workspace.get_connection(connection_id="456")
```

Once we have a `.CloudConnection` object, we can simply call `run_sync()`
to start a sync job and wait for it to complete.

```python
# Run a sync job
sync_result: SyncResult = connection.run_sync()
```

### Run a sync job and return immediately

By default, `run_sync()` will wait for the job to complete and raise an
exception if the job fails. You can instead return immediately by setting
`wait=False`.

```python
# Start the sync job and return immediately
sync_result: SyncResult = connection.run_sync(wait=False)

while not sync_result.is_job_complete():
    print("Job is still running...")
    time.sleep(5)

print(f"Job is complete! Status: {sync_result.get_job_status()}")
```

### Examining the sync result

You can examine the sync result to get more information about the job:

```python
sync_result: SyncResult = connection.run_sync()

# Print the job details
print(
    f'''
    Job ID: {sync_result.job_id}
    Job URL: {sync_result.job_url}
    Start Time: {sync_result.start_time}
    Records Synced: {sync_result.records_synced}
    Bytes Synced: {sync_result.bytes_synced}
    Job Status: {sync_result.get_job_status()}
    List of Stream Names: {', '.join(sync_result.stream_names)}
    '''
)
```

### Reading data from Airbyte Cloud sync result

**This feature is currently only available for specific SQL-based destinations.** This includes
SQL-based destinations such as Snowflake and BigQuery. The list of supported destinations may be
determined by inspecting the constant `airbyte.cloud.constants.READABLE_DESTINATION_TYPES`.

If your destination is supported, you can read records directly from the SyncResult object.

```python
# Assuming we've already created a `connection` object...
sync_result = connection.get_sync_result()

# Print a list of available stream names
print(sync_result.stream_names)

# Get a dataset from the sync result
dataset: CachedDataset = sync_result.get_dataset("users")

# Get the SQLAlchemy table to use in SQL queries...
users_table = dataset.to_sql_table()
print(f"Table name: {users_table.name}")

# Or iterate over the dataset directly
for record in dataset:
    print(record)
```

------

"""

from __future__ import annotations

import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, final

from airbyte._util import api_util
from airbyte.cloud._destination_util import create_cache_from_destination_config
from airbyte.cloud.constants import FAILED_STATUSES, FINAL_STATUSES
from airbyte.datasets import CachedDataset
from airbyte.exceptions import AirbyteConnectionSyncError, AirbyteConnectionSyncTimeoutError


DEFAULT_SYNC_TIMEOUT_SECONDS = 30 * 60  # 30 minutes
"""The default timeout for waiting for a sync job to complete, in seconds."""

if TYPE_CHECKING:
    import sqlalchemy

    from airbyte._util.api_imports import ConnectionResponse, JobResponse, JobStatusEnum
    from airbyte.caches.base import CacheBase
    from airbyte.cloud.connections import CloudConnection
    from airbyte.cloud.workspaces import CloudWorkspace


@dataclass
class SyncResult:
    """The result of a sync operation.

    **This class is not meant to be instantiated directly.** Instead, obtain a `SyncResult` by
    interacting with the `.CloudWorkspace` and `.CloudConnection` objects.
    """

    workspace: CloudWorkspace
    connection: CloudConnection
    job_id: str
    table_name_prefix: str = ""
    table_name_suffix: str = ""
    _latest_job_info: JobResponse | None = None
    _connection_response: ConnectionResponse | None = None
    _cache: CacheBase | None = None

    @property
    def job_url(self) -> str:
        """Return the URL of the sync job."""
        return f"{self.connection.job_history_url}/{self.job_id}"

    def _get_connection_info(self, *, force_refresh: bool = False) -> ConnectionResponse:
        """Return connection info for the sync job."""
        if self._connection_response and not force_refresh:
            return self._connection_response

        self._connection_response = api_util.get_connection(
            workspace_id=self.workspace.workspace_id,
            api_root=self.workspace.api_root,
            api_key=self.workspace.api_key,
            connection_id=self.connection.connection_id,
        )
        return self._connection_response

    def _get_destination_configuration(self, *, force_refresh: bool = False) -> dict[str, Any]:
        """Return the destination configuration for the sync job."""
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
        return self._fetch_latest_job_info().status

    def _fetch_latest_job_info(self) -> JobResponse:
        """Return the job info for the sync job."""
        if self._latest_job_info and self._latest_job_info.status in FINAL_STATUSES:
            return self._latest_job_info

        self._latest_job_info = api_util.get_job_info(
            job_id=self.job_id,
            api_root=self.workspace.api_root,
            api_key=self.workspace.api_key,
        )
        return self._latest_job_info

    @property
    def bytes_synced(self) -> int:
        """Return the number of records processed."""
        return self._fetch_latest_job_info().bytes_synced

    @property
    def records_synced(self) -> int:
        """Return the number of records processed."""
        return self._fetch_latest_job_info().rows_synced

    @property
    def start_time(self) -> datetime:
        """Return the start time of the sync job in UTC."""
        # Parse from ISO 8601 format:
        return datetime.fromisoformat(self._fetch_latest_job_info().start_time)

    def raise_failure_status(
        self,
        *,
        refresh_status: bool = False,
    ) -> None:
        """Raise an exception if the sync job failed.

        By default, this method will use the latest status available. If you want to refresh the
        status before checking for failure, set `refresh_status=True`. If the job has failed, this
        method will raise a `AirbyteConnectionSyncError`.

        Otherwise, do nothing.
        """
        if not refresh_status and self._latest_job_info:
            latest_status = self._latest_job_info.status
        else:
            latest_status = self.get_job_status()

        if latest_status in FAILED_STATUSES:
            raise AirbyteConnectionSyncError(
                workspace=self.workspace,
                connection_id=self.connection.connection_id,
                job_id=self.job_id,
                job_status=self.get_job_status(),
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
                    raise AirbyteConnectionSyncTimeoutError(
                        workspace=self.workspace,
                        connection_id=self.connection.connection_id,
                        job_id=self.job_id,
                        job_status=latest_status,
                        timeout=wait_timeout,
                    )

                return latest_status  # This will be a non-final status

            time.sleep(api_util.JOB_WAIT_INTERVAL_SECS)

    def get_sql_cache(self) -> CacheBase:
        """Return a SQL Cache object for working with the data in a SQL-based destination's."""
        if self._cache:
            return self._cache

        destination_configuration: dict[str, Any] = self._get_destination_configuration()
        self._cache = create_cache_from_destination_config(
            destination_configuration=destination_configuration
        )
        return self._cache

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
        """Retrieve an `airbyte.datasets.CachedDataset` object for a given stream name.

        This can be used to read and analyze the data in a SQL-based destination.

        TODO: In a future iteration, we can consider providing stream configuration information
              (catalog information) to the `CachedDataset` object via the "Get stream properties"
              API: https://reference.airbyte.com/reference/getstreamproperties
        """
        return CachedDataset(
            self.get_sql_cache(),
            stream_name=stream_name,
            stream_configuration=False,  # Don't look for stream configuration in cache.
        )

    def get_sql_database_name(self) -> str:
        """Return the SQL database name."""
        cache = self.get_sql_cache()
        return cache.get_database_name()

    def get_sql_schema_name(self) -> str:
        """Return the SQL schema name."""
        cache = self.get_sql_cache()
        return cache.schema_name

    @property
    def stream_names(self) -> list[str]:
        """Return the set of stream names."""
        return self.connection.stream_names

    @final
    @property
    def streams(
        self,
    ) -> _SyncResultStreams:
        """Return a mapping of stream names to `airbyte.CachedDataset` objects.

        This is a convenience wrapper around the `stream_names`
        property and `get_dataset()` method.
        """
        return self._SyncResultStreams(self)

    class _SyncResultStreams(Mapping[str, CachedDataset]):
        """A mapping of stream names to cached datasets."""

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


__all__ = [
    "SyncResult",
]
