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

import json
import time
import warnings
from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError
from rich.console import Console
from rich.live import Live as RichLive
from rich.table import Table
from typing_extensions import final

from airbyte_cdk.utils.datetime_helpers import ab_datetime_parse

from airbyte._util import api_util
from airbyte.caches._utils._dest_to_cache import destination_to_cache
from airbyte.cloud._connection_state import (
    ConnectionStateResponse,
    _get_stream_list,
)
from airbyte.cloud._sync_progress import (
    _extract_cursor_field_from_catalog,
    _find_cursor_value_in_state,
    compute_stream_progress,
)
from airbyte.cloud.constants import FAILED_STATUSES, FINAL_STATUSES, JobStatusEnum
from airbyte.datasets import CachedDataset
from airbyte.exceptions import (
    AirbyteConnectionSyncError,
    AirbyteConnectionSyncTimeoutError,
    AirbyteError,
)


DEFAULT_SYNC_TIMEOUT_SECONDS = 30 * 60  # 30 minutes
"""The default timeout for waiting for a sync job to complete, in seconds."""

MIN_RICH_UPDATE_INTERVAL_SECS = 15
"""Minimum polling interval when Rich status updates are enabled."""

DEFAULT_RICH_UPDATE_INTERVAL_SECS = 15
"""Default polling interval when `with_rich_status_updates=True`."""

if TYPE_CHECKING:
    import sqlalchemy

    from airbyte._util.api_imports import ConnectionResponse, JobResponse
    from airbyte.caches.base import CacheBase
    from airbyte.cloud.connections import CloudConnection
    from airbyte.cloud.workspaces import CloudWorkspace


def _resolve_rich_interval(*, with_rich_status_updates: bool | int) -> float:
    """Normalize `with_rich_status_updates` to a polling interval in seconds.

    `True` maps to `DEFAULT_RICH_UPDATE_INTERVAL_SECS`.  An `int` is
    clamped to `MIN_RICH_UPDATE_INTERVAL_SECS` with a warning when the
    caller-provided value is too low.
    """
    if with_rich_status_updates is True:
        return float(DEFAULT_RICH_UPDATE_INTERVAL_SECS)

    interval = int(with_rich_status_updates)
    if interval < MIN_RICH_UPDATE_INTERVAL_SECS:
        warnings.warn(
            f"Rich status update interval {interval}s is below the minimum "
            f"of {MIN_RICH_UPDATE_INTERVAL_SECS}s. Using {MIN_RICH_UPDATE_INTERVAL_SECS}s.",
            UserWarning,
            stacklevel=3,
        )
        return float(MIN_RICH_UPDATE_INTERVAL_SECS)

    return float(interval)


def _format_bytes(num_bytes: int) -> str:
    """Format a byte count as a human-readable string (e.g. ``1.2 MB``)."""
    if num_bytes < 1_000:  # noqa: PLR2004  # Byte thresholds are self-documenting
        return f"{num_bytes} B"
    if num_bytes < 1_000_000:  # noqa: PLR2004
        return f"{num_bytes / 1_000:,.1f} KB"
    if num_bytes < 1_000_000_000:  # noqa: PLR2004
        return f"{num_bytes / 1_000_000:,.1f} MB"
    return f"{num_bytes / 1_000_000_000:,.2f} GB"


def _extract_stream_stats(debug_info: dict[str, Any]) -> dict[str, dict[str, int]]:
    """Extract per-stream `recordsEmitted` / `bytesEmitted` from a debug-info payload.

    The Config API's `jobs/get_debug_info` endpoint returns an `attempts`
    list; the latest attempt's `streamStats` updates in real-time during
    a running sync and is our primary mid-sync proof-of-life signal.

    Returns a mapping from stream name to a dict of integer stats
    (`records_emitted`, `bytes_emitted`).  Missing or malformed payloads
    yield an empty dict.
    """
    attempts = debug_info.get("attempts") or []
    if not attempts:
        return {}

    latest_attempt = attempts[-1]
    attempt_inner = latest_attempt.get("attempt") or latest_attempt
    stream_stats_list = attempt_inner.get("streamStats") or []

    result: dict[str, dict[str, int]] = {}
    for entry in stream_stats_list:
        stream_name = entry.get("streamName") or entry.get("stream_name")
        if not stream_name:
            continue
        stats = entry.get("stats") or {}
        result[stream_name] = {
            "records_emitted": int(stats.get("recordsEmitted") or 0),
            "bytes_emitted": int(stats.get("bytesEmitted") or 0),
        }
    return result


def _append_progress_log_entry(
    log_path: Path,
    elapsed_secs: float,
    job_status: str,
    records_synced: int,
    bytes_synced: int,
    stream_progress: list[dict[str, Any]],
    current_stream_stats: dict[str, dict[str, int]],
    previous_stream_stats: dict[str, dict[str, int]],
) -> None:
    """Append a single JSONL entry describing per-stream sync progress.

    Factored into a module-level helper so the write happens in a scope
    that does not reference any credential attributes (`client_secret`,
    `bearer_token`). The values passed in are all non-sensitive progress
    metrics: stream names, cursor positions, record counts, and timestamps.
    """
    log_streams: list[dict[str, Any]] = []
    progress_names: dict[str, dict[str, Any]] = {
        str(e.get("stream_name", "")): e for e in stream_progress
    }
    all_names: list[str] = list(progress_names.keys())
    all_names.extend(name for name in current_stream_stats if name not in progress_names)

    for name in all_names:
        entry = progress_names.get(name, {"stream_name": name})
        stats = current_stream_stats.get(name, {})
        prev_stats = previous_stream_stats.get(name, {})
        prev_recs = prev_stats.get("records_emitted", 0)
        recs = stats.get("records_emitted", 0)
        records_progress = min(recs / prev_recs, 1.0) if prev_recs > 0 else None
        log_streams.append(
            {
                "stream_name": entry.get("stream_name", name),
                "progress_pct": entry.get("progress_pct"),
                "cursor_value": entry.get("cursor_value"),
                "previous_cursor_value": entry.get("previous_cursor_value"),
                "reason": entry.get("reason"),
                "records_emitted": recs,
                "bytes_emitted": stats.get("bytes_emitted", 0),
                "records_progress": records_progress,
                "previous_records_emitted": prev_recs,
            }
        )

    log_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "elapsed_secs": round(elapsed_secs, 1),
        "job_status": job_status,
        "records_synced": records_synced,
        "bytes_synced": bytes_synced,
        "streams": log_streams,
    }
    with log_path.open("a") as f:
        f.write(json.dumps(log_entry) + "\n")


def _build_rich_table(  # noqa: PLR0913, PLR0914, PLR0915, PLR0917
    stream_progress: list[dict[str, Any]],
    job_status: str,
    elapsed_secs: float,
    sync_start_time: datetime | None = None,
    total_selected_streams: int | None = None,
    records_synced: int = 0,
    bytes_synced: int = 0,
    stream_stats: dict[str, dict[str, int]] | None = None,
    previous_stream_stats: dict[str, dict[str, int]] | None = None,
) -> Table:
    """Build a Rich `Table` showing per-stream sync progress.

    `stream_stats` is the current mid-sync per-stream `recordsEmitted` /
    `bytesEmitted` map (from the Config API's `jobs/get_debug_info`
    endpoint).  `previous_stream_stats` is the same map from the previous
    completed sync, used as a rough denominator for the records-based
    progress signal.
    """
    elapsed_str = _format_elapsed(elapsed_secs)
    stream_stats = stream_stats or {}
    previous_stream_stats = previous_stream_stats or {}

    streams_with_pct = sum(1 for s in stream_progress if s.get("progress_pct") is not None)
    # Use the catalog stream count as the denominator when available;
    # fall back to the number of streams currently reporting state.
    total_streams = total_selected_streams or len(stream_progress)

    # Build throughput string (records/sec, bytes/sec) like Source.read()
    throughput_parts: list[str] = []
    if records_synced:
        throughput_parts.append(f"{records_synced:,} records")
    if bytes_synced:
        throughput_parts.append(_format_bytes(bytes_synced))
    if records_synced and elapsed_secs > 0:
        rps = records_synced / elapsed_secs
        throughput_parts.append(f"{rps:,.1f} records/s")

    title = (
        f"Sync Progress  |  Status: {job_status}  |  "
        f"Elapsed: {elapsed_str}  |  "
        f"Streams: {streams_with_pct}/{total_streams} reporting progress"
    )
    if throughput_parts:
        title += f"  |  {', '.join(throughput_parts)}"

    # Build a caption with start / end / elapsed timestamps
    caption_parts: list[str] = []
    if sync_start_time is not None:
        caption_parts.append(f"Start: {sync_start_time:%Y-%m-%d %H:%M:%S} UTC")
    end_time = datetime.now(timezone.utc)
    caption_parts.extend(
        [
            f"Current: {end_time:%Y-%m-%d %H:%M:%S} UTC",
            f"Elapsed: {elapsed_str}",
        ]
    )
    caption = "  |  ".join(caption_parts)

    table = Table(title=title, caption=caption, show_lines=False, expand=True)
    table.add_column("Stream", style="cyan", no_wrap=True)
    table.add_column("Records", justify="right", style="magenta")
    table.add_column("Bytes", justify="right", style="magenta")
    table.add_column("Records %", justify="right", style="yellow")
    table.add_column("Cursor %", justify="right", style="green")
    table.add_column("Stream Status", style="bold")
    table.add_column("Reason", style="dim")

    # Union of stream names from cursor-based progress and records-based stats
    # so proof-of-life shows up even when cursor progress can't compute a pct.
    progress_by_name: dict[str, dict[str, Any]] = {
        e.get("stream_name", "?"): e for e in stream_progress
    }
    all_names: list[str] = list(progress_by_name.keys())
    all_names.extend(name for name in stream_stats if name not in progress_by_name)

    for name in all_names:
        entry = progress_by_name.get(name, {})
        pct = entry.get("progress_pct")
        cursor_pct_str = f"{pct:.1%}" if pct is not None else "--"
        reason = entry.get("reason") or ""

        stats = stream_stats.get(name, {})
        recs = stats.get("records_emitted", 0)
        byts = stats.get("bytes_emitted", 0)
        recs_str = f"{recs:,}" if recs else "0"
        bytes_str = _format_bytes(byts) if byts else "0 B"

        prev_stats = previous_stream_stats.get(name, {})
        prev_recs = prev_stats.get("records_emitted", 0)
        if prev_recs > 0:
            records_pct = min(recs / prev_recs, 1.0)
            records_pct_str = f"{records_pct:.1%}"
        else:
            records_pct_str = "--"

        if recs > 0:
            stream_status = (
                "running"
                if job_status.lower() not in {"succeeded", "failed"}
                else ("complete" if job_status.lower() == "succeeded" else "failed")
            )
        else:
            stream_status = "pending"

        table.add_row(
            name,
            recs_str,
            bytes_str,
            records_pct_str,
            cursor_pct_str,
            stream_status,
            reason,
        )

    return table


def _format_elapsed(seconds: float) -> str:
    """Format elapsed seconds as `HH:MM:SS`."""
    total = int(seconds)
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02d}s"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


@dataclass
class SyncAttempt:
    """Represents a single attempt of a sync job.

    **This class is not meant to be instantiated directly.** Instead, obtain a `SyncAttempt` by
    calling `.SyncResult.get_attempts()`.
    """

    workspace: CloudWorkspace
    connection: CloudConnection
    job_id: int
    attempt_number: int
    _attempt_data: dict[str, Any] | None = None

    @property
    def attempt_id(self) -> int:
        """Return the attempt ID."""
        return self._get_attempt_data()["id"]

    @property
    def status(self) -> str:
        """Return the attempt status."""
        return self._get_attempt_data()["status"]

    @property
    def bytes_synced(self) -> int:
        """Return the number of bytes synced in this attempt."""
        return self._get_attempt_data().get("bytesSynced", 0)

    @property
    def records_synced(self) -> int:
        """Return the number of records synced in this attempt."""
        return self._get_attempt_data().get("recordsSynced", 0)

    @property
    def created_at(self) -> datetime:
        """Return the creation time of the attempt."""
        timestamp = self._get_attempt_data()["createdAt"]
        return ab_datetime_parse(timestamp)

    def _get_attempt_data(self) -> dict[str, Any]:
        """Get attempt data from the provided attempt data."""
        if self._attempt_data is None:
            raise ValueError(
                "Attempt data not provided. SyncAttempt should be created via "
                "SyncResult.get_attempts()."
            )
        return self._attempt_data["attempt"]

    def get_full_log_text(self) -> str:
        """Return the complete log text for this attempt.

        Returns:
            String containing all log text for this attempt, with lines separated by newlines.
        """
        if self._attempt_data is None:
            return ""

        logs_data = self._attempt_data.get("logs")
        if not logs_data:
            return ""

        result = ""

        if "events" in logs_data:
            log_events = logs_data["events"]
            if log_events:
                log_lines = []
                for event in log_events:
                    timestamp = event.get("timestamp", "")
                    level = event.get("level", "INFO")
                    message = event.get("message", "")
                    log_lines.append(
                        f"[{timestamp}] {level}: {message}"  # pyrefly: ignore[bad-argument-type]
                    )
                result = "\n".join(log_lines)
        elif "logLines" in logs_data:
            log_lines = logs_data["logLines"]
            if log_lines:
                result = "\n".join(log_lines)

        return result


def _update_first_seen_cursors(
    *,
    first_seen_cursors: dict[tuple[str, str | None], str],
    state_data: dict[str, Any],
    catalog_data: dict[str, Any] | None,
) -> None:
    """Record the first observed cursor value for each stream.

    On the first poll iteration where a stream appears in the state,
    its cursor value is captured.  This provides a fallback baseline
    for first-ever syncs where no previous completed state exists.
    """
    try:
        state = ConnectionStateResponse(**state_data)
        streams = _get_stream_list(state)
    except (ValidationError, TypeError, KeyError):
        return

    for stream in streams:
        key = (stream.stream_descriptor.name, stream.stream_descriptor.namespace)
        if key in first_seen_cursors:
            continue  # already recorded

        cursor_field: str | None = None
        if catalog_data:
            cursor_field = _extract_cursor_field_from_catalog(
                catalog_data,
                stream.stream_descriptor.name,
                stream.stream_descriptor.namespace,
            )
        cursor_val = _find_cursor_value_in_state(stream.stream_state, cursor_field)
        if cursor_val is not None:
            first_seen_cursors[key] = cursor_val


@dataclass
class SyncResult:
    """The result of a sync operation.

    **This class is not meant to be instantiated directly.** Instead, obtain a `SyncResult` by
    interacting with the `.CloudWorkspace` and `.CloudConnection` objects.
    """

    workspace: CloudWorkspace
    connection: CloudConnection
    job_id: int
    table_name_prefix: str = ""
    table_name_suffix: str = ""
    _latest_job_info: JobResponse | None = None
    _connection_response: ConnectionResponse | None = None
    _cache: CacheBase | None = None
    _job_with_attempts_info: dict[str, Any] | None = None
    _pre_sync_state: dict[str, Any] | None = None
    _pre_sync_stream_stats: dict[str, dict[str, int]] | None = None

    @property
    def job_url(self) -> str:
        """Return the URL of the sync job.

        Note: This currently returns the connection's job history URL, as there is no direct URL
        to a specific job in the Airbyte Cloud web app.

        TODO: Implement a direct job logs URL on top of the event-id of the specific attempt number.
              E.g. {self.connection.job_history_url}?eventId={event-guid}&openLogs=true
        """
        return f"{self.connection.job_history_url}"

    def _get_connection_info(self, *, force_refresh: bool = False) -> ConnectionResponse:
        """Return connection info for the sync job."""
        if self._connection_response and not force_refresh:
            return self._connection_response

        self._connection_response = api_util.get_connection(
            workspace_id=self.workspace.workspace_id,
            api_root=self.workspace.api_root,
            connection_id=self.connection.connection_id,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
        )
        return self._connection_response

    def _get_destination_configuration(self, *, force_refresh: bool = False) -> dict[str, Any]:
        """Return the destination configuration for the sync job."""
        connection_info: ConnectionResponse = self._get_connection_info(force_refresh=force_refresh)
        destination_response = api_util.get_destination(
            destination_id=connection_info.destination_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
        )
        return asdict(destination_response.configuration)

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
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
        )
        return self._latest_job_info

    @property
    def bytes_synced(self) -> int:
        """Return the number of records processed."""
        return self._fetch_latest_job_info().bytes_synced or 0

    @property
    def records_synced(self) -> int:
        """Return the number of records processed."""
        return self._fetch_latest_job_info().rows_synced or 0

    @property
    def start_time(self) -> datetime:
        """Return the start time of the sync job in UTC."""
        try:
            return ab_datetime_parse(self._fetch_latest_job_info().start_time)
        except (ValueError, TypeError) as e:
            if "Invalid isoformat string" in str(e):
                job_info_raw = api_util._make_config_api_request(  # noqa: SLF001
                    api_root=self.workspace.api_root,
                    path="/jobs/get",
                    json={"id": self.job_id},
                    client_id=self.workspace.client_id,
                    client_secret=self.workspace.client_secret,
                    bearer_token=self.workspace.bearer_token,
                )
                raw_start_time = job_info_raw.get("startTime")
                if raw_start_time:
                    return ab_datetime_parse(raw_start_time)
            raise

    def _fetch_job_with_attempts(self) -> dict[str, Any]:
        """Fetch job info with attempts from Config API using lazy loading pattern."""
        if self._job_with_attempts_info is not None:
            return self._job_with_attempts_info

        self._job_with_attempts_info = api_util._make_config_api_request(  # noqa: SLF001  # Config API helper
            api_root=self.workspace.api_root,
            path="/jobs/get",
            json={
                "id": self.job_id,
            },
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
        )
        return self._job_with_attempts_info

    def get_attempts(self) -> list[SyncAttempt]:
        """Return a list of attempts for this sync job."""
        job_with_attempts = self._fetch_job_with_attempts()
        attempts_data = job_with_attempts.get("attempts", [])

        return [
            SyncAttempt(
                workspace=self.workspace,
                connection=self.connection,
                job_id=self.job_id,
                attempt_number=i,
                _attempt_data=attempt_data,
            )
            for i, attempt_data in enumerate(attempts_data, start=0)
        ]

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
        with_rich_status_updates: bool | int = False,
        progress_log_path: str | Path | None = None,
    ) -> JobStatusEnum:
        """Wait for a job to finish running.

        When `with_rich_status_updates` is truthy, a Rich Live table is
        rendered to stderr showing per-stream sync progress.  Pass `True`
        for 15-second polling, or an `int` for a custom interval in
        seconds (minimum 15s -- values below 15 are clamped with a
        warning).  The rich polling interval replaces
        `JOB_WAIT_INTERVAL_SECS` as the sole loop cadence.

        When `progress_log_path` is set (requires Rich updates enabled),
        each polling iteration appends a JSONL line to the given file
        with timestamped per-stream progress data for auditing.
        """
        poll_interval: float = api_util.JOB_WAIT_INTERVAL_SECS
        rich_enabled = bool(with_rich_status_updates)

        if rich_enabled:
            poll_interval = _resolve_rich_interval(
                with_rich_status_updates=with_rich_status_updates,
            )

        log_path: Path | None = Path(progress_log_path) if progress_log_path else None

        start_time = time.time()

        if not rich_enabled:
            return self._poll_until_complete(
                start_time=start_time,
                poll_interval=poll_interval,
                wait_timeout=wait_timeout,
                raise_timeout=raise_timeout,
                raise_failure=raise_failure,
            )

        # Rich status updates path
        console = Console(stderr=True)
        live = RichLive(console=console, auto_refresh=False)
        try:
            live.start()
            return self._poll_until_complete_with_rich(
                live=live,
                start_time=start_time,
                poll_interval=poll_interval,
                wait_timeout=wait_timeout,
                raise_timeout=raise_timeout,
                raise_failure=raise_failure,
                progress_log_path=log_path,
            )
        finally:
            live.stop()

    # ------------------------------------------------------------------
    # Internal polling helpers
    # ------------------------------------------------------------------

    def _poll_until_complete(
        self,
        *,
        start_time: float,
        poll_interval: float,
        wait_timeout: int,
        raise_timeout: bool,
        raise_failure: bool,
    ) -> JobStatusEnum:
        """Plain polling loop without Rich output."""
        while True:
            latest_status = self.get_job_status()
            if latest_status in FINAL_STATUSES:
                if raise_failure:
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
                return latest_status

            time.sleep(poll_interval)

    def _poll_until_complete_with_rich(  # noqa: PLR0912, PLR0914
        self,
        *,
        live: RichLive,
        start_time: float,
        poll_interval: float,
        wait_timeout: int,
        raise_timeout: bool,
        raise_failure: bool,
        progress_log_path: Path | None = None,
    ) -> JobStatusEnum:
        """Polling loop with Rich Live table showing per-stream progress."""
        previous_state: dict[str, Any] | None = self._pre_sync_state
        previous_stream_stats: dict[str, dict[str, int]] = self._pre_sync_stream_stats or {}
        catalog_data: dict[str, Any] | None = None
        catalog_fetched = False

        # Track first-observed cursors as a fallback baseline when no
        # previous sync state is available.  Keys are (stream_name, namespace)
        # and values are the raw cursor string captured on first sighting.
        first_seen_cursors: dict[tuple[str, str | None], str] = {}

        # Total selected streams from catalog (resolved after first fetch).
        catalog_stream_count: int | None = None

        while True:
            latest_status = self.get_job_status()

            # Lazy-fetch catalog on first iteration
            if not catalog_fetched:
                catalog_data = api_util.get_connection_catalog(
                    connection_id=self.connection.connection_id,
                    api_root=self.workspace.api_root,
                    client_id=self.workspace.client_id,
                    client_secret=self.workspace.client_secret,
                    bearer_token=self.workspace.bearer_token,
                )
                catalog_fetched = True

                # Resolve total *selected* streams from the catalog.
                if catalog_data:
                    cat_streams = catalog_data.get("streams", [])
                    if not cat_streams and "syncCatalog" in catalog_data:
                        cat_streams = catalog_data["syncCatalog"].get("streams", [])
                    if cat_streams:
                        selected = [
                            s for s in cat_streams if s.get("config", {}).get("selected", False)
                        ]
                        catalog_stream_count = len(selected) if selected else len(cat_streams)

            # Fetch current state and compute progress
            state_data = api_util.get_connection_state(
                connection_id=self.connection.connection_id,
                api_root=self.workspace.api_root,
                client_id=self.workspace.client_id,
                client_secret=self.workspace.client_secret,
                bearer_token=self.workspace.bearer_token,
            )

            # Record first-observed cursors for streams we haven't seen yet
            _update_first_seen_cursors(
                first_seen_cursors=first_seen_cursors,
                state_data=state_data,
                catalog_data=catalog_data,
            )

            sync_start_time_dt: datetime
            try:
                sync_start_time_dt = self.start_time
            except (ValueError, TypeError):
                sync_start_time_dt = datetime.now(timezone.utc)

            stream_progress = compute_stream_progress(
                state_data=state_data,
                catalog_data=catalog_data,
                sync_start_time=sync_start_time_dt,
                previous_state_data=previous_state,
                first_seen_cursors=first_seen_cursors,
            )

            # Override progress to 100% for successful syncs.  The formula
            # compares cursors against `now`, so a source whose data stops
            # before "today" (e.g. GA4 data through 2025-12-27 when today
            # is 2026-04-04) would otherwise show <100% even after the job
            # completes successfully.
            if latest_status == JobStatusEnum.SUCCEEDED:
                for entry in stream_progress:
                    if entry.get("progress_pct") is not None:
                        entry["progress_pct"] = 1.0

            # Fetch live per-stream records/bytes from the Config API's
            # `jobs/get_debug_info` endpoint.  These counters update
            # mid-sync and serve as the primary proof-of-life signal when
            # cursor-based progress can't compute a percentage.
            current_stream_stats: dict[str, dict[str, int]] = {}
            try:
                debug_info = api_util.get_job_debug_info(
                    job_id=self.job_id,
                    api_root=self.workspace.api_root,
                    client_id=self.workspace.client_id,
                    client_secret=self.workspace.client_secret,
                    bearer_token=self.workspace.bearer_token,
                )
            except AirbyteError:
                # Progress signal is best-effort; swallow API errors and
                # render `--` rather than failing the sync polling loop.
                debug_info = {}
            if debug_info:
                current_stream_stats = _extract_stream_stats(debug_info)

            elapsed = time.time() - start_time
            job_info = self._latest_job_info
            table = _build_rich_table(
                stream_progress=stream_progress,
                job_status=str(latest_status),
                elapsed_secs=elapsed,
                sync_start_time=sync_start_time_dt,
                total_selected_streams=catalog_stream_count,
                records_synced=(job_info.rows_synced or 0) if job_info else 0,
                bytes_synced=(job_info.bytes_synced or 0) if job_info else 0,
                stream_stats=current_stream_stats,
                previous_stream_stats=previous_stream_stats,
            )
            live.update(table, refresh=True)

            # Write JSONL progress log entry when a log path is configured.
            if progress_log_path is not None:
                _append_progress_log_entry(
                    log_path=progress_log_path,
                    elapsed_secs=elapsed,
                    job_status=str(latest_status),
                    records_synced=(job_info.rows_synced or 0) if job_info else 0,
                    bytes_synced=(job_info.bytes_synced or 0) if job_info else 0,
                    stream_progress=stream_progress,
                    current_stream_stats=current_stream_stats,
                    previous_stream_stats=previous_stream_stats,
                )

            if latest_status in FINAL_STATUSES:
                if raise_failure:
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
                return latest_status

            time.sleep(poll_interval)

    def get_sql_cache(self) -> CacheBase:
        """Return a SQL Cache object for working with the data in a SQL-based destination's."""
        if self._cache:
            return self._cache

        destination_configuration = self._get_destination_configuration()
        self._cache = destination_to_cache(destination_configuration=destination_configuration)
        return self._cache

    def get_sql_engine(self) -> sqlalchemy.engine.Engine:
        """Return a SQL Engine for querying a SQL-based destination."""
        return self.get_sql_cache().get_sql_engine()

    def get_sql_table_name(self, stream_name: str) -> str:
        """Return the SQL table name of the named stream."""
        return self.get_sql_cache().processor.get_sql_table_name(stream_name=stream_name)

    def get_sql_table(
        self,
        stream_name: str,
    ) -> sqlalchemy.Table:
        """Return a SQLAlchemy table object for the named stream."""
        return self.get_sql_cache().processor.get_sql_table(stream_name)

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
    ) -> _SyncResultStreams:  # pyrefly: ignore[unknown-name]
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
            return iter(self.parent.stream_names)

        def __len__(self) -> int:
            return len(self.parent.stream_names)


__all__ = [
    "SyncResult",
    "SyncAttempt",
]
