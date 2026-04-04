# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Run a cloud sync with Rich Live progress tracking.

Demonstrates the `with_rich_status_updates` feature, which renders a
real-time Rich table to stderr showing per-stream sync progress while
waiting for a Cloud sync to complete.

Prerequisites:
    - An Airbyte Cloud API key exported as ``AIRBYTE_CLOUD_API_KEY``.
    - The workspace and connection IDs below must be accessible with
      that key.  The defaults point to the ``@devin-ai-sandbox``
      workspace (Google Analytics 4 connection).

Usage (from the PyAirbyte root directory):
    uv run python examples/run_cloud_sync_with_rich_progress.py

You can also pass a custom refresh interval (in seconds) instead of
``True`` to control how often the table refreshes:

    connection.run_sync(with_rich_status_updates=30)   # refresh every 30s
"""

from __future__ import annotations

import airbyte as ab
from airbyte.cloud import CloudWorkspace

# ---------------------------------------------------------------------------
# Configuration – hard-coded to the @devin-ai-sandbox workspace
# ---------------------------------------------------------------------------

WORKSPACE_ID = "266ebdfe-0d7b-4540-9817-de7e4505ba61"
CONNECTION_ID = "d9c752fe-515a-4066-9234-096b101ea16e"  # GA4 -> dev-null


def main() -> None:
    """Trigger a Cloud sync and display a Rich Live progress table."""
    workspace = CloudWorkspace(
        workspace_id=WORKSPACE_ID,
        api_key=ab.get_secret("AIRBYTE_CLOUD_API_KEY"),
    )

    connection = workspace.get_connection(connection_id=CONNECTION_ID)
    print(f"Connection: {connection.connection_id}")
    print(f"Streams:    {connection.stream_names}")
    print()
    print("Starting sync with Rich status updates (15 s refresh) ...")
    print("(The Rich table renders to stderr; watch your terminal.)")
    print()

    sync_result = connection.run_sync(
        wait_timeout=60 * 60,  # 1 hour
        with_rich_status_updates=True,  # 15s default refresh
    )

    print()
    print(f"Sync complete!  Status: {sync_result.get_job_status()}")
    print(f"  Records synced : {sync_result.records_synced:,}")
    print(f"  Bytes synced   : {sync_result.bytes_synced:,}")
    print(f"  Job URL        : {sync_result.job_url}")


if __name__ == "__main__":
    main()
