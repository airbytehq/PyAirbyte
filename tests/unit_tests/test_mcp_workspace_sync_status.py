# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for workspace sync status MCP UI tools."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import cast

from airbyte._util.api_imports import JobStatusEnum
from airbyte.mcp.interactive import show_workspace_sync_status
from fastmcp import Context
from mcp.types import TextContent


@dataclass
class _SyncResultLike:
    """Subset of `SyncResult` needed by the workspace status UI."""

    job_id: int
    status: JobStatusEnum
    start_time: datetime
    records_synced: int
    bytes_synced: int

    def get_job_status(self) -> JobStatusEnum:
        """Return the configured job status."""
        return self.status

    def is_job_complete(self) -> bool:
        """Return whether the test sync job is complete."""
        return self.status in {
            JobStatusEnum.SUCCEEDED,
            JobStatusEnum.FAILED,
            JobStatusEnum.CANCELLED,
        }


@dataclass
class _ConnectorLike:
    """Subset of a cloud source or destination used by the UI."""

    name: str
    connector_url: str


@dataclass
class _ConnectionLike:
    """Subset of `CloudConnection` needed by the workspace status UI."""

    connection_id: str
    name: str
    connection_url: str
    source_id: str
    destination_id: str
    source: _ConnectorLike
    destination: _ConnectorLike
    sync_results: list[_SyncResultLike]

    def get_previous_sync_logs(
        self,
        *,
        limit: int = 20,
        from_tail: bool = True,
    ) -> list[_SyncResultLike]:
        """Return recent sync results and capture the production call shape."""
        assert from_tail is True
        return self.sync_results[:limit]


class _WorkspaceLike:
    """Workspace test double for interactive sync status tests."""

    workspace_id = "workspace-id"
    workspace_url = "https://cloud.airbyte.com/workspaces/workspace-id"

    def __init__(self, connections: list[_ConnectionLike]) -> None:
        """Create a workspace with test connection data."""
        self.connections = connections
        self.limit: int | None = None

    def list_connections(self, *, limit: int | None = None) -> list[_ConnectionLike]:
        """Capture the requested connection limit."""
        self.limit = limit
        return self.connections if limit is None else self.connections[:limit]


def test_show_workspace_sync_status_summarizes_real_workspace_shape(
    monkeypatch,
) -> None:
    """Test workspace status UI uses workspace connections and sync logs."""
    now = datetime.now(tz=timezone.utc)
    workspace = _WorkspaceLike(
        connections=[
            _ConnectionLike(
                connection_id="connection-success",
                name="GitHub to Snowflake",
                connection_url="https://cloud.airbyte.com/connections/connection-success",
                source_id="source-github",
                destination_id="destination-snowflake",
                source=_ConnectorLike(
                    name="GitHub",
                    connector_url="https://cloud.airbyte.com/source/source-github",
                ),
                destination=_ConnectorLike(
                    name="Snowflake",
                    connector_url="https://cloud.airbyte.com/destination/destination-snowflake",
                ),
                sync_results=[
                    _SyncResultLike(
                        job_id=100,
                        status=JobStatusEnum.SUCCEEDED,
                        start_time=now - timedelta(hours=1),
                        records_synced=1_500,
                        bytes_synced=2_000,
                    ),
                ],
            ),
            _ConnectionLike(
                connection_id="connection-running",
                name="Postgres to Snowflake",
                connection_url="https://cloud.airbyte.com/connections/connection-running",
                source_id="source-postgres",
                destination_id="destination-snowflake",
                source=_ConnectorLike(
                    name="Postgres",
                    connector_url="https://cloud.airbyte.com/source/source-postgres",
                ),
                destination=_ConnectorLike(
                    name="Snowflake",
                    connector_url="https://cloud.airbyte.com/destination/destination-snowflake",
                ),
                sync_results=[
                    _SyncResultLike(
                        job_id=400,
                        status=JobStatusEnum.RUNNING,
                        start_time=now - timedelta(minutes=5),
                        records_synced=0,
                        bytes_synced=0,
                    ),
                    _SyncResultLike(
                        job_id=399,
                        status=JobStatusEnum.SUCCEEDED,
                        start_time=now - timedelta(hours=4),
                        records_synced=500,
                        bytes_synced=1_000,
                    ),
                ],
            ),
            _ConnectionLike(
                connection_id="connection-failed",
                name="Stripe to Snowflake",
                connection_url="https://cloud.airbyte.com/connections/connection-failed",
                source_id="source-stripe",
                destination_id="destination-snowflake",
                source=_ConnectorLike(
                    name="Stripe",
                    connector_url="https://cloud.airbyte.com/source/source-stripe",
                ),
                destination=_ConnectorLike(
                    name="Snowflake",
                    connector_url="https://cloud.airbyte.com/destination/destination-snowflake",
                ),
                sync_results=[
                    _SyncResultLike(
                        job_id=200,
                        status=JobStatusEnum.FAILED,
                        start_time=now - timedelta(hours=2),
                        records_synced=0,
                        bytes_synced=0,
                    ),
                ],
            ),
            _ConnectionLike(
                connection_id="connection-canceled",
                name="Shopify to Snowflake",
                connection_url="https://cloud.airbyte.com/connections/connection-canceled",
                source_id="source-shopify",
                destination_id="destination-snowflake",
                source=_ConnectorLike(
                    name="Shopify",
                    connector_url="https://cloud.airbyte.com/source/source-shopify",
                ),
                destination=_ConnectorLike(
                    name="Snowflake",
                    connector_url="https://cloud.airbyte.com/destination/destination-snowflake",
                ),
                sync_results=[
                    _SyncResultLike(
                        job_id=300,
                        status=JobStatusEnum.CANCELLED,
                        start_time=now - timedelta(hours=3),
                        records_synced=0,
                        bytes_synced=0,
                    ),
                ],
            ),
            _ConnectionLike(
                connection_id="connection-no-syncs",
                name="Salesforce to Snowflake",
                connection_url="https://cloud.airbyte.com/connections/connection-no-syncs",
                source_id="source-salesforce",
                destination_id="destination-snowflake",
                source=_ConnectorLike(
                    name="Salesforce",
                    connector_url="https://cloud.airbyte.com/source/source-salesforce",
                ),
                destination=_ConnectorLike(
                    name="Snowflake",
                    connector_url="https://cloud.airbyte.com/destination/destination-snowflake",
                ),
                sync_results=[],
            ),
        ]
    )
    monkeypatch.setattr(
        "airbyte.mcp.interactive._workspace_sync_status_ui._get_cloud_workspace",
        lambda ctx, workspace_id=None: workspace,
    )

    result = show_workspace_sync_status(
        ctx=cast(Context, object()),
        workspace_id="workspace-id",
        max_connections=10,
        max_jobs_per_connection=2,
        agent_context="verbose",
    )

    assert workspace.limit == 10
    assert result.meta is not None
    raw_result = result.meta["airbyte_mcp_raw_result"]
    assert raw_result["total_connections"] == 5
    assert raw_result["problem_connections"] == 2
    assert raw_result["running_connections"] == 1
    assert raw_result["recent_success_rate"] == 50.0
    assert raw_result["model_preview_count"] == 5
    running_connection = next(
        connection
        for connection in raw_result["connections"]
        if connection["connection_id"] == "connection-running"
    )
    assert running_connection["running_job_id"] == 400
    assert running_connection["latest_job_id"] == 399
    assert running_connection["latest_status"] == "succeeded"

    text_content = result.content[0]
    assert isinstance(text_content, TextContent)
    assert "connection-failed" in text_content.text
    assert "show_connection_sync_history" in text_content.text

    structured_content = result.structured_content
    assert structured_content is not None
    assert "$prefab" in structured_content
    view = str(structured_content["view"])
    assert "Workspace sync status" in view
    assert "PieChart" in view
    assert "'height': 360" in view
    assert "mt-4 items-start" in view
    assert "filtered_connection_rows" in view
    assert "Filter table:" in view
    assert "Succeeded" in view
    assert "#22c55e" in view
    assert "Canceled" in view
    assert "#eab308" in view
    assert "No syncs" in view
    assert "#94a3b8" in view
    assert "Ask for sync history" in view
    assert "connection-failed" in view

    text_only_result = show_workspace_sync_status(
        ctx=cast(Context, object()),
        workspace_id="workspace-id",
        max_connections=10,
        max_jobs_per_connection=2,
        suppress_ui=True,
    )
    assert text_only_result.structured_content is None
    assert text_only_result.meta is not None
    assert text_only_result.meta["airbyte_mcp_raw_result"]["suppress_ui"] is True
