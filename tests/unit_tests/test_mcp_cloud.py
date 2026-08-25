# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Airbyte Cloud MCP tools."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, cast

import pytest
from airbyte.cloud.connectors import CheckResult
from airbyte.cloud.models import JobStatusEnum
from airbyte.mcp import cloud as cloud_mcp
from airbyte.mcp.cloud import (
    CloudConnectionResult,
    CloudDestinationResult,
    CloudSourceResult,
    ConnectorCheckResult,
    SyncJobResult,
)
from fastmcp import Context


@dataclass
class _SyncResultLike:
    """Subset of `SyncResult` used by connection status tests."""

    job_id: int
    status: JobStatusEnum
    start_time: datetime
    bytes_synced: int = 0
    records_synced: int = 0
    job_url: str = "https://cloud.airbyte.com/jobs"

    def get_job_status(self) -> JobStatusEnum:
        """Return the configured job status."""
        return self.status

    def is_job_complete(self) -> bool:
        """Return whether the test sync job is complete."""
        return True


@dataclass
class _CloudSourceLike:
    """Subset of `CloudSource` used by tested MCP list tools."""

    source_id: str
    name: str
    connector_url: str


@dataclass
class _CloudDestinationLike:
    """Subset of `CloudDestination` used by tested MCP list tools."""

    destination_id: str
    name: str
    connector_url: str


@dataclass
class _CheckableConnectorLike:
    """Subset of `CloudConnector` used by connector check tests."""

    connector_id: str
    connector_type: str
    result: CheckResult
    received_raise_on_error: bool | None = None

    def check(self, *, raise_on_error: bool = True) -> CheckResult:
        """Capture the error handling option and return the configured result."""
        self.received_raise_on_error = raise_on_error
        return self.result


class _ConnectorCheckWorkspace:
    """Return checkable source and destination test doubles."""

    def __init__(self, result: CheckResult) -> None:
        """Create source and destination test doubles."""
        self.source = _CheckableConnectorLike(
            connector_id="source-id",
            connector_type="source",
            result=result,
        )
        self.destination = _CheckableConnectorLike(
            connector_id="destination-id",
            connector_type="destination",
            result=result,
        )

    def get_source(self, *, source_id: str) -> _CheckableConnectorLike:
        """Return the source test double."""
        assert source_id == self.source.connector_id
        return self.source

    def get_destination(self, *, destination_id: str) -> _CheckableConnectorLike:
        """Return the destination test double."""
        assert destination_id == self.destination.connector_id
        return self.destination


@dataclass
class _CloudConnectionLike:
    """Subset of `CloudConnection` used by tested MCP list tools."""

    connection_id: str
    name: str
    connection_url: str
    source_id: str
    destination_id: str
    failed: bool = False

    def get_previous_sync_logs(self, *, limit: int = 20) -> list[_SyncResultLike]:
        """Return one completed sync result for connection status tests."""
        _ = limit
        status = JobStatusEnum.FAILED if self.failed else JobStatusEnum.SUCCEEDED
        return [
            _SyncResultLike(
                job_id=1,
                status=status,
                start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
        ]


@dataclass
class _CancelableConnectionLike:
    """Subset of `CloudConnection` used by sync cancellation tests."""

    sync_result: _SyncResultLike
    received_job_id: int | None = None

    def cancel_sync(self, *, job_id: int | None = None) -> _SyncResultLike:
        """Capture the job ID and return the cancelled sync result."""
        self.received_job_id = job_id
        return self.sync_result


class _CancellationWorkspace:
    """Return a connection test double for sync cancellation tests."""

    def __init__(self, connection: _CancelableConnectionLike) -> None:
        """Create a workspace test double."""
        self.connection = connection

    def get_connection(self, *, connection_id: str) -> _CancelableConnectionLike:
        """Return the configured connection."""
        assert connection_id == "connection-id"
        return self.connection


class _CloudWorkspace:
    """Capture `limit` values passed from MCP list tools."""

    def __init__(self) -> None:
        """Create a workspace test double."""
        self.limits: dict[str, int | None] = {}

    def list_sources(self, *, limit: int | None = None) -> list[_CloudSourceLike]:
        """Capture source list limit and return source test data."""
        self.limits["sources"] = limit
        items = [
            _CloudSourceLike(
                source_id=f"source-{index}",
                name="target" if index == 2 else "miss",
                connector_url=f"https://cloud.airbyte.com/source-{index}",
            )
            for index in range(1, 3)
        ]
        return items if limit is None else items[:limit]

    def list_destinations(
        self, *, limit: int | None = None
    ) -> list[_CloudDestinationLike]:
        """Capture destination list limit and return destination test data."""
        self.limits["destinations"] = limit
        items = [
            _CloudDestinationLike(
                destination_id=f"destination-{index}",
                name="target" if index == 2 else "miss",
                connector_url=f"https://cloud.airbyte.com/destination-{index}",
            )
            for index in range(1, 3)
        ]
        return items if limit is None else items[:limit]

    def list_connections(
        self, *, limit: int | None = None
    ) -> list[_CloudConnectionLike]:
        """Capture connection list limit and return connection test data."""
        self.limits["connections"] = limit
        items = [
            _CloudConnectionLike(
                connection_id=f"connection-{index}",
                name="target" if index == 2 else "miss",
                connection_url=f"https://cloud.airbyte.com/connection-{index}",
                source_id=f"source-connection-{index}",
                destination_id=f"destination-connection-{index}",
                failed=index == 2,
            )
            for index in range(1, 3)
        ]
        return items if limit is None else items[:limit]


@pytest.mark.parametrize(
    "tool,limit_key,extra_kwargs",
    [
        pytest.param(
            cloud_mcp.list_deployed_cloud_source_connectors,
            "sources",
            {},
            id="sources",
        ),
        pytest.param(
            cloud_mcp.list_deployed_cloud_destination_connectors,
            "destinations",
            {},
            id="destinations",
        ),
        pytest.param(
            cloud_mcp.list_deployed_cloud_connections,
            "connections",
            {"with_connection_status": False, "failing_connections_only": False},
            id="connections",
        ),
    ],
)
def test_mcp_cloud_list_tools_pass_limit_to_workspace(
    monkeypatch: pytest.MonkeyPatch,
    tool: Callable[..., list[object]],
    limit_key: str,
    extra_kwargs: dict[str, object],
) -> None:
    """Verify Cloud MCP list tools forward `limit` to workspace list operations."""
    workspace = _CloudWorkspace()
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: workspace,
    )

    results = tool(
        ctx=object(),
        workspace_id="workspace-id",
        name_contains=None,
        limit=1,
        **extra_kwargs,
    )

    assert workspace.limits[limit_key] == 1
    assert len(results) == 1


@pytest.mark.parametrize(
    "tool,limit_key,extra_kwargs",
    [
        pytest.param(
            cloud_mcp.list_deployed_cloud_source_connectors,
            "sources",
            {},
            id="sources",
        ),
        pytest.param(
            cloud_mcp.list_deployed_cloud_destination_connectors,
            "destinations",
            {},
            id="destinations",
        ),
        pytest.param(
            cloud_mcp.list_deployed_cloud_connections,
            "connections",
            {"with_connection_status": False, "failing_connections_only": False},
            id="connections",
        ),
    ],
)
def test_mcp_cloud_list_tools_apply_limit_after_name_filter(
    monkeypatch: pytest.MonkeyPatch,
    tool: Callable[
        ...,
        list[CloudSourceResult]
        | list[CloudDestinationResult]
        | list[CloudConnectionResult],
    ],
    limit_key: str,
    extra_kwargs: dict[str, object],
) -> None:
    """Verify Cloud MCP list tools cap results after local name filtering."""
    workspace = _CloudWorkspace()
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: workspace,
    )

    results = tool(
        ctx=object(),
        workspace_id="workspace-id",
        name_contains="target",
        limit=1,
        **extra_kwargs,
    )

    assert workspace.limits[limit_key] is None
    assert len(results) == 1
    assert results[0].name == "target"


def test_mcp_cloud_connections_apply_limit_after_status_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify connection list caps results after local status filtering."""
    workspace = _CloudWorkspace()
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: workspace,
    )

    results = cloud_mcp.list_deployed_cloud_connections(
        ctx=cast(Context, object()),
        workspace_id="workspace-id",
        name_contains=None,
        limit=1,
        with_connection_status=False,
        failing_connections_only=True,
    )

    assert workspace.limits["connections"] is None
    assert len(results) == 1
    assert results[0].id == "connection-2"


@pytest.mark.parametrize(
    ("tool", "connector_id_parameter", "connector_id", "connector_type"),
    [
        pytest.param(
            cloud_mcp.check_cloud_source,
            "source_id",
            "source-id",
            "source",
            id="source",
        ),
        pytest.param(
            cloud_mcp.check_cloud_destination,
            "destination_id",
            "destination-id",
            "destination",
            id="destination",
        ),
    ],
)
@pytest.mark.parametrize(
    ("check_result", "expected_success", "expected_message"),
    [
        pytest.param(
            CheckResult(success=True),
            True,
            None,
            id="success",
        ),
        pytest.param(
            CheckResult(success=False, error_message="Invalid credentials"),
            False,
            "Invalid credentials",
            id="error-message",
        ),
        pytest.param(
            CheckResult(success=False, internal_error="Check service unavailable"),
            False,
            "Check service unavailable",
            id="internal-error",
        ),
        pytest.param(
            CheckResult(success=False),
            False,
            "Connector check failed without a failure message.",
            id="missing-message",
        ),
    ],
)
def test_mcp_cloud_connector_checks_map_results(
    monkeypatch: pytest.MonkeyPatch,
    tool: Callable[..., object],
    connector_id_parameter: str,
    connector_id: str,
    connector_type: str,
    check_result: CheckResult,
    expected_success: bool,
    expected_message: str | None,
) -> None:
    """Verify Cloud MCP connector checks map result fields and disable raising."""
    workspace = _ConnectorCheckWorkspace(check_result)
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: workspace,
    )

    result = cast(
        ConnectorCheckResult,
        tool(
            ctx=cast(Context, object()),
            workspace_id="workspace-id",
            **{connector_id_parameter: connector_id},
        ),
    )

    connector = (
        workspace.source if connector_type == "source" else workspace.destination
    )
    assert result.connector_id == connector_id
    assert result.connector_type == connector_type
    assert result.succeeded is expected_success
    assert result.message == expected_message
    assert connector.received_raise_on_error is False


def test_cancel_cloud_sync_returns_cancelled_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the Cloud MCP cancellation tool maps the cancelled sync result."""
    sync_result = _SyncResultLike(
        job_id=42,
        status=JobStatusEnum.CANCELLED,
        bytes_synced=123,
        records_synced=456,
        start_time=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        job_url="https://cloud.airbyte.com/jobs/42",
    )
    connection = _CancelableConnectionLike(sync_result=sync_result)
    workspace = _CancellationWorkspace(connection)
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: workspace,
    )

    result = cast(
        SyncJobResult,
        cloud_mcp.cancel_cloud_sync(
            ctx=cast(Context, object()),
            connection_id="connection-id",
            job_id=42,
            workspace_id="workspace-id",
        ),
    )

    assert connection.received_job_id == 42
    assert result.job_id == 42
    assert result.status == "cancelled"
    assert result.bytes_synced == 123
    assert result.records_synced == 456
    assert result.start_time == "2026-01-02T03:04:05+00:00"
    assert result.job_url == "https://cloud.airbyte.com/jobs/42"


def test_cancel_cloud_sync_forwards_missing_job_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify the Cloud MCP cancellation tool forwards a missing job ID."""
    connection = _CancelableConnectionLike(
        sync_result=_SyncResultLike(
            job_id=42,
            status=JobStatusEnum.CANCELLED,
            start_time=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
    )
    workspace = _CancellationWorkspace(connection)
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: workspace,
    )

    cloud_mcp.cancel_cloud_sync(
        ctx=cast(Context, object()),
        connection_id="connection-id",
        job_id=None,
        workspace_id="workspace-id",
    )

    assert connection.received_job_id is None
