# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Airbyte Cloud MCP tools."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, cast

import pytest
from airbyte.cloud.models import JobStatusEnum
from airbyte.mcp import cloud as cloud_mcp
from airbyte.mcp.cloud import (
    CloudConnectionResult,
    CloudDestinationResult,
    CloudSourceResult,
)
from fastmcp import Context


@dataclass
class _SyncResultLike:
    """Subset of `SyncResult` used by connection status tests."""

    job_id: int
    status: JobStatusEnum
    start_time: datetime

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
