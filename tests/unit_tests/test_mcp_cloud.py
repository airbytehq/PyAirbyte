# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Airbyte Cloud MCP tools."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pytest
from airbyte.mcp import cloud as cloud_mcp


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
                name="first",
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
                name="first",
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
                name="first",
                connection_url=f"https://cloud.airbyte.com/connection-{index}",
                source_id=f"source-connection-{index}",
                destination_id=f"destination-connection-{index}",
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
