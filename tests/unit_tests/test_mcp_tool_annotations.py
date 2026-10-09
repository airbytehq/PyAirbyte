# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Regression guards for MCP tool annotations."""

from __future__ import annotations

import airbyte.mcp.server  # noqa: F401  # Importing registers every MCP tool module.
from airbyte.mcp import cloud as cloud_mcp, guidance as guidance_mcp
from fastmcp_extensions.decorators import _REGISTERED_TOOLS  # noqa: PLC2701

READ_ONLY_NAME_PREFIXES = ("list_", "describe_", "get_", "check_")
OPEN_WORLD_TOOL_NAMES = {
    "execute_external_api_query",
    "execute_external_sql_query",
}


def _cloud_tool_entries() -> list[tuple[object, dict]]:
    return [(f, a) for f, a in _REGISTERED_TOOLS if f.__module__ == cloud_mcp.__name__]


def test_check_cloud_connector_is_read_only_and_idempotent() -> None:
    """`check_cloud_connector` only returns the connector check result."""
    (annotations,) = [
        a for f, a in _REGISTERED_TOOLS if f is cloud_mcp.check_cloud_connector
    ]
    assert annotations["readOnlyHint"] is True
    assert annotations["idempotentHint"] is True


def test_get_github_issue_creation_link_is_read_only_and_idempotent() -> None:
    """`get_github_issue_creation_link` only returns a GitHub URL."""
    (annotations,) = [
        a
        for f, a in _REGISTERED_TOOLS
        if f is guidance_mcp.get_github_issue_creation_link
    ]
    assert annotations["readOnlyHint"] is True
    assert annotations["idempotentHint"] is True


def test_read_prefixed_cloud_tools_are_all_read_only() -> None:
    """Every cloud tool named list_*/describe_*/get_*/check_* must be read-only."""
    violations = [
        f.__name__
        for f, a in _cloud_tool_entries()
        if f.__name__.startswith(READ_ONLY_NAME_PREFIXES) and not a.get("readOnlyHint")
    ]
    assert violations == []


def test_only_external_data_access_tools_are_open_world() -> None:
    """`openWorldHint` marks tools reaching the customer's external systems only."""
    open_world = {f.__name__ for f, a in _REGISTERED_TOOLS if a.get("openWorldHint")}
    assert open_world == OPEN_WORLD_TOOL_NAMES
