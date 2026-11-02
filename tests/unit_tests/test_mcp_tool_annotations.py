# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Regression guards for MCP tool annotations."""

from __future__ import annotations

import asyncio

from fastmcp.tools import Tool
from fastmcp_extensions.decorators import _REGISTERED_TOOLS  # noqa: PLC2701

from airbyte.mcp import cloud as cloud_mcp
from airbyte.mcp import server as server_mcp
from airbyte.mcp.registry import list_connector_definitions

READ_ONLY_NAME_PREFIXES = ("list_", "describe_", "get_", "check_")
TOOL_TITLE_OVERRIDES = {
    "execute_external_api_query": "Execute external API query",
    "execute_external_sql_query": "Execute external SQL query",
    "get_api_docs_urls": "Get API docs URLs",
    "get_local_source_stream_json_schema": "Get local source stream JSON schema",
    "run_local_sql_query": "Run local SQL query",
    "deploy_noop_destination": "Deploy no-op destination",
    "search_airbyte_knowledge_sources": "Search Airbyte knowledge sources",
    "update_connection_unsafe": "Update connection (unsafe)",
}
OPEN_WORLD_TOOL_NAMES = {
    "execute_external_api_query",
    "execute_external_sql_query",
    "execute_external_search_query",
}


def _cloud_tool_entries() -> list[tuple[object, dict]]:
    return [(f, a) for f, a in _REGISTERED_TOOLS if f.__module__ == cloud_mcp.__name__]


async def _server_tools_with_expected_names() -> tuple[
    list[Tool], dict[str, Tool | None]
]:
    tools = await server_mcp.app.list_tools()
    expected_names = (*TOOL_TITLE_OVERRIDES, "rename_connection")
    tools_by_name = {
        name: await server_mcp.app.get_tool(name) for name in expected_names
    }
    return tools, tools_by_name


def test_tool_names_are_unique_and_cloud_prefix_free() -> None:
    """Tool registrations expose only the new unambiguous names."""
    names = [f.__name__ for f, _ in _REGISTERED_TOOLS]
    assert len(names) == len(set(names))
    assert not any("cloud" in name for name in names)
    assert "list_connector_definitions" in names
    assert "list_connectors" in names
    assert "get_connector_definition" in names
    assert "show_connector_definitions" in names


def test_connector_listing_descriptions_distinguish_registry_from_workspace() -> None:
    """Registry types and deployed workspace connectors have separate discovery tools."""
    registry_description = list_connector_definitions.__doc__ or ""
    assert "connector types available to deploy" in registry_description
    assert "`list_connectors`" in registry_description
    assert "`list_custom_source_definitions`" in registry_description
    workspace_description = cloud_mcp.list_connectors.__doc__ or ""
    assert "deployed source and destination" in workspace_description
    assert "`list_connector_definitions`" in workspace_description


def test_check_cloud_connector_is_read_only_and_idempotent() -> None:
    """`check_connector` only returns the connector check result."""
    (annotations,) = [a for f, a in _REGISTERED_TOOLS if f is cloud_mcp.check_connector]
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


def test_server_tools_have_human_readable_titles() -> None:
    """Every advertised tool and requested override has matching title annotations."""
    tools, tools_by_name = asyncio.run(_server_tools_with_expected_names())

    for tool in tools:
        assert tool.title
        assert tool.annotations is not None
        assert tool.annotations.title == tool.title

    for name, expected_title in TOOL_TITLE_OVERRIDES.items():
        tool = tools_by_name[name]
        assert tool is not None
        assert tool.title == expected_title
        assert tool.annotations is not None
        assert tool.annotations.title == expected_title

    rename_tool = tools_by_name["rename_connection"]
    assert rename_tool is not None
    assert rename_tool.title == "Rename connection"
    assert rename_tool.annotations is not None
    assert rename_tool.annotations.title == "Rename connection"
