# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Verify deferred registry tools through a real MCP client session."""

from __future__ import annotations

import asyncio
from unittest.mock import Mock

import pytest
import requests
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from airbyte.mcp import registry
from airbyte.registry import InstallType


@pytest.fixture
def registry_app() -> FastMCP:
    app = FastMCP("registry-protocol-test")
    registry.register_registry_tools(app)
    return app


def test_registry_protocol_exposes_annotations_and_optional_arguments(
    registry_app: FastMCP,
) -> None:
    async def check() -> None:
        async with Client(registry_app) as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
        tool = tools["list_connectors"]
        assert tool.annotations is not None
        assert tool.annotations.read_only_hint is True
        assert tool.annotations.idempotent_hint is True
        assert set(tool.input_schema["properties"]) == {
            "keyword_filter",
            "connector_type_filter",
            "install_types",
        }
        assert not tool.input_schema.get("required")

    asyncio.run(check())


@pytest.mark.parametrize(
    ("arguments", "expected"),
    [
        ({}, ["destination-postgres", "source-faker", "source-github"]),
        ({"keyword_filter": "GITHUB"}, ["source-github"]),
        ({"connector_type_filter": "destination"}, ["destination-postgres"]),
        ({"install_types": "python"}, ["source-faker"]),
        ({"install_types": ["python", "yaml"]}, ["source-faker", "source-github"]),
    ],
)
def test_registry_protocol_calls_registered_tool(
    registry_app: FastMCP,
    monkeypatch: pytest.MonkeyPatch,
    arguments: dict[str, object],
    expected: list[str],
) -> None:
    def available(install_type: InstallType | str) -> list[str]:
        if install_type == InstallType.ANY:
            return ["source-github", "destination-postgres", "source-faker"]
        return {"python": ["source-faker"], "yaml": ["source-github"]}[install_type]

    boundary = Mock(side_effect=available)
    monkeypatch.setattr(registry, "get_available_connectors", boundary)

    async def check() -> None:
        async with Client(registry_app) as client:
            result = await client.call_tool("list_connectors", arguments)
        assert not result.is_error
        assert result.data == expected

    asyncio.run(check())
    boundary.assert_any_call(install_type=InstallType.ANY)


def test_registry_protocol_rejects_invalid_argument_before_external_lookup(
    registry_app: FastMCP, monkeypatch: pytest.MonkeyPatch
) -> None:
    boundary = Mock()
    monkeypatch.setattr(registry, "get_available_connectors", boundary)

    async def check() -> None:
        async with Client(registry_app) as client:
            with pytest.raises(ToolError):
                await client.call_tool(
                    "list_connectors", {"connector_type_filter": "invalid"}
                )

    asyncio.run(check())
    boundary.assert_not_called()


@pytest.mark.parametrize("failed", [False, True], ids=["empty-history", "lookup-error"])
def test_registry_protocol_preserves_history_result_contract(
    registry_app: FastMCP, monkeypatch: pytest.MonkeyPatch, failed: bool
) -> None:
    boundary = Mock(
        return_value=[],
        side_effect=requests.RequestException("offline lookup") if failed else None,
    )
    monkeypatch.setattr(registry, "_get_connector_version_history", boundary)

    async def check() -> None:
        async with Client(registry_app) as client:
            result = await client.call_tool(
                "get_connector_version_history", {"connector_name": "source-faker"}
            )
        assert not result.is_error
        assert result.data == ("Failed to fetch changelog." if failed else [])

    asyncio.run(check())
    boundary.assert_called_once_with(
        connector_name="source-faker", num_versions_to_validate=5
    )
