# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the Airbyte Agents MCP tools."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from airbyte.agents.models import (
    AgentConnectorDetails,
    AgentConnectorMetadata,
    AgentContextStoreEntity,
    AgentContextStoreReadiness,
    AgentExecuteResult,
    AgentExecutionMetadata,
)
from airbyte.exceptions import PyAirbyteInputError
from airbyte.mcp import agents as agents_mcp
from fastmcp import Context


class _AgentConnectorLike:
    """Records the arguments the MCP layer forwards to `AgentConnector.execute`."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def execute(
        self,
        entity: str,
        action: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to the recorded call.
    ) -> AgentExecuteResult:
        """Record the call and return a fixed successful result."""
        self.calls.append({
            "entity": entity,
            "action": action,
            "api_args": api_args,
            **kwargs,
        })
        return AgentExecuteResult(
            status="success",
            result=[{"id": "1"}],
            connector_metadata=AgentConnectorMetadata(
                has_next_page=True,
                end_cursor="cursor-2",
            ),
            execution_metadata=AgentExecutionMetadata(
                connector_instance_id="connector-id",
                execution_time_ms=42,
            ),
        )


@pytest.fixture
def connector(monkeypatch: pytest.MonkeyPatch) -> _AgentConnectorLike:
    """Patch the MCP connector resolver to return a recording stub."""
    stub = _AgentConnectorLike()
    monkeypatch.setattr(
        agents_mcp,
        "_get_agent_connector",
        lambda ctx, connector_id: stub,
    )
    return stub


def _execute_ro(**kwargs: Any) -> agents_mcp.AgentExecuteToolResult:  # noqa: ANN401
    """Call the read-only tool with defaults for its optional arguments."""
    return agents_mcp.execute_agent_connector_ro(
        ctx=cast(Context, object()),
        connector_id="connector-id",
        entity=kwargs.pop("entity", "issues"),
        action=kwargs.pop("action", "list"),
        api_args=kwargs.pop("api_args", None),
        select_fields=kwargs.pop("select_fields", None),
        exclude_fields=kwargs.pop("exclude_fields", None),
        limit=kwargs.pop("limit", None),
        cursor=kwargs.pop("cursor", None),
        intent=kwargs.pop("intent", None),
    )


def _execute(**kwargs: Any) -> agents_mcp.AgentExecuteToolResult:  # noqa: ANN401
    """Call the write-capable tool with defaults for its optional arguments."""
    return agents_mcp.execute_agent_connector(
        ctx=cast(Context, object()),
        connector_id="connector-id",
        entity=kwargs.pop("entity", "issues"),
        action=kwargs.pop("action", "create"),
        api_args=kwargs.pop("api_args", None),
        select_fields=kwargs.pop("select_fields", None),
        exclude_fields=kwargs.pop("exclude_fields", None),
        limit=kwargs.pop("limit", None),
        cursor=kwargs.pop("cursor", None),
        intent=kwargs.pop("intent", None),
        read_only=kwargs.pop("read_only", None),
    )


def test_execute_result_is_shaped_for_agents(connector: _AgentConnectorLike) -> None:
    """Verify the tool result exposes pagination and timing without the raw envelope."""
    result = _execute_ro()

    assert result.status == "success"
    assert result.result == [{"id": "1"}]
    assert result.has_next_page is True
    assert result.end_cursor == "cursor-2"
    assert result.execution_time_ms == 42


def test_api_args_accepts_json_string(connector: _AgentConnectorLike) -> None:
    """Verify `api_args` is parsed when an agent passes it as a JSON string."""
    _execute_ro(api_args='{"state": "open"}')

    assert connector.calls[0]["api_args"] == {"state": "open"}


@pytest.mark.parametrize(
    "api_args",
    [
        pytest.param("[1, 2]", id="json-array"),
        pytest.param("not json", id="not-json"),
    ],
)
def test_api_args_rejects_non_object_input(
    connector: _AgentConnectorLike,
    api_args: str,
) -> None:
    """Verify `api_args` must resolve to a JSON object."""
    with pytest.raises(PyAirbyteInputError):
        _execute_ro(api_args=api_args)


def test_field_lists_accept_csv_strings(connector: _AgentConnectorLike) -> None:
    """Verify field selection arguments accept CSV strings as well as lists."""
    _execute_ro(select_fields="id,title", exclude_fields=["body"])

    assert connector.calls[0]["select_fields"] == ["id", "title"]
    assert connector.calls[0]["exclude_fields"] == ["body"]


def test_write_tool_forwards_write_action(connector: _AgentConnectorLike) -> None:
    """Verify the write-capable tool passes write actions through to the connector."""
    _execute(action="create", api_args={"title": "Bug"})

    assert connector.calls[0]["action"] == "create"


def test_write_tool_rejects_writes_when_read_only_is_requested(
    connector: _AgentConnectorLike,
) -> None:
    """Verify `read_only=True` blocks write actions before any request is sent."""
    with pytest.raises(PyAirbyteInputError):
        _execute(action="delete", read_only=True)

    assert connector.calls == []


def test_write_tool_allows_reads_when_read_only_is_requested(
    connector: _AgentConnectorLike,
) -> None:
    """Verify `read_only=True` still permits read actions."""
    _execute(action="list", read_only=True)

    assert connector.calls[0]["action"] == "list"


def test_read_only_tool_action_type_excludes_writes() -> None:
    """Verify the read-only tool's action type offers no write or download actions."""
    read_actions = set(agents_mcp.get_args(agents_mcp.AgentReadAction))

    assert read_actions == {"list", "get", "search", "api_search"}
    assert "download" not in set(agents_mcp.get_args(agents_mcp.AgentAction))


def test_describe_tool_reports_context_store_entities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `describe_agent_connector` surfaces entities and warnings."""

    class _DescribableConnector:
        def describe(self) -> AgentConnectorDetails:
            return AgentConnectorDetails(
                connector_id="connector-id",
                name="GitHub",
                workspace_id="workspace-id",
                source_definition_name="GitHub",
                context_store_readiness=AgentContextStoreReadiness(
                    supported_context_store_entities=[AgentContextStoreEntity(entity="issues")],
                ),
                warnings=["Context Store is still syncing."],
            )

    monkeypatch.setattr(
        agents_mcp,
        "_get_agent_connector",
        lambda ctx, connector_id: _DescribableConnector(),
    )

    result = agents_mcp.describe_agent_connector(
        ctx=cast(Context, object()),
        connector_id="connector-id",
    )

    assert result.context_store_entities == ["issues"]
    assert result.warnings == ["Context Store is still syncing."]


def test_agents_tools_are_registered_with_expected_read_only_hints() -> None:
    """Verify the Agents tools reach the server with the intended readonly annotations."""
    from airbyte.mcp.server import app  # noqa: PLC0415  # Importing builds the server.

    tools = {
        tool.name: tool
        for tool in asyncio.run(app._list_tools())
        if "agent" in tool.name
    }  # noqa: SLF001

    assert tools["execute_agent_connector_ro"].annotations.readOnlyHint is True
    assert tools["execute_agent_connector"].annotations.readOnlyHint is False
    assert "read_only" in tools["execute_agent_connector"].parameters["properties"]
    assert (
        "read_only" not in tools["execute_agent_connector_ro"].parameters["properties"]
    )
