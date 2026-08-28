# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the typed Airbyte Agents entity MCP tools."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from airbyte.agents.models import (
    AgentConnectorMetadata,
    AgentExecuteResult,
    AgentExecutionMetadata,
)
from airbyte.exceptions import PyAirbyteInputError
from airbyte.mcp import agents as agents_mcp
from airbyte.mcp import agents_entities as entities_mcp
from fastmcp import Context


CTX = cast(Context, object())
DEFAULT_PAYLOAD: Any = [{"id": "1"}]


class _AgentConnectorLike:
    """Records forwarded arguments and returns a caller-supplied payload."""

    def __init__(
        self,
        payload: Any = DEFAULT_PAYLOAD,  # noqa: ANN401  # Any action payload.
    ) -> None:
        self.payload = payload
        self.calls: list[dict[str, Any]] = []

    def execute(
        self,
        entity: str,
        action: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to the recorded call.
    ) -> AgentExecuteResult:
        """Record the call and return the configured payload."""
        self.calls.append({
            "entity": entity,
            "action": action,
            "api_args": api_args,
            **kwargs,
        })
        return AgentExecuteResult(
            status="success",
            result=self.payload,
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
def connector_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> Any:  # noqa: ANN401  # Returns a closure over the patched resolver.
    """Return a factory that patches the connector resolver with a recording stub."""

    def _factory(payload: Any = DEFAULT_PAYLOAD) -> _AgentConnectorLike:  # noqa: ANN401
        stub = _AgentConnectorLike(payload)
        monkeypatch.setattr(
            agents_mcp,
            "_get_agent_connector",
            lambda ctx, connector_id, workspace_id=None: stub,
        )
        return stub

    return _factory


def test_list_entities_shapes_page(connector_factory: Any) -> None:  # noqa: ANN401
    """Verify `list_agent_entities` returns entities plus the pagination cursor."""
    connector = connector_factory([{"id": "1"}, {"id": "2"}])

    result = entities_mcp.list_agent_entities(
        ctx=CTX,
        connector_id="connector-id",
        entity_type="issues",
        api_args={"repository": "airbytehq/PyAirbyte"},
        select_fields="id,title",
        exclude_fields=None,
        page_size=2,
        cursor="cursor-1",
        intent=None,
        workspace_id="workspace-id",
    )

    assert result.entities == [{"id": "1"}, {"id": "2"}]
    assert result.has_next_page is True
    assert result.end_cursor == "cursor-2"
    assert connector.calls[0]["action"] == "list"
    assert connector.calls[0]["select_fields"] == ["id", "title"]
    assert connector.calls[0]["page_size"] == 2
    assert connector.calls[0]["cursor"] == "cursor-1"


@pytest.mark.parametrize(
    ("use_api_search", "expected_action"),
    [
        pytest.param(False, "search", id="context_store_search"),
        pytest.param(True, "api_search", id="upstream_api_search"),
    ],
)
def test_search_entities_selects_action(
    connector_factory: Any,  # noqa: ANN401
    use_api_search: bool,
    expected_action: str,
) -> None:
    """Verify `use_api_search` chooses between the two search actions."""
    connector = connector_factory()

    entities_mcp.search_agent_entities(
        ctx=CTX,
        connector_id="connector-id",
        entity_type="issues",
        api_args='{"query": "flaky"}',
        use_api_search=use_api_search,
        select_fields=None,
        exclude_fields=None,
        page_size=None,
        cursor=None,
        intent=None,
        workspace_id="workspace-id",
    )

    assert connector.calls[0]["action"] == expected_action
    assert connector.calls[0]["api_args"] == {"query": "flaky"}


@pytest.mark.parametrize(
    ("payload", "expected_entity", "is_rejected"),
    [
        pytest.param({"id": "1"}, {"id": "1"}, False, id="single_object"),
        pytest.param([{"id": "1"}], {"id": "1"}, False, id="single_item_list"),
        pytest.param([], None, False, id="no_match"),
        pytest.param(None, None, False, id="null_payload"),
        pytest.param([{"id": "1"}, {"id": "2"}], None, True, id="multiple_matches"),
        pytest.param("not-an-entity", None, True, id="non_entity_payload"),
    ],
)
def test_get_entity_coerces_payload(
    connector_factory: Any,  # noqa: ANN401
    payload: Any,  # noqa: ANN401
    expected_entity: dict[str, Any] | None,
    is_rejected: bool,
) -> None:
    """Verify `get_agent_entity` returns at most one entity, or raises when it cannot."""
    connector_factory(payload)

    def _call() -> entities_mcp.AgentEntityResult:
        return entities_mcp.get_agent_entity(
            ctx=CTX,
            connector_id="connector-id",
            entity_type="issues",
            api_args={"number": 1127},
            select_fields=None,
            exclude_fields=None,
            intent=None,
            workspace_id="workspace-id",
        )

    if is_rejected:
        with pytest.raises(PyAirbyteInputError):
            _call()
        return

    assert _call().entity == expected_entity


@pytest.mark.parametrize(
    ("tool_name", "expected_action"),
    [
        pytest.param("create_agent_entity", "create", id="create"),
        pytest.param("update_agent_entity", "update", id="update"),
        pytest.param("delete_agent_entity", "delete", id="delete"),
    ],
)
def test_write_tools_forward_action_and_payload(
    connector_factory: Any,  # noqa: ANN401
    tool_name: str,
    expected_action: str,
) -> None:
    """Verify each write tool runs its own action and echoes the written entity."""
    connector = connector_factory({"id": "1", "state": "closed"})

    result = getattr(entities_mcp, tool_name)(
        ctx=CTX,
        connector_id="connector-id",
        entity_type="issues",
        api_args='{"number": 1127}',
        intent="closing a stale issue",
        workspace_id="workspace-id",
    )

    assert connector.calls[0]["action"] == expected_action
    assert connector.calls[0]["api_args"] == {"number": 1127}
    assert connector.calls[0]["intent"] == "closing a stale issue"
    assert result.entity == {"id": "1", "state": "closed"}


def test_entity_tools_are_registered_with_expected_hints() -> None:
    """Verify the entity tools reach the server with the intended annotations."""
    from airbyte.mcp.server import app  # noqa: PLC0415  # Importing builds the server.

    tools = {tool.name: tool for tool in asyncio.run(app._list_tools())}  # noqa: SLF001

    for read_tool in (
        "list_agent_entities",
        "search_agent_entities",
        "get_agent_entity",
    ):
        assert tools[read_tool].annotations.readOnlyHint is True

    for write_tool in (
        "create_agent_entity",
        "update_agent_entity",
        "delete_agent_entity",
    ):
        assert tools[write_tool].annotations.readOnlyHint is False

    assert tools["delete_agent_entity"].annotations.destructiveHint is True
    assert tools["create_agent_entity"].annotations.destructiveHint is False
