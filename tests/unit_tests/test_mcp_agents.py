# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the Airbyte Agents MCP tools."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
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
from airbyte.agents.connectors import AgentConnector
from airbyte.constants import MCP_CONFIG_BEARER_TOKEN, MCP_CONFIG_ORGANIZATION_ID
from airbyte.exceptions import AirbyteError, PyAirbyteInputError
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


class _RaisingOrganization:
    """Stands in for `AgentOrganization` and fails the way the Agents API does."""

    def __init__(self, error: AirbyteError) -> None:
        self._error = error

    def list_workspaces(self) -> list[Any]:
        """Raise the configured error."""
        raise self._error


class _RaisingWorkspace:
    """Stands in for `AgentWorkspace` and fails the way the Agents API does."""

    def __init__(self, error: AirbyteError) -> None:
        self._error = error

    def list_connectors(self) -> list[Any]:
        """Raise the configured error."""
        raise self._error


class _RaisingConnector:
    """Stands in for `AgentConnector` and fails the way the Agents API does."""

    def __init__(self, error: AirbyteError) -> None:
        self._error = error

    def execute(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        """Raise the configured error."""
        raise self._error

    def describe(self) -> Any:  # noqa: ANN401
        """Raise the configured error."""
        raise self._error


@pytest.fixture
def connector(monkeypatch: pytest.MonkeyPatch) -> _AgentConnectorLike:
    """Patch the MCP connector resolver to return a recording stub."""
    stub = _AgentConnectorLike()
    monkeypatch.setattr(
        agents_mcp,
        "_get_agent_connector",
        lambda ctx, connector_id, workspace_id=None: stub,
    )
    return stub


def _execute_ro(**kwargs: Any) -> agents_mcp.AgentExecuteToolResult:  # noqa: ANN401
    """Call the read-only tool with defaults for its optional arguments."""
    return agents_mcp.execute_agent_connector_ro(
        ctx=cast(Context, object()),
        connector_id="connector-id",
        entity_type=kwargs.pop("entity_type", "issues"),
        action=kwargs.pop("action", "list"),
        api_args=kwargs.pop("api_args", None),
        select_fields=kwargs.pop("select_fields", None),
        exclude_fields=kwargs.pop("exclude_fields", None),
        page_size=kwargs.pop("page_size", None),
        cursor=kwargs.pop("cursor", None),
        intent=kwargs.pop("intent", None),
        workspace_id=kwargs.pop("workspace_id", "workspace-id"),
    )


def _execute(**kwargs: Any) -> agents_mcp.AgentExecuteToolResult:  # noqa: ANN401
    """Call the write-capable tool with defaults for its optional arguments."""
    return agents_mcp.execute_agent_connector(
        ctx=cast(Context, object()),
        connector_id="connector-id",
        entity_type=kwargs.pop("entity_type", "issues"),
        action=kwargs.pop("action", "create"),
        api_args=kwargs.pop("api_args", None),
        select_fields=kwargs.pop("select_fields", None),
        exclude_fields=kwargs.pop("exclude_fields", None),
        page_size=kwargs.pop("page_size", None),
        cursor=kwargs.pop("cursor", None),
        intent=kwargs.pop("intent", None),
        read_only=kwargs.pop("read_only", None),
        workspace_id=kwargs.pop("workspace_id", "workspace-id"),
    )


def test_execute_result_is_shaped_for_agents(connector: _AgentConnectorLike) -> None:
    """Verify the tool result exposes pagination and timing without the raw envelope."""
    result = _execute_ro()

    assert result.status == "success"
    assert result.result == [{"id": "1"}]
    assert result.has_next_page is True
    assert result.end_cursor == "cursor-2"
    assert result.execution_time_ms == 42


@pytest.mark.parametrize(
    ("tool_kwargs", "expected_forwarded"),
    [
        pytest.param(
            {"api_args": '{"state": "open"}'},
            {"api_args": {"state": "open"}},
            id="api_args_json_string",
        ),
        pytest.param(
            {"api_args": {"state": "open"}},
            {"api_args": {"state": "open"}},
            id="api_args_dict",
        ),
        pytest.param(
            {"select_fields": "id,title", "exclude_fields": ["body"]},
            {"select_fields": ["id", "title"], "exclude_fields": ["body"]},
            id="field_lists_csv_and_list",
        ),
        pytest.param(
            {"api_args": "[1, 2]"},
            None,
            id="api_args_json_array_rejected",
        ),
        pytest.param(
            {"api_args": "not json"},
            None,
            id="api_args_not_json_rejected",
        ),
    ],
)
def test_argument_coercion(
    connector: _AgentConnectorLike,
    tool_kwargs: dict[str, Any],
    expected_forwarded: dict[str, Any] | None,
) -> None:
    """Verify agent-supplied arguments are coerced, or rejected when unusable."""
    if expected_forwarded is None:
        with pytest.raises(PyAirbyteInputError):
            _execute_ro(**tool_kwargs)
        assert connector.calls == []
        return

    _execute_ro(**tool_kwargs)
    for key, expected_value in expected_forwarded.items():
        assert connector.calls[0][key] == expected_value


@pytest.mark.parametrize(
    ("action", "read_only", "is_rejected"),
    [
        pytest.param("create", None, False, id="write_allowed_by_default"),
        pytest.param("delete", None, False, id="delete_allowed_by_default"),
        pytest.param("delete", True, True, id="write_rejected_when_read_only"),
        pytest.param("create", True, True, id="create_rejected_when_read_only"),
        pytest.param("list", True, False, id="read_allowed_when_read_only"),
    ],
)
def test_write_tool_read_only_enforcement(
    connector: _AgentConnectorLike,
    action: str,
    read_only: bool | None,
    is_rejected: bool,
) -> None:
    """Verify the write-capable tool honors the caller's `read_only` request."""
    if is_rejected:
        with pytest.raises(PyAirbyteInputError):
            _execute(action=action, read_only=read_only)
        assert connector.calls == []
        return

    _execute(action=action, read_only=read_only)
    assert connector.calls[0]["action"] == action


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
                    supported_context_store_entities=[
                        AgentContextStoreEntity(entity="issues")
                    ],
                ),
                warnings=["Context Store is still syncing."],
            )

    monkeypatch.setattr(
        agents_mcp,
        "_get_agent_connector",
        lambda ctx, connector_id, workspace_id=None: _DescribableConnector(),
    )

    result = agents_mcp.describe_agent_connector(
        ctx=cast(Context, object()),
        connector_id="connector-id",
        workspace_id="workspace-id",
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


@pytest.mark.parametrize(
    ("workspace_connector_ids", "requested_workspace_id", "expect_error"),
    [
        pytest.param(
            ["connector-id"], "workspace-1", False, id="connector_in_workspace"
        ),
        pytest.param(
            ["other-connector-id"],
            "workspace-1",
            True,
            id="connector_in_other_workspace",
        ),
        pytest.param([], "workspace-1", True, id="empty_workspace"),
        pytest.param(["connector-id"], None, True, id="missing_workspace_rejected"),
    ],
)
def test_connector_resolution_validates_workspace_scope(
    monkeypatch: pytest.MonkeyPatch,
    workspace_connector_ids: list[str],
    requested_workspace_id: str | None,
    expect_error: bool,
) -> None:
    """Verify a connector outside the requested workspace is rejected before it is used."""
    monkeypatch.setattr(
        agents_mcp,
        "get_mcp_config",
        lambda ctx, key: "fake-token" if key == MCP_CONFIG_BEARER_TOKEN else None,
    )
    monkeypatch.setattr(
        agents_mcp.AgentWorkspace,
        "list_connectors",
        lambda self: [
            AgentConnector(connector_id=connector_id, credentials=self._credentials)  # noqa: SLF001
            for connector_id in workspace_connector_ids
        ],
    )

    if expect_error:
        with pytest.raises((PyAirbyteInputError, AirbyteError)):
            agents_mcp._get_agent_connector(  # noqa: SLF001
                cast(Context, object()),
                "connector-id",
                requested_workspace_id,
            )
        return

    connector = agents_mcp._get_agent_connector(  # noqa: SLF001
        cast(Context, object()),
        "connector-id",
        requested_workspace_id,
    )
    assert connector.connector_id == "connector-id"


def _agents_error(status_code: int | None) -> AirbyteError:
    """Return an Agents API error carrying the given HTTP status code."""
    return AirbyteError(
        message="Agents API request failed.",
        context={"status_code": status_code} if status_code is not None else {},
    )


def _patch_mcp_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch `get_mcp_config` with a token and a configured organization ID."""
    monkeypatch.setattr(
        agents_mcp,
        "get_mcp_config",
        lambda ctx, key: {
            MCP_CONFIG_BEARER_TOKEN: "fake-token",
            MCP_CONFIG_ORGANIZATION_ID: "org-from-config",
        }.get(key),
    )


_ACCESS_FAILURE_CASES = [
    pytest.param(
        "_get_agent_organization",
        _RaisingOrganization,
        lambda: agents_mcp.list_agent_workspaces(
            ctx=cast(Context, object()),
            organization_id=None,
        ),
        {"workspaces": []},
        id="list_workspaces",
    ),
    pytest.param(
        "_get_agent_workspace",
        _RaisingWorkspace,
        lambda: agents_mcp.list_agent_connectors(
            ctx=cast(Context, object()),
            workspace_id="workspace-1",
        ),
        {"connectors": []},
        id="list_connectors",
    ),
    pytest.param(
        "_get_agent_connector",
        _RaisingConnector,
        _execute_ro,
        {"result": None, "status": agents_mcp.AGENTS_ACCESS_DENIED_STATUS},
        id="execute",
    ),
    pytest.param(
        "_get_agent_connector",
        _RaisingConnector,
        lambda: agents_mcp.describe_agent_connector(
            ctx=cast(Context, object()),
            connector_id="connector-id",
            workspace_id="workspace-1",
        ),
        {"context_store_entities": [], "connector_id": "connector-id"},
        id="describe",
    ),
]
"""Each Agents tool, the resolver it fails in, how to call it, and its empty payload."""


@pytest.mark.parametrize(
    ("status_code", "expected_message"),
    [
        pytest.param(401, agents_mcp.AGENTS_UNAUTHORIZED_MESSAGE, id="unauthorized"),
        pytest.param(403, agents_mcp.AGENTS_FORBIDDEN_MESSAGE, id="forbidden"),
    ],
)
@pytest.mark.parametrize(
    ("resolver_name", "raising_stub", "call_tool", "expected_result_fields"),
    _ACCESS_FAILURE_CASES,
)
def test_agents_tools_report_access_failures(
    monkeypatch: pytest.MonkeyPatch,
    resolver_name: str,
    raising_stub: Callable[[AirbyteError], Any],
    call_tool: Callable[[], Any],
    expected_result_fields: dict[str, Any],
    status_code: int,
    expected_message: str,
) -> None:
    """Verify an unentitled caller gets a concise message instead of an exception."""
    monkeypatch.setattr(
        agents_mcp,
        resolver_name,
        lambda *args, **kwargs: raising_stub(_agents_error(status_code)),  # noqa: ARG005
    )

    result = call_tool()

    assert result.message == expected_message
    for field, expected_value in expected_result_fields.items():
        assert getattr(result, field) == expected_value


@pytest.mark.parametrize(
    ("resolver_name", "raising_stub", "call_tool", "expected_result_fields"),
    _ACCESS_FAILURE_CASES,
)
def test_agents_tools_reraise_unrelated_errors(
    monkeypatch: pytest.MonkeyPatch,
    resolver_name: str,
    raising_stub: Callable[[AirbyteError], Any],
    call_tool: Callable[[], Any],
    expected_result_fields: dict[str, Any],  # noqa: ARG001  # Shared case list.
) -> None:
    """Verify a non-authorization failure keeps its original error."""
    monkeypatch.setattr(
        agents_mcp,
        resolver_name,
        lambda *args, **kwargs: raising_stub(_agents_error(500)),  # noqa: ARG005
    )

    with pytest.raises(AirbyteError):
        call_tool()


@pytest.mark.parametrize(
    ("explicit_organization_id", "expected_organization_id"),
    [
        pytest.param(None, "org-from-config", id="falls_back_to_config"),
        pytest.param("org-from-argument", "org-from-argument", id="explicit_wins"),
    ],
)
def test_organization_id_resolution(
    monkeypatch: pytest.MonkeyPatch,
    explicit_organization_id: str | None,
    expected_organization_id: str,
) -> None:
    """Verify the org ID comes from the tool argument first, then the header or env var."""
    _patch_mcp_config(monkeypatch)

    organization = agents_mcp._get_agent_organization(  # noqa: SLF001
        cast(Context, object()),
        explicit_organization_id,
    )

    assert organization.organization_id == expected_organization_id


def test_workspace_organization_id_comes_from_mcp_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify workspace-scoped tools send the configured organization ID too."""
    _patch_mcp_config(monkeypatch)

    workspace = agents_mcp._get_agent_workspace(  # noqa: SLF001
        cast(Context, object()),
        "workspace-1",
    )

    assert workspace.organization_id == "org-from-config"
    assert workspace._credentials.organization_id == "org-from-config"  # noqa: SLF001
