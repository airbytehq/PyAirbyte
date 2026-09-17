# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the Airbyte Agents MCP tools."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, cast
from unittest.mock import Mock

import pytest
from airbyte.agents.models import (
    AgentConnectorDetails,
    AgentConnectorMetadata,
    AgentContextStoreEntity,
    AgentContextStoreReadiness,
    AgentExecuteResult,
    AgentExecutionMetadata,
    AgentSkillDocs,
    AgentSkillInfo,
    AgentSkillSection,
)
from airbyte.agents.connectors import AgentConnector, AgentReadAction, AgentWriteAction
from airbyte.cloud.client import CloudClient
from airbyte.constants import (
    MCP_CONFIG_API_URL,
    MCP_CONFIG_BEARER_TOKEN,
    MCP_CONFIG_CONFIG_API_URL,
    MCP_CONFIG_ORGANIZATION_ID,
    MCP_CONFIG_WORKSPACE_ID,
)
from airbyte.exceptions import AirbyteError, PyAirbyteInputError
from airbyte.mcp import agents as agents_mcp
from airbyte.mcp._tool_utils import SafeModeError
from fastmcp import Context


class _AgentConnectorLike:
    """Records the arguments the MCP layer forwards to `AgentConnector.execute`."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def execute(
        self,
        entity_type: str,
        action: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to the recorded call.
    ) -> AgentExecuteResult:
        """Record the call and return a fixed successful result."""
        self.calls.append({
            "entity": entity_type,
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


class _FakeSource:
    """Stand-in for `CloudSource` that never calls the Cloud API."""

    def __init__(self, connector_id: str, name: str) -> None:
        self.connector_id = connector_id
        self.name = name


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

    def get_connector(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        """Raise the configured error."""
        raise self._error

    def list_connectors(self) -> list[Any]:
        """Raise the configured error."""
        raise self._error

    def list_skills(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        """Raise the configured error."""
        raise self._error

    def search_skills(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        """Raise the configured error."""
        raise self._error

    def read_skill_docs(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        """Raise the configured error."""
        raise self._error


class _RaisingConnector:
    """Stands in for `AgentConnector` and fails the way the Agents API does."""

    def __init__(self, error: AirbyteError) -> None:
        self._error = error

    def execute(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        """Raise the configured error."""
        raise self._error

    def inspect(self) -> Any:  # noqa: ANN401
        """Raise the configured error."""
        raise self._error


@pytest.fixture
def connector(monkeypatch: pytest.MonkeyPatch) -> _AgentConnectorLike:
    """Patch the MCP connector resolver to return a recording stub."""
    stub = _AgentConnectorLike()
    monkeypatch.setattr(
        agents_mcp,
        "_get_agent_connector",
        lambda ctx, connector_id, workspace_id=None, organization_id=None: stub,
    )
    return stub


@pytest.fixture(autouse=True)
def safe_mode_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep existing Agents behavior tests focused on their API behavior."""
    monkeypatch.setattr(
        "airbyte.mcp._tool_utils.AIRBYTE_CLOUD_MCP_SAFE_MODE",
        False,
    )


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
        organization_id=kwargs.pop("organization_id", None),
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
        organization_id=kwargs.pop("organization_id", None),
    )


def test_execute_result_is_shaped_for_agents(connector: _AgentConnectorLike) -> None:
    """Verify the tool result exposes pagination and timing without the raw envelope."""
    result = _execute_ro()

    assert result.status == "success"
    assert result.result == [{"id": "1"}]
    assert result.has_next_page is True
    assert result.end_cursor == "cursor-2"
    assert result.execution_time_ms == 42


def test_execute_sql_select_falls_back_to_cloud_destinations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `sql_select` falls back to Cloud destinations when Agents listing omits one."""
    _patch_mcp_config(monkeypatch)
    monkeypatch.setattr(agents_mcp.AgentWorkspace, "list_connectors", lambda self: [])

    class _CloudDestination:
        connector_id = "connector-id"

    class _CloudWorkspace:
        def list_destinations(self) -> list[_CloudDestination]:
            return [_CloudDestination()]

    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id: _CloudWorkspace(),
    )
    monkeypatch.setattr(
        agents_mcp.AgentConnector,
        "execute",
        lambda self, *args, **kwargs: AgentExecuteResult(status="success", result=[]),
    )

    result = _execute(
        action="sql_select",
        api_args={"sql": "SELECT 1", "sql_dialect": "snowflake"},
    )

    assert result.status == "success"


def test_execute_unknown_connector_raises_when_not_a_cloud_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify an unknown connector still raises after the Cloud destination fallback."""
    _patch_mcp_config(monkeypatch)
    monkeypatch.setattr(agents_mcp.AgentWorkspace, "list_connectors", lambda self: [])

    class _CloudWorkspace:
        def list_destinations(self) -> list[Any]:
            return []

        def list_sources(self) -> list[Any]:
            return []

    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id: _CloudWorkspace(),
    )

    with pytest.raises(
        AirbyteError, match="No connector found with the given ID or name"
    ) as excinfo:
        _execute(action="sql_select")

    assert "list_agent_connectors" in str(excinfo.value)


def test_execute_reports_cloud_source_not_enabled_for_agents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Cloud source the Agents API does not list is reported as disabled, not missing."""
    _patch_mcp_config(monkeypatch)
    monkeypatch.setattr(agents_mcp.AgentWorkspace, "list_connectors", lambda self: [])

    class _CloudWorkspace:
        def list_destinations(self) -> list[Any]:
            return []

        def list_sources(self) -> list[Any]:
            return [_FakeSource(connector_id="connector-id", name="GitHub prod")]

    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id: _CloudWorkspace(),
    )

    result = _execute(action="list")

    assert result.status == agents_mcp.AGENTS_ACCESS_DENIED_STATUS
    assert result.message is not None
    assert "'GitHub prod' (connector-id)" in result.message
    assert "not enabled for Agents access" in result.message
    assert "Context layer" in result.message
    assert "cannot be enabled from this tool" in result.message


def test_execute_connector_lookup_error_is_not_treated_as_missing_connector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify unrelated connector lookup errors do not trigger Cloud destination fallback."""
    _patch_mcp_config(monkeypatch)

    def raise_lookup_error(self: Any) -> list[Any]:
        raise AirbyteError(message="Connector listing failed.")

    monkeypatch.setattr(
        agents_mcp.AgentWorkspace, "list_connectors", raise_lookup_error
    )
    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id: pytest.fail("Cloud fallback should not run"),
    )

    with pytest.raises(AirbyteError, match="Connector listing failed"):
        _execute(action="sql_select")


def test_execute_list_uses_positional_connector_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify non-SQL actions keep the workspace-validating connector lookup."""
    _patch_mcp_config(monkeypatch)
    connector = _AgentConnectorLike()
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def get_connector(self, *args: Any, **kwargs: Any) -> _AgentConnectorLike:
        calls.append((args, kwargs))
        return connector

    monkeypatch.setattr(agents_mcp.AgentWorkspace, "get_connector", get_connector)

    _execute(action="list")

    assert calls == [(("connector-id",), {})]


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
    assert {member.value for member in AgentReadAction} == {
        "list",
        "get",
        "search",
        "sql_select",
    }
    assert {member.value for member in AgentWriteAction} == {
        "create",
        "update",
        "delete",
    }
    assert "api_search" not in {member.value for member in AgentReadAction}
    assert "api_search" not in {member.value for member in AgentWriteAction}
    assert "download" not in {member.value for member in AgentReadAction}
    assert "download" not in {member.value for member in AgentWriteAction}


def test_inspect_tool_reports_context_store_entities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `inspect_agent_connector` surfaces entities, docs, and warnings."""

    class _InspectableConnector:
        def inspect(self) -> AgentConnectorDetails:
            return AgentConnectorDetails(
                connector_id="connector-id",
                name="GitHub",
                workspace_id="workspace-id",
                source_definition_name="GitHub",
                docs_skill_id="connector:github",
                context_store_readiness=AgentContextStoreReadiness(
                    supported_context_store_entities=[
                        AgentContextStoreEntity(entity="issues")
                    ],
                ),
                warnings=["Context Store is still syncing."],
            )

    class _InspectableWorkspace:
        def get_connector(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            return _InspectableConnector()

    monkeypatch.setattr(
        agents_mcp,
        "_get_agent_workspace",
        lambda *args, **kwargs: _InspectableWorkspace(),  # noqa: ARG005
    )

    result = agents_mcp.inspect_agent_connector(
        ctx=cast(Context, object()),
        connector_id="connector-id",
        workspace_id="workspace-id",
        organization_id=None,
    )

    assert result.context_store_entities == ["issues"]
    assert result.docs_skill_id == "connector:github"
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
    assert tools["list_agent_skills"].annotations.readOnlyHint is True
    assert tools["search_agent_skills"].annotations.readOnlyHint is True
    assert tools["read_agent_skill_docs"].annotations.readOnlyHint is True
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
        agents_mcp,
        "_get_cloud_client",
        lambda ctx: type(
            "_CloudClient",
            (),
            {
                "resolve_default_workspace_id": lambda self: None,
                "get_workspace_parent_organization_id": lambda self, workspace_id: None,
            },
        )(),
    )
    monkeypatch.setattr(
        agents_mcp.AgentWorkspace,
        "list_connectors",
        lambda self: [
            AgentConnector(connector_id=connector_id, credentials=self._credentials)  # noqa: SLF001
            for connector_id in workspace_connector_ids
        ],
    )
    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id: type(
            "_CloudWorkspace",
            (),
            {"list_destinations": lambda self: [], "list_sources": lambda self: []},
        )(),
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


def _agents_error(
    status_code: int | None,
    response_text: str | None = None,
) -> AirbyteError:
    """Return an Agents API error carrying the given HTTP status code and body."""
    context: dict[str, Any] = {}
    if status_code is not None:
        context["status_code"] = status_code
    if response_text is not None:
        context["response_text"] = response_text
    return AirbyteError(message="Agents API request failed.", context=context)


_ACTOR_NOT_ENABLED_BODY = (
    '{"message": "Actor is not enabled for Agents access.", '
    '"errors": [{"field": "general", "message": "Actor is not enabled for Agents access.", '
    '"error_code": "unknown"}]}'
)


@pytest.mark.parametrize(
    ("response_text", "expected_message"),
    [
        pytest.param(
            _ACTOR_NOT_ENABLED_BODY,
            f"{agents_mcp.AGENTS_ACTOR_NOT_ENABLED_DETAIL} "
            f"{agents_mcp.AGENTS_ENABLE_ACTOR_GUIDANCE}",
            id="actor_not_enabled_sonar_envelope",
        ),
        pytest.param(
            '{"detail": "Actor is not enabled for Agents access."}',
            f"{agents_mcp.AGENTS_ACTOR_NOT_ENABLED_DETAIL} "
            f"{agents_mcp.AGENTS_ENABLE_ACTOR_GUIDANCE}",
            id="actor_not_enabled_fastapi_detail",
        ),
        pytest.param(
            '{"errors": [{"message": "Organization is not onboarded to Agents."}]}',
            "The Airbyte Agents API denied access: Organization is not onboarded to Agents.",
            id="other_detail_in_errors_list",
        ),
        pytest.param(
            '{"message": "Organization is not onboarded to Agents."}',
            "The Airbyte Agents API denied access: Organization is not onboarded to Agents.",
            id="other_detail",
        ),
        pytest.param(None, agents_mcp.AGENTS_FORBIDDEN_MESSAGE, id="no_body"),
        pytest.param("", agents_mcp.AGENTS_FORBIDDEN_MESSAGE, id="empty_body"),
        pytest.param(
            "<html>nginx</html>", agents_mcp.AGENTS_FORBIDDEN_MESSAGE, id="non_json"
        ),
        pytest.param(
            '["x"]', agents_mcp.AGENTS_FORBIDDEN_MESSAGE, id="json_not_object"
        ),
        pytest.param(
            '{"message": "  "}', agents_mcp.AGENTS_FORBIDDEN_MESSAGE, id="blank"
        ),
        pytest.param(
            '{"message": 42}', agents_mcp.AGENTS_FORBIDDEN_MESSAGE, id="non_str"
        ),
    ],
)
def test_forbidden_message_surfaces_api_detail(
    response_text: str | None,
    expected_message: str,
) -> None:
    """Verify a 403 surfaces the API's reason, falling back to the generic explanation."""
    message = agents_mcp._agents_access_message(  # noqa: SLF001
        _agents_error(403, response_text)
    )

    assert message == expected_message


def test_sql_select_forbidden_reports_actor_not_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `sql_select` on a not-enabled destination tells the agent how to fix it."""
    monkeypatch.setattr(
        agents_mcp,
        "_get_agent_connector",
        lambda *args, **kwargs: _RaisingConnector(  # noqa: ARG005
            _agents_error(403, _ACTOR_NOT_ENABLED_BODY)
        ),
    )

    result = _execute_ro(
        action="sql_select",
        api_args={"sql": "SHOW TABLES", "sql_dialect": "snowflake"},
    )

    assert result.status == agents_mcp.AGENTS_ACCESS_DENIED_STATUS
    assert result.message is not None
    assert result.message.startswith("Actor is not enabled for Agents access.")
    assert "Settings -> Context layer" in result.message
    assert "Do not retry" in result.message


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


@pytest.mark.parametrize(
    "call_tool",
    [
        pytest.param(
            lambda: agents_mcp.list_agent_workspaces(
                ctx=cast(Context, object()),
                organization_id="organization-id",
            ),
            id="list_workspaces",
        ),
        pytest.param(
            lambda: agents_mcp.list_agent_connectors(
                ctx=cast(Context, object()),
                workspace_id="workspace-id",
                organization_id="organization-id",
            ),
            id="list_connectors",
        ),
        pytest.param(
            lambda: agents_mcp.inspect_agent_connector(
                ctx=cast(Context, object()),
                connector_id="connector-id",
                workspace_id="workspace-id",
                organization_id="organization-id",
            ),
            id="inspect_connector",
        ),
        pytest.param(
            lambda: agents_mcp.execute_agent_connector_ro(
                ctx=cast(Context, object()),
                connector_id="connector-id",
                entity_type="issues",
                action="list",
                api_args=None,
                select_fields=None,
                exclude_fields=None,
                page_size=None,
                cursor=None,
                intent=None,
                workspace_id="workspace-id",
                organization_id="organization-id",
            ),
            id="execute_connector_ro",
        ),
        pytest.param(
            lambda: agents_mcp.execute_agent_connector(
                ctx=cast(Context, object()),
                connector_id="connector-id",
                entity_type="issues",
                action="create",
                api_args=None,
                select_fields=None,
                exclude_fields=None,
                page_size=None,
                cursor=None,
                intent=None,
                read_only=None,
                workspace_id="workspace-id",
                organization_id="organization-id",
            ),
            id="execute_connector",
        ),
        pytest.param(
            lambda: agents_mcp.list_agent_skills(
                ctx=cast(Context, object()),
                workspace_id="workspace-id",
            ),
            id="list_skills",
        ),
        pytest.param(
            lambda: agents_mcp.search_agent_skills(
                ctx=cast(Context, object()),
                query="github",
                workspace_id="workspace-id",
            ),
            id="search_skills",
        ),
        pytest.param(
            lambda: agents_mcp.read_agent_skill_docs(
                ctx=cast(Context, object()),
                skill_id="connector:github",
                section=None,
                workspace_id="workspace-id",
            ),
            id="read_skill_docs",
        ),
    ],
)
def test_agents_entry_points_are_blocked_by_safe_mode(
    monkeypatch: pytest.MonkeyPatch,
    call_tool: Callable[[], Any],
) -> None:
    """Safe mode rejects every Agents entry point before resolver or API access."""
    monkeypatch.setattr(
        "airbyte.mcp._tool_utils.AIRBYTE_CLOUD_MCP_SAFE_MODE",
        True,
    )
    get_mcp_config = Mock(side_effect=AssertionError("config access"))
    agent_organization = Mock(name="AgentOrganization")
    agent_workspace = Mock(name="AgentWorkspace")
    cloud_client = Mock(name="_get_cloud_client")
    monkeypatch.setattr(agents_mcp, "get_mcp_config", get_mcp_config)
    monkeypatch.setattr(agents_mcp, "AgentOrganization", agent_organization)
    monkeypatch.setattr(agents_mcp, "AgentWorkspace", agent_workspace)
    monkeypatch.setattr(agents_mcp, "_get_cloud_client", cloud_client)

    with pytest.raises(SafeModeError):
        call_tool()

    get_mcp_config.assert_not_called()
    agent_organization.assert_not_called()
    agent_workspace.assert_not_called()
    cloud_client.assert_not_called()


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
            organization_id=None,
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
        "_get_agent_workspace",
        _RaisingWorkspace,
        lambda: agents_mcp.list_agent_skills(
            ctx=cast(Context, object()),
            workspace_id="workspace-1",
        ),
        {"skills": []},
        id="list_skills",
    ),
    pytest.param(
        "_get_agent_workspace",
        _RaisingWorkspace,
        lambda: agents_mcp.search_agent_skills(
            ctx=cast(Context, object()),
            query="github",
            workspace_id="workspace-1",
        ),
        {"skills": []},
        id="search_skills",
    ),
    pytest.param(
        "_get_agent_workspace",
        _RaisingWorkspace,
        lambda: agents_mcp.read_agent_skill_docs(
            ctx=cast(Context, object()),
            skill_id="connector:github",
            section=None,
            workspace_id="workspace-1",
        ),
        {"skill_id": "connector:github", "outline": [], "content": []},
        id="read_skill_docs",
    ),
    pytest.param(
        "_get_agent_workspace",
        _RaisingWorkspace,
        lambda: agents_mcp.inspect_agent_connector(
            ctx=cast(Context, object()),
            connector_id="connector-id",
            workspace_id="workspace-1",
            organization_id=None,
        ),
        {"context_store_entities": [], "connector_id": "connector-id"},
        id="inspect",
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


@pytest.mark.parametrize(
    ("explicit_organization_id", "expected_organization_id"),
    [
        pytest.param(None, "org-from-config", id="falls_back_to_config"),
        pytest.param("org-from-argument", "org-from-argument", id="explicit_wins"),
    ],
)
def test_list_agent_connectors_threads_organization_id(
    monkeypatch: pytest.MonkeyPatch,
    explicit_organization_id: str | None,
    expected_organization_id: str,
) -> None:
    """Verify `list_agent_connectors` passes `organization_id` to the workspace."""
    _patch_mcp_config(monkeypatch)
    constructed: list[dict[str, Any]] = []

    class _RecordingWorkspace:
        def __init__(self, **kwargs: Any) -> None:
            constructed.append(kwargs)
            self.workspace_id = kwargs["workspace_id"]
            self.organization_id = kwargs["organization_id"]

        def list_connectors(self) -> list[Any]:
            return []

    monkeypatch.setattr(agents_mcp, "AgentWorkspace", _RecordingWorkspace)
    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id: type(
            "_CloudWorkspace",
            (),
            {"list_destinations": lambda self: [], "list_sources": lambda self: []},
        )(),
    )

    result = agents_mcp.list_agent_connectors(
        ctx=cast(Context, object()),
        workspace_id="workspace-1",
        organization_id=explicit_organization_id,
    )

    assert constructed[0]["organization_id"] == expected_organization_id
    assert result.connectors == []


def test_list_agent_connectors_includes_sql_passthrough_destinations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify SQL destinations are listed as `sql_select`-only after the Agents sources."""
    _patch_mcp_config(monkeypatch)

    class _Source:
        connector_id = "source-gong"
        name = "Gong"

    class _Workspace:
        workspace_id = "workspace-1"

        def list_connectors(self) -> list[Any]:
            return [_Source()]

    class _CloudWorkspace:
        def list_destinations(self) -> list[Any]:
            return [_SNOWFLAKE_DESTINATION, _UNSUPPORTED_DESTINATION]

    monkeypatch.setattr(
        agents_mcp,
        "_get_agent_workspace",
        lambda *args, **kwargs: _Workspace(),  # noqa: ARG005
    )
    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id: _CloudWorkspace(),
    )

    result = agents_mcp.list_agent_connectors(
        ctx=cast(Context, object()),
        workspace_id="workspace-1",
        organization_id=None,
    )

    assert [c.connector_id for c in result.connectors] == [
        "source-gong",
        "dest-snowflake",
    ]
    source, destination = result.connectors
    assert source.connector_kind == "source"
    assert source.supported_actions is None
    assert destination.connector_kind == "destination"
    assert destination.connector_name == "Snowflake dev"
    assert destination.supported_actions == ["sql_select"]
    assert destination.sql_dialect == "snowflake"
    assert destination.note is not None
    assert "Settings -> Context layer" in destination.note
    assert "SHOW TABLES" in destination.note


def test_list_agent_connectors_access_denied_skips_destination_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a denied Agents listing returns early without a Cloud destination lookup."""
    monkeypatch.setattr(
        agents_mcp,
        "_get_agent_workspace",
        lambda *args, **kwargs: _RaisingWorkspace(_agents_error(403)),  # noqa: ARG005
    )
    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id: pytest.fail("Cloud lookup should not run"),
    )

    result = agents_mcp.list_agent_connectors(
        ctx=cast(Context, object()),
        workspace_id="workspace-1",
        organization_id=None,
    )

    assert result.connectors == []
    assert result.message == agents_mcp.AGENTS_FORBIDDEN_MESSAGE


def test_workspace_organization_id_comes_from_mcp_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify configured workspace and organization IDs avoid CloudClient lookup."""
    monkeypatch.setattr(
        agents_mcp,
        "get_mcp_config",
        lambda ctx, key: (
            "workspace-from-config"
            if key == MCP_CONFIG_WORKSPACE_ID
            else "org-from-config"
            if key == MCP_CONFIG_ORGANIZATION_ID
            else "fake-token"
            if key == MCP_CONFIG_BEARER_TOKEN
            else None
        ),
    )
    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_client",
        lambda ctx: pytest.fail("CloudClient lookup should not be called"),
    )

    workspace = agents_mcp._get_agent_workspace(  # noqa: SLF001
        cast(Context, object()),
        None,
    )

    assert workspace.workspace_id == "workspace-from-config"
    assert workspace.organization_id == "org-from-config"
    assert workspace._credentials.organization_id == "org-from-config"  # noqa: SLF001


def test_workspace_api_roots_come_from_mcp_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify configured API roots are forwarded into Agent credentials."""
    monkeypatch.setattr(
        agents_mcp,
        "get_mcp_config",
        lambda ctx, key: {
            MCP_CONFIG_API_URL: "https://proxy.example/v1",
            MCP_CONFIG_CONFIG_API_URL: "https://config.proxy.example/v1",
            MCP_CONFIG_BEARER_TOKEN: "fake-token",
            MCP_CONFIG_ORGANIZATION_ID: "org-from-config",
            MCP_CONFIG_WORKSPACE_ID: "workspace-from-config",
        }.get(key),
    )
    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_client",
        lambda ctx: pytest.fail("CloudClient lookup should not be called"),
    )

    workspace = agents_mcp._get_agent_workspace(  # noqa: SLF001
        cast(Context, object()),
        None,
    )

    assert workspace._credentials.public_api_root == "https://proxy.example/v1"  # noqa: SLF001
    assert (
        workspace._credentials.config_api_root == "https://config.proxy.example/v1"  # noqa: SLF001
    )


def test_skills_tools_shape_results(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify the skills tools shape `AgentSkillList`/`AgentSkillDocs` into results."""

    class _SkillLike:
        def __init__(self, info: AgentSkillInfo) -> None:
            self.info = info

    class _SkilledWorkspace:
        def list_skills(self) -> list[Any]:
            # Two skills spanning two pages; pagination is internal to the workspace.
            return [
                _SkillLike(
                    AgentSkillInfo(
                        id="connector:github",
                        kind="connector_source",
                        title="GitHub",
                        summary="GitHub usage docs.",
                        tags=["github"],
                    )
                ),
                _SkillLike(AgentSkillInfo(id="context-store", title="Context Store")),
            ]

        def search_skills(self, query: str) -> list[Any]:
            return []

        def read_skill_docs(
            self,
            skill_id: str,
            *,
            section: str | None = None,
        ) -> AgentSkillDocs:
            return AgentSkillDocs(
                metadata=AgentSkillInfo(
                    id=skill_id,
                    title="GitHub",
                    warnings=["Partial runtime metadata."],
                ),
                outline=[
                    AgentSkillSection(id="setup", title="Setup", available=True),
                    AgentSkillSection(id="faq", title="FAQ", available=False),
                ],
                section_id=section,
                content=[{"type": "paragraph", "text": "Hello"}],
            )

    monkeypatch.setattr(
        agents_mcp,
        "_get_agent_workspace",
        lambda ctx, workspace_id=None: _SkilledWorkspace(),  # noqa: ARG005
    )

    listed = agents_mcp.list_agent_skills(
        ctx=cast(Context, object()),
        workspace_id="workspace-1",
    )
    assert listed.skills == [
        agents_mcp.AgentSkillResult(
            skill_id="connector:github",
            kind="connector_source",
            title="GitHub",
            summary="GitHub usage docs.",
            tags=["github"],
        ),
        agents_mcp.AgentSkillResult(
            skill_id="context-store",
            title="Context Store",
            tags=[],
        ),
    ]

    searched = agents_mcp.search_agent_skills(
        ctx=cast(Context, object()),
        query="github",
        workspace_id="workspace-1",
    )
    assert searched.skills == []

    docs = agents_mcp.read_agent_skill_docs(
        ctx=cast(Context, object()),
        skill_id="connector:github",
        section="setup",
        workspace_id="workspace-1",
    )
    assert docs.skill_id == "connector:github"
    assert docs.title == "GitHub"
    assert docs.section_id == "setup"
    assert [section.section_id for section in docs.outline] == ["setup", "faq"]
    assert docs.outline[1].available is False
    assert docs.content == [{"type": "paragraph", "text": "Hello"}]
    assert docs.warnings == ["Partial runtime metadata."]


def test_explicit_workspace_derives_parent_organization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify an explicit workspace derives its parent organization."""
    monkeypatch.setattr(
        agents_mcp,
        "get_mcp_config",
        lambda ctx, key: "fake-token" if key == MCP_CONFIG_BEARER_TOKEN else None,
    )

    class _CloudClient:
        def resolve_default_workspace_id(self) -> str:
            pytest.fail("default workspace lookup should not be called")

        def get_workspace_parent_organization_id(self, workspace_id: str) -> str:
            assert workspace_id == "workspace-explicit"
            return "org-parent"

    monkeypatch.setattr(agents_mcp, "_get_cloud_client", lambda ctx: _CloudClient())

    workspace = agents_mcp._get_agent_workspace(  # noqa: SLF001
        cast(Context, object()),
        "workspace-explicit",
    )

    assert workspace.workspace_id == "workspace-explicit"
    assert workspace.organization_id == "org-parent"


def test_configured_workspace_derives_parent_organization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a configured workspace derives its parent organization."""
    monkeypatch.setattr(
        agents_mcp,
        "get_mcp_config",
        lambda ctx, key: (
            "workspace-from-config"
            if key == MCP_CONFIG_WORKSPACE_ID
            else "fake-token"
            if key == MCP_CONFIG_BEARER_TOKEN
            else None
        ),
    )

    class _CloudClient:
        def resolve_default_workspace_id(self) -> str:
            pytest.fail("default workspace lookup should not be called")

        def get_workspace_parent_organization_id(self, workspace_id: str) -> str:
            assert workspace_id == "workspace-from-config"
            return "org-parent"

    monkeypatch.setattr(agents_mcp, "_get_cloud_client", lambda ctx: _CloudClient())

    workspace = agents_mcp._get_agent_workspace(  # noqa: SLF001
        cast(Context, object()),
        None,
    )

    assert workspace.workspace_id == "workspace-from-config"
    assert workspace.organization_id == "org-parent"


def test_workspace_fallback_uses_user_default_workspace_parent_organization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify workspace fallback derives the parent organization."""
    monkeypatch.setattr(
        agents_mcp,
        "get_mcp_config",
        lambda ctx, key: "fake-token" if key == MCP_CONFIG_BEARER_TOKEN else None,
    )

    class _CloudClient:
        def resolve_default_workspace_id(self) -> str:
            return "ws-default"

        def get_workspace_parent_organization_id(self, _workspace_id: str) -> str:
            return "org-parent"

    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_client",
        lambda ctx: _CloudClient(),
    )

    workspace = agents_mcp._get_agent_workspace(  # noqa: SLF001
        cast(Context, object()),
        None,
    )

    assert workspace.workspace_id == "ws-default"
    assert workspace.organization_id == "org-parent"


def test_workspace_fallback_preserves_configured_organization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a configured organization wins over parent organization lookup."""
    monkeypatch.setattr(
        agents_mcp,
        "get_mcp_config",
        lambda ctx, key: (
            "fake-token"
            if key == MCP_CONFIG_BEARER_TOKEN
            else "org-cfg"
            if key == MCP_CONFIG_ORGANIZATION_ID
            else None
        ),
    )

    class _CloudClient:
        def resolve_default_workspace_id(self) -> str:
            return "ws-default"

        def get_workspace_parent_organization_id(self, _workspace_id: str) -> str:
            pytest.fail("parent organization lookup should not be called")

    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_client",
        lambda ctx: _CloudClient(),
    )

    workspace = agents_mcp._get_agent_workspace(  # noqa: SLF001
        cast(Context, object()),
        None,
    )

    assert workspace.workspace_id == "ws-default"
    assert workspace.organization_id == "org-cfg"


def test_workspace_fallback_ignores_parent_organization_lookup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify parent organization lookup errors leave the organization unset."""
    client = CloudClient(bearer_token="token")

    def raise_parent_organization_error(_workspace_id: str) -> str:
        raise AirbyteError(message="lookup failed")

    monkeypatch.setattr(
        client,
        "resolve_default_workspace_id",
        lambda: "ws-default",
    )
    monkeypatch.setattr(
        client,
        "_get_workspace_parent_organization_id",
        raise_parent_organization_error,
    )
    monkeypatch.setattr(
        agents_mcp,
        "get_mcp_config",
        lambda ctx, key: "fake-token" if key == MCP_CONFIG_BEARER_TOKEN else None,
    )
    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_client",
        lambda ctx: client,
    )

    workspace = agents_mcp._get_agent_workspace(  # noqa: SLF001
        cast(Context, object()),
        None,
    )

    assert workspace.workspace_id == "ws-default"
    assert workspace.organization_id is None


class _FakeDestinationForDocs:
    """Stand-in for `CloudDestination` in the skill-docs fallback tests."""

    def __init__(
        self,
        connector_id: str,
        name: str,
        definition_id: str,
        connections: list[Any] | None = None,
        sources: list[Any] | None = None,
    ) -> None:
        self.connector_id = connector_id
        self.name = name
        self.definition_id = definition_id
        self.connections_looked_up = False
        self._connections = connections or []
        self._sources = sources or []
        self.workspace = type(
            "_FakeWorkspace",
            (),
            {
                "workspace_id": "workspace-1",
                "list_connections": lambda _self: self.list_connections(),
                "list_sources": lambda _self: list(self._sources),
            },
        )()

    def list_connections(self) -> list[Any]:
        self.connections_looked_up = True
        return list(self._connections)


class _FakeConnectionForDocs:
    """Stand-in for `CloudConnection` in the skill-docs fallback tests."""

    def __init__(
        self,
        connection_id: str,
        name: str,
        destination_id: str,
        stream_names: list[str] | None = None,
        table_prefix: str = "",
    ) -> None:
        self.connection_id = connection_id
        self.name = name
        self.destination_id = destination_id
        self.source_id = "source-1"
        self.stream_names = stream_names or []
        self.table_prefix = table_prefix

    @property
    def source(self) -> Any:
        raise AssertionError("source must not be fetched lazily")


def _patch_destination_404(
    monkeypatch: pytest.MonkeyPatch,
    destinations: list[_FakeDestinationForDocs],
    sources: list[_FakeSource] | None = None,
) -> Any:
    """Make the Agents layer 404 and the Cloud workspace serve the given destinations."""
    not_found = _agents_error(404)

    class _NotFoundConnector:
        def inspect(self) -> Any:
            raise not_found

    class _NotFoundWorkspace:
        workspace_id = "workspace-1"
        organization_id = "org-1"

        def get_connector(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            return _NotFoundConnector()

        def read_skill_docs(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            raise not_found

    class _CloudWorkspaceWithDestinations:
        def __init__(self) -> None:
            self.list_destinations_calls = 0

        def list_destinations(self) -> list[Any]:
            self.list_destinations_calls += 1
            return list(destinations)

        def list_sources(self) -> list[Any]:
            return list(sources or [])

    monkeypatch.setattr(
        agents_mcp,
        "_get_agent_connector",
        lambda *args, **kwargs: _NotFoundConnector(),  # noqa: ARG005
    )
    monkeypatch.setattr(
        agents_mcp,
        "_get_agent_workspace",
        lambda *args, **kwargs: _NotFoundWorkspace(),  # noqa: ARG005
    )
    cloud_workspace = _CloudWorkspaceWithDestinations()
    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_workspace",
        lambda *args, **kwargs: cloud_workspace,  # noqa: ARG005
    )
    return cloud_workspace


_SNOWFLAKE_DESTINATION = _FakeDestinationForDocs(
    connector_id="dest-snowflake",
    name="Snowflake dev",
    definition_id="424892c4-daac-4491-b35d-c6688ba547ba",
)
_UNSUPPORTED_DESTINATION = _FakeDestinationForDocs(
    connector_id="dest-null",
    name="End-to-End Testing (/dev/null)",
    definition_id="f7a7d195-377f-cf5b-70a5-be6b819019dc",
)


def _inspect(connector_id: str) -> agents_mcp.AgentConnectorDetailsResult:
    return agents_mcp.inspect_agent_connector(
        ctx=cast(Context, object()),
        connector_id=connector_id,
        workspace_id="workspace-1",
        organization_id=None,
    )


def _read_docs(
    skill_id: str,
    section: str | None = None,
) -> agents_mcp.AgentSkillDocsResult:
    return agents_mcp.read_agent_skill_docs(
        ctx=cast(Context, object()),
        skill_id=skill_id,
        section=section,
        workspace_id="workspace-1",
    )


def test_inspect_destination_fallback_reports_docs_skill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A SQL passthrough destination gets built-in details instead of a 404."""
    _patch_destination_404(monkeypatch, [_SNOWFLAKE_DESTINATION])

    result = _inspect("dest-snowflake")

    assert result.connector_id == "dest-snowflake"
    assert result.connector_name == "Snowflake dev"
    assert result.docs_skill_id == "connector-destination:dest-snowflake"
    assert result.message is None


def test_inspect_destination_fallback_reports_unsupported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-SQL-passthrough destination gets a message instead of a 404."""
    _patch_destination_404(monkeypatch, [_UNSUPPORTED_DESTINATION])

    result = _inspect("dest-null")

    assert result.message is not None
    assert "not a SQL passthrough destination" in result.message
    assert result.docs_skill_id is None


def test_inspect_destination_fallback_reports_unknown_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ID that is neither a connector nor a destination gets a message, not a 404."""
    _patch_destination_404(monkeypatch, [_SNOWFLAKE_DESTINATION])

    result = _inspect("dest-unknown")

    assert result.message is not None
    assert "not found" in result.message
    assert "list_agent_connectors" in result.message


def test_inspect_reports_cloud_source_not_enabled_for_agents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Cloud source the Agents API 404s on is reported as disabled, not as not found."""
    _patch_destination_404(
        monkeypatch,
        [_SNOWFLAKE_DESTINATION],
        sources=[_FakeSource(connector_id="src-disabled", name="GitHub prod")],
    )

    result = _inspect("src-disabled")

    assert result.message is not None
    assert "'GitHub prod' (src-disabled)" in result.message
    assert "not enabled for Agents access" in result.message
    assert "Context layer" in result.message
    assert "not found" not in result.message


def test_list_agent_connectors_empty_explains_how_to_enable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An enrolled workspace with nothing enabled gets a message, not a bare empty list."""
    _patch_mcp_config(monkeypatch)
    monkeypatch.setattr(agents_mcp.AgentWorkspace, "list_connectors", lambda self: [])

    class _CloudWorkspace:
        def list_destinations(self) -> list[Any]:
            return []

        def list_sources(self) -> list[Any]:
            return [_FakeSource(connector_id="src-disabled", name="GitHub prod")]

    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id: _CloudWorkspace(),
    )

    result = agents_mcp.list_agent_connectors(
        ctx=cast(Context, object()),
        workspace_id="workspace-1",
        organization_id=None,
    )

    assert result.connectors == []
    assert result.message == agents_mcp.agents_no_connectors_enabled_message(
        "org-from-config"
    )
    assert (
        "https://cloud.airbyte.com/organization/org-from-config/settings/context-layer"
        in result.message
    )


def test_list_agent_connectors_empty_cloud_workspace_says_no_sources_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Cloud workspace with no sources is told to create one, not to enable one."""
    _patch_mcp_config(monkeypatch)
    monkeypatch.setattr(agents_mcp.AgentWorkspace, "list_connectors", lambda self: [])

    class _CloudWorkspace:
        def list_destinations(self) -> list[Any]:
            return []

        def list_sources(self) -> list[Any]:
            return []

    monkeypatch.setattr(
        agents_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id: _CloudWorkspace(),
    )

    result = agents_mcp.list_agent_connectors(
        ctx=cast(Context, object()),
        workspace_id="workspace-1",
        organization_id=None,
    )

    assert result.connectors == []
    assert result.message == agents_mcp.agents_workspace_has_no_sources_message(
        "org-from-config"
    )
    assert "has no source connectors" in result.message


def test_context_layer_guidance_omits_url_without_organization_id() -> None:
    """Without an organization ID there is no URL to interpolate, only the menu path."""
    with_org = agents_mcp.context_layer_enable_guidance("org-1")
    without_org = agents_mcp.context_layer_enable_guidance(None)

    assert (
        "https://cloud.airbyte.com/organization/org-1/settings/context-layer"
        in with_org
    )
    assert "https://" not in without_org
    assert "Organization settings -> Context layer" in without_org


def test_read_docs_destination_fallback_outline_skips_connections_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The outline response must not hit the Cloud connections listing."""
    destination = _FakeDestinationForDocs(
        connector_id="dest-snowflake",
        name="Snowflake dev",
        definition_id="424892c4-daac-4491-b35d-c6688ba547ba",
    )
    _patch_destination_404(monkeypatch, [destination])

    result = _read_docs("connector-destination:dest-snowflake")

    assert [section.section_id for section in result.outline] == [
        "sql-passthrough",
        "connections",
        "streams",
    ]
    assert result.content
    assert not destination.connections_looked_up


@pytest.mark.parametrize(
    ("definition_id", "dialect"),
    [
        pytest.param(
            "424892c4-daac-4491-b35d-c6688ba547ba", "snowflake", id="snowflake"
        ),
        pytest.param("22f6c74f-5699-40ff-833c-4a879ea40133", "bigquery", id="bigquery"),
    ],
)
def test_read_docs_destination_fallback_sql_passthrough_section(
    monkeypatch: pytest.MonkeyPatch,
    definition_id: str,
    dialect: str,
) -> None:
    _patch_destination_404(
        monkeypatch,
        [
            _FakeDestinationForDocs(
                connector_id="dest-1",
                name="Warehouse",
                definition_id=definition_id,
            )
        ],
    )

    result = _read_docs("connector-destination:dest-1", section="sql-passthrough")

    rendered = str(result.content)
    assert "SHOW TABLES" in rendered
    assert f'"sql_dialect": "{dialect}"' in rendered


def test_read_docs_destination_fallback_connections_and_streams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    matching = _FakeConnectionForDocs(
        connection_id="conn-1",
        name="GitHub to Snowflake",
        destination_id="dest-snowflake",
        stream_names=["issues"],
        table_prefix="raw_",
    )
    other = _FakeConnectionForDocs(
        connection_id="conn-2",
        name="Slack elsewhere",
        destination_id="dest-elsewhere",
    )
    _patch_destination_404(
        monkeypatch,
        [
            _FakeDestinationForDocs(
                connector_id="dest-snowflake",
                name="Snowflake dev",
                definition_id="424892c4-daac-4491-b35d-c6688ba547ba",
                connections=[matching, other],
                sources=[
                    type(
                        "_FakeSource",
                        (),
                        {"connector_id": "source-1", "name": "GitHub"},
                    )()
                ],
            )
        ],
    )

    connections_result = _read_docs(
        "connector-destination:dest-snowflake", section="connections"
    )
    rendered = str(connections_result.content)
    assert "GitHub to Snowflake" in rendered
    assert "conn-1" in rendered
    assert "GitHub" in rendered
    assert "Slack elsewhere" not in rendered

    streams_result = _read_docs(
        "connector-destination:dest-snowflake", section="streams"
    )
    rendered = str(streams_result.content)
    assert "GitHub to Snowflake" in rendered
    assert "issues" in rendered


def test_read_docs_destination_fallback_empty_connections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_destination_404(monkeypatch, [_SNOWFLAKE_DESTINATION])

    result = _read_docs("connector-destination:dest-snowflake", section="connections")

    assert "No connections" in str(result.content)


def test_read_docs_destination_fallback_source_prefix_resolves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`connector-source:<destination id>` resolves the destination as well."""
    _patch_destination_404(monkeypatch, [_SNOWFLAKE_DESTINATION])

    result = _read_docs("connector-source:dest-snowflake")

    assert result.message is None
    assert result.content


def test_read_docs_destination_fallback_rejects_unknown_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_destination_404(monkeypatch, [_SNOWFLAKE_DESTINATION])

    with pytest.raises(PyAirbyteInputError, match="sql-passthrough"):
        _read_docs("connector-destination:dest-snowflake", section="bogus")


def test_read_docs_destination_fallback_reports_unsupported_and_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_destination_404(monkeypatch, [_UNSUPPORTED_DESTINATION])

    unsupported = _read_docs("connector-destination:dest-null")
    assert unsupported.message is not None
    assert "not a SQL passthrough destination" in unsupported.message

    unknown = _read_docs("connector-destination:dest-unknown")
    assert unknown.message is not None
    assert "not found" in unknown.message


def test_inspect_destination_fallback_handles_get_connector_miss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ID rejected by `get_connector` itself is reported as not found, not raised."""

    class _MissingConnectorWorkspace:
        workspace_id = "workspace-1"

        def get_connector(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            raise AirbyteError(message="No connector found with the given ID or name.")

        def read_skill_docs(self, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            raise AssertionError("read_skill_docs must not run")

    _patch_destination_404(monkeypatch, [_SNOWFLAKE_DESTINATION])
    monkeypatch.setattr(
        agents_mcp,
        "_get_agent_workspace",
        lambda *args, **kwargs: _MissingConnectorWorkspace(),  # noqa: ARG005
    )

    result = _inspect("dest-unknown")

    assert result.message is not None
    assert "not found" in result.message


def test_inspect_destination_fallback_lists_destinations_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fallback inspect enumerates the workspace destinations exactly once."""
    cloud_workspace = _patch_destination_404(monkeypatch, [_SNOWFLAKE_DESTINATION])

    result = _inspect("dest-snowflake")

    assert result.docs_skill_id == "connector-destination:dest-snowflake"
    assert cloud_workspace.list_destinations_calls == 1
