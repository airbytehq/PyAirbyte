# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Cloud MCP contract tests: real SDK discovery and HTTP-boundary execution fixtures.

Expected wire contracts come from the Cloud config API and the direct-read migration
spec, rather than from the standalone Agents API or replacement implementation.
"""

from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import replace
from collections.abc import Iterator
from typing import Any, cast
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import responses
from fastmcp import Client, Context, FastMCP
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client

from airbyte.constants import (
    MCP_CONFIG_API_URL,
    MCP_CONFIG_BEARER_TOKEN,
    MCP_CONFIG_CLIENT_ID,
    MCP_CONFIG_CLIENT_SECRET,
    MCP_CONFIG_CONFIG_API_URL,
    MCP_CONFIG_ORGANIZATION_ID,
    MCP_CONFIG_WORKSPACE_ID,
)
from airbyte.exceptions import AirbyteError, PyAirbyteInputError
from airbyte.mcp import agents as agents_mcp
from airbyte.mcp.server import app as server_app

WORKSPACE = "11111111-1111-4111-8111-111111111111"
SOURCE = "22222222-2222-4222-8222-222222222222"
DESTINATION = "33333333-3333-4333-8333-333333333333"
ORGANIZATION = "44444444-4444-4444-8444-444444444444"
OTHER = "55555555-5555-4555-8555-555555555555"
DEFINITION = "66666666-6666-4666-8666-666666666666"
PUBLIC = "https://cloud-fixture.example/public/v1"
CONFIG = "https://cloud-fixture.example/api/v1"
TOKEN = "caller-secret-sentinel"
SECRET = "actor-configuration-secret-sentinel"
CTX = cast(Context, object())


@pytest.fixture
def config(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    values = {
        MCP_CONFIG_API_URL: PUBLIC,
        MCP_CONFIG_CONFIG_API_URL: CONFIG,
        MCP_CONFIG_BEARER_TOKEN: TOKEN,
        MCP_CONFIG_WORKSPACE_ID: WORKSPACE,
        MCP_CONFIG_ORGANIZATION_ID: ORGANIZATION,
    }
    monkeypatch.setattr(agents_mcp, "get_mcp_config", lambda ctx, key: values.get(key))
    return values


@pytest.fixture
def http(config: dict[str, str]) -> Iterator[responses.RequestsMock]:
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        mock.post(
            f"{CONFIG}/workspaces/get_organization_info",
            json={"organizationId": ORGANIZATION, "organizationName": "Cloud Org"},
        )
        mock.get(
            f"{PUBLIC}/workspaces/{WORKSPACE}",
            json={
                "workspaceId": WORKSPACE,
                "name": "Cloud workspace",
                "dataResidency": "us",
                "notifications": {},
            },
        )
        mock.get(f"{PUBLIC}/sources", json={"data": [_source()]})
        mock.get(f"{PUBLIC}/destinations", json={"data": [_destination()]})
        yield mock


def _source(workspace: str = WORKSPACE) -> dict[str, Any]:
    return {
        "sourceId": SOURCE,
        "workspaceId": workspace,
        "name": "Cloud source",
        "sourceType": "faker",
        "definitionId": DEFINITION,
        "createdAt": 1,
        "configuration": {"sourceType": "faker", "secret": SECRET},
    }


def _destination(workspace: str = WORKSPACE) -> dict[str, Any]:
    return {
        "destinationId": DESTINATION,
        "workspaceId": workspace,
        "name": "Cloud warehouse",
        "destinationType": "snowflake",
        "definitionId": DEFINITION,
        "createdAt": 1,
        "configuration": {"destinationType": "snowflake", "secret": SECRET},
    }


def _execute(alias: bool = False, **kwargs: Any) -> Any:  # noqa: ANN401
    arguments = {
        "ctx": CTX,
        "connector_id": SOURCE,
        "entity_type": "users",
        "action": "list",
        "api_args": None,
        "select_fields": None,
        "exclude_fields": None,
        "page_size": None,
        "cursor": None,
        "intent": None,
        "workspace_id": WORKSPACE,
        "organization_id": None,
        **kwargs,
    }
    if alias:
        return agents_mcp.execute_agent_connector(**{"read_only": None, **arguments})
    return agents_mcp.execute_agent_connector_ro(**arguments)


def _docs(section: str | None = None) -> dict[str, Any]:
    return {
        "metadata": {
            "id": f"connector-source:{SOURCE}",
            "kind": "connector_source",
            "title": "Cloud source",
            "provenance": "connector_yaml",
            "version": "1",
            "freshness": {"state": "live"},
            "warnings": [],
        },
        "outline": [
            {"id": "actions.users.list", "title": "List users", "available": True}
        ],
        "section_id": section,
        "content": [{"type": "paragraph", "text": "Read users."}],
    }


@pytest.mark.parametrize("action", ["get", "list", "search"])
@pytest.mark.parametrize("alias", [False, True])
def test_source_reads_preserve_native_data_and_wire_contract(
    http: responses.RequestsMock,
    action: str,
    alias: bool,
) -> None:
    http.post(
        f"{CONFIG}/sources/{SOURCE}/execute",
        json={
            "data": [{"id": 1, "value": None}],
            "meta": {"pagination": {"cursor": "opaque"}, "unknown": [None, 4]},
        },
    )
    result = _execute(
        alias,
        action=action,
        api_args='{"query":"Ada"}',
        page_size=10,
        cursor="previous",
        select_fields=["id"],
        intent="Read users",
    )
    body = json.loads(http.calls[-1].request.body)
    assert body["entity"] == "users"
    assert body["action"] == action
    assert body["params"] == {"query": "Ada", "limit": 10, "cursor": "previous"}
    assert body["select_fields"] == ["id"]
    assert body["intent"] == "Read users"
    assert body["skip_truncation"] is True
    assert result.status == "success"
    assert result.result == [{"id": 1, "value": None}]
    assert result.meta == {"pagination": {"cursor": "opaque"}, "unknown": [None, 4]}
    assert result.has_next_page is None
    assert result.end_cursor is None
    assert result.execution_time_ms is None
    assert sum(call.request.url.endswith("/execute") for call in http.calls) == 1
    for call in http.calls:
        assert call.request.headers["Authorization"] == f"Bearer {TOKEN}"
        assert "api.airbyte.ai" not in call.request.url
        assert not any("sonar" in header.lower() for header in call.request.headers)


@pytest.mark.parametrize("value", [None, False, 0, "", [], {}])
def test_execute_preserves_all_json_data_values(
    http: responses.RequestsMock, value: Any
) -> None:  # noqa: ANN401
    http.post(f"{CONFIG}/sources/{SOURCE}/execute", json={"data": value})
    result = _execute()
    assert result.result == value
    assert result.has_next_page is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"action": "create"},
        {"action": "update"},
        {"action": "delete"},
        {"action": "api_search"},
        {"action": "describe"},
        {"api_args": "{"},
        {"api_args": "[]"},
        {"api_args": "null"},
        {"api_args": {"limit": 1}, "page_size": 2},
        {"api_args": {"cursor": "a"}, "cursor": "b"},
        {"page_size": 0},
        {"page_size": -1},
    ],
)
@pytest.mark.parametrize("alias", [False, True])
def test_invalid_source_inputs_reject_before_network(
    config: dict[str, str],
    kwargs: dict[str, Any],
    alias: bool,
) -> None:
    with (
        responses.RequestsMock() as mock,
        pytest.raises((PyAirbyteInputError, ValueError)),
    ):
        _execute(alias, **kwargs)
    assert len(mock.calls) == 0


@pytest.mark.parametrize("read_only", [None, False, True])
def test_legacy_read_only_flag_cannot_enable_writes(
    config: dict[str, str],
    read_only: bool | None,
) -> None:
    with (
        responses.RequestsMock() as mock,
        pytest.raises((PyAirbyteInputError, ValueError)),
    ):
        _execute(True, action="delete", read_only=read_only)
    assert len(mock.calls) == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"api_args": {}},
        {"api_args": {"sql": "SELECT 1"}},
        {"api_args": {"sql": "SELECT 1", "sql_dialect": "postgres"}},
        {"api_args": {"sql": "SELECT 1", "sql_dialect": "snowflake", "dry_run": True}},
        {"api_args": {"sql": "SELECT 1", "sql_dialect": "snowflake", "binds": []}},
        {"page_size": 1},
        {"cursor": "c"},
        {"select_fields": ["x"]},
        {"exclude_fields": ["x"]},
    ],
)
def test_unsupported_sql_options_reject_before_network(
    config: dict[str, str],
    kwargs: dict[str, Any],
) -> None:
    with (
        responses.RequestsMock() as mock,
        pytest.raises((PyAirbyteInputError, ValueError)),
    ):
        _execute(
            action="sql_select",
            connector_id=DESTINATION,
            **{"api_args": {"sql": "SELECT 1", "sql_dialect": "snowflake"}, **kwargs},
        )
    assert len(mock.calls) == 0


def test_snowflake_keeps_positional_rows_duplicate_columns_and_truncation(
    http: responses.RequestsMock,
) -> None:
    http.post(
        f"{CONFIG}/destinations/{DESTINATION}/execute",
        json={
            "data": {
                "data": [["9007199254740993", None]],
                "resultSetMetaData": {"rowType": [{"name": "X"}, {"name": "X"}]},
            },
            "meta": {
                "page": {
                    "returnedRows": 1,
                    "rowLimit": 1000,
                    "byteLimit": 1048576,
                    "truncated": True,
                    "reasons": ["provider_partition"],
                    "nextCursor": None,
                }
            },
        },
    )
    result = _execute(
        action="sql_select",
        connector_id=DESTINATION,
        api_args={"sql": "SELECT 1", "sql_dialect": "snowflake", "dry_run": False},
    )
    body = json.loads(http.calls[-1].request.body)
    assert body["entity"] == "record"
    assert body["action"] == "list"
    assert body["params"] == {"statement": "SELECT 1"}
    assert result.result["data"] == [["9007199254740993", None]]
    assert [col["name"] for col in result.result["resultSetMetaData"]["rowType"]] == [
        "X",
        "X",
    ]
    assert result.meta["page"]["truncated"] is True
    assert result.has_next_page is None
    assert result.end_cursor is None


@pytest.mark.parametrize("kind", ["sources", "destinations"])
@pytest.mark.parametrize("inventory", ["empty", "foreign"])
def test_actor_must_belong_to_selected_workspace(
    http: responses.RequestsMock,
    kind: str,
    inventory: str,
) -> None:
    actor = _source(OTHER) if kind == "sources" else _destination(OTHER)
    http.replace(
        responses.GET,
        f"{PUBLIC}/{kind}",
        json={"data": [] if inventory == "empty" else [actor]},
    )
    with pytest.raises(AirbyteError):
        _execute(
            **(
                {}
                if kind == "sources"
                else {
                    "action": "sql_select",
                    "connector_id": DESTINATION,
                    "api_args": {"sql": "SELECT 1", "sql_dialect": "snowflake"},
                }
            )
        )
    assert not any(call.request.url.endswith("/execute") for call in http.calls)


@pytest.mark.parametrize("explicit", [True, False])
def test_org_mismatch_rejects_before_execution(
    http: responses.RequestsMock,
    config: dict[str, str],
    explicit: bool,
) -> None:
    if not explicit:
        config[MCP_CONFIG_ORGANIZATION_ID] = OTHER
    with pytest.raises(AirbyteError):
        _execute(organization_id=OTHER if explicit else None)
    assert not any(call.request.url.endswith("/execute") for call in http.calls)


def test_explicit_org_overrides_config(
    http: responses.RequestsMock, config: dict[str, str]
) -> None:
    config[MCP_CONFIG_ORGANIZATION_ID] = OTHER
    http.post(f"{CONFIG}/sources/{SOURCE}/execute", json={"data": None})
    assert _execute(organization_id=ORGANIZATION).status == "success"


@pytest.mark.parametrize("section", [None, "actions.users.list"])
def test_docs_preserve_outline_section_and_content(
    http: responses.RequestsMock, section: str | None
) -> None:
    http.get(f"{CONFIG}/workspaces/{WORKSPACE}/skills/docs", json=_docs(section))
    result = agents_mcp.read_agent_skill_docs(
        CTX, f"connector-source:{SOURCE}", section=section, workspace_id=WORKSPACE
    )
    query = parse_qs(urlparse(http.calls[-1].request.url).query)
    assert query["id"] == [f"connector-source:{SOURCE}"]
    assert query.get("section") == ([section] if section else None)
    assert result.skill_id == f"connector-source:{SOURCE}"
    assert result.section_id == section
    assert result.outline[0].section_id == "actions.users.list"
    assert result.content[0]["text"] == "Read users."


def test_discovery_inspection_exclude_credentials(http: responses.RequestsMock) -> None:
    http.get(f"{CONFIG}/workspaces/{WORKSPACE}/skills/docs", json=_docs())
    listed = agents_mcp.list_agent_connectors(
        CTX, workspace_id=WORKSPACE, organization_id=None
    )
    inspected = agents_mcp.inspect_agent_connector(
        CTX, SOURCE, workspace_id=WORKSPACE, organization_id=None
    )
    assert listed.connectors[0].connector_id == SOURCE
    assert inspected.docs_skill_id == f"connector-source:{SOURCE}"
    for result in [listed, inspected]:
        serialized = result.model_dump_json()
        assert SECRET not in serialized
        assert TOKEN not in serialized
        assert "configuration" not in serialized.lower()
        assert "context_store_entities" not in serialized


@pytest.mark.parametrize("status", [401, 403])
def test_execution_denial_is_cloud_specific_and_sanitized(
    http: responses.RequestsMock, status: int
) -> None:
    http.post(
        f"{CONFIG}/sources/{SOURCE}/execute",
        status=status,
        json={
            "status": status,
            "type": "denied",
            "title": "Denied",
            "detail": TOKEN + SECRET,
        },
    )
    result = _execute()
    assert result.status != "success"
    assert "cloud" in result.message.lower()
    assert TOKEN not in result.model_dump_json()
    assert SECRET not in result.model_dump_json()
    assert sum(call.request.url.endswith("/execute") for call in http.calls) == 1


def test_registered_tools_expose_only_read_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AIRBYTE_MCP_INSIDERS", "true")
    app = FastMCP("cloud-contract")
    agents_mcp.register_agents_tools(app)
    tools = {tool.name: tool for tool in asyncio.run(app._list_tools())}  # noqa: SLF001
    assert set(tools) == {
        "list_agent_workspaces",
        "list_agent_connectors",
        "inspect_agent_connector",
        "execute_agent_connector_ro",
        "execute_agent_connector",
        "read_agent_skill_docs",
    }
    for tool in tools.values():
        assert tool.annotations.readOnlyHint is True
    for name in ["execute_agent_connector", "execute_agent_connector_ro"]:
        action = tools[name].parameters["properties"]["action"]
        if "$ref" in action:
            action = tools[name].parameters["$defs"][action["$ref"].rsplit("/", 1)[1]]
        assert set(action["enum"]) == {"get", "list", "search", "sql_select"}
    assert "read_only" in tools["execute_agent_connector"].parameters["properties"]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"connector_id": DESTINATION},
        {
            "connector_id": SOURCE,
            "action": "sql_select",
            "api_args": {"sql": "SELECT 1", "sql_dialect": "snowflake"},
        },
    ],
)
def test_actor_kind_cannot_fall_back_to_other_inventory(
    http: responses.RequestsMock,
    kwargs: dict[str, Any],
) -> None:
    with pytest.raises(AirbyteError):
        _execute(**kwargs)
    assert not any(call.request.url.endswith("/execute") for call in http.calls)


def test_inspection_rejects_ambiguous_actor_identity(
    http: responses.RequestsMock,
) -> None:
    destination = _destination()
    destination["destinationId"] = SOURCE
    http.replace(responses.GET, f"{PUBLIC}/destinations", json={"data": [destination]})
    with pytest.raises(AirbyteError):
        agents_mcp.inspect_agent_connector(
            CTX, SOURCE, workspace_id=WORKSPACE, organization_id=None
        )
    assert not any("/skills/docs" in call.request.url for call in http.calls)


@pytest.mark.parametrize("status", [401, 403, 500])
def test_discovery_errors_never_expose_http_or_actor_secrets(
    http: responses.RequestsMock,
    status: int,
) -> None:
    http.replace(
        responses.GET,
        f"{PUBLIC}/sources",
        status=status,
        json={"message": SECRET, "configuration": {"password": SECRET}},
    )
    try:
        result = agents_mcp.list_agent_connectors(
            CTX, workspace_id=WORKSPACE, organization_id=None
        )
    except AirbyteError as error:
        assert status == 500
        text = str(error)
    else:
        assert status in {401, 403}
        assert result.message
        assert "cloud" in result.message.lower()
        text = result.model_dump_json()
    assert TOKEN not in text
    assert SECRET not in text


def test_inspection_retains_safe_metadata_when_docs_denied(
    http: responses.RequestsMock,
) -> None:
    http.get(
        f"{CONFIG}/workspaces/{WORKSPACE}/skills/docs",
        status=403,
        json={"status": 403, "type": "denied", "title": "Denied", "detail": SECRET},
    )
    result = agents_mcp.inspect_agent_connector(
        CTX, SOURCE, workspace_id=WORKSPACE, organization_id=None
    )
    assert result.connector_id == SOURCE
    assert result.connector_name == "Cloud source"
    assert result.message or result.warnings
    assert SECRET not in result.model_dump_json()
    assert not any(call.request.url.endswith("/execute") for call in http.calls)


@pytest.mark.parametrize("transport", ["stdio-config", "hosted-headers"])
def test_registered_tools_keep_interleaved_callers_isolated(
    monkeypatch: pytest.MonkeyPatch,
    transport: str,
) -> None:
    monkeypatch.setenv("AIRBYTE_MCP_INSIDERS", "1")
    for name in [
        "AIRBYTE_CLOUD_BEARER_TOKEN",
        "AIRBYTE_CLOUD_WORKSPACE_ID",
        "AIRBYTE_CLOUD_ORGANIZATION_ID",
    ]:
        monkeypatch.delenv(name, raising=False)
    apps = []
    for number, workspace in [(1, WORKSPACE), (2, OTHER)]:
        app = FastMCP(f"caller-{number}")
        values = {
            MCP_CONFIG_API_URL: f"https://caller-{number}.example/public/v1",
            MCP_CONFIG_CONFIG_API_URL: f"https://caller-{number}.example/api/v1",
            MCP_CONFIG_BEARER_TOKEN: f"token-{number}",
            MCP_CONFIG_WORKSPACE_ID: workspace,
            MCP_CONFIG_ORGANIZATION_ID: ORGANIZATION,
        }
        if transport == "hosted-headers":
            values.pop(MCP_CONFIG_BEARER_TOKEN)
            values.pop(MCP_CONFIG_WORKSPACE_ID)
        app.x_mcp_server_config = replace(
            server_app.x_mcp_server_config,
            config_args=[
                replace(arg, default=values[arg.name], env_var=None)
                if arg.name in values
                else arg
                for arg in server_app.x_mcp_server_config.config_args
            ],
        )
        agents_mcp.register_agents_tools(app)
        apps.append(app)

    async def invoke(number: int) -> None:
        app = apps[number - 1]
        arguments = {"connector_id": SOURCE, "entity_type": "users", "action": "list"}
        if transport == "stdio-config":
            async with Client(app) as client:
                result = await client.call_tool("execute_agent_connector_ro", arguments)
                assert not result.is_error
        else:
            http_app = app.http_app(
                path="/mcp", transport="streamable-http", stateless_http=True
            )
            async with (
                http_app.router.lifespan_context(http_app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=http_app),
                    base_url="http://testserver",
                    headers={
                        "Authorization": f"Bearer token-{number}",
                        "X-Airbyte-Workspace-Id": WORKSPACE if number == 1 else OTHER,
                    },
                ) as client,
            ):
                async with streamable_http_client(
                    "http://testserver/mcp", http_client=client
                ) as (read, write, _):
                    async with ClientSession(read, write) as session:
                        await session.initialize()
                        result = await session.call_tool(
                            "execute_agent_connector_ro", arguments
                        )
                        assert not result.isError

    async def run() -> None:
        await invoke(1)
        await invoke(2)
        await invoke(1)

    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        for number, workspace in [(1, WORKSPACE), (2, OTHER)]:
            root = f"https://caller-{number}.example"
            mock.post(
                f"{root}/api/v1/workspaces/get_organization_info",
                json={"organizationId": ORGANIZATION, "organizationName": "Org"},
            )
            mock.get(f"{root}/public/v1/sources", json={"data": [_source(workspace)]})
            mock.post(f"{root}/api/v1/sources/{SOURCE}/execute", json={"data": None})
        asyncio.run(run())
        executions = [
            call.request for call in mock.calls if call.request.url.endswith("/execute")
        ]
        assert [request.headers["Authorization"] for request in executions] == [
            "Bearer token-1",
            "Bearer token-2",
            "Bearer token-1",
        ]
        for call in mock.calls:
            number = 1 if "caller-1" in call.request.url else 2
            assert call.request.headers["Authorization"] == f"Bearer token-{number}"
            assert "api.airbyte.ai" not in call.request.url
            if "/sources?" in call.request.url:
                assert (WORKSPACE if number == 1 else OTHER) in call.request.url


def test_cloud_workspace_discovery_returns_owning_organization(
    http: responses.RequestsMock,
) -> None:
    http.post(
        f"{CONFIG}/workspaces/list_by_organization_id",
        json={
            "workspaces": [
                {
                    "workspaceId": WORKSPACE,
                    "name": "Cloud workspace",
                    "organizationId": ORGANIZATION,
                },
            ]
        },
    )
    result = agents_mcp.list_agent_workspaces(CTX, organization_id=ORGANIZATION)
    assert result.workspaces[0].workspace_id == WORKSPACE
    assert result.workspaces[0].organization_id == ORGANIZATION
    assert TOKEN not in result.model_dump_json()
    assert SECRET not in result.model_dump_json()


@pytest.mark.parametrize("workspaces", [[], [WORKSPACE, OTHER]])
def test_missing_or_ambiguous_default_workspace_never_executes(
    http: responses.RequestsMock,
    config: dict[str, str],
    workspaces: list[str],
) -> None:
    config.pop(MCP_CONFIG_WORKSPACE_ID)
    config.pop(MCP_CONFIG_ORGANIZATION_ID)
    payload = (
        base64.urlsafe_b64encode(json.dumps({"sub": OTHER}).encode())
        .decode()
        .rstrip("=")
    )
    config[MCP_CONFIG_BEARER_TOKEN] = f"e30.{payload}.signature"
    http.post(f"{CONFIG}/users/get_by_auth_id", json={"userId": OTHER})
    http.post(
        f"{CONFIG}/permissions/list_by_user",
        json={
            "permissions": [
                {"permissionType": "workspace_reader", "workspaceId": workspace}
                for workspace in workspaces
            ]
        },
    )
    http.get(
        f"{PUBLIC}/workspaces/{OTHER}",
        json={
            "workspaceId": OTHER,
            "name": "Other workspace",
            "dataResidency": "us",
            "notifications": {},
        },
    )
    with pytest.raises(AirbyteError):
        _execute(workspace_id=None)
    assert not any(call.request.url.endswith("/execute") for call in http.calls)


def test_configured_workspace_is_used_when_argument_is_omitted(
    http: responses.RequestsMock,
) -> None:
    http.post(f"{CONFIG}/sources/{SOURCE}/execute", json={"data": None})
    assert _execute(workspace_id=None).status == "success"
    inventory = [call.request for call in http.calls if "/sources?" in call.request.url]
    assert len(inventory) == 1
    assert WORKSPACE in inventory[0].url


def test_application_token_is_exchanged_once_and_reused_for_discovery(
    http: responses.RequestsMock,
    config: dict[str, str],
) -> None:
    config.pop(MCP_CONFIG_BEARER_TOKEN)
    config[MCP_CONFIG_CLIENT_ID] = "app-client"
    config[MCP_CONFIG_CLIENT_SECRET] = "app-secret"
    http.post(f"{PUBLIC}/applications/token", json={"access_token": "app-access-token"})
    http.post(f"{CONFIG}/sources/{SOURCE}/execute", json={"data": None})
    assert _execute().status == "success"
    exchanges = [
        call.request
        for call in http.calls
        if call.request.url.endswith("/applications/token")
    ]
    assert len(exchanges) == 1
    for call in http.calls:
        if not call.request.url.endswith("/applications/token"):
            assert call.request.headers["Authorization"] == "Bearer app-access-token"
