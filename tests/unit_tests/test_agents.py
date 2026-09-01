# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the `airbyte.agents` module."""

from __future__ import annotations

from typing import Any

import pytest
import requests
from airbyte.agents import _api_util
from airbyte.agents.connectors import AgentConnector
from airbyte.agents.models import AgentExecuteResult
from airbyte.agents.organizations import AgentOrganization
from airbyte.agents.workspaces import AgentWorkspace
from airbyte.cloud._credentials import _AirbyteCredentials
from airbyte.cloud.organizations import CloudOrganization
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteError, PyAirbyteInputError
from airbyte.secrets.base import SecretString


EXECUTE_RESPONSE: dict[str, Any] = {
    "status": "success",
    "result": [{"title": "Some issue"}],
    "connector_metadata": {"has_next_page": True, "end_cursor": "cursor-1"},
    "execution_metadata": {
        "connector_instance_id": "source_id:abc",
        "execution_time_ms": 42,
    },
}
INSPECT_RESPONSE: dict[str, Any] = {
    "connector_id": "connector-id",
    "name": "GitHub",
    "workspace_id": "workspace-id",
    "source_definition_name": "GitHub",
    "docs_skill_id": "connector:github",
    "context_store_readiness": {
        "supported_context_store_entities": [
            {"entity": "issues", "suggested": True},
            {"entity": "repositories", "suggested": False},
        ]
    },
}
CONNECTORS_RESPONSE: dict[str, Any] = {
    "data": [
        {"id": "connector-1", "name": "GitHub - workspace-id"},
        {"id": "connector-2", "name": "Slack"},
    ]
}
WORKSPACES_RESPONSE: dict[str, Any] = {
    "data": [
        {"id": "workspace-1", "name": "primary", "organization_id": "org-id"},
        {"id": "workspace-2", "name": "secondary", "organization_id": "org-id"},
    ]
}


class _FakeResponse:
    """Minimal stand-in for a `requests.Response`."""

    def __init__(
        self,
        payload: Any,
        status_code: int = 200,
        content_type: str = "application/json",
    ) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = ""
        self.headers = {"Content-Type": content_type}

    def json(self) -> Any:
        """Return the canned payload."""
        return self._payload


def _credentials(**overrides: Any) -> _AirbyteCredentials:
    """Build bearer-token credentials for tests."""
    kwargs: dict[str, Any] = {
        "client_id": None,
        "client_secret": None,
        "bearer_token": SecretString("test-token"),
        "public_api_root": "https://api.airbyte.com/v1",
        "config_api_root": None,
        "workspace_id": "workspace-id",
        "organization_id": "org-id",
    }
    kwargs.update(overrides)
    return _AirbyteCredentials(**kwargs)


@pytest.fixture
def captured_requests(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Capture Agents API requests, returning canned responses per endpoint."""
    calls: list[dict[str, Any]] = []

    def _fake_request(**kwargs: Any) -> Any:
        calls.append(kwargs)
        url = str(kwargs["url"])
        if url.endswith("/inspect"):
            return _FakeResponse(INSPECT_RESPONSE)
        if url.endswith("/execute"):
            return _FakeResponse(EXECUTE_RESPONSE)
        if url.endswith("/connectors"):
            return _FakeResponse(CONNECTORS_RESPONSE)
        if url.endswith("/workspaces"):
            return _FakeResponse(WORKSPACES_RESPONSE)
        return _FakeResponse({"id": "workspace-id", "name": "primary"})

    monkeypatch.setattr(requests, "request", _fake_request)
    return calls


def _connector(**credential_overrides: Any) -> AgentConnector:
    """Build a connector wired to test credentials."""
    return AgentConnector(
        connector_id="connector-id",
        credentials=_credentials(**credential_overrides),
    )


def test_make_agents_api_request(captured_requests: list[dict[str, Any]]) -> None:
    """Requests go to the Agents API root with bearer auth and the org header."""
    _api_util.make_agents_api_request(
        method="GET",
        path="/workspaces",
        credentials=_credentials(),
        organization_id="org-id",
    )
    call = captured_requests[0]
    assert call["url"] == "https://api.airbyte.ai/api/v1/workspaces"
    assert call["headers"]["Authorization"] == "Bearer test-token"
    assert call["headers"]["X-Organization-Id"] == "org-id"

    _api_util.make_agents_api_request(
        method="GET",
        path="/workspaces",
        credentials=_credentials(organization_id=None),
    )
    assert "X-Organization-Id" not in captured_requests[1]["headers"]


@pytest.mark.parametrize(
    ("response", "expected_match", "expected_status"),
    [
        pytest.param(
            _FakeResponse(b"", content_type="application/octet-stream"),
            "non-JSON response",
            None,
            id="non_json_response",
        ),
        pytest.param(
            _FakeResponse({"message": "forbidden"}, status_code=403),
            "status 403",
            403,
            id="error_status_code",
        ),
        pytest.param(
            _FakeResponse({"not_data": []}),
            "Unexpected list payload",
            None,
            id="missing_data_key",
        ),
        pytest.param(
            _FakeResponse({"data": [{"id": "workspace-1"}, "not-a-workspace"]}),
            "Unexpected list payload",
            None,
            id="non_dict_in_data",
        ),
    ],
)
def test_agents_api_request_failures(
    monkeypatch: pytest.MonkeyPatch,
    response: _FakeResponse,
    expected_match: str,
    expected_status: int | None,
) -> None:
    """Malformed and non-2xx Agents API responses raise `AirbyteError`."""
    monkeypatch.setattr(requests, "request", lambda **_: response)

    with pytest.raises(AirbyteError, match=expected_match) as error_info:
        _api_util.list_agent_workspaces(
            credentials=_credentials(),
            organization_id="org-id",
        )

    if expected_status is not None:
        assert error_info.value.context["status_code"] == expected_status


@pytest.mark.parametrize(
    ("kwargs", "expected_body"),
    [
        pytest.param(
            {},
            {
                "entity": "issues",
                "action": "list",
                "params": {},
                "skip_truncation": True,
            },
            id="minimal",
        ),
        pytest.param(
            {
                "api_args": {"repository": "airbytehq/PyAirbyte"},
                "page_size": 5,
                "cursor": "c1",
            },
            {
                "entity": "issues",
                "action": "list",
                "params": {
                    "repository": "airbytehq/PyAirbyte",
                    "limit": 5,
                    "cursor": "c1",
                },
                "skip_truncation": True,
            },
            id="api_args_merged_with_pagination",
        ),
        pytest.param(
            {
                "select_fields": ["title"],
                "exclude_fields": ["body"],
                "skip_truncation": False,
                "intent": "triage",
            },
            {
                "entity": "issues",
                "action": "list",
                "params": {},
                "skip_truncation": False,
                "select_fields": ["title"],
                "exclude_fields": ["body"],
                "intent": "triage",
            },
            id="all_pyairbyte_args",
        ),
    ],
)
def test_execute_request_body(
    captured_requests: list[dict[str, Any]],
    kwargs: dict[str, Any],
    expected_body: dict[str, Any],
) -> None:
    """`execute()` builds the Agents API request body from its arguments."""
    result = _connector().execute("issues", "list", **kwargs)

    assert captured_requests[0]["json"] == expected_body
    assert captured_requests[0]["url"].endswith("/connectors/connector-id/execute")
    assert result.entities == [{"title": "Some issue"}]
    assert result.has_next_page is True
    assert result.end_cursor == "cursor-1"
    assert result.execution_metadata.execution_time_ms == 42


@pytest.mark.parametrize(
    ("args", "kwargs", "expected_error"),
    [
        pytest.param(("files", "download"), {}, "not supported", id="download_action"),
        pytest.param(
            ("issues", "list", {"limit": 10}),
            {"page_size": 5},
            "twice",
            id="duplicated_page_size",
        ),
        pytest.param(
            ("issues", "list", {"cursor": "c1"}),
            {"cursor": "c2"},
            "twice",
            id="duplicated_cursor",
        ),
    ],
)
def test_execute_rejects_invalid_args(
    captured_requests: list[dict[str, Any]],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    expected_error: str,
) -> None:
    """`execute()` rejects unsupported actions and duplicated args before any request."""
    with pytest.raises(PyAirbyteInputError, match=expected_error):
        _connector().execute(*args, **kwargs)
    assert captured_requests == []


@pytest.mark.parametrize(
    ("method_name", "expected_action"),
    [
        pytest.param("list_entities", "list", id="list"),
        pytest.param("search_entities", "search", id="search"),
        pytest.param("get_entity", "get", id="get"),
        pytest.param("create_entity", "create", id="create"),
        pytest.param("update_entity", "update", id="update"),
        pytest.param("delete_entity", "delete", id="delete"),
    ],
)
def test_convenience_methods(
    captured_requests: list[dict[str, Any]],
    method_name: str,
    expected_action: str,
) -> None:
    """Each convenience method executes its corresponding action."""
    connector = _connector()
    getattr(connector, method_name)("issues", {"repository": "airbytehq/PyAirbyte"})

    assert captured_requests[0]["json"]["action"] == expected_action
    assert captured_requests[0]["json"]["entity"] == "issues"
    assert captured_requests[0]["json"]["params"] == {
        "repository": "airbytehq/PyAirbyte"
    }


@pytest.mark.parametrize(
    ("result_payload", "expectation"),
    [
        pytest.param([{"a": 1}], [{"a": 1}], id="list_of_entities"),
        pytest.param([], [], id="empty_list"),
        pytest.param({"a": 1}, None, id="single_object_raises"),
        pytest.param([{"a": 1}, "not-a-record"], None, id="mixed_list_raises"),
        pytest.param(None, None, id="null_raises"),
    ],
)
def test_entities(
    result_payload: Any, expectation: list[dict[str, Any]] | None
) -> None:
    """`entities` returns entity lists and raises on any other payload shape."""
    result = AgentExecuteResult(status="success", result=result_payload)
    if expectation is None:
        with pytest.raises(PyAirbyteInputError):
            _ = result.entities
    else:
        assert result.entities == expectation


def test_inspect(captured_requests: list[dict[str, Any]]) -> None:
    """`inspect()` parses the inspect response and caches it."""
    connector = _connector()
    details = connector.inspect()

    assert details.name == "GitHub"
    assert details.source_definition_name == "GitHub"
    assert details.docs_skill_id == "connector:github"
    assert details.context_store_entities == ["issues", "repositories"]
    assert connector.name == "GitHub"

    connector.inspect()
    assert len(captured_requests) == 1

    connector.inspect(force_refresh=True)
    assert len(captured_requests) == 2


@pytest.mark.parametrize(
    (
        "list_items",
        "expected_ids",
        "expected_names",
        "expected_path",
        "expected_params",
    ),
    [
        pytest.param(
            lambda: [
                (connector.connector_id, connector.name)
                for connector in AgentWorkspace(
                    workspace_id="workspace-id", bearer_token="test-token"
                ).list_connectors()
            ],
            ["connector-1", "connector-2"],
            ["GitHub - workspace-id", "Slack"],
            "/integrations/connectors",
            {"workspace_id": "workspace-id"},
            id="connectors",
        ),
        pytest.param(
            lambda: [
                (workspace.workspace_id, workspace.name)
                for workspace in AgentOrganization(
                    organization_id="org-id", bearer_token="test-token"
                ).list_workspaces()
            ],
            ["workspace-1", "workspace-2"],
            ["primary", "secondary"],
            "/workspaces",
            None,
            id="workspaces",
        ),
    ],
)
def test_listings(
    captured_requests: list[dict[str, Any]],
    list_items: Any,
    expected_ids: list[str],
    expected_names: list[str],
    expected_path: str,
    expected_params: dict[str, str] | None,
) -> None:
    """Listing methods return the API's records, scoped to the caller's container."""
    items = list_items()

    assert [item_id for item_id, _ in items] == expected_ids
    assert [name for _, name in items] == expected_names
    assert captured_requests[0]["url"].endswith(expected_path)
    if expected_params is not None:
        assert captured_requests[0]["params"] == expected_params


@pytest.mark.parametrize(
    ("pages", "limit", "expected_titles", "expected_request_count"),
    [
        pytest.param(
            [
                ("one", True, "cursor-1"),
                ("two", True, "cursor-2"),
                ("three", False, None),
            ],
            None,
            ["one", "two", "three"],
            3,
            id="follows_cursor_to_last_page",
        ),
        pytest.param(
            [("one", False, None)],
            None,
            ["one"],
            1,
            id="single_page",
        ),
        pytest.param(
            [("one", True, "cursor-1"), ("two", True, "cursor-2")],
            2,
            ["one", "two"],
            2,
            id="stops_at_limit",
        ),
        pytest.param(
            [("one", True, "cursor-1"), ("two", True, "cursor-1")],
            None,
            ["one", "two"],
            2,
            id="stops_when_cursor_does_not_advance",
        ),
        pytest.param(
            [("one", True, None)],
            None,
            ["one"],
            1,
            id="stops_when_next_page_has_no_cursor",
        ),
    ],
)
def test_iter_entities(
    monkeypatch: pytest.MonkeyPatch,
    pages: list[tuple[str, bool, str | None]],
    limit: int | None,
    expected_titles: list[str],
    expected_request_count: int,
) -> None:
    """`iter_entities()` follows the connector's cursor and stops without looping."""
    calls: list[dict[str, Any]] = []

    def _fake_request(**kwargs: Any) -> _FakeResponse:
        calls.append(kwargs)
        title, has_next_page, end_cursor = pages[min(len(calls) - 1, len(pages) - 1)]
        return _FakeResponse({
            "status": "success",
            "result": [{"title": title}],
            "connector_metadata": {
                "has_next_page": has_next_page,
                "end_cursor": end_cursor,
            },
        })

    monkeypatch.setattr(requests, "request", _fake_request)

    entities = list(_connector().iter_entities("issues", limit=limit))

    assert [entity["title"] for entity in entities] == expected_titles
    assert len(calls) == expected_request_count
    assert [call["json"]["params"].get("cursor") for call in calls] == [
        None,
        *[page[2] for page in pages[: expected_request_count - 1]],
    ]


@pytest.mark.parametrize(
    ("args", "kwargs", "expected_id", "expected_error"),
    [
        pytest.param(
            (),
            {"connector_id": "explicit-id"},
            "explicit-id",
            None,
            id="by_id",
        ),
        pytest.param((), {"id": "explicit-id"}, "explicit-id", None, id="by_id_alias"),
        pytest.param(
            (),
            {"id": "explicit-id", "connector_id": "explicit-id"},
            "explicit-id",
            None,
            id="by_id_alias_agreeing",
        ),
        pytest.param(
            (),
            {"id": "one-id", "connector_id": "other-id"},
            None,
            "conflicting values",
            id="by_id_alias_conflicting",
        ),
        pytest.param(
            (),
            {"id": "", "name": "GitHub"},
            None,
            "cannot be blank",
            id="blank_id",
        ),
        pytest.param(
            (),
            {"connector_id": "explicit-id", "name": " "},
            None,
            "cannot be blank",
            id="blank_name",
        ),
        pytest.param((), {"name": "Slack"}, "connector-2", None, id="by_exact_name"),
        pytest.param((), {"name": "GitHub"}, "connector-1", None, id="by_partial_name"),
        pytest.param(
            (), {"name": "slack"}, "connector-2", None, id="by_exact_name_other_case"
        ),
        pytest.param(
            (),
            {"name": "github"},
            "connector-1",
            None,
            id="by_partial_name_other_case",
        ),
        pytest.param(("connector-2",), {}, "connector-2", None, id="positional_id"),
        pytest.param(("Slack",), {}, "connector-2", None, id="positional_name"),
        pytest.param(("github",), {}, "connector-1", None, id="positional_partial"),
        pytest.param(
            ("missing",), {}, None, "No connector found", id="positional_no_match"
        ),
        pytest.param((" ",), {}, None, "cannot be blank", id="positional_blank"),
        pytest.param(
            ("Slack",),
            {"name": "Slack"},
            None,
            "cannot be combined with keyword arguments",
            id="positional_and_keyword",
        ),
        pytest.param(
            (), {"name": "Missing"}, None, "No connector found", id="no_match"
        ),
        pytest.param((), {}, None, "Exactly one", id="no_args"),
        pytest.param(
            (),
            {"connector_id": "id", "name": "GitHub"},
            None,
            "Exactly one",
            id="both_args",
        ),
    ],
)
def test_get_connector(
    captured_requests: list[dict[str, Any]],
    args: tuple[str, ...],
    kwargs: dict[str, Any],
    expected_id: str | None,
    expected_error: str | None,
) -> None:
    """`get_connector()` resolves by ID without an API call, or by name via listing."""
    workspace = AgentWorkspace(workspace_id="workspace-id", bearer_token="test-token")

    if expected_error:
        with pytest.raises((AirbyteError, PyAirbyteInputError), match=expected_error):
            workspace.get_connector(*args, **kwargs)
        return

    connector = workspace.get_connector(*args, **kwargs)
    assert connector.connector_id == expected_id
    if "connector_id" in kwargs or "id" in kwargs:
        assert captured_requests == []


@pytest.mark.parametrize(
    ("args", "kwargs", "expected_id", "expected_error"),
    [
        pytest.param(
            (),
            {"workspace_id": "explicit-id"},
            "explicit-id",
            None,
            id="by_id",
        ),
        pytest.param((), {"name": "secondary"}, "workspace-2", None, id="by_name"),
        pytest.param(
            (), {"name": "SECONDARY"}, "workspace-2", None, id="by_name_other_case"
        ),
        pytest.param(("workspace-2",), {}, "workspace-2", None, id="positional_id"),
        pytest.param(("secondary",), {}, "workspace-2", None, id="positional_name"),
        pytest.param(
            ("missing",), {}, None, "No workspace found", id="positional_no_match"
        ),
        pytest.param(("",), {}, None, "cannot be blank", id="positional_blank"),
        pytest.param(
            ("secondary",),
            {"name": "secondary"},
            None,
            "Exactly one",
            id="positional_and_keyword",
        ),
        pytest.param(
            (), {"name": "missing"}, None, "No workspace found", id="no_match"
        ),
        pytest.param((), {}, None, "Exactly one", id="no_args"),
    ],
)
def test_get_workspace(
    captured_requests: list[dict[str, Any]],
    args: tuple[str, ...],
    kwargs: dict[str, Any],
    expected_id: str | None,
    expected_error: str | None,
) -> None:
    """`get_workspace()` resolves by ID without an API call, or by name via listing."""
    organization = AgentOrganization(
        organization_id="org-id", bearer_token="test-token"
    )

    if expected_error:
        with pytest.raises((AirbyteError, PyAirbyteInputError), match=expected_error):
            organization.get_workspace(*args, **kwargs)
        return

    workspace = organization.get_workspace(*args, **kwargs)
    assert workspace.workspace_id == expected_id
    if "workspace_id" in kwargs:
        assert captured_requests == []


@pytest.mark.parametrize(
    ("convert", "expected_id", "expected_bearer_token", "expected_request_path"),
    [
        pytest.param(
            lambda: AgentWorkspace(
                workspace_id="workspace-id", bearer_token="test-token"
            ).as_cloud_workspace(),
            "workspace-id",
            "test-token",
            None,
            id="agent_workspace_to_cloud",
        ),
        pytest.param(
            lambda: AgentOrganization(
                organization_id="org-id", bearer_token="test-token"
            ).as_cloud_organization(),
            "org-id",
            None,
            None,
            id="agent_organization_to_cloud",
        ),
        pytest.param(
            lambda: AgentWorkspace.from_cloud_workspace(
                CloudWorkspace(workspace_id="workspace-id", bearer_token="test-token"),
                verify=False,
            ),
            "workspace-id",
            None,
            None,
            id="cloud_workspace_to_agent_unverified",
        ),
        pytest.param(
            lambda: AgentWorkspace.from_cloud_workspace(
                CloudWorkspace(workspace_id="workspace-id", bearer_token="test-token"),
                organization_id="org-id",
            ),
            "workspace-id",
            None,
            "/workspaces/workspace-id",
            id="cloud_workspace_to_agent_verified",
        ),
    ],
)
def test_cloud_conversions(
    captured_requests: list[dict[str, Any]],
    convert: Any,
    expected_id: str,
    expected_bearer_token: str | None,
    expected_request_path: str | None,
) -> None:
    """Converting between Cloud and Agents objects reuses credentials and identifiers."""
    converted = convert()

    if isinstance(converted, CloudWorkspace | AgentWorkspace):
        assert converted.workspace_id == expected_id
    else:
        assert converted.organization_id == expected_id

    if expected_bearer_token is not None:
        assert isinstance(converted, CloudWorkspace)
        assert str(converted.bearer_token) == expected_bearer_token

    if expected_request_path is None:
        assert captured_requests == []
    else:
        assert captured_requests[0]["url"].endswith(expected_request_path)


@pytest.mark.parametrize(
    "convert",
    [
        pytest.param(
            lambda: AgentWorkspace.from_cloud_workspace(
                CloudWorkspace(
                    workspace_id="workspace-id",
                    bearer_token="test-token",
                    api_root="https://airbyte.example.com/api/public/v1",
                ),
                verify=False,
            ),
            id="workspace_api_root",
        ),
        pytest.param(
            lambda: AgentWorkspace.from_cloud_workspace(
                CloudWorkspace(
                    workspace_id="workspace-id",
                    bearer_token="test-token",
                    config_api_root="https://airbyte.example.com/api/v1",
                ),
                verify=False,
            ),
            id="workspace_config_api_root",
        ),
        pytest.param(
            lambda: AgentOrganization.from_cloud_organization(
                CloudOrganization(
                    organization_id="org-id",
                    bearer_token="test-token",
                    public_api_root="https://airbyte.example.com/api/public/v1",
                ),
            ),
            id="organization_api_root",
        ),
    ],
)
def test_conversion_rejects_non_public_cloud_api_roots(
    captured_requests: list[dict[str, Any]],
    convert: Any,
) -> None:
    """A Cloud object with custom API roots cannot become an Agents object."""
    with pytest.raises(PyAirbyteInputError, match="only available on Airbyte Cloud"):
        convert()

    assert captured_requests == []
