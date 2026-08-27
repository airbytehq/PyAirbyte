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


def test_make_agents_api_request_rejects_non_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-JSON response raises rather than failing inside `json()` parsing."""
    monkeypatch.setattr(
        requests,
        "request",
        lambda **_: _FakeResponse(b"", content_type="application/octet-stream"),
    )
    with pytest.raises(AirbyteError, match="non-JSON response"):
        _api_util.make_agents_api_request(
            method="GET",
            path="/whatever",
            credentials=_credentials(),
        )


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
                "limit": 5,
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
    assert result.records == [{"title": "Some issue"}]
    assert result.has_next_page is True
    assert result.end_cursor == "cursor-1"
    assert result.execution_metadata.execution_time_ms == 42


def test_execute_rejects_download(captured_requests: list[dict[str, Any]]) -> None:
    """`download` is rejected before any request is sent."""
    with pytest.raises(PyAirbyteInputError, match="not supported"):
        _connector().execute("files", "download")
    assert captured_requests == []


def test_execute_rejects_duplicated_pagination_args(
    captured_requests: list[dict[str, Any]],
) -> None:
    """Pagination args cannot be given both explicitly and within `api_args`."""
    with pytest.raises(PyAirbyteInputError, match="twice"):
        _connector().execute("issues", "list", {"limit": 10}, limit=5)
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
        pytest.param([{"a": 1}], [{"a": 1}], id="list_of_records"),
        pytest.param([], [], id="empty_list"),
        pytest.param({"a": 1}, None, id="single_object_raises"),
        pytest.param([{"a": 1}, "not-a-record"], None, id="mixed_list_raises"),
        pytest.param(None, None, id="null_raises"),
    ],
)
def test_records(result_payload: Any, expectation: list[dict[str, Any]] | None) -> None:
    """`records` returns record lists and raises on any other payload shape."""
    result = AgentExecuteResult(status="success", result=result_payload)
    if expectation is None:
        with pytest.raises(PyAirbyteInputError):
            _ = result.records
    else:
        assert result.records == expectation


def test_describe(captured_requests: list[dict[str, Any]]) -> None:
    """`describe()` parses the inspect response and caches it."""
    connector = _connector()
    details = connector.describe()

    assert details.name == "GitHub"
    assert details.source_definition_name == "GitHub"
    assert details.context_store_entities == ["issues", "repositories"]
    assert connector.name == "GitHub"

    connector.describe()
    assert len(captured_requests) == 1

    connector.describe(force_refresh=True)
    assert len(captured_requests) == 2


def test_list_connectors(captured_requests: list[dict[str, Any]]) -> None:
    """`list_connectors()` returns connectors from the workspace listing."""
    connectors = AgentWorkspace(
        workspace_id="workspace-id", bearer_token="test-token"
    ).list_connectors()

    assert [connector.connector_id for connector in connectors] == [
        "connector-1",
        "connector-2",
    ]
    assert captured_requests[0]["url"].endswith("/integrations/connectors")
    assert captured_requests[0]["params"] == {"workspace_id": "workspace-id"}


@pytest.mark.parametrize(
    ("kwargs", "expected_id", "expected_error"),
    [
        pytest.param({"connector_id": "explicit-id"}, "explicit-id", None, id="by_id"),
        pytest.param({"name": "Slack"}, "connector-2", None, id="by_exact_name"),
        pytest.param({"name": "GitHub"}, "connector-1", None, id="by_partial_name"),
        pytest.param({"name": "Missing"}, None, "No connector found", id="no_match"),
        pytest.param({}, None, "Exactly one of", id="no_args"),
        pytest.param(
            {"connector_id": "id", "name": "GitHub"},
            None,
            "Exactly one of",
            id="both_args",
        ),
    ],
)
def test_get_connector(
    captured_requests: list[dict[str, Any]],
    kwargs: dict[str, Any],
    expected_id: str | None,
    expected_error: str | None,
) -> None:
    """`get_connector()` resolves by ID without an API call, or by name via listing."""
    workspace = AgentWorkspace(workspace_id="workspace-id", bearer_token="test-token")

    if expected_error:
        with pytest.raises((AirbyteError, PyAirbyteInputError), match=expected_error):
            workspace.get_connector(**kwargs)
        return

    connector = workspace.get_connector(**kwargs)
    assert connector.connector_id == expected_id
    if "connector_id" in kwargs:
        assert captured_requests == []


def test_list_workspaces(captured_requests: list[dict[str, Any]]) -> None:
    """`list_workspaces()` returns workspaces scoped to the organization."""
    workspaces = AgentOrganization(
        organization_id="org-id",
        bearer_token="test-token",
    ).list_workspaces()

    assert [workspace.workspace_id for workspace in workspaces] == [
        "workspace-1",
        "workspace-2",
    ]
    assert [workspace.name for workspace in workspaces] == ["primary", "secondary"]
    assert captured_requests[0]["headers"]["X-Organization-Id"] == "org-id"


@pytest.mark.parametrize(
    ("kwargs", "expected_id", "expected_error"),
    [
        pytest.param({"workspace_id": "explicit-id"}, "explicit-id", None, id="by_id"),
        pytest.param({"name": "secondary"}, "workspace-2", None, id="by_name"),
        pytest.param({"name": "missing"}, None, "No workspace found", id="no_match"),
        pytest.param({}, None, "Exactly one of", id="no_args"),
    ],
)
def test_get_workspace(
    captured_requests: list[dict[str, Any]],
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
            organization.get_workspace(**kwargs)
        return

    workspace = organization.get_workspace(**kwargs)
    assert workspace.workspace_id == expected_id
    if "workspace_id" in kwargs:
        assert captured_requests == []


def test_as_cloud_workspace() -> None:
    """An Agents workspace converts to a Cloud workspace without any API call."""
    cloud_workspace = AgentWorkspace(
        workspace_id="workspace-id",
        bearer_token="test-token",
    ).as_cloud_workspace()

    assert isinstance(cloud_workspace, CloudWorkspace)
    assert cloud_workspace.workspace_id == "workspace-id"
    assert str(cloud_workspace.bearer_token) == "test-token"


def test_from_cloud_workspace(captured_requests: list[dict[str, Any]]) -> None:
    """Converting from Cloud verifies Agents eligibility unless `verify=False`."""
    cloud_workspace = CloudWorkspace(
        workspace_id="workspace-id", bearer_token="test-token"
    )

    AgentWorkspace.from_cloud_workspace(cloud_workspace, verify=False)
    assert captured_requests == []

    workspace = AgentWorkspace.from_cloud_workspace(
        cloud_workspace, organization_id="org-id"
    )
    assert workspace.workspace_id == "workspace-id"
    assert captured_requests[0]["url"].endswith("/workspaces/workspace-id")


def test_as_cloud_organization() -> None:
    """An Agents organization converts to a Cloud organization without an API call."""
    cloud_organization = AgentOrganization(
        organization_id="org-id",
        bearer_token="test-token",
    ).as_cloud_organization()

    assert cloud_organization.organization_id == "org-id"


def test_agents_api_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-2xx response raises `AirbyteError` with the status code in context."""
    monkeypatch.setattr(
        requests,
        "request",
        lambda **_: _FakeResponse({"message": "forbidden"}, status_code=403),
    )
    with pytest.raises(AirbyteError) as error_info:
        _api_util.inspect_agent_connector(
            connector_id="connector-id",
            credentials=_credentials(),
        )
    assert error_info.value.context["status_code"] == 403
