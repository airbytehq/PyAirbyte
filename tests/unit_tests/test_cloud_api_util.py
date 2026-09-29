# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Cloud API utilities."""

from __future__ import annotations

import json
from collections.abc import Callable
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from airbyte.cloud.models import CloudConnectionInfo
import requests
from airbyte import constants
from airbyte._util import api_util, meta
from airbyte.exceptions import (
    AirbyteError,
    AirbyteMissingResourceError,
    AirbyteWorkspaceNotEmptyError,
    PyAirbyteInputError,
)
from airbyte.registry import ConnectorType
from airbyte.secrets.base import SecretString
from airbyte_api import api, models
from airbyte_api.errors import SDKError


def _job_response(job_id: int) -> models.JobResponse:
    """Create a minimal job response for pagination tests."""
    return models.JobResponse(
        connection_id="connection-id",
        job_id=job_id,
        job_type=models.JobTypeEnum.SYNC,
        start_time="2026-01-01T00:00:00Z",
        status=models.JobStatusEnum.SUCCEEDED,
    )


def _list_jobs_response(
    data: list[models.JobResponse],
    *,
    next_page: str | None,
) -> api.ListJobsResponse:
    """Create a paginated jobs API response."""
    raw_response = requests.Response()
    raw_response.url = "https://api.airbyte.com/v1/jobs"
    return api.ListJobsResponse(
        content_type="application/json",
        status_code=200,
        raw_response=raw_response,
        jobs_response=models.JobsResponse(
            data=data,
            next=next_page,
        ),
    )


def _connection_response(name: str, index: int) -> models.ConnectionResponse:
    """Create a minimal connection response for pagination tests."""
    return models.ConnectionResponse(
        configurations={},
        connection_id=f"connection-{index}",
        created_at=index,
        destination_id=f"destination-{index}",
        name=name,
        schedule={},
        source_id=f"source-{index}",
        status=models.ConnectionStatusEnum.ACTIVE,
        tags=[],
        workspace_id="workspace-id",
    )


def _workspace_response(name: str, index: int) -> models.WorkspaceResponse:
    """Create a minimal workspace response for pagination tests."""
    return models.WorkspaceResponse(
        data_residency="auto",
        name=name,
        notifications=models.NotificationsConfig(),
        workspace_id=f"workspace-{index}",
    )


def _list_connections_response(
    data: list[models.ConnectionResponse],
    *,
    next_page: str | None,
) -> api.ListConnectionsResponse:
    """Create a paginated connections API response."""
    raw_response = requests.Response()
    raw_response.url = "https://api.airbyte.com/v1/connections"
    return api.ListConnectionsResponse(
        content_type="application/json",
        status_code=200,
        raw_response=raw_response,
        connections_response=models.ConnectionsResponse(
            data=data,
            next=next_page,
        ),
    )


def _list_workspaces_response(
    data: list[models.WorkspaceResponse],
    *,
    next_page: str | None,
) -> api.ListWorkspacesResponse:
    """Create a paginated workspaces API response."""
    raw_response = requests.Response()
    raw_response.url = "https://api.airbyte.com/v1/workspaces"
    return api.ListWorkspacesResponse(
        content_type="application/json",
        status_code=200,
        raw_response=raw_response,
        workspaces_response=models.WorkspacesResponse(
            data=data,
            next=next_page,
        ),
    )


@pytest.mark.parametrize(
    ("status_code", "expected_error_type"),
    [
        pytest.param(404, AirbyteMissingResourceError, id="not_found"),
        pytest.param(500, AirbyteError, id="server_error"),
    ],
)
def test_wrap_sdk_error_classifies_not_found(
    status_code: int,
    expected_error_type: type[AirbyteError],
) -> None:
    raw_response = requests.Response()
    raw_response.status_code = status_code
    raw_response.url = "https://api.airbyte.com/v1/workspaces/workspace-id"
    error = SDKError(
        "Workspace lookup failed.", status_code, "response body", raw_response
    )

    wrapped = api_util._wrap_sdk_error(error, {"workspace_id": "workspace-id"})

    assert type(wrapped) is expected_error_type
    assert wrapped.get_message() == "API error occurred: Workspace lookup failed."
    assert wrapped.context["workspace_id"] == "workspace-id"
    assert wrapped.context["status_code"] == status_code


def test_get_user_id_from_bearer_token() -> None:
    assert (
        api_util.get_user_id_from_bearer_token(
            SecretString("header.eyJ1c2VyX2lkIjoiYXV0aC11c2VyLWlkIn0.signature")
        )
        == "auth-user-id"
    )


def test_get_user_id_from_bearer_token_falls_back_to_subject() -> None:
    assert (
        api_util.get_user_id_from_bearer_token(
            SecretString("header.eyJzdWIiOiJhdXRoLXVzZXItaWQifQ.signature")
        )
        == "auth-user-id"
    )


@pytest.mark.parametrize(
    ("token", "expected_message"),
    [
        pytest.param(
            "not-a-jwt",
            "not a valid JWT",
            id="invalid-jwt",
        ),
        pytest.param(
            "header.not-json.signature",
            "could not be decoded",
            id="undecodable-payload",
        ),
        pytest.param(
            "header.e30.signature",
            "does not contain a user_id or sub claim",
            id="missing-user-id",
        ),
    ],
)
def test_get_user_id_from_bearer_token_rejects_invalid_tokens(
    token: str,
    expected_message: str,
) -> None:
    with pytest.raises(PyAirbyteInputError, match=expected_message):
        api_util.get_user_id_from_bearer_token(SecretString(token))


@pytest.mark.parametrize(
    (
        "helper",
        "kwargs",
        "expected_path",
        "expected_json",
        "fake_response",
        "expected_result",
    ),
    [
        pytest.param(
            api_util.get_user_by_auth_id,
            {"auth_user_id": "auth-user-id"},
            "/users/get_by_auth_id",
            {"authUserId": "auth-user-id", "authProvider": "keycloak"},
            {"userId": "user-id"},
            {"userId": "user-id"},
            id="user-by-auth-id",
        ),
        pytest.param(
            api_util.list_permissions_for_user,
            {"user_id": "user-id"},
            "/permissions/list_by_user",
            {"userId": "user-id"},
            [{"permissionType": "organization_member", "organizationId": "org-id"}],
            [{"permissionType": "organization_member", "organizationId": "org-id"}],
            id="permissions-list",
        ),
        pytest.param(
            api_util.list_permissions_for_user,
            {"user_id": "user-id"},
            "/permissions/list_by_user",
            {"userId": "user-id"},
            {
                "permissions": [
                    {
                        "permissionType": "organization_member",
                        "organizationId": "org-id",
                    }
                ]
            },
            [{"permissionType": "organization_member", "organizationId": "org-id"}],
            id="permissions-envelope",
        ),
        pytest.param(
            api_util.update_user_default_workspace,
            {"user_id": "user-id", "workspace_id": "workspace-id"},
            "/users/update",
            {"userId": "user-id", "defaultWorkspaceId": "workspace-id"},
            {"userId": "user-id", "defaultWorkspaceId": "workspace-id"},
            {"userId": "user-id", "defaultWorkspaceId": "workspace-id"},
            id="update-user-default-workspace",
        ),
        pytest.param(
            api_util.get_workspace_config_api,
            {"workspace_id": "workspace-id"},
            "/workspaces/get",
            {"workspaceId": "workspace-id", "includeTombstone": True},
            {"workspaceId": "workspace-id", "organizationId": "org-id"},
            {"workspaceId": "workspace-id", "organizationId": "org-id"},
            id="workspace-get",
        ),
    ],
)
def test_config_api_helpers_forward_requests(
    monkeypatch: pytest.MonkeyPatch,
    helper: Callable[..., object],
    kwargs: dict[str, str],
    expected_path: str,
    expected_json: dict[str, str],
    fake_response: object,
    expected_result: object,
) -> None:
    captured: dict[str, object] = {}

    def fake_config_request(**request_kwargs: object) -> object:
        captured.update(request_kwargs)
        return fake_response

    monkeypatch.setattr(api_util, "_make_config_api_request", fake_config_request)

    result = helper(
        **kwargs,
        api_root="https://api.example",
        config_api_root="https://config.example",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    assert result == expected_result
    assert captured["path"] == expected_path
    assert captured["json"] == expected_json
    assert captured["config_api_root"] == "https://config.example"


@pytest.mark.parametrize(
    ("helper", "kwargs", "response", "expected_message"),
    [
        pytest.param(
            api_util.get_user_by_auth_id,
            {"auth_user_id": "auth-user-id"},
            [],
            "user API returned an unexpected response",
            id="user-list",
        ),
        pytest.param(
            api_util.get_user_by_auth_id,
            {"auth_user_id": "auth-user-id"},
            "unexpected",
            "user API returned an unexpected response",
            id="user-string",
        ),
        pytest.param(
            api_util.list_permissions_for_user,
            {"user_id": "user-id"},
            "unexpected",
            "permissions API returned an unexpected response",
            id="permissions-string",
        ),
        pytest.param(
            api_util.list_permissions_for_user,
            {"user_id": "user-id"},
            {"permissions": {}},
            "permissions API returned an unexpected response",
            id="permissions-non-list-envelope",
        ),
        pytest.param(
            api_util.list_organizations_for_user_id,
            {"user_id": "user-id"},
            [],
            "organizations API returned an unexpected response",
            id="organizations-list",
        ),
        pytest.param(
            api_util.list_organizations_for_user_id,
            {"user_id": "user-id"},
            {"organizations": {}},
            "organizations API returned an unexpected response",
            id="organizations-non-list-envelope",
        ),
        pytest.param(
            api_util.get_organization_info,
            {"organization_id": "organization-id"},
            [],
            "organization API returned an unexpected response",
            id="organization-list",
        ),
        pytest.param(
            api_util.get_organization_info,
            {"organization_id": "organization-id"},
            "unexpected",
            "organization API returned an unexpected response",
            id="organization-string",
        ),
        pytest.param(
            api_util.get_workspace_organization_info,
            {"workspace_id": "workspace-id"},
            [],
            "workspace API returned an unexpected response",
            id="workspace-list",
        ),
        pytest.param(
            api_util.get_workspace_organization_info,
            {"workspace_id": "workspace-id"},
            "unexpected",
            "workspace API returned an unexpected response",
            id="workspace-string",
        ),
        pytest.param(
            api_util.update_user_default_workspace,
            {"user_id": "user-id", "workspace_id": "workspace-id"},
            "unexpected",
            "user API returned an unexpected response",
            id="update-user-string",
        ),
        pytest.param(
            api_util.get_workspace_config_api,
            {"workspace_id": "workspace-id"},
            [],
            "workspace API returned an unexpected response",
            id="workspace-config-list",
        ),
    ],
)
def test_config_api_helpers_reject_unexpected_response(
    monkeypatch: pytest.MonkeyPatch,
    helper: Callable[..., object],
    kwargs: dict[str, str],
    response: object,
    expected_message: str,
) -> None:
    monkeypatch.setattr(
        api_util,
        "_make_config_api_request",
        lambda **_: response,
    )

    with pytest.raises(AirbyteError, match=expected_message) as exc_info:
        helper(
            **kwargs,
            api_root="https://api.example",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )

    assert exc_info.value.context == {"response": response}


@pytest.mark.parametrize(
    ("name_contains", "limit", "expected_count", "expected_last", "request_count"),
    [
        pytest.param(
            None,
            150,
            150,
            "organization-149",
            2,
            id="without-name-filter",
        ),
        pytest.param(
            "Airbyte",
            None,
            200,
            "organization-199",
            3,
            id="with-name-filter",
        ),
    ],
)
def test_list_organizations_for_user_id_paginates_and_forwards_filters(
    monkeypatch: pytest.MonkeyPatch,
    name_contains: str | None,
    limit: int | None,
    expected_count: int,
    expected_last: str,
    request_count: int,
) -> None:
    requests: list[dict[str, object]] = []
    pages = [
        {
            "organizations": [
                {"organizationId": f"organization-{index}"} for index in range(100)
            ]
        },
        {
            "organizations": [
                {"organizationId": f"organization-{index}"} for index in range(100, 200)
            ]
        },
        {"organizations": []},
    ]

    def fake_config_request(**kwargs: object) -> dict[str, object]:
        request = kwargs["json"]
        assert isinstance(request, dict)
        requests.append(request)
        return pages.pop(0)

    monkeypatch.setattr(api_util, "_make_config_api_request", fake_config_request)

    result = api_util.list_organizations_for_user_id(
        "user-id",
        api_root="https://api.airbyte.com/v1/",
        client_id=SecretString("client-id"),
        client_secret=SecretString("client-secret"),
        bearer_token=None,
        name_contains=name_contains,
        limit=limit,
    )

    result_ids = [organization["organizationId"] for organization in result]
    assert len(result_ids) == expected_count
    assert result_ids[0] == "organization-0"
    assert result_ids[-1] == expected_last
    assert requests[0] == {
        "userId": "user-id",
        "pagination": {"pageSize": 100, "rowOffset": 0},
        **({"nameContains": name_contains} if name_contains is not None else {}),
    }
    assert requests[1]["pagination"] == {"pageSize": 100, "rowOffset": 100}
    assert len(requests) == request_count


def test_create_workspace_forwards_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_request = None

    def create_workspace(
        *,
        request: models.WorkspaceCreateRequest,
    ) -> api.CreateWorkspaceResponse:
        nonlocal captured_request
        captured_request = request
        raw_response = requests.Response()
        raw_response.url = "https://api.airbyte.com/v1/workspaces"
        return api.CreateWorkspaceResponse(
            content_type="application/json",
            status_code=200,
            raw_response=raw_response,
            workspace_response=_workspace_response("New workspace", 1),
        )

    airbyte_instance = SimpleNamespace(
        workspaces=SimpleNamespace(create_workspace=create_workspace)
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    workspace = api_util.create_workspace(
        name="New workspace",
        organization_id="organization-id",
        region_id="us-east",
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    assert workspace.workspace_id == "workspace-1"
    assert captured_request is not None
    assert captured_request.name == "New workspace"
    assert captured_request.organization_id == "organization-id"
    assert captured_request.region_id == "us-east"


def test_rename_workspace_forwards_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_request = None

    def update_workspace(
        request: api.UpdateWorkspaceRequest,
    ) -> api.UpdateWorkspaceResponse:
        nonlocal captured_request
        captured_request = request
        raw_response = requests.Response()
        raw_response.url = "https://api.airbyte.com/v1/workspaces/workspace-1"
        return api.UpdateWorkspaceResponse(
            content_type="application/json",
            status_code=200,
            raw_response=raw_response,
            workspace_response=_workspace_response("Renamed workspace", 1),
        )

    airbyte_instance = SimpleNamespace(
        workspaces=SimpleNamespace(update_workspace=update_workspace)
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    workspace = api_util.rename_workspace(
        workspace_id="workspace-1",
        name="Renamed workspace",
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    assert workspace.name == "Renamed workspace"
    assert captured_request is not None
    assert captured_request.workspace_id == "workspace-1"
    assert captured_request.workspace_update_request.name == "Renamed workspace"


def test_patch_connection_normalizes_status_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify string connection statuses are normalized for the generated SDK."""
    captured_request = None

    def patch_connection(
        request: api.PatchConnectionRequest,
    ) -> api.PatchConnectionResponse:
        nonlocal captured_request
        captured_request = request
        raw_response = requests.Response()
        raw_response.url = "https://api.airbyte.com/v1/connections/connection-1"
        return api.PatchConnectionResponse(
            content_type="application/json",
            status_code=200,
            raw_response=raw_response,
            connection_response=_connection_response("Connection", 1),
        )

    airbyte_instance = SimpleNamespace(
        connections=SimpleNamespace(patch_connection=patch_connection)
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    api_util.patch_connection(
        connection_id="connection-1",
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
        status="inactive",
    )

    assert captured_request is not None
    assert (
        captured_request.connection_patch_request.status
        == models.ConnectionStatusEnum.INACTIVE
    )


def test_patch_connection_rejects_invalid_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify invalid string connection statuses produce PyAirbyte errors."""
    airbyte_instance = SimpleNamespace(connections=SimpleNamespace())
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    with pytest.raises(PyAirbyteInputError, match="`status` must be one of"):
        api_util.patch_connection(
            connection_id="connection-1",
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
            status="paused",
        )


@pytest.mark.parametrize(
    "workspace_name,should_delete",
    [
        pytest.param("delete-me workspace", True, id="delete_me_with_hyphen"),
        pytest.param("deleteme workspace", True, id="deleteme_without_hyphen"),
        pytest.param("production workspace", False, id="unsafe_name"),
    ],
)
def test_permanently_delete_workspace_requires_safe_name(
    monkeypatch: pytest.MonkeyPatch,
    workspace_name: str,
    should_delete: bool,
) -> None:
    delete_calls = 0

    def get_workspace(**_: object) -> models.WorkspaceResponse:
        return models.WorkspaceResponse(
            data_residency="auto",
            name=workspace_name,
            notifications=models.NotificationsConfig(),
            workspace_id="workspace-1",
        )

    def delete_workspace(
        request: api.DeleteWorkspaceRequest,
    ) -> api.DeleteWorkspaceResponse:
        nonlocal delete_calls
        delete_calls += 1
        assert request.workspace_id == "workspace-1"
        raw_response = requests.Response()
        raw_response.url = "https://api.airbyte.com/v1/workspaces/workspace-1"
        return api.DeleteWorkspaceResponse(
            content_type="",
            status_code=204,
            raw_response=raw_response,
        )

    airbyte_instance = SimpleNamespace(
        workspaces=SimpleNamespace(delete_workspace=delete_workspace)
    )
    monkeypatch.setattr(api_util, "get_workspace", get_workspace)
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )
    monkeypatch.setattr(api_util, "list_connections", lambda **_: [])

    if should_delete:
        api_util.permanently_delete_workspace(
            workspace_id="workspace-1",
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )
        assert delete_calls == 1
    else:
        with pytest.raises(PyAirbyteInputError):
            api_util.permanently_delete_workspace(
                workspace_id="workspace-1",
                api_root="https://api.airbyte.com/v1",
                client_id=None,
                client_secret=None,
                bearer_token=SecretString("token"),
            )
        assert delete_calls == 0


def test_permanently_delete_workspace_requires_empty_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delete_calls = 0

    def delete_workspace(
        request: api.DeleteWorkspaceRequest,
    ) -> api.DeleteWorkspaceResponse:
        nonlocal delete_calls
        delete_calls += 1
        raw_response = requests.Response()
        raw_response.url = "https://api.airbyte.com/v1/workspaces/workspace-1"
        return api.DeleteWorkspaceResponse(
            content_type="",
            status_code=204,
            raw_response=raw_response,
        )

    airbyte_instance = SimpleNamespace(
        workspaces=SimpleNamespace(delete_workspace=delete_workspace)
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )
    monkeypatch.setattr(
        api_util,
        "list_connections",
        lambda **_: [_connection_response("existing connection", 1)],
    )

    with pytest.raises(AirbyteWorkspaceNotEmptyError) as exc_info:
        api_util.permanently_delete_workspace(
            workspace_id="workspace-id",
            workspace_name="delete-me workspace",
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )

    assert exc_info.value.workspace_id == "workspace-id"
    assert exc_info.value.connection_ids == ["connection-1"]
    assert delete_calls == 0


@pytest.mark.parametrize(
    "kwargs,pages,expected_names,expected_requests",
    [
        pytest.param(
            {"limit": 1, "name_filter": lambda name: name == "target"},
            [
                _list_connections_response(
                    [
                        _connection_response("miss", 1),
                        _connection_response("target", 2),
                    ],
                    next_page=None,
                ),
            ],
            ["target"],
            [(100, 0)],
            id="filtered_limit_uses_full_page",
        ),
        pytest.param(
            {"limit": 2, "name_filter": lambda name: name == "target"},
            [
                _list_connections_response(
                    [_connection_response("target", 1)],
                    next_page="next",
                ),
                _list_connections_response(
                    [
                        _connection_response("target", 2),
                        _connection_response("extra", 3),
                    ],
                    next_page=None,
                ),
            ],
            ["target", "target"],
            [(100, 0), (100, 1)],
            id="filtered_limit_continues_until_enough_matches",
        ),
        pytest.param(
            {"name": ""},
            [
                _list_connections_response(
                    [
                        _connection_response("", 1),
                        _connection_response("non-empty", 2),
                    ],
                    next_page=None,
                ),
            ],
            [""],
            [(100, 0)],
            id="empty_name_filters_exactly",
        ),
        pytest.param(
            {},
            [
                _list_connections_response(
                    [_connection_response("first", 1)],
                    next_page="next",
                ),
                _list_connections_response(
                    [_connection_response("second", 2)],
                    next_page=None,
                ),
            ],
            ["first", "second"],
            [(100, 0), (100, 1)],
            id="no_limit_auto_paginates",
        ),
    ],
)
def test_list_connections_paginates_resources(
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict,
    pages: list[api.ListConnectionsResponse],
    expected_names: list[str],
    expected_requests: list[tuple[int | None, int | None]],
) -> None:
    """Verify resource list pagination, filtering, and request sizing."""
    captured_requests: list[api.ListConnectionsRequest] = []

    def list_connections(
        request: api.ListConnectionsRequest,
    ) -> api.ListConnectionsResponse:
        """Capture connection list requests and return queued pages."""
        captured_requests.append(request)
        return pages.pop(0)

    airbyte_instance = SimpleNamespace(
        connections=SimpleNamespace(list_connections=list_connections),
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    result = api_util.list_connections(
        workspace_id="workspace-id",
        api_root="https://api.airbyte.com/v1/",
        client_id=SecretString("client-id"),
        client_secret=SecretString("client-secret"),
        bearer_token=None,
        **kwargs,
    )

    assert [connection.name for connection in result] == expected_names
    assert [
        (request.limit, request.offset) for request in captured_requests
    ] == expected_requests


def test_list_workspaces_does_not_filter_by_workspace_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify workspace listing fetches accessible workspaces across pages."""
    captured_requests: list[api.ListWorkspacesRequest] = []
    pages = [
        _list_workspaces_response(
            [_workspace_response("first", 1)],
            next_page="next",
        ),
        _list_workspaces_response(
            [_workspace_response("second", 2)],
            next_page=None,
        ),
    ]

    def list_workspaces(
        request: api.ListWorkspacesRequest,
    ) -> api.ListWorkspacesResponse:
        """Capture workspace list requests and return queued pages."""
        captured_requests.append(request)
        return pages.pop(0)

    airbyte_instance = SimpleNamespace(
        workspaces=SimpleNamespace(list_workspaces=list_workspaces),
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    result = api_util.list_workspaces(
        workspace_id="context-workspace-id",
        api_root="https://api.airbyte.com/v1/",
        client_id=SecretString("client-id"),
        client_secret=SecretString("client-secret"),
        bearer_token=None,
    )

    assert [workspace.workspace_id for workspace in result] == [
        "workspace-1",
        "workspace-2",
    ]
    assert [
        (request.workspace_ids, request.limit, request.offset)
        for request in captured_requests
    ] == [
        (None, 100, 0),
        (None, 100, 1),
    ]


def test_list_workspaces_caps_unfiltered_api_page_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify unfiltered workspace listing uses requested limit as API page size."""
    captured_requests: list[api.ListWorkspacesRequest] = []
    pages = [
        _list_workspaces_response(
            [_workspace_response("first", 1)],
            next_page="next",
        ),
    ]

    def list_workspaces(
        request: api.ListWorkspacesRequest,
    ) -> api.ListWorkspacesResponse:
        """Capture workspace list requests and return queued pages."""
        captured_requests.append(request)
        return pages.pop(0)

    airbyte_instance = SimpleNamespace(
        workspaces=SimpleNamespace(list_workspaces=list_workspaces),
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    result = api_util.list_workspaces(
        workspace_id="context-workspace-id",
        api_root="https://api.airbyte.com/v1/",
        client_id=SecretString("client-id"),
        client_secret=SecretString("client-secret"),
        bearer_token=None,
        limit=1,
    )

    assert [workspace.workspace_id for workspace in result] == ["workspace-1"]
    assert [
        (request.workspace_ids, request.limit, request.offset)
        for request in captured_requests
    ] == [(None, 1, 0)]


@pytest.mark.parametrize("limit", [0, -1])
def test_list_connections_rejects_invalid_limits(limit: int) -> None:
    """Verify connection list pagination rejects non-positive limits."""
    with pytest.raises(PyAirbyteInputError, match="`limit` must be greater than 0."):
        api_util.list_connections(
            workspace_id="workspace-id",
            api_root="https://api.airbyte.com/v1/",
            client_id=SecretString("client-id"),
            client_secret=SecretString("client-secret"),
            bearer_token=None,
            limit=limit,
        )


def test_get_job_logs_paginates_until_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify job log pagination stops after collecting the requested limit."""
    captured_requests: list[api.ListJobsRequest] = []
    pages = [
        _list_jobs_response(
            [_job_response(job_id) for job_id in range(100)],
            next_page="next",
        ),
        _list_jobs_response(
            [_job_response(job_id) for job_id in range(100, 150)],
            next_page=None,
        ),
    ]

    def list_jobs(request: api.ListJobsRequest) -> api.ListJobsResponse:
        """Capture job list requests and return queued pages."""
        captured_requests.append(request)
        return pages.pop(0)

    airbyte_instance = SimpleNamespace(jobs=SimpleNamespace(list_jobs=list_jobs))
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    result = api_util.get_job_logs(
        workspace_id="workspace-id",
        connection_id="connection-id",
        limit=150,
        api_root="https://api.airbyte.com/v1/",
        client_id=SecretString("client-id"),
        client_secret=SecretString("client-secret"),
        bearer_token=None,
    )

    assert [job.job_id for job in result] == list(range(150))
    assert [(request.limit, request.offset) for request in captured_requests] == [
        (100, 0),
        (50, 100),
    ]


def test_get_job_logs_uses_offset_and_allows_unbounded_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify job log pagination preserves offset and treats `None` as unbounded."""
    captured_requests: list[api.ListJobsRequest] = []
    pages = [
        _list_jobs_response(
            [_job_response(job_id) for job_id in range(100)],
            next_page="next",
        ),
        _list_jobs_response(
            [_job_response(job_id) for job_id in range(100, 125)],
            next_page=None,
        ),
    ]

    def list_jobs(request: api.ListJobsRequest) -> api.ListJobsResponse:
        """Capture job list requests and return queued pages."""
        captured_requests.append(request)
        return pages.pop(0)

    airbyte_instance = SimpleNamespace(jobs=SimpleNamespace(list_jobs=list_jobs))
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    result = api_util.get_job_logs(
        workspace_id="workspace-id",
        connection_id="connection-id",
        limit=None,
        offset=10,
        api_root="https://api.airbyte.com/v1/",
        client_id=SecretString("client-id"),
        client_secret=SecretString("client-secret"),
        bearer_token=None,
    )

    assert [job.job_id for job in result] == list(range(125))
    assert [(request.limit, request.offset) for request in captured_requests] == [
        (100, 10),
        (100, 110),
    ]


def test_cancel_job_forwards_request_and_returns_job_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cancelling a job forwards its ID and returns the API job response."""
    captured_request: api.CancelJobRequest | None = None
    job_response = _job_response(42)
    raw_response = requests.Response()
    raw_response.url = "https://api.airbyte.com/v1/jobs/42"

    def cancel_job(request: api.CancelJobRequest) -> api.CancelJobResponse:
        """Capture the cancellation request."""
        nonlocal captured_request
        captured_request = request
        return api.CancelJobResponse(
            content_type="application/json",
            status_code=200,
            raw_response=raw_response,
            job_response=job_response,
        )

    airbyte_instance = SimpleNamespace(jobs=SimpleNamespace(cancel_job=cancel_job))
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    result = api_util.cancel_job(
        job_id=42,
        api_root="https://api.airbyte.com/v1/",
        client_id=SecretString("client-id"),
        client_secret=SecretString("client-secret"),
        bearer_token=None,
    )

    assert result is job_response
    assert captured_request is not None
    assert captured_request.job_id == 42


def test_cancel_job_raises_for_non_ok_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cancelling a job raises when the API response is not successful."""
    raw_response = requests.Response()
    raw_response.status_code = 404
    raw_response.url = "https://api.airbyte.com/v1/jobs/42"

    def cancel_job(request: api.CancelJobRequest) -> api.CancelJobResponse:
        """Return a not-found cancellation response."""
        _ = request
        return api.CancelJobResponse(
            content_type="application/json",
            status_code=404,
            raw_response=raw_response,
        )

    airbyte_instance = SimpleNamespace(jobs=SimpleNamespace(cancel_job=cancel_job))
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    with pytest.raises(AirbyteMissingResourceError):
        api_util.cancel_job(
            job_id=42,
            api_root="https://api.airbyte.com/v1/",
            client_id=SecretString("client-id"),
            client_secret=SecretString("client-secret"),
            bearer_token=None,
        )


def test_cancel_job_raises_airbyte_error_for_non_not_found_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify non-not-found cancellation failures use a general API error."""
    raw_response = requests.Response()
    raw_response.status_code = 409
    raw_response.url = "https://api.airbyte.com/v1/jobs/42"

    def cancel_job(request: api.CancelJobRequest) -> api.CancelJobResponse:
        """Return a conflict cancellation response."""
        _ = request
        return api.CancelJobResponse(
            content_type="application/json",
            status_code=409,
            raw_response=raw_response,
        )

    airbyte_instance = SimpleNamespace(jobs=SimpleNamespace(cancel_job=cancel_job))
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    with pytest.raises(AirbyteError) as error:
        api_util.cancel_job(
            job_id=42,
            api_root="https://api.airbyte.com/v1/",
            client_id=SecretString("client-id"),
            client_secret=SecretString("client-secret"),
            bearer_token=None,
        )

    assert not isinstance(error.value, AirbyteMissingResourceError)


@pytest.mark.parametrize(
    ("mcp_mode", "hosted_mcp_mode", "expected"),
    [
        (False, False, "pyairbyte"),
        (True, False, "pyairbyte-mcp-local"),
        (True, True, "pyairbyte-mcp-hosted"),
    ],
)
def test_get_analytic_source_reflects_runtime_mode(
    monkeypatch: pytest.MonkeyPatch,
    mcp_mode: bool,
    hosted_mcp_mode: bool,
    expected: str,
) -> None:
    monkeypatch.setattr(meta, "_MCP_MODE_ENABLED", mcp_mode)
    monkeypatch.setattr(constants, "_HOSTED_MCP_MODE_ENABLED", hosted_mcp_mode)

    assert meta.get_cloud_api_analytic_source() == expected


@pytest.mark.parametrize(
    ("mcp_mode", "hosted_mcp_mode", "headers", "expected"),
    [
        pytest.param(
            True,
            True,
            {"x-airbyte-analytic-source": "Coral-Support-Agent"},
            "coral-support-agent",
            id="upstream_wins",
        ),
        pytest.param(
            True,
            True,
            {"x-airbyte-analytic-source": "evil"},
            "pyairbyte-mcp-hosted",
            id="unlisted_source_rejected",
        ),
        pytest.param(True, True, {}, "pyairbyte-mcp-hosted", id="no_header_falls_back"),
        pytest.param(
            False,
            False,
            {"x-airbyte-analytic-source": "coral-support-agent"},
            "pyairbyte",
            id="header_ignored_outside_mcp_mode",
        ),
        pytest.param(
            True,
            False,
            {"x-airbyte-analytic-source": "coral-support-agent"},
            "coral-support-agent",
            id="upstream_wins_local_mcp_mode",
        ),
    ],
)
def test_get_analytic_source_upstream_header(
    monkeypatch: pytest.MonkeyPatch,
    mcp_mode: bool,
    hosted_mcp_mode: bool,
    headers: dict,
    expected: str,
) -> None:
    monkeypatch.setattr(meta, "_MCP_MODE_ENABLED", mcp_mode)
    monkeypatch.setattr(constants, "_HOSTED_MCP_MODE_ENABLED", hosted_mcp_mode)
    monkeypatch.setattr(meta, "get_http_headers", lambda **_: headers)

    assert meta.get_cloud_api_analytic_source() == expected


def test_config_api_request_sends_analytic_source_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(meta, "_MCP_MODE_ENABLED", True)
    monkeypatch.setattr(constants, "_HOSTED_MCP_MODE_ENABLED", True)
    captured: dict[str, object] = {}

    def fake_request(**kwargs: object) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(status_code=200, json=lambda: {})

    monkeypatch.setattr(api_util.requests, "request", fake_request)

    api_util._make_config_api_request(
        path="/workspaces/get",
        json={},
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    headers = captured["headers"]
    assert isinstance(headers, dict)
    assert headers[meta.AIRBYTE_ANALYTIC_SOURCE_HEADER] == "pyairbyte-mcp-hosted"


def test_public_api_client_sends_analytic_source_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(meta, "_MCP_MODE_ENABLED", True)
    monkeypatch.setattr(constants, "_HOSTED_MCP_MODE_ENABLED", False)

    airbyte_instance = api_util.get_airbyte_server_instance(
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    session = airbyte_instance.sdk_configuration.client
    assert session.headers[meta.AIRBYTE_ANALYTIC_SOURCE_HEADER] == "pyairbyte-mcp-local"


def test_get_bearer_token_sends_analytic_source_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(meta, "_MCP_MODE_ENABLED", False)
    captured: dict[str, object] = {}

    def fake_post(**kwargs: object) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(status_code=200, json=lambda: {"access_token": "token"})

    monkeypatch.setattr(api_util.requests, "post", fake_post)

    api_util.get_bearer_token(
        client_id=SecretString("client-id"),
        client_secret=SecretString("client-secret"),
        api_root="https://api.airbyte.com/v1",
    )

    headers = captured["headers"]
    assert isinstance(headers, dict)
    assert headers[meta.AIRBYTE_ANALYTIC_SOURCE_HEADER] == "pyairbyte"


def _sdk_404_error(resource_type: str) -> SDKError:
    """Create an SDKError like the Speakeasy SDK raises on a 404."""
    raw_response = requests.Response()
    raw_response.status_code = 404
    raw_response.url = "https://api.airbyte.com/v1/connectors/connector-id"
    return SDKError(
        "API error occurred: Status 404",
        404,
        f'{{"resourceType":"{resource_type}"}}',
        raw_response,
    )


def _sdk_status_error(status_code: int) -> SDKError:
    """Create an SDKError like the Speakeasy SDK raises on a non-2xx status."""
    raw_response = requests.Response()
    raw_response.status_code = status_code
    raw_response.url = "https://api.airbyte.com/v1/connectors/connector-id"
    return SDKError(
        f"API error occurred: Status {status_code}",
        status_code,
        '{"message":"Caller does not have the required permissions"}',
        raw_response,
    )


@pytest.mark.parametrize(
    "source_status",
    [
        pytest.param(404, id="source_404"),
        # The API hides a destination behind a 403 from the source endpoint.
        pytest.param(403, id="source_403"),
    ],
)
def test_get_connector_falls_back_to_destination(
    monkeypatch: pytest.MonkeyPatch,
    source_status: int,
) -> None:
    """A bare destination ID must still resolve when the source lookup 404s or 403s."""
    raw_response = requests.Response()
    raw_response.status_code = 200
    raw_response.url = "https://api.airbyte.com/v1/destinations/dest-id"
    raw_response._content = (
        b'{"destinationId":"dest-id","name":"dest",'
        b'"destinationType":"duckdb","workspaceId":"ws"}'
    )
    destination_response = models.DestinationResponse(
        configuration=models.DestinationDuckdb(destination_path="/tmp/test.duckdb"),
        created_at=0,
        definition_id="definition-id",
        destination_id="dest-id",
        destination_type="duckdb",
        name="dest",
        workspace_id="ws",
    )
    airbyte_instance = SimpleNamespace(
        sources=SimpleNamespace(
            get_source=Mock(side_effect=_sdk_status_error(source_status)),
        ),
        destinations=SimpleNamespace(
            get_destination=Mock(
                return_value=api.GetDestinationResponse(
                    content_type="application/json",
                    status_code=200,
                    raw_response=raw_response,
                    destination_response=destination_response,
                ),
            ),
        ),
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    connector_type, response = api_util.get_connector(
        "dest-id",
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    assert connector_type is ConnectorType.DESTINATION
    assert response is destination_response


def test_get_connector_raises_missing_resource_when_neither_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    airbyte_instance = SimpleNamespace(
        sources=SimpleNamespace(
            get_source=Mock(side_effect=_sdk_404_error("SOURCE_CONNECTION")),
        ),
        destinations=SimpleNamespace(
            get_destination=Mock(
                side_effect=_sdk_404_error("DESTINATION_CONNECTION"),
            ),
        ),
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    with pytest.raises(AirbyteMissingResourceError):
        api_util.get_connector(
            "missing-id",
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )


@pytest.mark.parametrize(
    (
        "source_status",
        "destination_status",
        "expected_status",
        "expect_destination_call",
    ),
    [
        pytest.param(403, 403, 403, True, id="both_forbidden_raises_source_error"),
        pytest.param(
            403, 404, 403, True, id="forbidden_then_missing_raises_source_error"
        ),
        pytest.param(
            404, 403, 404, True, id="missing_then_forbidden_raises_source_error"
        ),
        pytest.param(403, 500, 500, True, id="destination_server_error_propagates"),
        pytest.param(401, None, 401, False, id="unauthorized_does_not_fall_back"),
        pytest.param(500, None, 500, False, id="server_error_does_not_fall_back"),
    ],
)
def test_get_connector_error_fallback(
    monkeypatch: pytest.MonkeyPatch,
    source_status: int,
    destination_status: int | None,
    expected_status: int,
    expect_destination_call: bool,
) -> None:
    get_destination = Mock(
        side_effect=_sdk_status_error(destination_status or 200),
    )
    airbyte_instance = SimpleNamespace(
        sources=SimpleNamespace(
            get_source=Mock(side_effect=_sdk_status_error(source_status)),
        ),
        destinations=SimpleNamespace(get_destination=get_destination),
    )
    monkeypatch.setattr(
        api_util, "get_airbyte_server_instance", lambda **_: airbyte_instance
    )

    with pytest.raises(AirbyteError) as exc_info:
        api_util.get_connector(
            "connector-id",
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )

    context = exc_info.value.context or {}
    assert context["status_code"] == expected_status
    assert ("source_id" in context) is (expected_status == source_status)
    assert get_destination.called is expect_destination_call


def test_get_connector_does_not_fall_back_on_non_raised_5xx_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 5xx the SDK returns instead of raising is not read as "maybe a destination"."""
    get_destination = Mock()
    airbyte_instance = SimpleNamespace(
        sources=SimpleNamespace(
            get_source=Mock(
                return_value=SimpleNamespace(
                    status_code=500,
                    source_response=None,
                    raw_response=SimpleNamespace(text="boom", url="https://api"),
                ),
            ),
        ),
        destinations=SimpleNamespace(get_destination=get_destination),
    )
    monkeypatch.setattr(
        api_util, "get_airbyte_server_instance", lambda **_: airbyte_instance
    )

    with pytest.raises(AirbyteMissingResourceError) as exc_info:
        api_util.get_connector(
            "connector-id",
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )

    assert (exc_info.value.context or {})["status_code"] == 500
    get_destination.assert_not_called()


def test_get_source_reraises_non_404_sdk_error_as_airbyte_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_response = requests.Response()
    raw_response.status_code = 500
    raw_response.url = "https://api.airbyte.com/v1/sources/source-id"
    error = SDKError(
        "API error occurred: Status 500", 500, "response body", raw_response
    )
    airbyte_instance = SimpleNamespace(
        sources=SimpleNamespace(get_source=Mock(side_effect=error)),
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    with pytest.raises(AirbyteError) as exc_info:
        api_util.get_source(
            "source-id",
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )

    assert type(exc_info.value) is AirbyteError


def test_config_api_request_is_bounded_by_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Config API requests carry connect and read timeouts, so a stalled endpoint raises."""
    captured: dict[str, object] = {}

    def fake_request(**kwargs: object) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(status_code=200, json=lambda: {})

    monkeypatch.setattr(api_util.requests, "request", fake_request)

    api_util._make_config_api_request(
        path="/jobs/get",
        json={},
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    assert captured["timeout"] == (5.0, 120.0)


@pytest.mark.parametrize(
    ("send_kwargs", "expected"),
    [
        pytest.param({}, (5.0, 120.0), id="default"),
        pytest.param({"timeout": None}, (5.0, 120.0), id="none"),
        pytest.param({"timeout": 3}, 3, id="explicit"),
    ],
)
def test_public_api_client_bounds_requests_by_timeout(
    monkeypatch: pytest.MonkeyPatch,
    send_kwargs: dict[str, object],
    expected: object,
) -> None:
    """The public API client applies a timeout to requests sent without one."""
    captured: dict[str, object] = {}

    def fake_send(_self: object, _request: object, **kwargs: object) -> str:
        captured.update(kwargs)
        return "response"

    monkeypatch.setattr(api_util.requests.Session, "send", fake_send)
    airbyte_instance = api_util.get_airbyte_server_instance(
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    airbyte_instance.sdk_configuration.client.send(
        requests.Request("GET", "https://api.airbyte.com/v1/jobs").prepare(),
        **send_kwargs,
    )

    assert captured["timeout"] == expected


def _check_with_response(
    monkeypatch: pytest.MonkeyPatch, response: object
) -> tuple[bool, str | None]:
    monkeypatch.setattr(api_util, "_make_config_api_request", lambda **_: response)
    return api_util.check_connector(
        actor_id="source-id",
        connector_type=ConnectorType.SOURCE,
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )


@pytest.mark.parametrize(
    ("status", "origin"),
    [
        pytest.param("failed", ["source"], id="list-origin"),
        pytest.param("failed", {"origin": "source"}, id="dict-origin"),
        pytest.param("failed", "SOURCE", id="uppercase-origin"),
        pytest.param("failed", " source\n", id="padded-origin"),
        pytest.param("failed", "  ", id="blank-origin"),
        pytest.param("FAILED", "Destination", id="uppercase-status"),
    ],
)
def test_check_connector_normalizes_status_and_failure_origin(
    monkeypatch: pytest.MonkeyPatch, status: str, origin: object
) -> None:
    """A malformed or differently cased origin still yields a failed connector check."""
    response = {
        "status": status,
        "jobInfo": {
            "failureReason": {
                "failureOrigin": origin,
                "externalMessage": "bad password",
            }
        },
    }

    assert _check_with_response(monkeypatch, response) == (False, "bad password")


def test_check_connector_platform_origin_is_case_insensitive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A platform failure is not reported as a connector failure, whatever its casing."""
    response = {
        "status": "failed",
        "jobInfo": {
            "failureReason": {
                "failureOrigin": "AIRBYTE_PLATFORM",
                "externalMessage": "pod failed to start",
            }
        },
    }

    with pytest.raises(AirbyteError, match="Connector check did not complete.") as e:
        _check_with_response(monkeypatch, response)

    assert e.value.context is not None
    assert e.value.context["failure_origin"] == "airbyte_platform"


@pytest.mark.parametrize("data", [None, "leaked-body", [None], [{"jobId": 1}]])
def test_get_job_logs_rejects_malformed_job_list(
    monkeypatch: pytest.MonkeyPatch, data: object
) -> None:
    """A job list that is not a list of jobs raises the safe unexpected-response error."""
    response = _list_jobs_response([], next_page=None)
    assert response.jobs_response is not None
    response.jobs_response.data = data  # type: ignore[assignment]
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: SimpleNamespace(
            jobs=SimpleNamespace(list_jobs=lambda _request: response)
        ),
    )

    with pytest.raises(AirbyteError, match="Unexpected API response.") as exc_info:
        api_util.get_job_logs(
            workspace_id="workspace-id",
            connection_id="connection-id",
            limit=5,
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )

    assert "leaked-body" not in str(exc_info.value)


@pytest.mark.parametrize("destination_type", [["snowflake"], {"type": "snowflake"}, 5])
def test_get_destination_tolerates_non_str_destination_type(
    monkeypatch: pytest.MonkeyPatch, destination_type: object
) -> None:
    """A non-string `destinationType` leaves the decoded destination unchanged."""
    raw_response = requests.Response()
    raw_response._content = json.dumps({  # noqa: SLF001
        "destinationType": destination_type,
        "configuration": {"host": "example.com"},
    }).encode()
    destination_response = models.DestinationResponse(
        configuration={"host": "example.com"},  # type: ignore[arg-type]
        created_at=1,
        definition_id="definition-id",
        destination_id="dest-id",
        destination_type="snowflake",
        name="dest",
        workspace_id="ws",
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: SimpleNamespace(
            destinations=SimpleNamespace(
                get_destination=lambda _request: api.GetDestinationResponse(
                    content_type="application/json",
                    status_code=200,
                    raw_response=raw_response,
                    destination_response=destination_response,
                )
            )
        ),
    )

    result = api_util.get_destination(
        "dest-id",
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    assert result is destination_response
    assert result.configuration == {"host": "example.com"}


@pytest.mark.parametrize(
    ("payment_status", "subscription_status", "expected"),
    [
        ("LOCKED", None, True),
        ("Disabled", "subscribed", True),
        ("okay", "UNSUBSCRIBED", True),
        ("OKAY", "Subscribed", False),
    ],
)
def test_is_account_locked_ignores_case(
    payment_status: str | None, subscription_status: str | None, expected: bool
) -> None:
    """Billing statuses lock the account whatever their casing."""
    assert api_util.is_account_locked(payment_status, subscription_status) is expected


def test_connection_info_keeps_null_status_as_none() -> None:
    """A null connection status is `None`, not the string `"None"` and not an error."""
    connection = _connection_response("Connection", 1)
    connection.schedule = None  # type: ignore[assignment]
    connection.status = None  # type: ignore[assignment]

    assert CloudConnectionInfo.from_api_response(connection).status is None


@pytest.mark.parametrize(
    "body", [{"error": "leaked-body"}, ["leaked-body"], {"access_token": 5}]
)
def test_get_bearer_token_rejects_malformed_body(
    monkeypatch: pytest.MonkeyPatch, body: object
) -> None:
    """A token response without a string token raises an error that omits the body."""
    monkeypatch.setattr(
        api_util.requests,
        "post",
        lambda **_: SimpleNamespace(status_code=200, json=lambda: body),
    )

    with pytest.raises(
        AirbyteError, match="did not include an access token"
    ) as exc_info:
        api_util.get_bearer_token(
            client_id=SecretString("id"), client_secret=SecretString("secret")
        )

    assert "leaked-body" not in str(exc_info.value)


def test_config_api_request_replaces_null_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit null timeout does not make a Config API request unbounded."""
    captured: dict[str, object] = {}

    def fake_request(**kwargs: object) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(status_code=200, json=lambda: {})

    monkeypatch.setattr(api_util.requests, "request", fake_request)

    api_util._make_config_api_request(
        path="/jobs/get",
        json={},
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
        timeout=None,
    )

    assert captured["timeout"] == (5.0, 120.0)


@pytest.mark.parametrize(
    ("url", "status_code", "raises"),
    [
        ("https://api.airbyte.com/v1/applications/token", 500, True),
        ("https://api.airbyte.com/v1/applications/token", 429, True),
        ("https://api.airbyte.com/v1/applications/token", 304, True),
        ("https://api.airbyte.com/v1/applications/token", 200, False),
        ("https://api.airbyte.com/v1/jobs", 500, False),
    ],
)
def test_public_api_client_raises_http_error_for_failed_token_request(
    monkeypatch: pytest.MonkeyPatch, url: str, status_code: int, raises: bool
) -> None:
    """A failed token request raises `requests.HTTPError`; other responses are returned."""
    response = requests.Response()
    response.status_code = status_code
    response.url = url
    monkeypatch.setattr(
        api_util.requests.Session, "send", lambda _self, _request, **_: response
    )
    session = api_util._TimeoutSession()  # noqa: SLF001
    request = requests.Request("POST", url).prepare()

    if raises:
        with pytest.raises(requests.HTTPError) as exc_info:
            session.send(request)
        assert exc_info.value.response.status_code == status_code
    else:
        assert session.send(request) is response


def test_get_bearer_token_is_bounded_by_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A token request made without a timeout still carries the default one."""
    captured: dict[str, object] = {}

    def fake_post(**kwargs: object) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(status_code=200, json=lambda: {"access_token": "t"})

    monkeypatch.setattr(api_util.requests, "post", fake_post)

    api_util.get_bearer_token(
        client_id=SecretString("id"), client_secret=SecretString("secret")
    )

    assert captured["timeout"] == (5.0, 120.0)


@pytest.mark.parametrize(
    "error",
    [
        requests.exceptions.JSONDecodeError("Expecting value", "leaked-body", 0),
        ValueError("Exceeds the limit (4300 digits) for integer string conversion"),
        RecursionError("maximum recursion depth exceeded"),
    ],
    ids=["not-json", "huge-integer", "deep-nesting"],
)
def test_config_api_request_rejects_undecodable_body(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    """A body that cannot be decoded raises the safe error without carrying the body."""
    response = SimpleNamespace(status_code=200, json=Mock(side_effect=error))
    monkeypatch.setattr(api_util.requests, "request", lambda **_: response)

    with pytest.raises(AirbyteError, match="Unexpected API response.") as exc_info:
        api_util._make_config_api_request(
            path="/jobs/get",
            json={},
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )

    assert exc_info.value.context == {"path": "/jobs/get", "status_code": 200}
    assert "leaked-body" not in str(exc_info.value)


@pytest.mark.parametrize(
    "error",
    [OverflowError("cannot convert float infinity to integer"), RecursionError()],
)
def test_get_source_wraps_decode_errors(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    """A response the generated client cannot decode raises the safe error."""
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: SimpleNamespace(
            sources=SimpleNamespace(get_source=Mock(side_effect=error))
        ),
    )

    with pytest.raises(AirbyteError, match="Unexpected API response."):
        api_util.get_source(
            "source-id",
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )


@pytest.mark.parametrize("body", [["leaked-body"], "leaked-body", None, 5, True])
def test_config_api_request_rejects_non_object_body(
    monkeypatch: pytest.MonkeyPatch, body: object
) -> None:
    """A decoded body that is not an object raises the safe error without carrying the body."""
    response = SimpleNamespace(status_code=200, json=lambda: body)
    monkeypatch.setattr(api_util.requests, "request", lambda **_: response)

    with pytest.raises(AirbyteError, match="Unexpected API response.") as exc_info:
        api_util._make_config_api_request(
            path="/workspaces/get_organization_info",
            json={},
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )

    assert exc_info.value.context == {
        "path": "/workspaces/get_organization_info",
        "status_code": 200,
    }
    assert "leaked-body" not in str(exc_info.value)


def test_config_api_request_error_omits_request_and_response_bodies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed Config API request raises an error without the request or response body."""
    response = requests.Response()
    response.status_code = 500
    response._content = b'{"internalMessage": "leaked-response"}'  # noqa: SLF001
    response.request = requests.Request(
        "POST", "https://api.airbyte.com/v1/jobs/get", data="leaked-request"
    ).prepare()
    monkeypatch.setattr(api_util.requests, "request", lambda **_: response)

    with pytest.raises(AirbyteError) as exc_info:
        api_util._make_config_api_request(
            path="/jobs/get",
            json={},
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )

    assert exc_info.value.context is not None
    assert exc_info.value.context["status_code"] == 500
    assert "leaked-response" not in str(exc_info.value)
    assert "leaked-request" not in str(exc_info.value)
