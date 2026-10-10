# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Cloud API utilities."""

from __future__ import annotations

import json
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
import requests
import responses
from airbyte import constants
from airbyte._util import api_util, cloud_errors, meta
from airbyte.cloud.models import (
    CloudConnectionInfo,
    CloudJobInfo,
    ConnectionStatus,
    JobStatusEnum,
)
from airbyte.exceptions import (
    AirbyteCloudError,
    AirbyteConnectorNotReadyError,
    AirbyteMissingResourceError,
    AirbyteWorkspaceNotEmptyError,
    AirbyteLibInputError,
)
from airbyte.registry import ConnectorType
from airbyte.secrets.base import SecretString
from airbyte_api import api, models, utils
from airbyte_api.errors import SDKError


@pytest.mark.parametrize("interval_hours", [1, 24, 168])
@pytest.mark.parametrize("use_client_credentials", [False, True])
def test_set_connection_interval_schedule_payload(
    monkeypatch: pytest.MonkeyPatch,
    interval_hours: int,
    *,
    use_client_credentials: bool,
) -> None:
    """Send only a basic schedule with the configured root and authentication."""
    token = Mock(return_value=SecretString("minted-token"))
    monkeypatch.setattr(api_util, "get_bearer_token", token)
    with responses.RequestsMock() as http:
        http.post(
            "https://config.example/custom/web_backend/connections/update",
            json={"connectionId": "connection-id", "scheduleType": "basic"},
        )
        result = api_util.set_connection_interval_schedule(
            connection_id="connection-id",
            interval_hours=interval_hours,
            api_root="https://public.example/custom",
            config_api_root="https://config.example/custom",
            client_id=SecretString("client-id") if use_client_credentials else None,
            client_secret=SecretString("client-secret")
            if use_client_credentials
            else None,
            bearer_token=None
            if use_client_credentials
            else SecretString("bearer-token"),
        )
        captured_calls = list(http.calls)

    assert result["scheduleType"] == "basic"
    assert len(captured_calls) == 1
    request = captured_calls[0].request
    assert json.loads(request.body) == {
        "connectionId": "connection-id",
        "scheduleType": "basic",
        "scheduleData": {
            "basicSchedule": {"timeUnit": "hours", "units": interval_hours}
        },
        "skipReset": True,
    }
    assert request.headers["Authorization"] == (
        "Bearer minted-token" if use_client_credentials else "Bearer bearer-token"
    )
    if use_client_credentials:
        token.assert_called_once_with(
            client_id=SecretString("client-id"),
            client_secret=SecretString("client-secret"),
            api_root="https://public.example/custom",
            timeout=None,
        )
    else:
        token.assert_not_called()


@pytest.mark.parametrize("interval_hours", [0, -1, True, False, 1.0, 1.5, "24", None])
def test_set_connection_interval_schedule_rejects_invalid_hours(
    interval_hours: object,
) -> None:
    """Reject invalid intervals before any network request or authentication."""
    with responses.RequestsMock() as http:
        with pytest.raises(AirbyteLibInputError, match="positive whole number"):
            api_util.set_connection_interval_schedule(
                connection_id="connection-id",
                interval_hours=interval_hours,
                api_root="https://api.airbyte.com/v1",
                client_id=None,
                client_secret=None,
                bearer_token=SecretString("token"),
            )
        assert len(http.calls) == 0


def test_set_connection_interval_schedule_reports_api_failure() -> None:
    """Propagate configuration API failures instead of reporting success."""
    with responses.RequestsMock() as http:
        http.post(
            "https://cloud.airbyte.com/api/v1/web_backend/connections/update",
            json={"message": "schedule rejected"},
            status=400,
        )
        with pytest.raises(AirbyteCloudError, match="HTTP 400"):
            api_util.set_connection_interval_schedule(
                connection_id="connection-id",
                interval_hours=24,
                api_root="https://api.airbyte.com/v1",
                client_id=None,
                client_secret=None,
                bearer_token=SecretString("token"),
            )


def _job_response(job_id: int) -> models.JobResponse:
    """Create a minimal job response for pagination tests."""
    return models.JobResponse(
        connection_id="connection-id",
        job_id=job_id,
        job_type=models.JobTypeEnum.SYNC,
        start_time="2026-01-01T00:00:00Z",
        status=models.JobStatusEnum.SUCCEEDED,
    )


def _raw_job_response(job_id: int, status: str) -> dict[str, Any]:
    return {
        "connectionId": "connection-id",
        "jobId": job_id,
        "jobType": "sync",
        "startTime": "2026-01-01T00:00:00Z",
        "status": status,
    }


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


def _connection_with_field_filtering_mapper(
    connection_id: str,
) -> dict[str, Any]:
    return {
        "connectionId": connection_id,
        "name": "test connection",
        "sourceId": "source-id",
        "destinationId": "destination-id",
        "workspaceId": "workspace-id",
        "status": "active",
        "schedule": {"scheduleType": "manual"},
        "dataResidency": "auto",
        "nonBreakingSchemaUpdatesBehavior": "ignore",
        "namespaceDefinition": "destination",
        "createdAt": 1,
        "tags": [],
        "configurations": {
            "streams": [
                {
                    "name": "leads",
                    "syncMode": "full_refresh_overwrite",
                    "mappers": [
                        {
                            "id": "00000000-0000-0000-0000-000000000000",
                            "type": "field-filtering",
                            "mapperConfiguration": {"targetField": "foo"},
                        }
                    ],
                }
            ]
        },
    }


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
    ("status_code", "expected_error_type", "expected_message", "expected_guidance"),
    [
        pytest.param(
            403,
            AirbyteMissingResourceError,
            "The requested resource was not found, or these credentials can't access it "
            "(HTTP 403).",
            api_util.FORBIDDEN_RESOURCE_GUIDANCE,
            id="forbidden",
        ),
        pytest.param(
            404,
            AirbyteMissingResourceError,
            "The resource was not found. (HTTP 404)",
            "Check the ID; list the resources to find the right one.",
            id="not_found",
        ),
        pytest.param(
            500,
            AirbyteCloudError,
            "Airbyte Cloud hit an unexpected error. This is often caused by an "
            "invalid argument value. (HTTP 500)",
            "Check the arguments (ranges, IDs, formats). If they look right, "
            "don't retry more than once; tell the user.",
            id="server_error",
        ),
    ],
)
def test_wrap_sdk_error_classifies_missing_or_forbidden(
    status_code: int,
    expected_error_type: type[AirbyteCloudError],
    expected_message: str,
    expected_guidance: str | None,
) -> None:
    raw_response = requests.Response()
    raw_response.status_code = status_code
    raw_response.url = "https://api.airbyte.com/v1/workspaces/workspace-id"
    error = SDKError(
        "Workspace lookup failed.", status_code, "response body", raw_response
    )

    wrapped = api_util._wrap_sdk_error(error, {"workspace_id": "workspace-id"})

    assert type(wrapped) is expected_error_type
    assert wrapped.get_message() == expected_message
    assert wrapped.guidance == expected_guidance
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
    with pytest.raises(AirbyteLibInputError, match=expected_message):
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

    with pytest.raises(AirbyteCloudError, match=expected_message) as exc_info:
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


@pytest.mark.parametrize(
    (
        "page_lengths",
        "name_contains",
        "limit",
        "expected_count",
        "expected_offsets",
    ),
    [
        pytest.param(
            [100, 3],
            "sandbox",
            None,
            103,
            [0, 100],
            id="short-last-page",
        ),
        pytest.param(
            [100, 100],
            None,
            50,
            50,
            [0],
            id="returns-at-limit",
        ),
        pytest.param(
            [0],
            None,
            None,
            0,
            [0],
            id="empty-first-page",
        ),
    ],
)
def test_list_workspaces_by_user_paginates_and_respects_limit(
    monkeypatch: pytest.MonkeyPatch,
    page_lengths: list[int],
    name_contains: str | None,
    limit: int | None,
    expected_count: int,
    expected_offsets: list[int],
) -> None:
    captured_requests: list[dict[str, object]] = []
    pages: list[dict[str, object]] = []
    next_workspace_id = 0
    for page_length in page_lengths:
        pages.append({
            "workspaces": [
                {"workspaceId": f"workspace-{workspace_id}"}
                for workspace_id in range(
                    next_workspace_id,
                    next_workspace_id + page_length,
                )
            ]
        })
        next_workspace_id += page_length

    def fake_config_request(**kwargs: object) -> dict[str, object]:
        json_request = kwargs["json"]
        assert isinstance(json_request, dict)
        captured_requests.append({"path": kwargs["path"], "json": json_request})
        return pages.pop(0)

    monkeypatch.setattr(api_util, "_make_config_api_request", fake_config_request)

    result = api_util.list_workspaces_by_user(
        "user-id",
        api_root="https://api.airbyte.com/v1/",
        client_id=SecretString("client-id"),
        client_secret=SecretString("client-secret"),
        bearer_token=None,
        config_api_root="https://config.airbyte.com",
        name_contains=name_contains,
        limit=limit,
    )

    assert [workspace["workspaceId"] for workspace in result] == [
        f"workspace-{workspace_id}" for workspace_id in range(expected_count)
    ]
    assert [request["path"] for request in captured_requests] == [
        "/workspaces/list_by_user_id"
    ] * len(expected_offsets)
    assert [request["json"] for request in captured_requests] == [
        {
            "userId": "user-id",
            "pagination": {"pageSize": 100, "rowOffset": offset},
            **({"nameContains": name_contains} if name_contains is not None else {}),
        }
        for offset in expected_offsets
    ]


@pytest.mark.parametrize(
    ("page_names", "expected_names", "expected_offsets"),
    [
        pytest.param(
            [["target", *(f"miss-{index}" for index in range(99))]],
            ["target"],
            [0],
            id="match-on-first-full-page-stops-at-limit",
        ),
        pytest.param(
            [
                [f"miss-{index}" for index in range(100)],
                ["target"],
            ],
            ["target"],
            [0, 100],
            id="match-after-first-full-page",
        ),
    ],
)
def test_list_workspaces_by_user_filters_each_page_before_limit(
    monkeypatch: pytest.MonkeyPatch,
    page_names: list[list[str]],
    expected_names: list[str],
    expected_offsets: list[int],
) -> None:
    captured_requests: list[dict[str, object]] = []
    pages = [
        {
            "workspaces": [
                {"workspaceId": f"workspace-{index}", "name": name}
                for index, name in enumerate(names)
            ]
        }
        for names in page_names
    ]

    def fake_config_request(**kwargs: object) -> dict[str, object]:
        json_request = kwargs["json"]
        assert isinstance(json_request, dict)
        captured_requests.append({"path": kwargs["path"], "json": json_request})
        return pages.pop(0)

    monkeypatch.setattr(api_util, "_make_config_api_request", fake_config_request)

    result = api_util.list_workspaces_by_user(
        "user-id",
        api_root="https://api.airbyte.com/v1/",
        client_id=SecretString("client-id"),
        client_secret=SecretString("client-secret"),
        bearer_token=None,
        config_api_root="https://config.airbyte.com",
        name_filter=lambda workspace_name: workspace_name == "target",
        limit=1,
    )

    assert [workspace["name"] for workspace in result] == expected_names
    assert [
        request["json"]["pagination"]["rowOffset"] for request in captured_requests
    ] == (expected_offsets)
    assert [request["path"] for request in captured_requests] == [
        "/workspaces/list_by_user_id"
    ] * len(expected_offsets)


def test_list_workspaces_by_user_honors_page_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_payloads: list[dict[str, object]] = []

    def fake_config_request(**kwargs: object) -> dict[str, object]:
        json_request = kwargs["json"]
        assert isinstance(json_request, dict)
        captured_payloads.append(json_request)
        return {
            "workspaces": [
                {"workspaceId": "workspace-0"},
                {"workspaceId": "workspace-1"},
            ]
        }

    monkeypatch.setattr(api_util, "_make_config_api_request", fake_config_request)

    result = api_util.list_workspaces_by_user(
        "user-id",
        api_root="https://api.airbyte.com/v1/",
        client_id=SecretString("client-id"),
        client_secret=SecretString("client-secret"),
        bearer_token=None,
        config_api_root="https://config.airbyte.com",
        limit=2,
        page_size=2,
    )

    assert len(result) == 2
    assert captured_payloads == [
        {"userId": "user-id", "pagination": {"pageSize": 2, "rowOffset": 0}}
    ]


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

    with pytest.raises(AirbyteLibInputError, match="`status` must be one of"):
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
        with pytest.raises(AirbyteLibInputError):
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


@responses.activate
def test_get_connection_retries_without_undecodable_stream_mappers() -> None:
    """A mapper the SDK cannot decode is omitted from the returned connection."""
    api_root = "https://api.airbyte.test/api/public/v1"
    connection_id = "connection-id"
    url = f"{api_root}/connections/{connection_id}"
    responses.get(url, json=_connection_with_field_filtering_mapper(connection_id))

    connection = api_util.get_connection(
        workspace_id="workspace-id",
        connection_id=connection_id,
        api_root=api_root,
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("bearer-token"),
    )

    assert isinstance(connection, models.ConnectionResponse)
    assert connection.configurations.streams[0].name == "leads"
    assert len(responses.calls) == 2
    assert [call.request.url for call in responses.calls] == [url, url]
    assert (
        responses.calls[1].request.req_kwargs["timeout"]
        == api_util.PUBLIC_API_FALLBACK_TIMEOUT_SECS
    )


@responses.activate
def test_get_connection_fallback_404_raises_missing_resource_error() -> None:
    """A missing resource during raw fallback raises the missing-resource error."""
    api_root = "https://api.airbyte.test/api/public/v1"
    connection_id = "connection-id"
    url = f"{api_root}/connections/{connection_id}"
    responses.get(url, json=_connection_with_field_filtering_mapper(connection_id))
    responses.get(url, status=404, json={"message": "Not found"})

    with pytest.raises(AirbyteMissingResourceError) as exc_info:
        api_util.get_connection(
            workspace_id="workspace-id",
            connection_id=connection_id,
            api_root=api_root,
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("bearer-token"),
        )

    assert len(responses.calls) == 2
    assert exc_info.value.context["status_code"] == 404
    assert exc_info.value.guidance == (
        "Check the ID; list the resources to find the right one."
    )


@responses.activate
@pytest.mark.parametrize(
    ("lookup", "definitions_path"),
    [
        pytest.param(api_util.get_source_definition, "sources", id="source"),
        pytest.param(
            api_util.get_destination_definition, "destinations", id="destination"
        ),
    ],
)
def test_get_definition_wraps_sdk_errors(
    lookup: Callable[..., models.DefinitionResponse], definitions_path: str
) -> None:
    """A missing definition raises the missing-resource error, chained to the SDK error."""
    api_root = "https://api.airbyte.test/api/public/v1"
    url = f"{api_root}/workspaces/workspace-id/definitions/{definitions_path}/definition-id"
    responses.get(url, status=404, json={"message": "Not found"})

    with pytest.raises(AirbyteMissingResourceError) as exc_info:
        lookup(
            definition_id="definition-id",
            workspace_id="workspace-id",
            api_root=api_root,
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("bearer-token"),
        )

    assert isinstance(exc_info.value.__cause__, SDKError)
    assert exc_info.value.context["status_code"] == 404
    assert exc_info.value.context["definition_id"] == "definition-id"


@responses.activate
def test_list_connections_retries_page_without_undecodable_stream_mappers() -> None:
    """Connection listing falls back to raw JSON when a page has unknown mappers."""
    api_root = "https://api.airbyte.test/api/public/v1"
    responses.get(
        f"{api_root}/connections",
        json={
            "data": [_connection_with_field_filtering_mapper("connection-id")],
            "next": None,
        },
    )

    connections = api_util.list_connections(
        workspace_id="workspace-id",
        api_root=api_root,
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("bearer-token"),
    )

    assert [
        connection.configurations.streams[0].name for connection in connections
    ] == ["leads"]
    assert len(responses.calls) == 2


@responses.activate
def test_get_connection_without_mappers_uses_sdk_response_once() -> None:
    """Connections without mappers keep using the typed SDK response."""
    api_root = "https://api.airbyte.test/api/public/v1"
    connection_id = "connection-id"
    url = f"{api_root}/connections/{connection_id}"
    body = _connection_with_field_filtering_mapper(connection_id)
    body["configurations"]["streams"][0].pop("mappers")
    responses.get(url, json=body)

    connection = api_util.get_connection(
        workspace_id="workspace-id",
        connection_id=connection_id,
        api_root=api_root,
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("bearer-token"),
    )

    assert connection.configurations.streams[0].name == "leads"
    assert len(responses.calls) == 1


@responses.activate
def test_get_job_info_preserves_unknown_status() -> None:
    """Job lookup retries when the SDK cannot decode a status value."""
    api_root = "https://api.airbyte.test/api/public/v1"
    job_id = 42
    url = f"{api_root}/jobs/{job_id}"
    responses.get(url, json=_raw_job_response(job_id, "future-status"))

    job = api_util.get_job_info(
        job_id=job_id,
        api_root=api_root,
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("bearer-token"),
    )

    assert job.status == "future-status"
    assert len(responses.calls) == 2


@responses.activate
def test_get_job_logs_preserves_unknown_statuses_and_stops_without_next_page() -> None:
    """Job listing retries the page and stops when the raw response has no next page."""
    api_root = "https://api.airbyte.test/api/public/v1"
    responses.get(
        f"{api_root}/jobs",
        json={
            "data": [
                _raw_job_response(42, "future-status"),
                _raw_job_response(43, "succeeded"),
            ]
        },
    )

    jobs = api_util.get_job_logs(
        workspace_id="workspace-id",
        connection_id="connection-id",
        limit=2,
        api_root=api_root,
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("bearer-token"),
    )

    assert len(jobs) == 2
    assert jobs[0].status == "future-status"
    assert jobs[1].status is models.JobStatusEnum.SUCCEEDED
    assert len(responses.calls) == 2


@responses.activate
def test_get_connection_preserves_unknown_status() -> None:
    """Connection lookup retries when the SDK cannot decode a status value."""
    api_root = "https://api.airbyte.test/api/public/v1"
    connection_id = "connection-id"
    url = f"{api_root}/connections/{connection_id}"
    raw = _connection_with_field_filtering_mapper(connection_id)
    raw["configurations"]["streams"][0].pop("mappers")
    raw["status"] = "locked"
    responses.get(url, json=raw)

    connection = api_util.get_connection(
        workspace_id="workspace-id",
        connection_id=connection_id,
        api_root=api_root,
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("bearer-token"),
    )

    assert connection.status == "locked"
    assert (
        CloudConnectionInfo.from_api_response(connection).status
        is ConnectionStatus.LOCKED
    )
    assert len(responses.calls) == 2


def test_decode_with_unknown_status_keeps_known_enum() -> None:
    """Known SDK status values are decoded to their enum members."""
    job = api_util._decode_with_unknown_status(
        _raw_job_response(42, "succeeded"),
        models.JobResponse,
        status_enum=models.JobStatusEnum,
        placeholder=models.JobStatusEnum.RUNNING,
    )

    assert job.status is models.JobStatusEnum.SUCCEEDED


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
    with pytest.raises(AirbyteLibInputError, match="`limit` must be greater than 0."):
        api_util.list_connections(
            workspace_id="workspace-id",
            api_root="https://api.airbyte.com/v1/",
            client_id=SecretString("client-id"),
            client_secret=SecretString("client-secret"),
            bearer_token=None,
            limit=limit,
        )


def test_sdk_decodes_queued_job_status() -> None:
    payload = {
        "data": [
            {
                "connectionId": "connection-id",
                "jobId": 1,
                "jobType": "sync",
                "startTime": "2026-01-01T00:00:00Z",
                "status": "queued",
            }
        ]
    }

    response = utils.unmarshal_json(json.dumps(payload), models.JobsResponse)
    job = response.data[0]

    assert job.status is models.JobStatusEnum("queued")
    assert CloudJobInfo.from_api_response(job).status is JobStatusEnum.QUEUED


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

    with pytest.raises(AirbyteCloudError) as error:
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
            {"x-airbyte-application-name": "io.airbyte.coral-support-agent"},
            "coral-support-agent",
            id="mapped-application-name",
        ),
        pytest.param(
            True,
            True,
            {"x-airbyte-application-name": "  IO.Airbyte.Coral-Support-Agent "},
            "coral-support-agent",
            id="normalized-mapped-application-name",
        ),
        pytest.param(
            True,
            True,
            {"x-airbyte-application-name": "my-agent"},
            "pyairbyte-mcp-hosted",
            id="unmapped-application-name",
        ),
        pytest.param(True, True, {}, "pyairbyte-mcp-hosted", id="no_header_falls_back"),
        pytest.param(
            True,
            True,
            {"x-airbyte-analytic-source": "coral-support-agent"},
            "pyairbyte-mcp-hosted",
            id="analytic-source-header-ignored",
        ),
        pytest.param(
            False,
            False,
            {"x-airbyte-application-name": "io.airbyte.coral-support-agent"},
            "pyairbyte",
            id="application-name-ignored-outside-mcp-mode",
        ),
        pytest.param(
            True,
            False,
            {"x-airbyte-analytic-source": "coral-support-agent"},
            "pyairbyte-mcp-local",
            id="analytic-source-header-ignored-in-local-mode",
        ),
    ],
)
def test_get_analytic_source_uses_application_name_mapping(
    monkeypatch: pytest.MonkeyPatch,
    mcp_mode: bool,
    hosted_mcp_mode: bool,
    headers: dict[str, str],
    expected: str,
) -> None:
    monkeypatch.setattr(meta, "_MCP_MODE_ENABLED", mcp_mode)
    monkeypatch.setattr(constants, "_HOSTED_MCP_MODE_ENABLED", hosted_mcp_mode)
    monkeypatch.setattr(meta, "get_http_headers", lambda: headers)

    assert meta.get_cloud_api_analytic_source() == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("My Agent!", "my-agent"),
        ("IO.AIRBYTE.X", "io.airbyte.x"),
        ("x" * 200, "x" * 128),
        ("   ", None),
        ("!!!", None),
        (None, None),
    ],
)
def test_normalize_application_name(
    value: str | None,
    expected: str | None,
) -> None:
    assert meta.normalize_application_name(value) == expected


@pytest.mark.parametrize(
    ("mcp_mode", "headers", "expected_declared_name", "expected_application_name"),
    [
        pytest.param(
            True,
            {"x-airbyte-application-name": "com.example.my-agent"},
            "com.example.my-agent",
            "com.example.my-agent",
            id="mcp-header",
        ),
        pytest.param(
            True,
            {"x-airbyte-analytic-source": "coral-support-agent"},
            None,
            "local-script",
            id="analytic-source-header-ignored",
        ),
        pytest.param(
            True,
            {"x-airbyte-application-name": "My Agent!"},
            "my-agent",
            "my-agent",
            id="normalized-application-name",
        ),
        pytest.param(
            True,
            {"x-airbyte-application-name": "!!!"},
            None,
            "local-script",
            id="empty-normalized-name",
        ),
        pytest.param(True, {}, None, "local-script", id="mcp-no-header"),
        pytest.param(
            False,
            {"x-airbyte-application-name": "com.example.my-agent"},
            None,
            "local-script",
            id="non-mcp-header-ignored",
        ),
    ],
)
def test_get_declared_application_name_and_get_application_name(
    monkeypatch: pytest.MonkeyPatch,
    mcp_mode: bool,
    headers: dict[str, str],
    expected_declared_name: str | None,
    expected_application_name: str,
) -> None:
    monkeypatch.setattr(meta, "_MCP_MODE_ENABLED", mcp_mode)
    monkeypatch.setattr(meta, "get_http_headers", lambda: headers)
    monkeypatch.setattr(meta, "_get_local_application_name", lambda: "local-script")

    assert meta.get_declared_application_name() == expected_declared_name
    assert meta.get_application_name() == expected_application_name


def test_get_application_name_resolves_current_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    headers: dict[str, str] = {}
    monkeypatch.setattr(meta, "_MCP_MODE_ENABLED", True)
    monkeypatch.setattr(meta, "get_http_headers", lambda: headers)
    monkeypatch.setattr(meta, "_get_local_application_name", lambda: "local-script")

    assert meta.get_application_name() == "local-script"
    headers["x-airbyte-analytic-source"] = "coral-support-agent"
    assert meta.get_application_name() == "local-script"
    headers["x-airbyte-application-name"] = "!!!"
    assert meta.get_application_name() == "local-script"
    headers["x-airbyte-application-name"] = "My Agent!"
    assert meta.get_application_name() == "my-agent"


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
    assert headers[meta.AIRBYTE_CLOUD_ANALYTIC_SOURCE_HEADER] == "pyairbyte-mcp-hosted"


def test_config_api_request_handles_no_content_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = requests.Response()
    response.status_code = 204
    response.url = "https://config.airbyte.com/v1/connector_builder_projects/update"
    response.request = requests.Request("POST", response.url).prepare()
    monkeypatch.setattr(api_util.requests, "request", Mock(return_value=response))

    result = api_util._make_config_api_request(
        path="/connector_builder_projects/update",
        json={},
        api_root="https://api.airbyte.com/v1",
        config_api_root="https://config.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    assert result == {}


def test_list_connector_builder_projects(monkeypatch: pytest.MonkeyPatch) -> None:
    projects = [
        {
            "builderProjectId": "builder-project-id",
            "sourceDefinitionId": "definition-id",
        }
    ]
    config_api_request = Mock(return_value={"projects": projects})
    monkeypatch.setattr(api_util, "_make_config_api_request", config_api_request)

    result = api_util.list_connector_builder_projects(
        "workspace-id",
        api_root="https://api.airbyte.com/v1",
        config_api_root="https://config.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=None,
    )

    assert result == projects
    config_api_request.assert_called_once_with(
        path="/connector_builder_projects/list",
        json={"workspaceId": "workspace-id"},
        api_root="https://api.airbyte.com/v1",
        config_api_root="https://config.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=None,
    )


@pytest.mark.parametrize(
    ("draft_manifest", "components_file_content", "expected_builder_project"),
    [
        pytest.param(
            {"version": "0.1.0"},
            "components: []",
            {
                "name": "Renamed source",
                "draftManifest": {"version": "0.1.0"},
                "componentsFileContent": "components: []",
            },
            id="preserve-draft-and-components",
        ),
        pytest.param(
            None,
            None,
            {"name": "Renamed source"},
            id="omit-empty-draft-and-components",
        ),
    ],
)
def test_update_connector_builder_project_payload(
    monkeypatch: pytest.MonkeyPatch,
    draft_manifest: dict[str, object] | None,
    components_file_content: str | None,
    expected_builder_project: dict[str, object],
) -> None:
    config_api_request = Mock(return_value={})
    monkeypatch.setattr(api_util, "_make_config_api_request", config_api_request)

    result = api_util.update_connector_builder_project(
        workspace_id="owner-workspace",
        builder_project_id="builder-project-id",
        name="Renamed source",
        draft_manifest=draft_manifest,
        components_file_content=components_file_content,
        api_root="https://api.airbyte.com/v1",
        config_api_root="https://config.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    assert result is None
    config_api_request.assert_called_once_with(
        path="/connector_builder_projects/update",
        json={
            "workspaceId": "owner-workspace",
            "builderProjectId": "builder-project-id",
            "builderProject": expected_builder_project,
        },
        api_root="https://api.airbyte.com/v1",
        config_api_root="https://config.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )


@pytest.mark.parametrize(
    ("status_code", "expected_error_type", "expected_message", "expected_guidance"),
    [
        pytest.param(
            403,
            AirbyteMissingResourceError,
            "The requested resource was not found, or these credentials can't access it "
            "(HTTP 403).",
            api_util.FORBIDDEN_RESOURCE_GUIDANCE,
            id="forbidden",
        ),
        pytest.param(
            500,
            AirbyteCloudError,
            "Airbyte Cloud hit an unexpected error. This is often caused by an "
            "invalid argument value. (HTTP 500)",
            "Check the arguments (ranges, IDs, formats). If they look right, "
            "don't retry more than once; tell the user.",
            id="server-error",
        ),
    ],
)
def test_config_api_request_maps_forbidden_as_missing_resource(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    expected_error_type: type[AirbyteCloudError],
    expected_message: str,
    expected_guidance: str | None,
) -> None:
    response = requests.Response()
    response.status_code = status_code
    response.url = "https://config.airbyte.com/v1/workspaces/get"
    response.request = requests.Request("POST", response.url).prepare()
    request = Mock(return_value=response)
    monkeypatch.setattr(api_util.requests, "request", request)

    with pytest.raises(expected_error_type) as exc_info:
        api_util._make_config_api_request(
            path="/workspaces/get",
            json={"workspaceId": "workspace-id"},
            api_root="https://api.airbyte.com/v1",
            config_api_root="https://config.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )

    error = exc_info.value
    assert type(error) is expected_error_type
    assert error.get_message() == expected_message
    assert error.guidance == expected_guidance
    assert error.context["status_code"] == status_code
    assert error.context["path"] == "/workspaces/get"
    assert error.context["full_url"] == "https://config.airbyte.com/v1/workspaces/get"
    assert error.context["config_api_root"] == "https://config.airbyte.com/v1"
    assert error.context["url"] == response.request.url
    assert "body" not in error.context
    assert "response" not in error.context
    assert error.context["problem_type"] is None
    assert isinstance(error.__cause__, requests.HTTPError)
    assert error.__cause__.response is response
    assert (
        request.call_args.kwargs["url"]
        == "https://config.airbyte.com/v1/workspaces/get"
    )


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
    assert (
        session.headers[meta.AIRBYTE_CLOUD_ANALYTIC_SOURCE_HEADER]
        == "pyairbyte-mcp-local"
    )


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
    assert headers[meta.AIRBYTE_CLOUD_ANALYTIC_SOURCE_HEADER] == "pyairbyte"


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
    ("operation", "status_code"),
    [
        pytest.param("get-job", 403, id="get-job"),
        pytest.param("patch-connection", 400, id="patch-connection"),
    ],
)
def test_api_util_calls_wrap_sdk_errors_with_status_context(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    status_code: int,
) -> None:
    """SDK errors from job and connection calls retain status and request context."""
    sdk_error = _sdk_status_error(status_code)
    get_job = Mock(side_effect=sdk_error)
    patch_connection = Mock(side_effect=sdk_error)
    airbyte_instance = SimpleNamespace(
        jobs=SimpleNamespace(get_job=get_job),
        connections=SimpleNamespace(patch_connection=patch_connection),
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    with pytest.raises(AirbyteCloudError) as exc_info:
        if operation == "get-job":
            api_util.get_job_info(
                job_id=42,
                api_root="https://api.airbyte.com/v1",
                client_id=None,
                client_secret=None,
                bearer_token=None,
            )
        else:
            api_util.patch_connection(
                connection_id="connection-1",
                api_root="https://api.airbyte.com/v1",
                client_id=None,
                client_secret=None,
                bearer_token=SecretString("token"),
            )

    assert exc_info.value.context is not None
    assert exc_info.value.context["status_code"] == status_code
    assert exc_info.value.__cause__ is sdk_error
    if operation == "get-job":
        assert isinstance(exc_info.value, AirbyteMissingResourceError)
        assert exc_info.value.context["job_id"] == 42
        get_job.assert_called_once_with(api.GetJobRequest(job_id=42))
        patch_connection.assert_not_called()
    else:
        assert not isinstance(exc_info.value, AirbyteMissingResourceError)
        assert exc_info.value.context["connection_id"] == "connection-1"
        patch_connection.assert_called_once()
        get_job.assert_not_called()


def _call_create_connection() -> None:
    """Invoke `create_connection` with canned IDs for SDKError tests."""
    api_util.create_connection(
        "connection-name",
        source_id="source-id",
        destination_id="destination-id",
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
        prefix="",
        selected_stream_names=["no_such_stream"],
    )


def test_create_connection_400_raises_input_error_without_api_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 400 raises `AirbyteLibInputError`; `data.message` is never relayed."""
    api_message = (
        "No streams found with name [no_such_stream] and namespace [null]. "
        "Please call the https://reference.airbyte.com/reference/getstreamproperties "
        "endpoint to see the valid stream name and namespace combinations."
    )
    body = json.dumps({
        "status": 400,
        "type": "https://reference.airbyte.com/reference/errors#bad-request",
        "title": "bad-request",
        "detail": "The request could not be understood by the server "
        "due to malformed syntax.",
        "documentationUrl": None,
        "data": {"message": api_message},
    })
    raw_response = requests.Response()
    raw_response.status_code = 400
    raw_response.url = "https://api.airbyte.com/v1/connections"
    sdk_error = SDKError("API error occurred: Status 400", 400, body, raw_response)
    create_connection = Mock(side_effect=sdk_error)
    airbyte_instance = SimpleNamespace(
        connections=SimpleNamespace(create_connection=create_connection),
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    with pytest.raises(AirbyteLibInputError) as exc_info:
        _call_create_connection()

    assert exc_info.value.get_message() == (
        "Airbyte Cloud rejected the request as invalid. "
        "(Cloud error: bad-request, HTTP 400)"
    )
    assert api_message not in str(exc_info.value)
    assert exc_info.value.context["source_id"] == "source-id"
    assert exc_info.value.context["destination_id"] == "destination-id"
    assert exc_info.value.context["status_code"] == 400
    assert exc_info.value.__cause__ is sdk_error


def test_create_connection_400_with_non_json_body_uses_fallback_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 400 with an unparseable body falls back to a generic input error."""
    raw_response = requests.Response()
    raw_response.status_code = 400
    raw_response.url = "https://api.airbyte.com/v1/connections"
    sdk_error = SDKError(
        "API error occurred: Status 400",
        400,
        "<html>Bad Request</html>",
        raw_response,
    )
    airbyte_instance = SimpleNamespace(
        connections=SimpleNamespace(
            create_connection=Mock(side_effect=sdk_error),
        ),
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    with pytest.raises(AirbyteLibInputError) as exc_info:
        _call_create_connection()

    assert exc_info.value.get_message() == (
        "Airbyte Cloud rejected the request as invalid. (HTTP 400)"
    )
    assert exc_info.value.__cause__ is sdk_error


@pytest.mark.parametrize(
    ("body", "mcp_mode", "expected_error", "guidance_fragment"),
    [
        pytest.param(
            json.dumps({"title": "actor-not-ready"}),
            True,
            AirbyteConnectorNotReadyError,
            "`check_cloud_connector`",
            id="actor-not-ready-mcp",
        ),
        pytest.param(
            json.dumps({"title": "actor-not-ready"}),
            False,
            AirbyteConnectorNotReadyError,
            "`connector.check()`",
            id="actor-not-ready-python",
        ),
        pytest.param(
            json.dumps({
                "type": "https://reference.airbyte.com/reference/errors#409-actor-not-ready"
            }),
            True,
            AirbyteConnectorNotReadyError,
            "`check_cloud_connector`",
            id="actor-not-ready-by-type",
        ),
        pytest.param(
            json.dumps({
                "title": "locked",
                "type": "https://reference.airbyte.com/reference/errors#connection/locked",
            }),
            False,
            AirbyteCloudError,
            None,
            id="different-problem",
        ),
        pytest.param(
            "<html>Conflict</html>",
            False,
            AirbyteCloudError,
            None,
            id="non-json-body",
        ),
    ],
)
def test_create_connection_409_actor_not_ready_error_mapping(
    monkeypatch: pytest.MonkeyPatch,
    body: str,
    mcp_mode: bool,
    expected_error: type[AirbyteCloudError],
    guidance_fragment: str | None,
) -> None:
    """Map only actor-not-ready conflicts to the actionable draft-connector error."""
    monkeypatch.setattr(api_util, "is_mcp_mode", lambda: mcp_mode)
    raw_response = requests.Response()
    raw_response.status_code = 409
    raw_response.url = "https://api.airbyte.com/v1/connections"
    sdk_error = SDKError("API error occurred: Status 409", 409, body, raw_response)
    airbyte_instance = SimpleNamespace(
        connections=SimpleNamespace(
            create_connection=Mock(side_effect=sdk_error),
        ),
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    with pytest.raises(expected_error) as exc_info:
        _call_create_connection()

    assert type(exc_info.value) is expected_error
    assert exc_info.value.context is not None
    assert exc_info.value.context["status_code"] == 409
    assert exc_info.value.__cause__ is sdk_error
    if guidance_fragment:
        assert guidance_fragment in (exc_info.value.guidance or "")


@pytest.mark.parametrize(
    ("status_code", "expect_missing_resource"),
    [
        pytest.param(403, True, id="403-missing-resource"),
        pytest.param(500, False, id="500-cloud-error"),
    ],
)
def test_create_connection_non_400_sdk_errors_keep_status_context(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    expect_missing_resource: bool,
) -> None:
    """Non-400 SDK errors go through `_wrap_sdk_error` with the create context."""
    sdk_error = _sdk_status_error(status_code)
    airbyte_instance = SimpleNamespace(
        connections=SimpleNamespace(
            create_connection=Mock(side_effect=sdk_error),
        ),
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        lambda **_: airbyte_instance,
    )

    with pytest.raises(AirbyteCloudError) as exc_info:
        _call_create_connection()

    assert isinstance(exc_info.value, AirbyteMissingResourceError) == (
        expect_missing_resource
    )
    assert exc_info.value.context["status_code"] == status_code
    assert exc_info.value.context["source_id"] == "source-id"
    assert exc_info.value.context["destination_id"] == "destination-id"
    assert exc_info.value.__cause__ is sdk_error


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

    with pytest.raises(AirbyteCloudError) as exc_info:
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

    with pytest.raises(AirbyteCloudError) as exc_info:
        api_util.get_source(
            "source-id",
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )

    assert type(exc_info.value) is AirbyteCloudError


_INTERNAL_MARKER = "INTERNAL_MARKER select x from y"


def _sdk_error(status_code: int, body: str | None) -> SDKError:
    """Create an SDKError like the Speakeasy SDK raises on a non-2xx status."""
    raw_response = requests.Response()
    raw_response.status_code = status_code
    raw_response.url = "https://api.airbyte.com/v1/resources/resource-id"
    return SDKError(
        f"API error occurred: Status {status_code}", status_code, body, raw_response
    )


def _problem_body(
    problem_type: str | None = None,
    title: str | None = None,
    data: dict[str, Any] | None = None,
    **extra: Any,
) -> str:
    """Build a Cloud problem body like the public API returns."""
    return json.dumps({
        "type": problem_type,
        "title": title,
        "detail": _INTERNAL_MARKER,
        "data": data,
        **extra,
    })


@pytest.mark.parametrize(
    ("status_code", "body", "expected_message", "expected_guidance", "problem_type"),
    [
        pytest.param(
            400,
            _problem_body(
                "https://reference.airbyte.com/reference/errors#bad-request",
                data={"message": _INTERNAL_MARKER},
            ),
            "Airbyte Cloud rejected the request as invalid. "
            "(Cloud error: bad-request, HTTP 400)",
            "Check the arguments against the tool description.",
            "bad-request",
            id="bad-request",
        ),
        pytest.param(
            404,
            _problem_body(
                "https://reference.airbyte.com/reference/errors#resource-not-found",
                data={"resourceType": "source", "resourceId": _INTERNAL_MARKER},
            ),
            "The source was not found. (Cloud error: resource-not-found, HTTP 404)",
            "Check the ID; list the resources to find the right one.",
            "resource-not-found",
            id="resource-not-found-with-type",
        ),
        pytest.param(
            401,
            _problem_body(
                "https://reference.airbyte.com/reference/errors#invalid-api-key"
            ),
            "Airbyte Cloud rejected the credentials. "
            "(Cloud error: invalid-api-key, HTTP 401)",
            "Ask the user to check the client ID/secret or token; don't retry.",
            "invalid-api-key",
            id="invalid-api-key",
        ),
        pytest.param(
            400,
            _problem_body("error:cron-validation/invalid-expression"),
            "The connection schedule is invalid. "
            "(Cloud error: cron-validation/invalid-expression, HTTP 400)",
            "Fix the schedule (cron expression, timezone, or frequency) and retry.",
            "invalid-expression",
            id="schedule-invalid",
        ),
        pytest.param(
            400,
            _problem_body("error:mapper-validation/secret-not-found"),
            "The mapper configuration is invalid. "
            "(Cloud error: mapper-validation/secret-not-found, HTTP 400)",
            "Fix the mapper configuration and retry. "
            "If a mapper secret is missing, resend the secret values.",
            "secret-not-found",
            id="mapper-secret-not-found",
        ),
        pytest.param(
            409,
            _problem_body("error:tag-already-exists"),
            "A resource with this name or membership already exists. "
            "(Cloud error: tag-already-exists, HTTP 409)",
            "Use the existing one or pick another name; don't retry as is.",
            "error:tag-already-exists",
            id="already-exists",
        ),
        pytest.param(
            503,
            _problem_body(
                "https://reference.airbyte.com/reference/errors",
                title="service-unavailable",
            ),
            "Airbyte Cloud is temporarily unavailable. "
            "(Cloud error: service-unavailable, HTTP 503)",
            "Wait and retry once.",
            "service-unavailable",
            id="service-unavailable",
        ),
        pytest.param(
            429,
            '{"message":"You are being rate limited."}',
            "Airbyte Cloud is refusing requests from these credentials "
            "(rate limited). (HTTP 429)",
            "Stop calling Airbyte Cloud tools and tell the user; don't retry.",
            None,
            id="load-shed-429",
        ),
        pytest.param(
            401,
            '{"message":"Unauthorized"}',
            "Airbyte Cloud rejected the credentials. (HTTP 401)",
            "Ask the user to check the client ID/secret or token; don't retry.",
            None,
            id="non-problem-401",
        ),
        pytest.param(
            408,
            "not json",
            "Airbyte Cloud timed out. The change may or may not have been "
            "applied. (HTTP 408)",
            "Check the resource's current state before retrying; don't blindly "
            "retry a create.",
            None,
            id="timeout-fallback",
        ),
        pytest.param(
            409,
            "not json",
            "The resource is in a state that doesn't allow this operation "
            "(for example, a job is running or already finished). (HTTP 409)",
            "Check the resource's current status before retrying.",
            None,
            id="conflict-fallback",
        ),
        pytest.param(
            422,
            "not json",
            "Airbyte Cloud rejected the request as invalid. (HTTP 422)",
            "Check the arguments against the tool description.",
            None,
            id="other-4xx-fallback",
        ),
        pytest.param(
            500,
            _problem_body(
                "https://reference.airbyte.com/reference/errors",
                title="unexpected-problem",
                data={"message": _INTERNAL_MARKER},
            ),
            "Airbyte Cloud hit an unexpected error. This is often caused by an "
            "invalid argument value. (Cloud error: unexpected-problem, HTTP 500)",
            "Check the arguments (ranges, IDs, formats). If they look right, "
            "don't retry more than once; tell the user.",
            "unexpected-problem",
            id="unexpected-problem-hides-message",
        ),
        pytest.param(
            500,
            '{"errorId":"123e4567-e89b-42d3-a456-426614174000"}',
            "Airbyte Cloud hit an unexpected error. This is often caused by an "
            "invalid argument value. (HTTP 500)",
            "Check the arguments (ranges, IDs, formats). If they look right, "
            "don't retry more than once; tell the user. "
            "Error ID: 123e4567-e89b-42d3-a456-426614174000.",
            None,
            id="error-id-relayed-for-5xx",
        ),
        pytest.param(
            500,
            '{"errorId":"not-a-uuid"}',
            "Airbyte Cloud hit an unexpected error. This is often caused by an "
            "invalid argument value. (HTTP 500)",
            "Check the arguments (ranges, IDs, formats). If they look right, "
            "don't retry more than once; tell the user.",
            None,
            id="error-id-ignored-unless-uuid",
        ),
        pytest.param(
            500,
            json.dumps({"type": "too-big", "x": "y" * 70000}),
            "Airbyte Cloud hit an unexpected error. This is often caused by an "
            "invalid argument value. (HTTP 500)",
            "Check the arguments (ranges, IDs, formats). If they look right, "
            "don't retry more than once; tell the user.",
            None,
            id="oversized-body-falls-back",
        ),
    ],
)
def test_wrap_sdk_error_describes_cloud_problem(
    status_code: int,
    body: str,
    expected_message: str,
    expected_guidance: str,
    problem_type: str | None,
) -> None:
    """Each Cloud problem maps to its fixed message; `data.message` never relays."""
    wrapped = api_util._wrap_sdk_error(_sdk_error(status_code, body))

    assert wrapped.get_message() == expected_message
    assert wrapped.guidance == expected_guidance
    assert wrapped.context["problem_type"] == problem_type
    assert wrapped.context["status_code"] == status_code
    assert _INTERNAL_MARKER not in wrapped.get_message()
    assert _INTERNAL_MARKER not in (wrapped.guidance or "")


def test_wrap_sdk_error_403_keeps_fixed_forbidden_text() -> None:
    wrapped = api_util._wrap_sdk_error(
        _sdk_error(
            403,
            _problem_body(
                "https://reference.airbyte.com/reference/errors#forbidden",
                data={"message": _INTERNAL_MARKER},
            ),
        )
    )

    assert type(wrapped) is AirbyteMissingResourceError
    assert wrapped.get_message() == (
        "The requested resource was not found, or these credentials can't "
        "access it (HTTP 403)."
    )
    assert wrapped.guidance == api_util.FORBIDDEN_RESOURCE_GUIDANCE
    assert wrapped.context["problem_type"] == "forbidden"
    assert _INTERNAL_MARKER not in str(wrapped)


def test_public_api_json_error_context_hides_body_and_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raw-HTTP helpers keep URL/status/slug in context, never bodies."""
    response = requests.Response()
    response.status_code = 400
    response.url = "https://api.airbyte.com/v1/connections/connection-id"
    response._content = json.dumps({
        "type": "https://reference.airbyte.com/reference/errors#bad-request",
        "data": {"message": _INTERNAL_MARKER},
    }).encode()
    response.request = requests.Request("GET", response.url).prepare()
    request = Mock(return_value=response)
    monkeypatch.setattr(api_util.requests, "get", request)
    monkeypatch.setattr(api_util, "get_bearer_token", lambda **_: SecretString("token"))

    with pytest.raises(AirbyteCloudError) as exc_info:
        api_util._get_public_api_json(
            api_root="https://api.airbyte.com/v1",
            path="/connections/connection-id",
            params=None,
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )

    error = exc_info.value
    assert "body" not in error.context
    assert "response" not in error.context
    assert error.context["problem_type"] == "bad-request"
    assert error.context["url"] == response.request.url
    assert _INTERNAL_MARKER not in str(error)


# Every (status, type, title) default published in problems-api's
# api-problems.yaml, hard-coded so the test does not read the platform repo.
_PROBLEM_TYPE_DEFAULTS: list[tuple[int, str, str]] = [
    (
        401,
        "https://reference.airbyte.com/reference/errors#invalid-api-key",
        "invalid-api-key",
    ),
    (403, "https://reference.airbyte.com/reference/errors#forbidden", "forbidden"),
    (403, "error:auth/sso-required", "SSO Sign-in Required"),
    (301, "error:embedded/endpoint-moved", "Airbyte Embedded Endpoint Moved"),
    (403, "error:license/entitlement", "License Entitlement Error"),
    (409, "error:auth/user-already-exists", "User already exists"),
    (412, "error:failed-precondition", "Failed Precondition"),
    (
        500,
        "https://reference.airbyte.com/reference/errors#oauth-callback-failure",
        "oauth-callback-failure",
    ),
    (
        500,
        "https://reference.airbyte.com/reference/errors#invalid-consent-url",
        "invalid-consent-url",
    ),
    (
        422,
        "https://reference.airbyte.com/reference/errors#invalid-redirect-url",
        "invalid-redirect-url",
    ),
    (
        422,
        "https://reference.airbyte.com/reference/errors#unprocessable-entity",
        "unprocessable-entity",
    ),
    (400, "https://reference.airbyte.com/reference/errors", "value-not-found"),
    (
        409,
        "https://reference.airbyte.com/reference/errors#409-state-conflict",
        "state-conflict",
    ),
    (
        409,
        "https://reference.airbyte.com/reference/errors#409-actor-not-ready",
        "actor-not-ready",
    ),
    (409, "error:connection/locked", "Connection is locked"),
    (
        409,
        "https://reference.airbyte.com/reference/errors#try-again-later",
        "try-again-later",
    ),
    (500, "https://reference.airbyte.com/reference/errors", "unexpected-problem"),
    (400, "https://reference.airbyte.com/reference/errors#bad-request", "bad-request"),
    (
        501,
        "error:implementation/not-implemented-in-oss",
        "API not implemented in Airbyte OSS",
    ),
    (400, "error:dbtcloud/access-denied", "Incorrect integration credentials"),
    (
        401,
        "error:dbtcloud/paid-plan-required",
        "Unable to access dbt Cloud integration",
    ),
    (401, "error:dbtcloud/generic", "Unable to access dbt Cloud integration"),
    (400, "error:cron-validation/missing-cron-data", "Cron data is missing"),
    (400, "error:cron-validation/missing-component", "Cron is missing a component"),
    (400, "error:cron-validation/unsupported-timezone", "Unsupported cron timezone"),
    (400, "error:cron-validation/invalid-expression", "Invalid cron expression"),
    (400, "error:cron-validation/invalid-timezone", "Invalid cron timezone"),
    (
        400,
        "error:cron-validation/under-one-hour-not-allowed",
        "Cron sync schedules more frequent than once per hour are not allowed",
    ),
    (
        400,
        "error:basic-schedule-validation/under-one-hour-not-allowed",
        "Basic sync schedules more frequent than once per hour are not allowed",
    ),
    (400, "error:mapper-validation", "Mapper validation failed"),
    (
        400,
        "error:mapper-validation/missing-required-param",
        "Mapper configuration missing required parameter",
    ),
    (400, "error:mapper-validation/invalid-config", "Invalid Mapper Configuration"),
    (400, "error:mapper-validation/secret-not-found", "Mapper secret not found"),
    (
        400,
        "error:connection-validation/file-transfer/connection-unsupported",
        "Connection does not support file transfers",
    ),
    (
        400,
        "error:connection-validation/file-transfer/stream-unsupported",
        "Stream does not support file transfers",
    ),
    (
        400,
        "error:connection-conflicting-destination-stream",
        "Connection contains conflicting stream(s).",
    ),
    (
        409,
        "error:mapper-validation/runtime-secrets-manager-required",
        "Runtime Secrets Manager Required",
    ),
    (503, "https://reference.airbyte.com/reference/errors", "service-unavailable"),
    (
        404,
        "https://reference.airbyte.com/reference/errors#resource-not-found",
        "resource-not-found",
    ),
    (
        409,
        "error:generate-contribution/connector-image-name-in-use",
        "The image name provided is already in use.",
    ),
    (
        401,
        "error:generate-contribution/invalid-github-token",
        "Invalid GitHub token provided.",
    ),
    (
        403,
        "error:generate-contribution/insufficient-github-token-permissions",
        "Failed to create fork of Airbyte repository.",
    ),
    (
        500,
        "error:generate-contribution",
        "An unexpected error occurred when creating your GitHub contribution.",
    ),
    (
        404,
        "error:billing/subscription/subscription-required",
        "A subscription is required for this operation to succeed.",
    ),
    (
        422,
        "error:billing/insufficient-payment-status",
        "The payment status of the associated Organization is insufficient.",
    ),
    (
        422,
        "error:billing/insufficient-credit-balance",
        "The credit balance of the associated Workspace or Organization is insufficient.",
    ),
    (
        404,
        "error:billing/no-active-subscription",
        "The organization doesn't have an active subscription.",
    ),
    (
        400,
        "error:billing/no-cancelable-subscription",
        "The organization doesn't have any active cancelable subscription.",
    ),
    (
        400,
        "error:billing/no-scheduled-cancellation-subscription",
        "The organization doesn't have any cancelable subscription with a scheduled cancellation.",
    ),
    (
        400,
        "error:billing/no-scheduled-plan-change",
        "The organization doesn't have any self-serve subscription with a scheduled plan change.",
    ),
    (
        400,
        "error:connector-rollout/invalid-request",
        "Invalid Connector Rollout request",
    ),
    (
        400,
        "error:connector-rollout/rollout-percentage-reached",
        "Max rollout percentage already reached",
    ),
    (
        400,
        "error:connector-rollout/not-enough-actors",
        "Not Enough Actors for Connector Rollout",
    ),
    (
        409,
        "error:dataplane-group-name-already-exists",
        "Data plane group name already exists",
    ),
    (409, "error:dataplane-name-already-exists", "Data plane name already exists"),
    (409, "error:tag-already-exists", "Tag already exists"),
    (409, "error:group-already-exists", "Group already exists"),
    (409, "error:group-permission-already-exists", "Group permission already exists"),
    (409, "error:group-managed-by-scim", "Group managed by SCIM"),
    (409, "error:group-member-already-exists", "Group member already exists"),
    (400, "error:tag-invalid-hex-color", "Invalid hex color"),
    (400, "error:tag-name-too-long", "Tag name too long"),
    (400, "error:tag-limit-for-workspace-reached", "Tag limit for workspace reached"),
    (
        403,
        "error:workspace-limit-for-organization-reached",
        "Workspace limit for organization reached",
    ),
    (408, "error:request-timeout-exceeded", "Request timeout exceeded"),
    (400, "error:notification/config/required", "Notification required"),
    (400, "error:notification/config/missing-url", "Notification missing URL"),
    (
        400,
        "error:destination/discover-not-supported",
        "Destination does not support discover",
    ),
    (404, "error:destination/catalog-not-found", "Destination catalog not found"),
    (
        400,
        "error:connection/destination-catalog/missing-object-name",
        "Configured stream missing destination object name",
    ),
    (
        400,
        "error:connection/destination-catalog/invalid-operation",
        "Invalid destination operation configuration",
    ),
    (
        400,
        "error:connection/destination-catalog/missing-required-field",
        "Configured stream missing required field",
    ),
    (
        400,
        "error:connection/destination-catalog/invalid-additional-field",
        "Invalid additional field in configured stream",
    ),
    (
        400,
        "error:connection/destination-catalog/required",
        "Destination catalog is required",
    ),
    (
        400,
        "error:connection/destination-catalog/missing-primary-key",
        "Primary key required when matching keys are defined",
    ),
    (
        400,
        "error:connection/destination-catalog/invalid-primary-key",
        "Primary key must match one of the matching keys",
    ),
    (500, "error:sso-config-retrieval", "SSO config retrieval error"),
    (500, "error:sso-setup", "SSO setup configuration error"),
    (500, "error:sso-deletion", "SSO deletion failed"),
    (500, "error:sso-credential-update", "SSO credential update failed"),
    (500, "error:sso-activation", "SSO Activation Error"),
    (401, "error:sso-token-validation", "SSO Token Validation Failed"),
    (
        503,
        "error:entitlement-service/error-adding-organization",
        "entitlement-service-error-adding-organization",
    ),
    (
        500,
        "error:entitlement-service/invalid-organization-state",
        "entitlement-service-invalid-organization-state",
    ),
]


@pytest.mark.parametrize(
    ("status_code", "problem_type", "title"),
    [pytest.param(*row, id=row[1]) for row in _PROBLEM_TYPE_DEFAULTS],
)
def test_every_published_problem_type_matches_a_fixed_message(
    status_code: int,
    problem_type: str,
    title: str,
) -> None:
    """Each published `type`/`title` pair hits a `cloud_errors.yaml` row."""
    body = _problem_body(problem_type, title)
    wrapped = api_util._wrap_sdk_error(_sdk_error(status_code, body))

    if problem_type.endswith("409-actor-not-ready"):
        assert type(wrapped) is AirbyteConnectorNotReadyError
        return

    if status_code == 403:
        assert type(wrapped) is AirbyteMissingResourceError
        assert wrapped.get_message() == (
            "The requested resource was not found, or these credentials can't "
            "access it (HTTP 403)."
        )
        assert wrapped.guidance == api_util.FORBIDDEN_RESOURCE_GUIDANCE
        return

    problem = cloud_errors.parse_cloud_error(status_code, body)
    assert problem.key in cloud_errors._messages()
    entry = cloud_errors._messages()[problem.key]
    expected_message, expected_guidance = entry["message"], entry["guidance"]
    # A table hit can never be the status fallback; a few rows (bad-request,
    # state-conflict, ...) intentionally share the fallback wording anyway.
    assert wrapped.get_message() == (
        f"{expected_message} (Cloud error: {problem.key}, HTTP {status_code})"
    )
    assert wrapped.guidance == expected_guidance
    assert _INTERNAL_MARKER not in wrapped.get_message()


@pytest.mark.parametrize(
    ("problem", "expected_slug", "expected_key"),
    [
        pytest.param(
            {
                "type": (
                    "https://reference.airbyte.com/reference/errors"
                    "#f47ac10b-58cc-4372-a567-0e02b2c3d479"
                ),
                "title": "resource-not-found",
            },
            None,
            None,
            id="id-like-type-never-reads-title",
        ),
        pytest.param(
            {
                "type": "https://reference.airbyte.com/reference/errors",
                "title": "unexpected-problem",
            },
            "unexpected-problem",
            "unexpected-problem",
            id="generic-type-reads-title",
        ),
        pytest.param(
            {"title": "unexpected-problem"},
            None,
            "unexpected-problem",
            id="missing-type-does-not-set-slug",
        ),
        pytest.param(
            {"title": "password=hunter2"},
            None,
            "password=hunter2",
            id="missing-type-does-not-set-untrusted-slug",
        ),
        pytest.param(
            {"type": 123, "title": "try-again-later"},
            None,
            "try-again-later",
            id="non-string-type-does-not-set-slug",
        ),
    ],
)
def test_parse_cloud_error_title_fallback_only_for_generic_type(
    problem: dict[str, Any],
    expected_slug: str | None,
    expected_key: str | None,
) -> None:
    """Only a generic string `type` permits a title slug."""
    parsed = cloud_errors.parse_cloud_error(None, json.dumps(problem))

    assert parsed.slug == expected_slug
    assert parsed.key == expected_key


def test_resource_type_in_agent_text_must_be_allowlisted() -> None:
    body = _problem_body(
        "https://reference.airbyte.com/reference/errors#resource-not-found",
        data={"resourceType": "secretvalue"},
    )
    problem = cloud_errors.parse_cloud_error(404, body)

    message, _ = cloud_errors.describe_cloud_error(problem)

    assert problem.resource_type is None
    assert (
        message
        == "The resource was not found. (Cloud error: resource-not-found, HTTP 404)"
    )
    assert "secretvalue" not in message


def test_cloud_errors_yaml_entries_have_message_and_guidance() -> None:
    """Every YAML entry is a mapping with only `message` and `guidance` strings."""
    messages = cloud_errors._messages()

    assert set(messages) > {"status_fallbacks"}
    for key, entry in messages.items():
        if key == "status_fallbacks":
            for status, fallback in entry.items():
                assert set(fallback) == {"message", "guidance"}
                assert isinstance(fallback["message"], str) and fallback["message"]
                assert isinstance(fallback["guidance"], str) and fallback["guidance"]
            continue
        assert set(entry) == {"message", "guidance"}
        assert isinstance(entry["message"], str) and entry["message"]
        assert isinstance(entry["guidance"], str) and entry["guidance"]


@pytest.mark.parametrize(
    ("status_code", "body", "absent", "suffix"),
    [
        pytest.param(
            500,
            _problem_body(
                "https://reference.airbyte.com/reference/errors",
                title="password=hunter2-secret-value",
            ),
            ("hunter2", "password"),
            "(HTTP 500)",
            id="unknown-title-never-relays",
        ),
        pytest.param(
            500,
            _problem_body("error:some/unknown-thing contains sk_live_abc"),
            ("sk_live", "unknown-thing"),
            "(HTTP 500)",
            id="unknown-type-never-relays",
        ),
        pytest.param(
            500,
            _problem_body(
                "https://reference.airbyte.com/reference/errors",
                title="unexpected-problem",
            ),
            (),
            "(Cloud error: unexpected-problem, HTTP 500)",
            id="known-key-shown",
        ),
        pytest.param(
            None,
            _problem_body("error:some/unknown-thing contains sk_live_abc"),
            ("sk_live", "unknown-thing"),
            None,
            id="unknown-key-no-status-no-suffix",
        ),
    ],
)
def test_describe_cloud_error_suffix_only_for_known_keys(
    status_code: int | None,
    body: str,
    absent: tuple[str, ...],
    suffix: str | None,
) -> None:
    """An unmatched `type`/`title` is never echoed into agent-facing text."""
    problem = cloud_errors.parse_cloud_error(status_code, body)
    message, guidance = cloud_errors.describe_cloud_error(problem)

    for needle in absent:
        assert needle not in message
        assert needle not in guidance
    if suffix is None:
        assert "(Cloud error:" not in message and "(HTTP" not in message
    else:
        assert message.endswith(suffix)
