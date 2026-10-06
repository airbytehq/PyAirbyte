# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Cloud API utilities."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import pytest
import requests
from airbyte import constants
from airbyte._util import api_util, meta
from airbyte.exceptions import (
    AirbyteCloudError,
    AirbyteMissingResourceError,
    AirbyteWorkspaceNotEmptyError,
    AirbyteLibInputError,
)
from airbyte.cloud.models import CloudConnectionInfo, CloudJobInfo, JobStatusEnum
from airbyte.registry import ConnectorType
from airbyte.secrets.base import SecretString
from airbyte_server_models.public_api import models
from airbyte_server_models._config_api import (
    CheckConnectionRead,
    JobConfigType,
    OrganizationInfoRead,
    OrganizationRead,
    OrganizationReadList,
    PermissionRead,
    PermissionReadList,
    PermissionType,
    Status4,
    SynchronousJobRead,
    UserRead,
)

USER_ID = UUID("00000000-0000-0000-0000-000000000001")
ORGANIZATION_ID = UUID("00000000-0000-0000-0000-000000000002")
PERMISSION_ID = UUID("00000000-0000-0000-0000-000000000003")


def _id_for_index(index: int) -> UUID:
    return UUID(int=index + 10)


def _job_response(job_id: int) -> models.JobResponse:
    """Create a minimal job response for pagination tests."""
    return models.JobResponse(
        connection_id="connection-id",
        job_id=job_id,
        job_type=models.JobTypeEnum.SYNC,
        start_time="2026-01-01T00:00:00Z",
        status=models.JobStatusEnum.SUCCEEDED,
    )


def _job_wire(job_id: int) -> dict[str, object]:
    """Create a minimal job wire dict for pagination tests."""
    return _job_response(job_id).model_dump(mode="json", by_alias=True)


def _connection_response(name: str, index: int) -> models.ConnectionResponse:
    """Create a minimal connection response for pagination tests."""
    return models.ConnectionResponse.model_construct(
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


def _connection_wire(name: str, index: int) -> dict[str, object]:
    """Create a minimal connection wire dict for pagination tests."""
    return {
        "connectionId": f"connection-{index}",
        "name": name,
        "sourceId": f"source-{index}",
        "destinationId": f"destination-{index}",
        "workspaceId": "workspace-id",
        "status": "active",
        "schedule": {"scheduleType": "manual"},
        "nonBreakingSchemaUpdatesBehavior": "ignore",
        "configurations": {"streams": []},
        "createdAt": index,
        "tags": [],
    }


def _workspace_response(name: str, index: int) -> models.WorkspaceResponse:
    """Create a minimal workspace response for pagination tests."""
    return models.WorkspaceResponse(
        data_residency="auto",
        name=name,
        notifications=models.NotificationsConfig(),
        workspace_id=f"workspace-{index}",
    )


def _workspace_wire(name: str, index: int) -> dict[str, object]:
    """Create a minimal workspace wire dict for pagination tests."""
    return _workspace_response(name, index).model_dump(mode="json", by_alias=True)


def _page_body(items: list[dict[str, object]], next_page: str | None = None) -> dict:
    """Create a paged list-endpoint wire body."""
    return {"data": items, "next": next_page}


def _public_api_spy(
    monkeypatch: pytest.MonkeyPatch,
    results: list[object],
) -> list[dict[str, object]]:
    """Patch `_make_public_api_request`, capturing call kwargs and returning queued results."""
    captured: list[dict[str, object]] = []
    pages = list(results)

    def fake_request(**kwargs: object) -> object:
        captured.append(kwargs)
        result = pages.pop(0)
        response_model = kwargs.get("response_model")
        if response_model is not None and isinstance(result, dict):
            return response_model.model_validate(result)
        return result

    monkeypatch.setattr(api_util, "_make_public_api_request", fake_request)
    return captured


def _http_error_response(
    status_code: int, *, url: str, text: str = ""
) -> requests.Response:
    """Build a `requests.Response` like a failed Public API call."""
    response = requests.Response()
    response.status_code = status_code
    response.url = url
    response._content = text.encode()
    response.request = requests.Request("GET", url).prepare()
    return response


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
            "API error occurred: Workspace lookup failed.",
            None,
            id="not_found",
        ),
        pytest.param(
            500,
            AirbyteCloudError,
            "API error occurred: Workspace lookup failed.",
            None,
            id="server_error",
        ),
    ],
)
def test_wrap_api_error_classifies_missing_or_forbidden(
    status_code: int,
    expected_error_type: type[AirbyteCloudError],
    expected_message: str,
    expected_guidance: str | None,
) -> None:
    raw_response = _http_error_response(
        status_code,
        url="https://api.airbyte.com/v1/workspaces/workspace-id",
        text="Workspace lookup failed.",
    )

    wrapped = api_util._wrap_api_error(raw_response, {"workspace_id": "workspace-id"})

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


def test_get_user_by_auth_id_forwards_typed_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_config_request(**request_kwargs: object) -> object:
        captured.update(request_kwargs)
        return UserRead(userId=USER_ID, email="user@example.com", metadata={})

    monkeypatch.setattr(api_util, "_make_config_api_request", fake_config_request)

    result = api_util.get_user_by_auth_id(
        "auth-user-id",
        api_root="https://api.example",
        config_api_root="https://config.example",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    assert result.userId == USER_ID
    assert captured["path"] == "/users/get_by_auth_id"
    assert captured["request"].model_dump(mode="json", exclude_none=True) == {
        "authUserId": "auth-user-id"
    }
    assert captured["config_api_root"] == "https://config.example"


def test_list_permissions_for_user_forwards_typed_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    permission = PermissionRead(
        permissionId=PERMISSION_ID,
        permissionType=PermissionType.organization_member,
        userId=USER_ID,
        organizationId=ORGANIZATION_ID,
    )

    def fake_config_request(**request_kwargs: object) -> object:
        captured.update(request_kwargs)
        return PermissionReadList(permissions=[permission])

    monkeypatch.setattr(api_util, "_make_config_api_request", fake_config_request)

    result = api_util.list_permissions_for_user(
        str(USER_ID),
        api_root="https://api.example",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    assert result == [permission]
    assert captured["request"].model_dump(mode="json", exclude_none=True) == {
        "userId": str(USER_ID)
    }


def test_make_config_api_request_wraps_response_validation_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = requests.Response()
    response.status_code = 200
    response._content = b'{"invalid": true}'
    response.url = "https://config.example/users/get_by_auth_id"
    monkeypatch.setattr(requests, "request", lambda **_: response)

    with pytest.raises(
        AirbyteCloudError, match="did not match the expected schema"
    ) as exc_info:
        api_util._make_config_api_request(
            api_root="https://api.example",
            path="/users/get_by_auth_id",
            request=api_util.UserAuthIdRequestBody(authUserId="auth-user-id"),
            response_model=UserRead,
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
            config_api_root="https://config.example",
        )

    assert exc_info.value.context == {
        "full_url": "https://config.example/users/get_by_auth_id",
        "path": "/users/get_by_auth_id",
        "response": '{"invalid": true}',
    }


def test_make_config_api_request_wraps_invalid_json_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = requests.Response()
    response.status_code = 200
    response._content = b"not json"
    response.url = "https://config.example/users/get_by_auth_id"

    def raise_json_error() -> object:
        raise requests.exceptions.JSONDecodeError("x", "doc", 0)

    monkeypatch.setattr(response, "json", raise_json_error)
    monkeypatch.setattr(requests, "request", lambda **_: response)

    with pytest.raises(
        AirbyteCloudError, match="did not match the expected schema"
    ) as exc_info:
        api_util._make_config_api_request(
            api_root="https://api.example",
            path="/users/get_by_auth_id",
            request=api_util.UserAuthIdRequestBody(authUserId="auth-user-id"),
            response_model=UserRead,
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
            config_api_root="https://config.example",
        )

    assert exc_info.value.context == {
        "full_url": "https://config.example/users/get_by_auth_id",
        "path": "/users/get_by_auth_id",
        "response": "not json",
    }


def test_get_organization_info_forwards_typed_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        api_util,
        "_make_config_api_request",
        lambda **kwargs: captured.update(kwargs)
        or OrganizationInfoRead(
            organizationId=ORGANIZATION_ID,
            organizationName="Organization",
            sso=False,
            scim=False,
        ),
    )

    result = api_util.get_organization_info(
        str(ORGANIZATION_ID),
        api_root="https://api.example",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    assert result.organizationId == ORGANIZATION_ID
    assert captured["request"].model_dump(mode="json", exclude_none=True) == {
        "organizationId": str(ORGANIZATION_ID)
    }


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
        OrganizationReadList(
            organizations=[
                OrganizationRead(
                    organizationId=_id_for_index(index),
                    organizationName=f"Organization {index}",
                    email=f"organization-{index}@example.com",
                )
                for index in range(100)
            ]
        ),
        OrganizationReadList(
            organizations=[
                OrganizationRead(
                    organizationId=_id_for_index(index),
                    organizationName=f"Organization {index}",
                    email=f"organization-{index}@example.com",
                )
                for index in range(100, 200)
            ]
        ),
        OrganizationReadList(organizations=[]),
    ]

    def fake_config_request(**kwargs: object) -> OrganizationReadList:
        request = kwargs["request"]
        requests.append(request.model_dump(mode="json", exclude_none=True))
        return pages.pop(0)

    monkeypatch.setattr(api_util, "_make_config_api_request", fake_config_request)

    result = api_util.list_organizations_for_user_id(
        str(USER_ID),
        api_root="https://api.airbyte.com/v1/",
        client_id=SecretString("client-id"),
        client_secret=SecretString("client-secret"),
        bearer_token=None,
        name_contains=name_contains,
        limit=limit,
    )

    result_ids = [str(organization.organizationId) for organization in result]
    assert len(result_ids) == expected_count
    assert result_ids[0] == str(_id_for_index(0))
    assert result_ids[-1] == str(
        _id_for_index(int(expected_last.removeprefix("organization-")))
    )
    assert requests[0] == {
        "userId": str(USER_ID),
        "pagination": {"pageSize": 100, "rowOffset": 0},
        **({"nameContains": name_contains} if name_contains is not None else {}),
    }
    assert requests[1]["pagination"] == {"pageSize": 100, "rowOffset": 100}
    assert len(requests) == request_count


@pytest.mark.parametrize(
    ("connector_type", "request_field"),
    [("source", "sourceId"), ("destination", "destinationId")],
)
def test_check_connector_uses_connector_specific_request_body(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: str,
    request_field: str,
) -> None:
    captured: dict[str, object] = {}
    actor_id = str(_id_for_index(50))

    def fake_config_request(**kwargs: object) -> CheckConnectionRead:
        captured.update(kwargs)
        return CheckConnectionRead(
            status=Status4.succeeded,
            jobInfo=SynchronousJobRead(
                id=_id_for_index(51),
                configType=(
                    JobConfigType.check_connection_source
                    if connector_type == "source"
                    else JobConfigType.check_connection_destination
                ),
                createdAt=1,
                endedAt=2,
                succeeded=True,
            ),
        )

    monkeypatch.setattr(api_util, "_make_config_api_request", fake_config_request)

    assert api_util.check_connector(
        actor_id=actor_id,
        connector_type=connector_type,  # type: ignore[arg-type]
        api_root="https://api.example",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    ) == (True, None)
    assert captured["request"].model_dump(mode="json", exclude_none=True) == {
        request_field: actor_id
    }


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
        json_request = kwargs["request"]
        assert isinstance(json_request, dict)
        captured_requests.append({"path": kwargs["path"], "request": json_request})
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
    assert [request["request"] for request in captured_requests] == [
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
        json_request = kwargs["request"]
        assert isinstance(json_request, dict)
        captured_requests.append({"path": kwargs["path"], "request": json_request})
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
        request["request"]["pagination"]["rowOffset"] for request in captured_requests
    ] == (expected_offsets)
    assert [request["path"] for request in captured_requests] == [
        "/workspaces/list_by_user_id"
    ] * len(expected_offsets)


def test_list_workspaces_by_user_honors_page_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_payloads: list[dict[str, object]] = []

    def fake_config_request(**kwargs: object) -> dict[str, object]:
        json_request = kwargs["request"]
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
    captured = _public_api_spy(monkeypatch, [_workspace_response("New workspace", 1)])

    organization_id = str(_id_for_index(0))
    region_id = str(_id_for_index(1))
    workspace = api_util.create_workspace(
        name="New workspace",
        organization_id=organization_id,
        region_id=region_id,
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    assert workspace.workspace_id == "workspace-1"
    assert captured[0]["method"] == "POST"
    assert captured[0]["path"] == "/workspaces"
    captured_request = captured[0]["request"]
    assert captured_request.name == "New workspace"
    assert str(captured_request.organization_id) == organization_id
    assert str(captured_request.region_id) == region_id


def test_rename_workspace_forwards_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _public_api_spy(
        monkeypatch, [_workspace_response("Renamed workspace", 1)]
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
    assert captured[0]["method"] == "PATCH"
    assert captured[0]["path"] == "/workspaces/workspace-1"
    assert captured[0]["request"].name == "Renamed workspace"


def test_patch_connection_normalizes_status_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify string connection statuses are normalized for the API request model."""
    captured = _public_api_spy(monkeypatch, [_connection_response("Connection", 1)])

    api_util.patch_connection(
        connection_id="connection-1",
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
        status="inactive",
    )

    assert captured[0]["request"].status == models.ConnectionStatusEnum.INACTIVE


def test_patch_connection_rejects_invalid_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify invalid string connection statuses produce PyAirbyte errors."""
    monkeypatch.setattr(
        api_util,
        "_make_public_api_request",
        Mock(side_effect=AssertionError("API should not be called")),
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
    delete_calls = []

    def get_workspace(**_: object) -> models.WorkspaceResponse:
        return models.WorkspaceResponse(
            data_residency="auto",
            name=workspace_name,
            notifications=models.NotificationsConfig(),
            workspace_id="workspace-1",
        )

    def fake_request(**kwargs: object) -> None:
        if kwargs["method"] == "DELETE":
            delete_calls.append(kwargs["path"])
        return None

    monkeypatch.setattr(api_util, "get_workspace", get_workspace)
    monkeypatch.setattr(api_util, "_make_public_api_request", fake_request)
    monkeypatch.setattr(api_util, "list_connections", lambda **_: [])

    if should_delete:
        api_util.permanently_delete_workspace(
            workspace_id="workspace-1",
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )
        assert delete_calls == ["/workspaces/workspace-1"]
    else:
        with pytest.raises(AirbyteLibInputError):
            api_util.permanently_delete_workspace(
                workspace_id="workspace-1",
                api_root="https://api.airbyte.com/v1",
                client_id=None,
                client_secret=None,
                bearer_token=SecretString("token"),
            )
        assert delete_calls == []


def test_permanently_delete_workspace_requires_empty_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delete_calls = []
    monkeypatch.setattr(
        api_util,
        "_make_public_api_request",
        lambda **kwargs: delete_calls.append(kwargs) or None,
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
    assert delete_calls == []


@pytest.mark.parametrize(
    "kwargs,pages,expected_names,expected_requests",
    [
        pytest.param(
            {"limit": 1, "name_filter": lambda name: name == "target"},
            [
                _page_body(
                    [
                        _connection_wire("miss", 1),
                        _connection_wire("target", 2),
                    ],
                ),
            ],
            ["target"],
            [(100, 0)],
            id="filtered_limit_uses_full_page",
        ),
        pytest.param(
            {"limit": 2, "name_filter": lambda name: name == "target"},
            [
                _page_body(
                    [_connection_wire("target", 1)],
                    next_page="next",
                ),
                _page_body(
                    [
                        _connection_wire("target", 2),
                        _connection_wire("extra", 3),
                    ],
                ),
            ],
            ["target", "target"],
            [(100, 0), (100, 1)],
            id="filtered_limit_continues_until_enough_matches",
        ),
        pytest.param(
            {"name": ""},
            [
                _page_body(
                    [
                        _connection_wire("", 1),
                        _connection_wire("non-empty", 2),
                    ],
                ),
            ],
            [""],
            [(100, 0)],
            id="empty_name_filters_exactly",
        ),
        pytest.param(
            {},
            [
                _page_body(
                    [_connection_wire("first", 1)],
                    next_page="next",
                ),
                _page_body(
                    [_connection_wire("second", 2)],
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
    pages: list[dict],
    expected_names: list[str],
    expected_requests: list[tuple[int | None, int | None]],
) -> None:
    """Verify resource list pagination, filtering, and request sizing."""
    captured = _public_api_spy(monkeypatch, pages)

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
        (call["params"]["limit"], call["params"]["offset"]) for call in captured
    ] == expected_requests


def test_list_workspaces_does_not_filter_by_workspace_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify workspace listing fetches accessible workspaces across pages."""
    captured = _public_api_spy(
        monkeypatch,
        [
            _page_body([_workspace_wire("first", 1)], next_page="next"),
            _page_body([_workspace_wire("second", 2)]),
        ],
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
        (call["params"]["limit"], call["params"]["offset"]) for call in captured
    ] == [
        (100, 0),
        (100, 1),
    ]
    assert all("workspaceIds" not in call["params"] for call in captured)


def test_list_workspaces_caps_unfiltered_api_page_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify unfiltered workspace listing uses requested limit as API page size."""
    captured = _public_api_spy(
        monkeypatch,
        [_page_body([_workspace_wire("first", 1)], next_page="next")],
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
        (call["params"]["limit"], call["params"]["offset"]) for call in captured
    ] == [(1, 0)]


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


def test_get_job_logs_paginates_until_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify job log pagination stops after collecting the requested limit."""
    captured = _public_api_spy(
        monkeypatch,
        [
            _page_body([_job_wire(job_id) for job_id in range(100)], next_page="next"),
            _page_body([_job_wire(job_id) for job_id in range(100, 150)]),
        ],
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
    assert [
        (call["params"]["limit"], call["params"]["offset"]) for call in captured
    ] == [
        (100, 0),
        (50, 100),
    ]


def test_get_job_logs_uses_offset_and_allows_unbounded_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify job log pagination preserves offset and treats `None` as unbounded."""
    captured = _public_api_spy(
        monkeypatch,
        [
            _page_body([_job_wire(job_id) for job_id in range(100)], next_page="next"),
            _page_body([_job_wire(job_id) for job_id in range(100, 125)]),
        ],
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
    assert [
        (call["params"]["limit"], call["params"]["offset"]) for call in captured
    ] == [
        (100, 10),
        (100, 110),
    ]


def test_cancel_job_forwards_request_and_returns_job_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cancelling a job forwards its ID and returns the API job response."""
    job_response = _job_response(42)
    captured = _public_api_spy(monkeypatch, [job_response])

    result = api_util.cancel_job(
        job_id=42,
        api_root="https://api.airbyte.com/v1/",
        client_id=SecretString("client-id"),
        client_secret=SecretString("client-secret"),
        bearer_token=None,
    )

    assert result is job_response
    assert captured[0]["method"] == "DELETE"
    assert captured[0]["path"] == "/jobs/42"


def test_cancel_job_raises_for_non_ok_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cancelling a job raises when the API response is not successful."""
    error_response = _http_error_response(404, url="https://api.airbyte.com/v1/jobs/42")
    monkeypatch.setattr(
        api_util,
        "_make_public_api_request",
        Mock(side_effect=api_util._wrap_api_error(error_response)),
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
    error_response = _http_error_response(409, url="https://api.airbyte.com/v1/jobs/42")
    monkeypatch.setattr(
        api_util,
        "_make_public_api_request",
        Mock(side_effect=api_util._wrap_api_error(error_response)),
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
        request={},
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    headers = captured["headers"]
    assert isinstance(headers, dict)
    assert headers[meta.AIRBYTE_ANALYTIC_SOURCE_HEADER] == "pyairbyte-mcp-hosted"


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
        request={},
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
        request={"workspaceId": "workspace-id"},
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
        request={
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
            "API request failed with status 500",
            None,
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
            request={"workspaceId": "workspace-id"},
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
    assert error.context["body"] == response.request.body
    assert error.context["response"] is response.__dict__
    assert isinstance(error.__cause__, requests.HTTPError)
    assert error.__cause__.response is response
    assert (
        request.call_args.kwargs["url"]
        == "https://config.airbyte.com/v1/workspaces/get"
    )


def test_public_api_request_sends_bearer_and_analytic_source_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Public API path sends bearer auth plus the analytic source header."""
    monkeypatch.setattr(meta, "_MCP_MODE_ENABLED", True)
    monkeypatch.setattr(constants, "_HOSTED_MCP_MODE_ENABLED", False)
    captured: dict[str, object] = {}

    def fake_request(**kwargs: object) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(
            status_code=200,
            content=b"{}",
            json=lambda: {},
            text="{}",
            url="https://api.airbyte.com/v1/x",
            headers={},
            request=SimpleNamespace(url="https://api.airbyte.com/v1/x", method="GET"),
        )

    monkeypatch.setattr(api_util.requests, "request", fake_request)

    api_util._make_public_api_request(
        method="GET",
        api_root="https://api.airbyte.com/v1",
        path="/x",
        response_model=None,
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    headers = captured["headers"]
    assert isinstance(headers, dict)
    assert headers["Authorization"] == "Bearer token"
    assert headers[meta.AIRBYTE_ANALYTIC_SOURCE_HEADER] == "pyairbyte-mcp-local"


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


def _api_404_error() -> AirbyteCloudError:
    """Wrap a failed Public API response like a 404 connector lookup."""
    return api_util._wrap_api_error(
        _http_error_response(
            404,
            url="https://api.airbyte.com/v1/connectors/connector-id",
            text="not found",
        )
    )


def _api_status_error(status_code: int) -> AirbyteCloudError:
    """Wrap a failed Public API response for a non-2xx status."""
    return api_util._wrap_api_error(
        _http_error_response(
            status_code,
            url="https://api.airbyte.com/v1/connectors/connector-id",
            text='{"message":"Caller does not have the required permissions"}',
        )
    )


@pytest.mark.parametrize(
    ("operation", "status_code"),
    [
        pytest.param("get-job", 403, id="get-job"),
        pytest.param("patch-connection", 400, id="patch-connection"),
    ],
)
def test_api_util_calls_wrap_api_errors_with_status_context(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    status_code: int,
) -> None:
    """Errors from job and connection calls retain status and request context."""
    api_error = _api_status_error(status_code)

    def fake_request(**kwargs: object) -> object:
        context = dict(api_error.context or {})
        context.update(kwargs.get("error_context") or {})
        error_cls = (
            api_util.AirbyteMissingResourceError
            if status_code == 403
            else api_util.AirbyteCloudError
        )
        raise error_cls(message=api_error.get_message(), context=context)

    monkeypatch.setattr(
        api_util,
        "_make_public_api_request",
        fake_request,
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
    if operation == "get-job":
        assert isinstance(exc_info.value, AirbyteMissingResourceError)
        assert exc_info.value.context["job_id"] == 42
    else:
        assert not isinstance(exc_info.value, AirbyteMissingResourceError)
        assert exc_info.value.context["connection_id"] == "connection-1"


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
    destination_wire = {
        "destinationId": "dest-id",
        "name": "dest",
        "destinationType": "duckdb",
        "definitionId": "definition-id",
        "workspaceId": "ws",
        "configuration": {"destination_path": "/tmp/test.duckdb"},
        "createdAt": 0,
    }
    calls: list[dict[str, object]] = []

    def fake_request(**kwargs: object) -> object:
        calls.append(kwargs)
        if kwargs["path"].startswith("/sources/"):
            raise api_util._wrap_api_error(
                _http_error_response(
                    source_status,
                    url=f"https://api.airbyte.com/v1{kwargs['path']}",
                    text="boom",
                ),
                kwargs.get("error_context"),
            )
        return destination_wire

    monkeypatch.setattr(api_util, "_make_public_api_request", fake_request)

    connector_type, response = api_util.get_connector(
        "dest-id",
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    assert connector_type is ConnectorType.DESTINATION
    assert response.destination_id == "dest-id"


def test_get_connector_raises_missing_resource_when_neither_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_request(**kwargs: object) -> object:
        _ = kwargs
        raise _api_404_error()

    monkeypatch.setattr(api_util, "_make_public_api_request", fake_request)

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
    calls: list[str] = []

    def fake_request(**kwargs: object) -> object:
        path = kwargs["path"]
        calls.append(path)
        status = (
            source_status
            if path.startswith("/sources/")
            else (destination_status or 200)
        )
        raise api_util._wrap_api_error(
            _http_error_response(
                status, url=f"https://api.airbyte.com/v1{path}", text="boom"
            ),
            kwargs.get("error_context"),
        )

    monkeypatch.setattr(api_util, "_make_public_api_request", fake_request)

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
    assert (
        any(path.startswith("/destinations/") for path in calls)
        is expect_destination_call
    )


def test_get_connector_does_not_fall_back_on_5xx_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 5xx source lookup is not read as "maybe a destination"."""
    calls: list[str] = []

    def fake_request(**kwargs: object) -> object:
        calls.append(kwargs["path"])
        raise _api_status_error(500)

    monkeypatch.setattr(api_util, "_make_public_api_request", fake_request)

    with pytest.raises(AirbyteCloudError) as exc_info:
        api_util.get_connector(
            "connector-id",
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )

    assert (exc_info.value.context or {})["status_code"] == 500
    assert not any(path.startswith("/destinations/") for path in calls)


def test_get_source_reraises_non_404_api_error_as_airbyte_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        api_util,
        "_make_public_api_request",
        Mock(side_effect=_api_status_error(500)),
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


_FIELD_FILTERING_MAPPER = {
    "type": "field-filtering",
    "mapperConfiguration": {"targetField": "foo"},
}


def _connection_wire_with_mapper(mapper: dict) -> dict[str, object]:
    """Create a connection wire dict carrying a single stream mapper."""
    return {
        "connectionId": "connection-1",
        "name": "Connection",
        "sourceId": "source-1",
        "destinationId": "destination-1",
        "workspaceId": "workspace-id",
        "status": "active",
        "schedule": {"scheduleType": "manual"},
        "nonBreakingSchemaUpdatesBehavior": "ignore",
        "configurations": {"streams": [{"name": "s", "mappers": [mapper]}]},
        "createdAt": 0,
        "tags": [],
    }


@pytest.mark.parametrize(
    "mapper",
    [
        pytest.param(_FIELD_FILTERING_MAPPER, id="field-filtering"),
        pytest.param(
            {"type": "future-mapper", "mapperConfiguration": {"someKey": 1}},
            id="unknown-mapper-type",
        ),
    ],
)
def test_get_connection_decodes_stream_mappers(
    monkeypatch: pytest.MonkeyPatch,
    mapper: dict,
) -> None:
    """Connections with field-filtering or unknown mappers decode without failing."""
    _public_api_spy(monkeypatch, [_connection_wire_with_mapper(mapper)])

    connection = api_util.get_connection(
        workspace_id="workspace-id",
        connection_id="connection-1",
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    stream_mapper = connection.configurations.streams[0].mappers[0]
    assert stream_mapper.type == mapper["type"]
    if mapper["type"] == "field-filtering":
        assert stream_mapper.mapper_configuration.target_field == "foo"
    else:
        assert stream_mapper.mapper_configuration == {"someKey": 1}

    info = CloudConnectionInfo.from_api_response(connection)
    assert info.connection_id == "connection-1"


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        pytest.param("queued", JobStatusEnum.QUEUED, id="queued"),
        pytest.param("some_future_status", "some_future_status", id="unknown-status"),
    ],
)
def test_get_job_info_decodes_queued_and_unknown_statuses(
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    expected: object,
) -> None:
    """Queued and future job statuses decode without raising."""
    _public_api_spy(
        monkeypatch,
        [
            {
                "jobId": 42,
                "status": status,
                "jobType": "sync",
                "startTime": "2026-01-01T00:00:00Z",
                "connectionId": "connection-id",
            }
        ],
    )

    job = api_util.get_job_info(
        job_id=42,
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    assert job.status == expected
    job_info = CloudJobInfo.from_api_response(job)
    assert job_info.status == expected


def test_list_connections_skips_invalid_items_and_logs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed item in a paged list is skipped with a warning.

    Pagination must advance by the RAW item count, not the decoded count:
    a skipped item must not shift the next page's offset (which would yield
    duplicates), and a page of only invalid items must not stop pagination
    while `next` is still set.
    """
    invalid_item = {"connectionId": "connection-bad"}
    invalid_only = {"connectionId": "connection-bad-2"}
    captured = _public_api_spy(
        monkeypatch,
        [
            # Page 1: one invalid item among three raw items.
            _page_body(
                [
                    _connection_wire("good", 1),
                    invalid_item,
                    _connection_wire("also-good", 2),
                ],
                next_page="next",
            ),
            # Page 2: every item fails to decode, but `next` is still set.
            _page_body([invalid_only], next_page="next"),
            _page_body([_connection_wire("third", 3)]),
        ],
    )

    warnings: list[str] = []

    def record_warning(message: str, *args: object, **kwargs: object) -> None:
        warnings.append(message % args if args else message)

    monkeypatch.setattr(api_util.logger, "warning", record_warning)

    result = api_util.list_connections(
        workspace_id="workspace-id",
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    # No duplicates: offset advanced by the raw page lengths (3, then 1).
    assert [connection.name for connection in result] == ["good", "also-good", "third"]
    assert [call["params"]["offset"] for call in captured] == [0, 3, 4]
    assert any("failed schema validation" in message for message in warnings)


def test_cloud_connection_info_configurations_keep_snake_case_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`CloudConnectionInfo.configurations` serializes with snake_case keys (MCP output)."""
    _public_api_spy(
        monkeypatch,
        [
            {
                "connectionId": "connection-1",
                "name": "Connection",
                "sourceId": "source-1",
                "destinationId": "destination-1",
                "workspaceId": "workspace-id",
                "status": "active",
                "schedule": {"scheduleType": "manual"},
                "nonBreakingSchemaUpdatesBehavior": "ignore",
                "configurations": {
                    "streams": [
                        {
                            "name": "users",
                            "syncMode": "incremental_append",
                            "cursorField": ["updated_at"],
                            "primaryKey": [["id"]],
                        }
                    ]
                },
                "createdAt": 0,
                "tags": [],
            }
        ],
    )

    connection = api_util.get_connection(
        workspace_id="workspace-id",
        connection_id="connection-1",
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token=SecretString("token"),
    )

    info = CloudConnectionInfo.from_api_response(connection)
    dumped = info.model_dump(mode="json")
    stream = dumped["configurations"]["streams"][0]
    assert stream["sync_mode"] == "incremental_append"
    assert stream["cursor_field"] == ["updated_at"]
    assert stream["primary_key"] == [["id"]]
    assert "syncMode" not in stream
