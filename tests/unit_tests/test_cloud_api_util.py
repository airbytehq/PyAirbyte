# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Cloud API utilities."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests
from airbyte._util import api_util
from airbyte.exceptions import AirbyteWorkspaceNotEmptyError, PyAirbyteInputError
from airbyte.secrets.base import SecretString
from airbyte_api import api, models


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
