# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Cloud API utilities."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests
from airbyte._util import api_util
from airbyte.secrets.base import SecretString
from airbyte_api import api, models


def _job_response(job_id: int) -> models.JobResponse:
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


def _list_connections_response(
    data: list[models.ConnectionResponse],
    *,
    next_page: str | None,
) -> api.ListConnectionsResponse:
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


@pytest.mark.parametrize(
    "kwargs,pages,expected_names,expected_requests",
    [
        pytest.param(
            {"limit": 0},
            [],
            [],
            [],
            id="limit_zero_short_circuits",
        ),
        pytest.param(
            {"limit": 2, "offset": 5},
            [
                _list_connections_response(
                    [
                        _connection_response("first", 1),
                        _connection_response("second", 2),
                    ],
                    next_page=None,
                ),
            ],
            ["first", "second"],
            [(100, 5)],
            id="non_zero_offset_uses_full_page",
        ),
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
    captured_requests: list[api.ListConnectionsRequest] = []

    def list_connections(
        request: api.ListConnectionsRequest,
    ) -> api.ListConnectionsResponse:
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


def test_get_job_logs_paginates_until_limit(monkeypatch: pytest.MonkeyPatch) -> None:
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
