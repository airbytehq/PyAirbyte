# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Cloud API utilities."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
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
    raw_response = SimpleNamespace(url="https://api.airbyte.com/v1/jobs")
    return api.ListJobsResponse(
        content_type="application/json",
        status_code=200,
        raw_response=raw_response,
        jobs_response=models.JobsResponse(
            data=data,
            next=next_page,
        ),
    )


def test_get_job_logs_paginates_until_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    requests: list[api.ListJobsRequest] = []
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
        requests.append(request)
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
    assert [(request.limit, request.offset) for request in requests] == [
        (100, 0),
        (50, 100),
    ]
