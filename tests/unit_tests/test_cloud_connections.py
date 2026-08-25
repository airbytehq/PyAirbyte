# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Airbyte Cloud connections."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from airbyte._util import api_util
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.models import JobStatusEnum
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import PyAirbyteInputError
from airbyte_api import models


def _job_response(job_id: int, status: models.JobStatusEnum) -> models.JobResponse:
    """Create a minimal job response."""
    return models.JobResponse(
        connection_id="connection-id",
        job_id=job_id,
        job_type=models.JobTypeEnum.SYNC,
        start_time="2026-01-01T00:00:00Z",
        status=status,
    )


@dataclass
class _SyncResultDouble:
    """Subset of `SyncResult` needed by cancellation tests."""

    job_id: int
    status: JobStatusEnum
    complete: bool

    def is_job_complete(self) -> bool:
        """Return whether the job is complete."""
        return self.complete

    def get_job_status(self) -> JobStatusEnum:
        """Return the job status."""
        return self.status


def _connection() -> CloudConnection:
    """Create a CloudConnection with local credentials."""
    workspace = CloudWorkspace(
        workspace_id="workspace-id",
        bearer_token="token",
        api_root="https://api.airbyte.com/v1",
    )
    return CloudConnection(workspace=workspace, connection_id="connection-id")


def _patch_cancel_job(
    monkeypatch: pytest.MonkeyPatch,
    captured_job_ids: list[int],
    response: models.JobResponse,
) -> None:
    """Patch the API utility cancellation call and capture its job IDs."""

    def cancel_job(
        *,
        job_id: int,
        api_root: str,
        client_id: object,
        client_secret: object,
        bearer_token: object,
    ) -> models.JobResponse:
        """Capture cancellation arguments and return the configured response."""
        _ = (api_root, client_id, client_secret, bearer_token)
        captured_job_ids.append(job_id)
        return response

    monkeypatch.setattr(api_util, "cancel_job", cancel_job)


def test_cancel_sync_resolves_latest_incomplete_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cancellation without an ID uses the latest incomplete job."""
    connection = _connection()
    latest = _SyncResultDouble(
        job_id=17,
        status=JobStatusEnum.RUNNING,
        complete=False,
    )
    captured_job_ids: list[int] = []
    _patch_cancel_job(
        monkeypatch,
        captured_job_ids,
        _job_response(17, models.JobStatusEnum.CANCELLED),
    )

    def get_sync_result(job_id: int | None = None) -> _SyncResultDouble:
        """Return the configured latest job."""
        assert job_id is None
        return latest

    monkeypatch.setattr(connection, "get_sync_result", get_sync_result)

    result = connection.cancel_sync()

    assert captured_job_ids == [17]
    assert result.job_id == 17


def test_cancel_sync_rejects_latest_completed_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cancellation without an ID rejects an already completed latest job."""
    connection = _connection()
    latest = _SyncResultDouble(
        job_id=17,
        status=JobStatusEnum.SUCCEEDED,
        complete=True,
    )

    def get_sync_result(job_id: int | None = None) -> _SyncResultDouble:
        """Return the configured latest job."""
        assert job_id is None
        return latest

    monkeypatch.setattr(connection, "get_sync_result", get_sync_result)

    with pytest.raises(PyAirbyteInputError, match="succeeded"):
        connection.cancel_sync()


def test_cancel_sync_rejects_connection_without_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cancellation without an ID rejects a connection with no jobs."""
    connection = _connection()

    def get_sync_result(job_id: int | None = None) -> None:
        """Return no jobs."""
        assert job_id is None
        return None

    monkeypatch.setattr(connection, "get_sync_result", get_sync_result)

    with pytest.raises(PyAirbyteInputError, match="No sync jobs found"):
        connection.cancel_sync()


def test_cancel_sync_with_explicit_job_id_skips_latest_job_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify explicit cancellation IDs bypass latest-job resolution."""
    connection = _connection()
    captured_job_ids: list[int] = []
    _patch_cancel_job(
        monkeypatch,
        captured_job_ids,
        _job_response(99, models.JobStatusEnum.CANCELLED),
    )

    def get_sync_result(job_id: int | None = None) -> None:
        """Fail if latest-job resolution is attempted."""
        _ = job_id
        pytest.fail("get_sync_result should not be called for an explicit job ID")

    monkeypatch.setattr(connection, "get_sync_result", get_sync_result)

    result = connection.cancel_sync(job_id=123)

    assert captured_job_ids == [123]
    assert result.job_id == 99
