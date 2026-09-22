# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Airbyte Cloud connections."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from airbyte._util import api_util
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.models import JobStatusEnum, JobTypeEnum
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import PyAirbyteInputError
from airbyte_api import models


def _job_response(
    job_id: int,
    status: models.JobStatusEnum,
    *,
    connection_id: str = "connection-id",
) -> models.JobResponse:
    """Create a minimal job response."""
    return models.JobResponse(
        connection_id=connection_id,
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


def _patch_get_job_info(
    monkeypatch: pytest.MonkeyPatch,
    captured_job_ids: list[int],
    response: models.JobResponse,
) -> None:
    """Patch the API utility job lookup and capture its job IDs."""

    def get_job_info(
        job_id: int,
        *,
        api_root: str,
        client_id: object,
        client_secret: object,
        bearer_token: object,
    ) -> models.JobResponse:
        """Capture lookup arguments and return the configured response."""
        _ = (api_root, client_id, client_secret, bearer_token)
        captured_job_ids.append(job_id)
        return response

    monkeypatch.setattr(api_util, "get_job_info", get_job_info)


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

    def get_previous_sync_logs(
        *,
        limit: int,
        job_type: JobTypeEnum,
    ) -> list[_SyncResultDouble]:
        """Return the configured latest sync job."""
        assert limit == 1
        assert job_type == JobTypeEnum.SYNC
        return [latest]

    monkeypatch.setattr(connection, "get_previous_sync_logs", get_previous_sync_logs)

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

    def get_previous_sync_logs(
        *,
        limit: int,
        job_type: JobTypeEnum,
    ) -> list[_SyncResultDouble]:
        """Return the configured latest sync job."""
        assert limit == 1
        assert job_type == JobTypeEnum.SYNC
        return [latest]

    monkeypatch.setattr(connection, "get_previous_sync_logs", get_previous_sync_logs)

    with pytest.raises(PyAirbyteInputError, match="succeeded"):
        connection.cancel_sync()


def test_cancel_sync_rejects_connection_without_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cancellation without an ID rejects a connection with no jobs."""
    connection = _connection()

    def get_previous_sync_logs(
        *,
        limit: int,
        job_type: JobTypeEnum,
    ) -> list[_SyncResultDouble]:
        """Return no jobs."""
        assert limit == 1
        assert job_type == JobTypeEnum.SYNC
        return []

    monkeypatch.setattr(connection, "get_previous_sync_logs", get_previous_sync_logs)

    with pytest.raises(PyAirbyteInputError, match="No sync jobs found"):
        connection.cancel_sync()


def test_cancel_sync_with_explicit_running_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify an explicit running job ID is validated and cancelled."""
    connection = _connection()
    captured_lookup_job_ids: list[int] = []
    _patch_get_job_info(
        monkeypatch,
        captured_lookup_job_ids,
        _job_response(123, models.JobStatusEnum.RUNNING),
    )
    captured_job_ids: list[int] = []
    _patch_cancel_job(
        monkeypatch,
        captured_job_ids,
        _job_response(123, models.JobStatusEnum.CANCELLED),
    )

    def get_sync_result(job_id: int | None = None) -> None:
        """Fail if latest-job resolution is attempted for an explicit ID."""
        _ = job_id
        pytest.fail("get_sync_result should not be called for an explicit job ID")

    monkeypatch.setattr(connection, "get_sync_result", get_sync_result)

    result = connection.cancel_sync(job_id=123)

    assert captured_lookup_job_ids == [123]
    assert captured_job_ids == [123]
    assert result.job_id == 123


def test_cancel_sync_rejects_explicit_job_from_different_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify an explicit job ID from another connection cannot be cancelled."""
    connection = _connection()
    captured_lookup_job_ids: list[int] = []
    _patch_get_job_info(
        monkeypatch,
        captured_lookup_job_ids,
        _job_response(
            123,
            models.JobStatusEnum.RUNNING,
            connection_id="different-connection-id",
        ),
    )
    captured_job_ids: list[int] = []
    _patch_cancel_job(
        monkeypatch,
        captured_job_ids,
        _job_response(123, models.JobStatusEnum.CANCELLED),
    )

    with pytest.raises(
        PyAirbyteInputError,
        match="different-connection-id.*connection-id",
    ):
        connection.cancel_sync(job_id=123)

    assert captured_lookup_job_ids == [123]
    assert captured_job_ids == []


def test_cancel_sync_rejects_explicit_completed_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify an explicit completed job cannot be cancelled."""
    connection = _connection()
    captured_lookup_job_ids: list[int] = []
    _patch_get_job_info(
        monkeypatch,
        captured_lookup_job_ids,
        _job_response(123, models.JobStatusEnum.SUCCEEDED),
    )
    captured_job_ids: list[int] = []
    _patch_cancel_job(
        monkeypatch,
        captured_job_ids,
        _job_response(123, models.JobStatusEnum.CANCELLED),
    )

    with pytest.raises(PyAirbyteInputError, match="succeeded"):
        connection.cancel_sync(job_id=123)

    assert captured_lookup_job_ids == [123]
    assert captured_job_ids == []


@pytest.mark.parametrize(
    "cron_expression",
    [
        pytest.param("0 0 0 * * ?", id="daily_6_fields"),
        pytest.param("0 0 */6 * * ?", id="every_6_hours"),
        pytest.param("0 0 0 ? * SUN", id="weekly_day_of_week"),
        pytest.param("0 0 0 * * ? 2026", id="7_fields_with_year"),
        pytest.param("0 0 9 ? * MON-FRI US/Pacific", id="with_timezone"),
    ],
)
def test_set_schedule_accepts_quartz_cron(
    monkeypatch: pytest.MonkeyPatch,
    cron_expression: str,
) -> None:
    """Verify Quartz cron expressions are passed through to the API."""
    connection = _connection()
    captured: list[models.AirbyteAPIConnectionSchedule] = []

    def patch_connection(
        *,
        connection_id: str,
        api_root: str,
        client_id: object,
        client_secret: object,
        bearer_token: object,
        schedule: models.AirbyteAPIConnectionSchedule,
    ) -> models.ConnectionResponse:
        _ = (connection_id, api_root, client_id, client_secret, bearer_token)
        captured.append(schedule)
        return models.ConnectionResponse(
            connection_id="connection-id",
            created_at=0,
            destination_id="destination-id",
            name="name",
            source_id="source-id",
            status=models.ConnectionStatusEnum.ACTIVE,
            workspace_id="workspace-id",
            configurations=models.StreamConfigurations(streams=[]),
            schedule=schedule,
            tags=[],
        )

    monkeypatch.setattr(api_util, "patch_connection", patch_connection)

    connection.set_schedule(cron_expression=cron_expression)

    assert len(captured) == 1
    assert captured[0].cron_expression == cron_expression
    assert captured[0].schedule_type == models.ScheduleTypeEnum.CRON


@pytest.mark.parametrize(
    "cron_expression",
    [
        pytest.param("0 0 * * *", id="unix_5_fields"),
        pytest.param("0 */6 * * *", id="unix_every_6_hours"),
        pytest.param("* * * * *", id="unix_every_minute"),
        pytest.param("0 0 0 * * ? 2026 UTC extra", id="too_many_fields"),
        pytest.param("", id="empty"),
    ],
)
def test_set_schedule_rejects_non_quartz_cron(
    monkeypatch: pytest.MonkeyPatch,
    cron_expression: str,
) -> None:
    """Verify non-Quartz cron expressions fail client-side without calling the API."""
    connection = _connection()

    def patch_connection(**kwargs: object) -> models.ConnectionResponse:
        raise AssertionError(f"API should not be called: {kwargs}")

    monkeypatch.setattr(api_util, "patch_connection", patch_connection)

    with pytest.raises(PyAirbyteInputError, match="Quartz"):
        connection.set_schedule(cron_expression=cron_expression)
