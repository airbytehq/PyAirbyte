# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Airbyte Cloud connections."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
from airbyte._util import api_util
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.models import (
    CloudConnectionInfo,
    ConnectionStatus,
    JobStatusEnum,
    JobTypeEnum,
)
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import (
    AirbyteCloudApiError,
    AirbyteConnectionSyncError,
    AirbyteCloudError,
    AirbyteMissingResourceError,
    AirbyteLibInputError,
)
from airbyte.secrets.base import SecretString
from airbyte_api import models
from airbyte_api.errors import SDKError


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


@pytest.mark.parametrize(
    ("api_status", "expected_status"),
    [
        pytest.param("active", ConnectionStatus.ACTIVE, id="active"),
        pytest.param("inactive", ConnectionStatus.INACTIVE, id="inactive"),
        pytest.param("deprecated", ConnectionStatus.DEPRECATED, id="deprecated"),
    ],
)
def test_connection_status_returns_connection_status_enum(
    monkeypatch: pytest.MonkeyPatch,
    api_status: str,
    expected_status: ConnectionStatus,
) -> None:
    """Reading a cached API status returns its public enum value."""
    connection = _connection()
    connection._connection_info = (  # noqa: SLF001  # Seed cache.
        CloudConnectionInfo.from_api_response(
            models.ConnectionResponse(
                connection_id="connection-id",
                workspace_id="workspace-id",
                source_id="source-id",
                destination_id="destination-id",
                name="sync",
                configurations={},
                created_at=1,
                schedule=models.ConnectionScheduleResponse(
                    schedule_type=models.ScheduleTypeWithBasicEnum.MANUAL,
                ),
                status=models.ConnectionStatusEnum(api_status),
                tags=[],
            )
        )
    )
    monkeypatch.setattr(
        CloudConnection,
        "_fetch_connection_info",
        MagicMock(side_effect=AssertionError),
    )

    assert connection.status is expected_status


@pytest.mark.parametrize(
    ("enabled", "current_status", "expected_status"),
    [
        pytest.param(
            True,
            ConnectionStatus.INACTIVE,
            ConnectionStatus.ACTIVE,
            id="enable",
        ),
        pytest.param(
            False,
            ConnectionStatus.ACTIVE,
            ConnectionStatus.INACTIVE,
            id="disable",
        ),
    ],
)
def test_set_enabled_passes_connection_status_enum(
    monkeypatch: pytest.MonkeyPatch,
    enabled: bool,
    current_status: ConnectionStatus,
    expected_status: ConnectionStatus,
) -> None:
    """Setting enabled state passes its StrEnum directly to the API utility."""
    connection = _connection()
    fetch_connection_info = MagicMock(
        return_value=SimpleNamespace(status=current_status)
    )
    monkeypatch.setattr(connection, "_fetch_connection_info", fetch_connection_info)
    updated_response = SimpleNamespace(
        connection_id="connection-id",
        workspace_id="workspace-id",
        source_id="source-id",
        destination_id="destination-id",
        name="sync",
        configurations=None,
        prefix=None,
        namespace_definition=None,
        namespace_format=None,
        schedule=None,
        status=expected_status,
    )
    patch_connection = MagicMock(return_value=updated_response)
    monkeypatch.setattr(api_util, "patch_connection", patch_connection)

    connection.enabled = enabled

    fetch_connection_info.assert_called_once_with(force_refresh=True)
    patch_connection.assert_called_once_with(
        connection_id="connection-id",
        api_root="https://api.airbyte.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token="token",
        status=expected_status,
    )
    assert connection.status is expected_status


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


@pytest.mark.parametrize(
    ("status_code", "job_connection_id"),
    [
        pytest.param(403, None, id="forbidden"),
        pytest.param(404, None, id="not-found"),
        pytest.param(500, None, id="other-lookup-error"),
        pytest.param(None, "other-connection", id="different-connection"),
        pytest.param(None, "connection-id", id="matching-connection"),
    ],
)
def test_get_sync_result_validates_job_id(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int | None,
    job_connection_id: str | None,
) -> None:
    """Job lookup status and ownership determine the sync result outcome."""
    if status_code == 404:
        original_error: AirbyteCloudError | None = AirbyteCloudApiError(
            message="Job lookup failed",
            status_code=status_code,
        )
    elif status_code is not None:
        original_error = AirbyteCloudError(
            message="Job lookup failed",
            context={"status_code": status_code},
        )
    else:
        original_error = None

    if original_error is not None:
        get_job_info = MagicMock(side_effect=original_error)
    else:
        get_job_info = MagicMock(
            return_value=_job_response(
                42,
                models.JobStatusEnum.SUCCEEDED,
                connection_id=job_connection_id or "connection-id",
            )
        )
    monkeypatch.setattr(api_util, "get_job_info", get_job_info)
    connection = _connection()

    if status_code in (403, 404):
        with pytest.raises(AirbyteMissingResourceError) as exc_info:
            connection.get_sync_result(job_id=42)

        assert exc_info.value.get_message() == (
            "Job 42 was not found on connection connection-id, or you don't have access to it."
        )
        assert exc_info.value.__cause__ is original_error
        assert exc_info.value.resource_type == "sync job"
        assert exc_info.value.resource_name_or_id == "42"
        assert exc_info.value.guidance == (
            "Use `list_cloud_sync_jobs` to find valid job IDs for this connection."
        )
        assert exc_info.value.context == {
            "connection_id": "connection-id",
            "job_id": 42,
        }
    elif status_code is not None:
        with pytest.raises(AirbyteCloudError) as exc_info:
            connection.get_sync_result(job_id=42)

        assert exc_info.value is original_error
    elif job_connection_id != "connection-id":
        with pytest.raises(AirbyteMissingResourceError) as exc_info:
            connection.get_sync_result(job_id=42)

        assert exc_info.value.get_message() == (
            "Job 42 belongs to a different connection, not connection-id."
        )
        assert exc_info.value.resource_type == "sync job"
        assert exc_info.value.resource_name_or_id == "42"
        assert exc_info.value.guidance == (
            "Use `list_cloud_sync_jobs` to find valid job IDs for this connection."
        )
        assert exc_info.value.context == {
            "connection_id": "connection-id",
            "job_id": 42,
        }
        assert "other-connection" not in str(exc_info.value.context)
    else:
        result = connection.get_sync_result(job_id=42)

        assert result is not None
        assert result.get_job_status() == JobStatusEnum.SUCCEEDED

    get_job_info.assert_called_once_with(
        job_id=42,
        api_root=connection.workspace.api_root,
        client_id=None,
        client_secret=None,
        bearer_token="token",
    )


@pytest.mark.parametrize(
    ("status_code", "catalog_behavior"),
    [
        pytest.param(400, "available", id="bad-request-with-catalog"),
        pytest.param(400, "failure", id="bad-request-catalog-failure"),
        pytest.param(400, "unexpected-shape", id="bad-request-unexpected-shape"),
        pytest.param(400, "malformed-streams", id="bad-request-malformed-streams"),
        pytest.param(500, "available", id="server-error"),
    ],
)
def test_set_selected_streams_enriches_only_bad_requests(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
    catalog_behavior: str,
) -> None:
    """Only valid HTTP 400 catalog responses provide stream-name guidance."""
    original_error = AirbyteCloudError(
        message="Invalid stream name" if status_code == 400 else "API request failed",
        context={"status_code": status_code},
    )
    monkeypatch.setattr(
        api_util, "patch_connection", MagicMock(side_effect=original_error)
    )
    connection = _connection()
    if catalog_behavior == "available":
        catalog_response: object = {
            "streams": [
                {"stream": {"name": "orders"}, "selected": True},
                {"stream": {"name": "customers"}, "selected": False},
            ]
        }
        dump_raw_catalog = MagicMock(return_value=catalog_response)
    elif catalog_behavior == "failure":
        dump_raw_catalog = MagicMock(
            side_effect=AirbyteCloudError(message="Catalog unavailable")
        )
    elif catalog_behavior == "unexpected-shape":
        dump_raw_catalog = MagicMock(return_value={"streams": "unexpected"})
    else:
        dump_raw_catalog = MagicMock(return_value={"streams": [{"name": "orders"}]})
    monkeypatch.setattr(connection, "dump_raw_catalog", dump_raw_catalog)

    if status_code == 400 and catalog_behavior == "available":
        with pytest.raises(AirbyteLibInputError) as exc_info:
            connection.set_selected_streams(["missing", "orders"])

        assert exc_info.value.get_message().startswith(
            "Could not set selected streams for connection 'connection-id': Invalid stream name"
        )
        assert exc_info.value.guidance == "Use stream names from `available_streams`."
        assert exc_info.value.context == {
            "connection_id": "connection-id",
            "requested_streams": ["missing", "orders"],
            "available_streams": ["orders", "customers"],
        }
        assert exc_info.value.__cause__ is original_error
        dump_raw_catalog.assert_called_once_with()
    else:
        with pytest.raises(AirbyteCloudError) as exc_info:
            connection.set_selected_streams(["missing"])

        assert exc_info.value is original_error
        if status_code == 500:
            dump_raw_catalog.assert_not_called()
        else:
            dump_raw_catalog.assert_called_once_with()


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

    with pytest.raises(AirbyteLibInputError, match="succeeded"):
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

    with pytest.raises(AirbyteLibInputError, match="No sync jobs found"):
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
        AirbyteLibInputError,
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

    with pytest.raises(AirbyteLibInputError, match="succeeded"):
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
            schedule=models.ConnectionScheduleResponse(
                schedule_type=models.ScheduleTypeWithBasicEnum.CRON,
                cron_expression=schedule.cron_expression,
            ),
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

    with pytest.raises(AirbyteLibInputError, match="Quartz"):
        connection.set_schedule(cron_expression=cron_expression)


def _sync_error(status_code: int) -> AirbyteConnectionSyncError:
    """Create an AirbyteConnectionSyncError with the given status code."""
    return AirbyteConnectionSyncError(
        connection_id="connection-id",
        message=f"API error occurred: Status {status_code}",
        context={"workspace_id": "workspace-id", "status_code": status_code},
    )


def _connection_info_with_status(status: str) -> MagicMock:
    """Create a minimal connection info double with the given status."""
    info = MagicMock()
    info.status = status
    return info


def test_run_sync_conflict_on_disabled_connection_raises_input_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a 409 on an inactive connection surfaces a disabled-connection hint."""
    connection = _connection()

    def run_connection(**kwargs: object) -> None:
        """Raise the wrapped 409 sync error."""
        _ = kwargs
        raise _sync_error(409)

    monkeypatch.setattr(api_util, "run_connection", run_connection)
    fetch_mock = MagicMock(return_value=_connection_info_with_status("inactive"))
    monkeypatch.setattr(connection, "_fetch_connection_info", fetch_mock)

    with pytest.raises(AirbyteLibInputError) as exc_info:
        connection.run_sync()

    assert exc_info.value.message is not None
    assert exc_info.value.guidance is not None
    assert "disabled" in exc_info.value.message
    assert "enabled=True" in exc_info.value.guidance
    fetch_mock.assert_called_once_with(force_refresh=True)


def test_run_sync_conflict_on_active_connection_reraises_sync_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a 409 on an active connection re-raises the original sync error."""
    connection = _connection()
    error = _sync_error(409)

    def run_connection(**kwargs: object) -> None:
        """Raise the wrapped 409 sync error."""
        _ = kwargs
        raise error

    monkeypatch.setattr(api_util, "run_connection", run_connection)
    fetch_mock = MagicMock(return_value=_connection_info_with_status("active"))
    monkeypatch.setattr(connection, "_fetch_connection_info", fetch_mock)

    with pytest.raises(AirbyteConnectionSyncError) as exc_info:
        connection.run_sync()

    assert exc_info.value is error


def test_run_sync_non_conflict_error_reraises_without_checking_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify a non-409 sync error is re-raised without consulting `enabled`."""
    connection = _connection()
    error = _sync_error(500)

    def run_connection(**kwargs: object) -> None:
        """Raise a non-409 sync error."""
        _ = kwargs
        raise error

    monkeypatch.setattr(api_util, "run_connection", run_connection)
    fetch_mock = MagicMock()
    monkeypatch.setattr(connection, "_fetch_connection_info", fetch_mock)

    with pytest.raises(AirbyteConnectionSyncError) as exc_info:
        connection.run_sync()

    assert exc_info.value is error
    fetch_mock.assert_not_called()


def test_run_connection_wraps_sdk_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `run_connection` wraps a create_job SDKError in AirbyteConnectionSyncError."""
    airbyte_instance = MagicMock()
    airbyte_instance.jobs.create_job.side_effect = SDKError(
        message="Status 409",
        raw_response=httpx.Response(
            409,
            request=httpx.Request("POST", "https://api.airbyte.com/v1/jobs"),
        ),
        body="...",
    )
    monkeypatch.setattr(
        api_util,
        "get_airbyte_server_instance",
        MagicMock(return_value=airbyte_instance),
    )

    with pytest.raises(AirbyteConnectionSyncError) as exc_info:
        api_util.run_connection(
            workspace_id="workspace-id",
            connection_id="connection-id",
            api_root="https://api.airbyte.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token=SecretString("token"),
        )

    assert exc_info.value.context is not None
    assert exc_info.value.context["status_code"] == 409
