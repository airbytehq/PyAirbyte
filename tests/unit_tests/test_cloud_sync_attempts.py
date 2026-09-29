# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for structured sync attempt failures."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from collections.abc import Callable
from typing import Any, cast

import pytest
import requests
from airbyte._util import api_util
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.models import JobStatusEnum
from airbyte.cloud.sync_results import (
    SyncAttempt,
    SyncAttemptFailure,
    SyncJobSnapshot,
    SyncResult,
)
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.mcp import cloud as cloud_mcp
from fastmcp import Context


WORKSPACE = cast(
    CloudWorkspace,
    SimpleNamespace(
        api_root="https://api.example.com/v1",
        config_api_root="https://config.example.com/v1",
        client_id=None,
        client_secret=None,
        bearer_token="token",
    ),
)
CONNECTION = cast(CloudConnection, SimpleNamespace(connection_id="connection-id"))

FAILED_ATTEMPT: dict[str, Any] = {
    "attempt": {
        "id": 11,
        "status": "failed",
        "createdAt": 1767225600,
        "failureSummary": {
            "failures": [
                {
                    "failureOrigin": "source",
                    "failureType": "config_error",
                    "externalMessage": "Invalid credentials.",
                    "internalMessage": "secret-internal-detail",
                    "stacktrace": "Traceback: secret-stack",
                    "retryable": False,
                },
                {
                    "failureOrigin": "replication",
                    "failureType": "transient_error",
                    "externalMessage": "Connection reset.",
                    "retryable": True,
                },
            ],
        },
    },
}


def _attempt(attempt_data: dict[str, Any], number: int = 0) -> SyncAttempt:
    return SyncAttempt(
        workspace=WORKSPACE,
        connection=CONNECTION,
        job_id=123,
        attempt_number=number,
        _attempt_data=attempt_data,
    )


def test_failures_map_failure_summary_in_order() -> None:
    """Each failure maps the public fields, preserving the API's order."""
    assert _attempt(FAILED_ATTEMPT).failures == [
        SyncAttemptFailure(
            failure_origin="source",
            failure_type="config_error",
            external_message="Invalid credentials.",
            retryable=False,
        ),
        SyncAttemptFailure(
            failure_origin="replication",
            failure_type="transient_error",
            external_message="Connection reset.",
            retryable=True,
        ),
    ]


def test_failures_omit_internal_message_and_stacktrace() -> None:
    """Internal messages and stack traces are never exposed."""
    failure = _attempt(FAILED_ATTEMPT).failures[0]

    assert not hasattr(failure, "internal_message")
    assert not hasattr(failure, "stacktrace")
    assert "secret" not in repr(failure)


@pytest.mark.parametrize(
    "attempt_fields",
    [
        pytest.param({"status": "succeeded"}, id="no-summary"),
        pytest.param(
            {"status": "succeeded", "failureSummary": None}, id="null-summary"
        ),
        pytest.param({"status": "failed", "failureSummary": {}}, id="empty-summary"),
        pytest.param(
            {"status": "failed", "failureSummary": {"failures": None}},
            id="null-failures",
        ),
    ],
)
def test_failures_empty_without_failure_summary(attempt_fields: dict[str, Any]) -> None:
    """Attempts without failure details report no failures."""
    assert _attempt({"attempt": {"id": 1, **attempt_fields}}).failures == []


def test_get_attempts_fetches_job_by_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Attempts come from Config API `/jobs/get` for the job ID."""
    requests: list[dict[str, Any]] = []

    def fake_request(**kwargs: Any) -> dict[str, Any]:
        requests.append(kwargs)
        return {"attempts": [FAILED_ATTEMPT]}

    monkeypatch.setattr(api_util, "_make_config_api_request", fake_request)
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)

    (attempt,) = sync_result.get_attempts()

    assert [r["path"] for r in requests] == ["/jobs/get"]
    assert requests[0]["json"] == {"id": 123}
    assert attempt.failures[0].failure_type == "config_error"


def test_get_cloud_sync_status_includes_attempt_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`include_attempts` returns each attempt's structured failures."""
    succeeded_attempt = {
        "attempt": {"id": 12, "status": "succeeded", "createdAt": 1767225700},
    }
    sync_result = SimpleNamespace(
        job_id=123,
        bytes_synced=0,
        records_synced=0,
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        job_url="https://cloud.example.com/jobs",
        get_job_snapshot=lambda: SyncJobSnapshot(
            status=JobStatusEnum.FAILED,
            bytes_synced=0,
            records_synced=0,
            start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ),
        get_attempts=lambda: [
            _attempt(FAILED_ATTEMPT, 0),
            _attempt(succeeded_attempt, 1),
        ],
        get_raw_attempt_count=lambda: 2,
    )
    connection = SimpleNamespace(get_sync_result=lambda job_id=None: sync_result)
    workspace = SimpleNamespace(get_connection=lambda connection_id: connection)
    monkeypatch.setattr(
        cloud_mcp, "_get_cloud_workspace", lambda ctx, workspace_id=None: workspace
    )

    result = cloud_mcp.get_cloud_sync_status(
        cast(Context, object()),
        connection_id="connection-id",
        job_id=None,
        workspace_id=None,
        include_attempts=True,
    )

    failed, succeeded = result["attempts"]
    assert failed["failures"] == [
        {
            "failure_origin": "source",
            "failure_type": "config_error",
            "external_message": "Invalid credentials.",
            "external_message_truncated": False,
            "retryable": False,
        },
        {
            "failure_origin": "replication",
            "failure_type": "transient_error",
            "external_message": "Connection reset.",
            "external_message_truncated": False,
            "retryable": True,
        },
    ]
    assert succeeded["failures"] == []


@pytest.mark.parametrize(
    "job_with_attempts",
    [
        pytest.param({"attempts": None}, id="attempts-null"),
        pytest.param(
            {"attempts": [None, "bad", {"attempt": None}]}, id="malformed-entries"
        ),
    ],
)
def test_get_attempts_skips_null_and_malformed_attempts(
    job_with_attempts: dict[str, Any],
) -> None:
    """Null or malformed attempt entries yield no attempts instead of raising."""
    sync_result = SyncResult(
        workspace=WORKSPACE,
        connection=CONNECTION,
        job_id=123,
        _job_with_attempts_info=job_with_attempts,
    )

    assert sync_result.get_attempts() == []


def _status_with_attempts(
    monkeypatch: pytest.MonkeyPatch,
    attempts: list[SyncAttempt],
    get_attempts: Callable[[], list[SyncAttempt]] | None = None,
    raw_attempt_count: int | None = None,
) -> dict[str, Any]:
    sync_result = SimpleNamespace(
        job_id=123,
        job_url="https://cloud.example.com/jobs",
        get_job_snapshot=lambda: SyncJobSnapshot(
            status=JobStatusEnum.FAILED,
            bytes_synced=0,
            records_synced=0,
            start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ),
        get_attempts=get_attempts or (lambda: attempts),
        get_raw_attempt_count=lambda: (
            len(attempts) if raw_attempt_count is None else raw_attempt_count
        ),
    )
    connection = SimpleNamespace(get_sync_result=lambda job_id=None: sync_result)
    workspace = SimpleNamespace(get_connection=lambda connection_id: connection)
    monkeypatch.setattr(
        cloud_mcp, "_get_cloud_workspace", lambda ctx, workspace_id=None: workspace
    )
    return cloud_mcp.get_cloud_sync_status(
        cast(Context, object()),
        connection_id="connection-id",
        job_id=None,
        workspace_id=None,
        include_attempts=True,
    )


@pytest.mark.parametrize(
    ("job", "expected"),
    [
        pytest.param(
            {"startedAt": 1767225600, "createdAt": 1767225000},
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            id="started-at",
        ),
        pytest.param(
            {"startedAt": None, "createdAt": 1767225600},
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            id="created-at",
        ),
    ],
)
def test_parse_start_time_invalid_iso_falls_back_to_job_epoch(
    monkeypatch: pytest.MonkeyPatch, job: dict[str, Any], expected: datetime
) -> None:
    """An unparseable start time falls back to the Config API job's epoch timestamps."""
    monkeypatch.setattr(
        api_util,
        "_make_config_api_request",
        lambda **_: {"job": {"id": 123, **job}, "attempts": []},
    )
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)

    start_time = sync_result._parse_start_time(  # noqa: SLF001
        cast(Any, SimpleNamespace(start_time="not-a-date"))
    )

    assert start_time == expected


@pytest.mark.parametrize(
    "error",
    [
        cloud_mcp.AirbyteError(
            message="Forbidden.",
            context={"status_code": 403, "response": "leaked-body"},
        ),
        requests.ConnectionError("connection refused"),
        NotImplementedError("no config api root"),
        KeyError("attempts"),
    ],
)
def test_get_cloud_sync_status_isolates_attempts_errors(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    """An attempts lookup failure sets `attempts_error` and keeps the status fields."""

    def _raise() -> list[SyncAttempt]:
        raise error

    result = _status_with_attempts(monkeypatch, [], get_attempts=_raise)

    assert result["attempts"] == []
    assert result["attempts_error"].startswith("Attempts could not be read: ")
    assert result["status"] == JobStatusEnum.FAILED
    assert result["job_id"] == 123
    assert "leaked-body" not in str(result)
