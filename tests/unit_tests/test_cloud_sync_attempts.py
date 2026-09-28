# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for structured sync attempt failures."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast

import pytest
from airbyte._util import api_util
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.models import JobStatusEnum
from airbyte.cloud.sync_results import SyncAttempt, SyncAttemptFailure, SyncResult
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
        get_job_status=lambda: JobStatusEnum.FAILED,
        get_attempts=lambda: [
            _attempt(FAILED_ATTEMPT, 0),
            _attempt(succeeded_attempt, 1),
        ],
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


def test_get_cloud_sync_status_caps_failure_messages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Long attempt failure messages are cut and flagged in `get_cloud_sync_status`."""
    long_attempt = {
        "attempt": {
            "id": 1,
            "status": "failed",
            "createdAt": 1767225600,
            "failureSummary": {
                "failures": [
                    {"failureOrigin": "source", "externalMessage": "x" * 50_000}
                ]
            },
        }
    }
    sync_result = SimpleNamespace(
        job_id=123,
        bytes_synced=0,
        records_synced=0,
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        job_url="https://cloud.example.com/jobs",
        get_job_status=lambda: JobStatusEnum.FAILED,
        get_attempts=lambda: [_attempt(long_attempt, 0)],
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

    (failure,) = result["attempts"][0]["failures"]
    assert len(failure["external_message"]) == cloud_mcp.TROUBLESHOOT_MAX_MESSAGE_CHARS
    assert failure["external_message"].endswith(cloud_mcp.TRUNCATION_MARKER)
    assert failure["external_message_truncated"] is True
