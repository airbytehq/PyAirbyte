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
from airbyte.cloud.models import CloudJobInfo, JobStatusEnum
from airbyte.cloud.sync_results import (
    SyncAttempt,
    SyncAttemptFailure,
    SyncJobSnapshot,
    SyncResult,
)
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteError
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
        get_job_snapshot=lambda: SyncJobSnapshot(
            status=JobStatusEnum.FAILED,
            bytes_synced=0,
            records_synced=0,
            start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ),
        get_attempts=lambda: [_attempt(long_attempt, 0)],
        get_raw_attempt_count=lambda: 1,
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


def test_sync_result_job_snapshot_fetches_job_info_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A running job's snapshot reads status, counts and start time from one lookup."""
    lookups: list[int] = []

    def _get_job_info(*, job_id: int, **_kwargs: object) -> SimpleNamespace:
        lookups.append(job_id)
        return SimpleNamespace(
            job_id=job_id,
            status="running",
            bytes_synced=10 * len(lookups),
            rows_synced=2,
            start_time="2026-01-01T00:00:00Z",
        )

    monkeypatch.setattr(api_util, "get_job_info", _get_job_info)
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)

    snapshot = sync_result.get_job_snapshot()

    assert lookups == [123]
    assert snapshot.status == JobStatusEnum.RUNNING
    assert snapshot.bytes_synced == 10
    assert snapshot.records_synced == 2
    assert snapshot.start_time == datetime(2026, 1, 1, tzinfo=timezone.utc)


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


def test_failures_skip_non_dict_items() -> None:
    """Non-dict failure entries and a malformed summary are ignored."""
    attempt = _attempt({
        "attempt": {
            "id": 1,
            "status": "failed",
            "createdAt": 1767225600,
            "failureSummary": {
                "failures": [None, "bad", {"externalMessage": "Real failure."}]
            },
        }
    })
    malformed_summary = _attempt({
        "attempt": {"id": 2, "status": "failed", "createdAt": 1, "failureSummary": "x"}
    })

    assert [f.external_message for f in attempt.failures] == ["Real failure."]
    assert malformed_summary.failures == []


@pytest.mark.parametrize(
    ("logs", "expected"),
    [
        pytest.param({"events": [None, "oops"]}, "", id="non-dict-events"),
        pytest.param({"events": {}}, "", id="events-dict"),
        pytest.param({"logLines": ["a", None]}, "a", id="non-str-log-line"),
        pytest.param("not-a-dict", "", id="logs-not-dict"),
    ],
)
def test_get_full_log_text_tolerates_malformed_logs(
    logs: object, expected: str
) -> None:
    """Malformed log payloads yield the readable lines instead of raising."""
    attempt = _attempt({"attempt": {"id": 1, "status": "failed"}, "logs": logs})

    assert attempt.get_full_log_text() == expected


def test_failures_coerce_field_types() -> None:
    """Non-string text fields and non-bool `retryable` become `None`."""
    attempt = _attempt({
        "attempt": {
            "id": 1,
            "status": "failed",
            "createdAt": 1767225600,
            "failureSummary": {
                "failures": [
                    {
                        "failureOrigin": 7,
                        "failureType": ["x"],
                        "externalMessage": {"text": "nested"},
                        "retryable": "yes",
                    }
                ]
            },
        }
    })

    assert attempt.failures == [
        SyncAttemptFailure(
            failure_origin=None,
            failure_type=None,
            external_message=None,
            retryable=None,
        )
    ]


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


def test_get_cloud_sync_status_isolates_malformed_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An attempt missing `createdAt` gets an `error`; other attempts are unaffected."""
    missing_created_at = {"attempt": {"id": 12, "status": "failed"}}

    result = _status_with_attempts(
        monkeypatch,
        [_attempt(FAILED_ATTEMPT, 0), _attempt(missing_created_at, 1)],
    )

    good, bad = result["attempts"]
    assert good["created_at"] == "2026-01-01T00:00:00+00:00"
    assert "error" not in good
    assert bad["attempt_number"] == 1
    assert bad["error"].startswith("Attempt 1 could not be read: ")
    assert len(bad["error"]) <= cloud_mcp.TROUBLESHOOT_MAX_MESSAGE_CHARS
    assert result["status"] == "failed"


def test_get_cloud_sync_status_filters_failure_stack_traces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failure messages drop stack-trace lines in `get_cloud_sync_status`."""
    attempt = {
        "attempt": {
            "id": 1,
            "status": "failed",
            "createdAt": 1767225600,
            "failureSummary": {
                "failures": [
                    {
                        "failureOrigin": "source",
                        "externalMessage": "Sync failed.\n"
                        "java.lang.IllegalStateException: leaked\n"
                        "\tat io.airbyte.Foo.bar(Foo.java:12) ~[io.airbyte-foo.jar:?]",
                    }
                ]
            },
        }
    }

    result = _status_with_attempts(monkeypatch, [_attempt(attempt, 0)])

    failure = result["attempts"][0]["failures"][0]
    assert failure["external_message"] == "Sync failed. [stack trace removed]"


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


def test_parse_start_time_without_fallback_reraises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without job epoch timestamps, the original parse error propagates."""
    monkeypatch.setattr(
        api_util, "_make_config_api_request", lambda **_: {"job": {"id": 123}}
    )
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)

    with pytest.raises(ValueError, match="not-a-date"):
        sync_result._parse_start_time(  # noqa: SLF001
            cast(Any, SimpleNamespace(start_time="not-a-date"))
        )


@pytest.mark.parametrize(
    ("attempts", "raw_count", "readable_count"),
    [
        pytest.param([FAILED_ATTEMPT], 1, 1, id="all-readable"),
        pytest.param([FAILED_ATTEMPT, None, "oops"], 3, 1, id="some-dropped"),
        pytest.param([None, 5], 2, 0, id="all-dropped"),
        pytest.param(None, 0, 0, id="null"),
    ],
)
def test_get_raw_attempt_count_includes_skipped_attempts(
    monkeypatch: pytest.MonkeyPatch,
    attempts: object,
    raw_count: int,
    readable_count: int,
) -> None:
    """`get_raw_attempt_count` counts entries `get_attempts` skips as malformed."""
    monkeypatch.setattr(
        api_util,
        "_make_config_api_request",
        lambda **_: {"job": {"id": 123}, "attempts": attempts},
    )
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)

    assert len(sync_result.get_attempts()) == readable_count
    assert sync_result.get_raw_attempt_count() == raw_count


def test_parse_start_time_none_falls_back_to_job_epoch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A null start time falls back to the Config API job's `startedAt`."""
    monkeypatch.setattr(
        api_util,
        "_make_config_api_request",
        lambda **_: {"job": {"id": 123, "startedAt": 1767225600}, "attempts": []},
    )
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)

    start_time = sync_result._parse_start_time(  # noqa: SLF001
        CloudJobInfo(job_id=123, status=JobStatusEnum.FAILED, start_time=None)
    )

    assert start_time == datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize("bad_epoch", [10**20, -(10**20), float("nan")])
def test_parse_start_time_skips_invalid_epoch(
    monkeypatch: pytest.MonkeyPatch, bad_epoch: float
) -> None:
    """An out-of-range `startedAt` is skipped in favor of `createdAt`."""
    monkeypatch.setattr(
        api_util,
        "_make_config_api_request",
        lambda **_: {
            "job": {"id": 123, "startedAt": bad_epoch, "createdAt": 1767225600},
            "attempts": [],
        },
    )
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)

    start_time = sync_result._parse_start_time(  # noqa: SLF001
        cast(Any, SimpleNamespace(start_time="not-a-date"))
    )

    assert start_time == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_parse_start_time_all_invalid_epochs_reraise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When every fallback epoch is invalid, the original parse error propagates."""
    monkeypatch.setattr(
        api_util,
        "_make_config_api_request",
        lambda **_: {"job": {"id": 123, "startedAt": 10**20, "createdAt": 10**20}},
    )
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)

    with pytest.raises(ValueError, match="not-a-date"):
        sync_result._parse_start_time(  # noqa: SLF001
            cast(Any, SimpleNamespace(start_time="not-a-date"))
        )


def test_previous_sync_logs_tolerates_null_start_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A job with a null `startTime` does not wipe out the other jobs in the page."""
    jobs = [
        SimpleNamespace(
            job_id=2,
            status="failed",
            bytes_synced=None,
            rows_synced=None,
            start_time=None,
        ),
        SimpleNamespace(
            job_id=1,
            status="succeeded",
            bytes_synced=10,
            rows_synced=1,
            start_time="2026-01-01T00:00:00Z",
        ),
    ]
    monkeypatch.setattr(api_util, "get_job_logs", lambda **_: jobs)
    workspace = cast(
        CloudWorkspace,
        SimpleNamespace(
            workspace_id="workspace-id",
            api_root="https://api.example.com/v1",
            client_id=None,
            client_secret=None,
            bearer_token="token",
        ),
    )
    connection = CloudConnection(workspace=workspace, connection_id="connection-id")

    results = connection.get_previous_sync_logs(limit=5)

    assert [result.job_id for result in results] == [2, 1]
    assert results[1].start_time == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_get_cloud_sync_status_bounds_attempts_and_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only the last 10 attempts and the first 5 failures per attempt are returned."""
    failures = [
        {"failureOrigin": "source", "externalMessage": f"failure {index}"}
        for index in range(7)
    ]
    attempts = [
        _attempt(
            {
                "attempt": {
                    "id": number,
                    "status": "failed",
                    "createdAt": 1767225600,
                    "failureSummary": {"failures": failures},
                }
            },
            number,
        )
        for number in range(12)
    ]

    result = _status_with_attempts(monkeypatch, attempts)

    assert result["attempts_omitted"] == 2
    assert [a["attempt_number"] for a in result["attempts"]] == list(range(2, 12))
    last = result["attempts"][-1]
    assert [f["external_message"] for f in last["failures"]] == [
        f"failure {index}" for index in range(5)
    ]
    assert all(a["failures_omitted"] == 2 for a in result["attempts"])


def test_get_cloud_sync_status_bounds_attempt_strings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Attempt status and failure origin/type are capped in `get_cloud_sync_status`."""
    huge = "y" * 5_000
    attempt = _attempt({
        "attempt": {
            "id": 1,
            "status": huge,
            "createdAt": 1767225600,
            "failureSummary": {
                "failures": [{"failureOrigin": huge, "failureType": huge}]
            },
        }
    })

    entry = _status_with_attempts(monkeypatch, [attempt])["attempts"][0]

    limit = cloud_mcp.TROUBLESHOOT_MAX_MESSAGE_CHARS
    assert len(entry["status"]) == limit
    assert len(entry["failures"][0]["failure_origin"]) == limit
    assert len(entry["failures"][0]["failure_type"]) == limit


@pytest.mark.parametrize("body", [None, [], "leaked-body"])
def test_fetch_job_with_attempts_rejects_non_dict_body(
    monkeypatch: pytest.MonkeyPatch, body: object
) -> None:
    """A non-object Config API job body raises without carrying the body."""
    monkeypatch.setattr(api_util, "_make_config_api_request", lambda **_: body)
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)

    with pytest.raises(AirbyteError, match="Unexpected API response.") as exc_info:
        sync_result.get_attempts()

    assert exc_info.value.context == {"job_id": 123}
    assert "leaked-body" not in str(exc_info.value)


@pytest.mark.parametrize("body", [None, [], "leaked-body"])
def test_get_cloud_sync_status_non_dict_job_body(
    monkeypatch: pytest.MonkeyPatch, body: object
) -> None:
    """A non-object job body yields `attempts_error`, keeping the status fields."""
    monkeypatch.setattr(api_util, "_make_config_api_request", lambda **_: body)
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)

    result = _status_with_attempts(
        monkeypatch, cast(Any, None), get_attempts=sync_result.get_attempts
    )

    assert result["attempts"] == []
    assert result["attempts_error"] == (
        "Attempts could not be read: Unexpected API response."
    )
    assert result["status"] == JobStatusEnum.FAILED
    assert "leaked-body" not in str(result)


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


def test_get_cloud_sync_status_reports_unreadable_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Attempts the API returned but that could not be read are counted."""
    attempt = _attempt({
        "attempt": {"id": 1, "status": "failed", "createdAt": 1767225600}
    })

    result = _status_with_attempts(monkeypatch, [attempt], raw_attempt_count=3)

    assert result["attempts_unreadable"] == 2
    assert "attempts_error" not in result
    assert len(result["attempts"]) == 1


def test_get_cloud_sync_status_omits_unreadable_when_all_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`attempts_unreadable` is absent when every attempt was read."""
    attempt = _attempt({
        "attempt": {"id": 1, "status": "failed", "createdAt": 1767225600}
    })

    result = _status_with_attempts(monkeypatch, [attempt])

    assert "attempts_unreadable" not in result


@pytest.mark.parametrize(
    "attempts", [{"0": FAILED_ATTEMPT}, "leaked-body", 5, True], ids=type
)
def test_non_list_attempts_raise_unexpected_response(
    monkeypatch: pytest.MonkeyPatch, attempts: object
) -> None:
    """A non-list `attempts` value is an API error, not a job without attempts."""
    monkeypatch.setattr(
        api_util,
        "_make_config_api_request",
        lambda **_: {"job": {"id": 123}, "attempts": attempts},
    )
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)

    with pytest.raises(AirbyteError, match="Unexpected API response.") as exc_info:
        sync_result.get_attempts()
    with pytest.raises(AirbyteError, match="Unexpected API response."):
        sync_result.get_raw_attempt_count()

    assert exc_info.value.context == {"job_id": 123}
    assert "leaked-body" not in str(exc_info.value)


@pytest.mark.parametrize(
    "start_time", ["2026-01-01T00:00:00+24:00", "2026-01-01T00:00:00+99:99"]
)
def test_parse_start_time_out_of_range_offset_falls_back(
    monkeypatch: pytest.MonkeyPatch, start_time: str
) -> None:
    """A start time whose UTC offset cannot be rendered falls back to the job's epoch."""
    monkeypatch.setattr(
        api_util,
        "_make_config_api_request",
        lambda **_: {"job": {"id": 123, "startedAt": 1767225600}, "attempts": []},
    )
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)

    parsed = sync_result._parse_start_time(  # noqa: SLF001
        CloudJobInfo(job_id=123, status=JobStatusEnum.FAILED, start_time=start_time)
    )

    assert parsed.isoformat() == "2026-01-01T00:00:00+00:00"


def test_parse_start_time_is_converted_to_utc() -> None:
    """A start time with another offset is returned in UTC."""
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)

    parsed = sync_result._parse_start_time(  # noqa: SLF001
        CloudJobInfo(
            job_id=123,
            status=JobStatusEnum.FAILED,
            start_time="2026-01-01T02:00:00+02:00",
        )
    )

    assert parsed == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert parsed.utcoffset() == timezone.utc.utcoffset(None)


@pytest.mark.parametrize(
    "start_time", ["0001-01-01T00:00:00+05:00", "9999-12-31T23:59:59-05:00"]
)
def test_parse_start_time_boundary_date_falls_back(
    monkeypatch: pytest.MonkeyPatch, start_time: str
) -> None:
    """A start time that overflows when converted to UTC falls back to the job's epoch."""
    monkeypatch.setattr(
        api_util,
        "_make_config_api_request",
        lambda **_: {"job": {"id": 123, "startedAt": 1767225600}, "attempts": []},
    )
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)

    parsed = sync_result._parse_start_time(  # noqa: SLF001
        CloudJobInfo(job_id=123, status=JobStatusEnum.FAILED, start_time=start_time)
    )

    assert parsed == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_sync_status_attempt_drops_wrongly_typed_values() -> None:
    """Attempt values of the wrong type are reported as null instead of passed through."""
    attempt = cast(
        Any,
        SimpleNamespace(
            attempt_number=0,
            attempt_id="x" * 50_000,
            status=["failed"],
            bytes_synced={"bytes": 1},
            records_synced=True,
            created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            failures=[],
        ),
    )

    entry = cloud_mcp._sync_status_attempt(attempt)  # noqa: SLF001

    assert entry["attempt_id"] is None
    assert entry["status"] is None
    assert entry["bytes_synced"] is None
    assert entry["records_synced"] is None
    assert entry["created_at"] == "2026-01-01T00:00:00+00:00"


def test_parse_start_time_overflow_without_fallback_raises_value_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An overflowing start time with no usable fallback raises an error callers handle."""
    monkeypatch.setattr(
        api_util, "_make_config_api_request", lambda **_: {"job": {"id": 123}}
    )
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)

    with pytest.raises(ValueError, match="start time is out of range"):
        sync_result._parse_start_time(  # noqa: SLF001
            CloudJobInfo(
                job_id=123,
                status=JobStatusEnum.FAILED,
                start_time="0001-01-01T00:00:00+05:00",
            )
        )


@pytest.mark.parametrize(
    "timestamp",
    [
        "99999999999999999999-01-01T00:00:00",
        "2026-01-01T12:00999999999999999999999999999999",
    ],
)
def test_out_of_range_timestamps_raise_value_error(
    monkeypatch: pytest.MonkeyPatch, timestamp: str
) -> None:
    """A timestamp that overflows while parsing raises an error callers handle."""
    monkeypatch.setattr(
        api_util, "_make_config_api_request", lambda **_: {"job": {"id": 123}}
    )
    sync_result = SyncResult(workspace=WORKSPACE, connection=CONNECTION, job_id=123)
    attempt = SyncAttempt(
        workspace=WORKSPACE,
        connection=CONNECTION,
        job_id=123,
        attempt_number=0,
        _attempt_data={"attempt": {"createdAt": timestamp}},
    )

    with pytest.raises(ValueError, match="out of range"):
        _ = attempt.created_at
    with pytest.raises(ValueError, match="out of range"):
        sync_result._parse_start_time(  # noqa: SLF001
            CloudJobInfo(job_id=123, status=JobStatusEnum.FAILED, start_time=timestamp)
        )
