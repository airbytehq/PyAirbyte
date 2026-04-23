# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Tests for local-sync state-message observation in `ProgressTracker`."""

from __future__ import annotations

import json

import pytest
from airbyte._local_sync_progress import (
    _try_parse_datetime_cursor,
    compute_stream_progress_pct,
    extract_cursor_from_state_message,
)
from airbyte.progress import ProgressStyle, ProgressTracker
from airbyte_protocol.models import (
    AirbyteMessage,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    StreamDescriptor,
    Type,
)


def _state_msg(
    stream: str, cursor_value: str, cursor_field: str = "updatedAt"
) -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.STATE,
        state=AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name=stream),
                stream_state={cursor_field: cursor_value},
            ),
        ),
    )


def test_try_parse_datetime_cursor_rejects_numeric_strings() -> None:
    assert _try_parse_datetime_cursor("12345") is None
    assert _try_parse_datetime_cursor("2024-01-01T00:00:00Z") is not None


def test_extract_cursor_from_state_message_prefers_known_fields() -> None:
    msg = _state_msg("contacts", "2024-06-15T10:30:00Z", cursor_field="updatedAt")
    name, field, value = extract_cursor_from_state_message(msg.state)
    assert name == "contacts"
    assert field == "updatedAt"
    assert value == "2024-06-15T10:30:00Z"


def test_extract_cursor_falls_back_to_first_datetime_value() -> None:
    msg = AirbyteMessage(
        type=Type.STATE,
        state=AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name="events"),
                stream_state={"unknown_field": "2024-01-01T00:00:00Z"},
            ),
        ),
    )
    name, field, value = extract_cursor_from_state_message(msg.state)
    assert name == "events"
    assert field == "unknown_field"
    assert value == "2024-01-01T00:00:00Z"


def test_compute_stream_progress_pct_basic() -> None:
    pct = compute_stream_progress_pct(
        baseline_cursor="2024-01-01T00:00:00Z",
        latest_cursor="2024-07-01T00:00:00Z",
        now=None,
    )
    # We're past 2024, so from Jan 1 -> Jul 1 -> today should be a valid [0, 1]
    assert pct is None or 0.0 <= pct <= 1.0


def test_compute_stream_progress_pct_returns_none_for_non_datetime() -> None:
    assert (
        compute_stream_progress_pct(
            baseline_cursor="not-a-date",
            latest_cursor="also-not-a-date",
        )
        is None
    )


def test_tally_records_read_observes_source_state() -> None:
    tracker = ProgressTracker(
        ProgressStyle.NONE,
        source=None,
        cache=None,
        destination=None,
    )
    messages = [
        _state_msg("contacts", "2024-01-01T00:00:00Z"),
        _state_msg("contacts", "2024-06-15T10:30:00Z"),
        _state_msg("companies", "2024-03-15T00:00:00Z"),
    ]
    list(tracker.tally_records_read(iter(messages)))

    assert tracker.stream_baseline_cursors == {
        "contacts": "2024-01-01T00:00:00Z",
        "companies": "2024-03-15T00:00:00Z",
    }
    assert tracker.stream_latest_cursors == {
        "contacts": "2024-06-15T10:30:00Z",
        "companies": "2024-03-15T00:00:00Z",
    }
    assert tracker.stream_cursor_fields == {
        "contacts": "updatedAt",
        "companies": "updatedAt",
    }


def test_tally_confirmed_writes_observes_committed_state() -> None:
    tracker = ProgressTracker(
        ProgressStyle.NONE,
        source=None,
        cache=None,
        destination=None,
    )
    messages = [_state_msg("contacts", "2024-06-15T10:30:00Z")]
    list(tracker.tally_confirmed_writes(iter(messages)))

    assert tracker.stream_committed_cursors == {"contacts": "2024-06-15T10:30:00Z"}
    # Source-side fields should NOT be populated from committed state alone.
    assert tracker.stream_baseline_cursors == {}
    assert tracker.stream_latest_cursors == {}


def test_tally_pending_writes_observes_source_state() -> None:
    tracker = ProgressTracker(
        ProgressStyle.NONE,
        source=None,
        cache=None,
        destination=None,
    )
    messages = [_state_msg("contacts", "2024-02-01T00:00:00Z")]
    list(tracker.tally_pending_writes(iter(messages)))

    assert tracker.stream_latest_cursors == {"contacts": "2024-02-01T00:00:00Z"}


def test_build_progress_snapshot_shape() -> None:
    tracker = ProgressTracker(
        ProgressStyle.NONE,
        source=None,
        cache=None,
        destination=None,
    )
    list(
        tracker.tally_records_read(
            iter([
                _state_msg("contacts", "2024-01-01T00:00:00Z"),
                _state_msg("contacts", "2024-06-15T10:30:00Z"),
            ])
        )
    )
    snapshot = tracker._build_progress_snapshot()  # noqa: SLF001
    assert "timestamp" in snapshot
    assert "streams" in snapshot
    contacts = next(s for s in snapshot["streams"] if s["stream_name"] == "contacts")
    assert contacts["baseline_cursor"] == "2024-01-01T00:00:00Z"
    assert contacts["latest_cursor"] == "2024-06-15T10:30:00Z"
    assert contacts["cursor_field"] == "updatedAt"


def test_progress_audit_log_written(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("AIRBYTE_LOGGING_ROOT", str(tmp_path))
    # Override the module-level AIRBYTE_LOGGING_ROOT (resolved at import time).
    from airbyte import progress as progress_module

    monkeypatch.setattr(progress_module, "AIRBYTE_LOGGING_ROOT", tmp_path)

    tracker = ProgressTracker(
        ProgressStyle.NONE,
        source=None,
        cache=None,
        destination=None,
    )
    list(
        tracker.tally_records_read(
            iter([
                _state_msg("contacts", "2024-01-01T00:00:00Z"),
                _state_msg("contacts", "2024-06-15T10:30:00Z"),
            ])
        )
    )
    tracker._update_display(force_refresh=True)  # noqa: SLF001

    audit_path = tracker._get_progress_audit_log_path()  # noqa: SLF001
    assert audit_path is not None
    assert audit_path.exists()

    lines = audit_path.read_text().strip().splitlines()
    assert lines, "expected at least one JSONL line"
    last = json.loads(lines[-1])
    assert "streams" in last
    contacts = next(s for s in last["streams"] if s["stream_name"] == "contacts")
    assert contacts["latest_cursor"] == "2024-06-15T10:30:00Z"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
