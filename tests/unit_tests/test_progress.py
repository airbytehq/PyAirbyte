# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import datetime
import os
import time

import pytest
from airbyte.progress import (
    ProgressStyle,
    ProgressTracker,
    _get_elapsed_time_str,
    _to_time_str,
)
from airbyte_protocol.models import AirbyteMessage, AirbyteRecordMessage, Type
from dateutil.tz import tzlocal
from freezegun import freeze_time
from rich.errors import LiveError

# Calculate the offset from UTC in hours
tz_offset_hrs = int(datetime.datetime.now(tzlocal()).utcoffset().total_seconds() / 3600)


@pytest.fixture(scope="function")
def progress() -> ProgressTracker:
    with freeze_time("2022-01-01 00:00:00"):
        return ProgressTracker(
            source=None,
            cache=None,
            destination=None,
        )


@pytest.fixture(autouse=True)
def fixed_utc_timezone():
    """Fixture to set a fixed UTC timezone for the duration of a test."""
    original_timezone = os.environ.get("TZ")
    try:
        # Set the timezone to a fixed value, e.g., 'UTC'
        os.environ["TZ"] = "UTC"
        # Make sure the change is applied
        if hasattr(time, "tzset"):
            time.tzset()
        yield
    finally:
        # Restore the original timezone after the test
        if original_timezone is not None:
            os.environ["TZ"] = original_timezone
        else:
            del os.environ["TZ"]
        if hasattr(time, "tzset"):
            time.tzset()


@freeze_time("2022-01-01")
def test_read_progress_initialization(progress: ProgressTracker) -> None:
    assert progress.num_streams_expected == 0
    assert progress.read_start_time == 1640995200.0  # Unix timestamp for 2022-01-01
    assert progress.total_records_read == 0
    assert progress.total_records_written == 0
    assert progress.total_batches_written == 0
    assert progress.written_stream_names == set()
    assert progress.finalize_start_time is None
    assert progress.finalize_end_time is None
    assert progress.total_records_finalized == 0
    assert progress.total_batches_finalized == 0
    assert progress.finalized_stream_names == []
    assert progress._last_update_time is None


def fake_airbyte_record_message() -> AirbyteMessage:
    return AirbyteMessage(
        type=Type.RECORD,
        record=AirbyteRecordMessage(
            stream="stream1",
            data={"key": "value"},
            emitted_at=int(time.time()),
        ),
    )


@freeze_time("2022-01-01")
def test_read_progress_log_records_read(progress: ProgressTracker) -> None:
    fake_iterator = (fake_airbyte_record_message() for m in range(100))
    for m in progress.tally_records_read(fake_iterator):
        _ = m
    assert progress.total_records_read == 100


@freeze_time("2022-01-01")
def test_read_progress_log_batch_written(progress: ProgressTracker) -> None:
    progress.log_batch_written("stream1", 50)
    assert progress.total_records_written == 50
    assert progress.total_batches_written == 1
    assert progress.written_stream_names == {"stream1"}


@freeze_time("2022-01-01")
def test_read_progress_log_batches_finalizing(progress: ProgressTracker) -> None:
    progress.log_batches_finalizing("stream1", 1)
    assert progress.finalize_start_time == 1640995200.0


@freeze_time("2022-01-01")
def test_read_progress_log_batches_finalized(progress: ProgressTracker) -> None:
    progress.log_batches_finalized("stream1", 1)
    assert progress.total_batches_finalized == 1


@freeze_time("2022-01-01")
def test_read_progress_log_stream_finalized(progress: ProgressTracker) -> None:
    progress.log_stream_finalized("stream1")
    assert progress.finalized_stream_names == ["stream1"]


def test_get_elapsed_time_str():
    assert _get_elapsed_time_str(30) == "30 seconds"
    assert _get_elapsed_time_str(90) == "1min 30s"
    assert _get_elapsed_time_str(600) == "10min"
    assert _get_elapsed_time_str(3600) == "1hr 0min"


@freeze_time("2022-01-01 0:00:00")
def test_get_time_str():
    assert _to_time_str(time.time()) == "00:00:00"


def _assert_lines(expected_lines, actual_lines: list[str] | str):
    if isinstance(actual_lines, list):
        actual_lines = "\n".join(actual_lines)
    for line in expected_lines:
        assert line in actual_lines, (
            f"Missing line:\n{line}\n\nIn lines:\n\n{actual_lines}"
        )


def test_default_progress_style(monkeypatch):
    """Test the style when running in a notebook environment."""
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("NO_LIVE_PROGRESS", raising=False)
    monkeypatch.setattr("sys.stdout.isatty", lambda: True)
    progress = ProgressTracker(source=None, cache=None, destination=None)
    assert progress.style == ProgressStyle.RICH


def test_no_live_progress(monkeypatch):
    """Test the style when NO_LIVE_PROGRESS is set."""
    monkeypatch.setattr("sys.stdout.isatty", lambda: True)
    monkeypatch.setenv("NO_LIVE_PROGRESS", "1")
    progress = ProgressTracker(source=None, cache=None, destination=None)
    assert progress.style == ProgressStyle.PLAIN


def test_ci_environment_a_progress_style(monkeypatch):
    """Test the style in a CI environment."""
    monkeypatch.setattr("airbyte._util.meta.is_ci", lambda: True)
    progress = ProgressTracker(source=None, cache=None, destination=None)
    assert progress.style == ProgressStyle.PLAIN


def test_ci_environment_b_progress_style(monkeypatch):
    """Test the style in a CI environment."""
    monkeypatch.setenv("CI", "1")
    from airbyte._util import meta

    meta.is_ci.cache_clear()
    progress = ProgressTracker(source=None, cache=None, destination=None)
    assert progress.style == ProgressStyle.PLAIN


def test_rich_unavailable_progress_style(monkeypatch):
    """Test the fallback to PLAIN when Rich is unavailable."""
    monkeypatch.setattr(
        "rich.live.Live.start",
        lambda self: (_ for _ in ()).throw(LiveError("Live view not available")),
    )
    monkeypatch.setattr("rich.live.Live.stop", lambda self: None)
    progress = ProgressTracker(source=None, cache=None, destination=None)
    assert progress.style == ProgressStyle.PLAIN
