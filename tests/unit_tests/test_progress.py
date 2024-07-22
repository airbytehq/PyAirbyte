# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import datetime
import time

from airbyte.progress import (
    ProgressStyle,
    ProgressTracker,
    _get_elapsed_time_str,
    _to_time_str,
)
from dateutil.tz import tzlocal
from freezegun import freeze_time
from rich.errors import LiveError

# Calculate the offset from UTC in hours
tz_offset_hrs = int(datetime.datetime.now(tzlocal()).utcoffset().total_seconds() / 3600)


@freeze_time("2022-01-01")
def test_read_progress_initialization():
    progress = ProgressTracker()
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
    assert progress.finalized_stream_names == set()
    assert progress.last_update_time is None


@freeze_time("2022-01-01")
def test_read_progress_log_records_read():
    progress = ProgressTracker()
    fake_iterator = (m for m in range(100))
    for m in progress.tally_records_read(fake_iterator):
        _ = m
    assert progress.total_records_read == 100


@freeze_time("2022-01-01")
def test_read_progress_log_batch_written():
    progress = ProgressTracker()
    progress.log_batch_written("stream1", 50)
    assert progress.total_records_written == 50
    assert progress.total_batches_written == 1
    assert progress.written_stream_names == {"stream1"}


@freeze_time("2022-01-01")
def test_read_progress_log_batches_finalizing():
    progress = ProgressTracker()
    progress.log_batches_finalizing("stream1", 1)
    assert progress.finalize_start_time == 1640995200.0


@freeze_time("2022-01-01")
def test_read_progress_log_batches_finalized():
    progress = ProgressTracker()
    progress.log_batches_finalized("stream1", 1)
    assert progress.total_batches_finalized == 1


@freeze_time("2022-01-01")
def test_read_progress_log_stream_finalized():
    progress = ProgressTracker()
    progress.log_stream_finalized("stream1")
    assert progress.finalized_stream_names == {"stream1"}


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
        assert (
            line in actual_lines
        ), f"Missing line:\n{line}\n\nIn lines:\n\n{actual_lines}"


def test_get_status_message_after_finalizing_records():
    # Test that we can render the initial status message before starting to read
    with freeze_time("2022-01-01 00:00:00"):
        progress = ProgressTracker()
        expected_lines = [
            "Started reading from source at `00:00:00`",
            "Read **0** records over **0.00 seconds** (0.0 records / second).",
        ]
        _assert_lines(expected_lines, progress._get_status_message())

        # We need to read one record to start the "time since first record" timer
        progress.log_records_read(1)

    # Test after reading some records
    with freeze_time("2022-01-01 00:01:00"):
        progress.log_records_read(100)
        expected_lines = [
            "Started reading from source at `00:00:00`",
            "Read **100** records over **60 seconds** (1.7 records / second).",
        ]
        _assert_lines(expected_lines, progress._get_status_message())

    # Advance the day and reset the progress
    with freeze_time("2022-01-02 00:00:00"):
        progress = ProgressTracker()
        progress.reset(1)
        expected_lines = [
            "Started reading from source at `00:00:00`",
            "Read **0** records over **0.00 seconds** (0.0 records / second).",
        ]
        _assert_lines(expected_lines, progress._get_status_message())

        # We need to read one record to start the "time since first record" timer
        progress.log_records_read(1)

    # Test after writing some records and starting to finalize
    with freeze_time("2022-01-02 00:01:00"):
        progress.log_records_read(100)
        progress.log_batch_written("stream1", 50)
        progress.log_batches_finalizing("stream1", 1)
        expected_lines = [
            "## Read Progress",
            "Started reading from source at `00:00:00`",
            "Read **100** records over **60 seconds** (1.7 records / second).",
            "Cached **50** records into 1 local cache file(s).",
            "Finished reading from source at `00:01:00`",
            "Started cache processing at `00:01:00`",
        ]
        _assert_lines(expected_lines, progress._get_status_message())

    # Test after finalizing some records
    with freeze_time("2022-01-02 00:02:00"):
        progress.log_batches_finalized("stream1", 1)
        expected_lines = [
            "## Read Progress",
            "Started reading from source at `00:00:00`",
            "Read **100** records over **60 seconds** (1.7 records / second).",
            "Cached **50** records into 1 local cache file(s).",
            "Finished reading from source at `00:01:00`",
            "Started cache processing at `00:01:00`",
            "Processed **1** cache file(s) over **60 seconds**",
        ]
        _assert_lines(expected_lines, progress._get_status_message())

    # Test after finalizing all records
    with freeze_time("2022-01-02 00:02:00"):
        progress.log_stream_finalized("stream1")
        message = progress._get_status_message()
        expected_lines = [
            "## Read Progress",
            "Started reading from source at `00:00:00`",
            "Read **100** records over **60 seconds** (1.7 records / second).",
            "Cached **50** records into 1 local cache file(s).",
            "Finished reading from source at `00:01:00`",
            "Started cache processing at `00:01:00`",
            "Processed **1** cache file(s) over **60 seconds",
            "Completed processing 1 out of 1 streams",
            "- stream1",
            "Total time elapsed: 2min 0s",
        ]
        _assert_lines(expected_lines, message)


def test_default_progress_style(monkeypatch):
    """Test the style when running in a notebook environment."""
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("NO_LIVE_PROGRESS", raising=False)
    monkeypatch.setattr("sys.stdout.isatty", lambda: True)
    progress = ProgressTracker()
    assert progress.style == ProgressStyle.RICH


def test_no_live_progress(monkeypatch):
    """Test the style when NO_LIVE_PROGRESS is set."""
    monkeypatch.setattr("sys.stdout.isatty", lambda: True)
    monkeypatch.setenv("NO_LIVE_PROGRESS", "1")
    progress = ProgressTracker()
    assert progress.style == ProgressStyle.PLAIN


def test_ci_environment_a_progress_style(monkeypatch):
    """Test the style in a CI environment."""
    monkeypatch.setattr("airbyte._util.meta.is_ci", lambda: True)
    progress = ProgressTracker()
    assert progress.style == ProgressStyle.PLAIN


def test_ci_environment_b_progress_style(monkeypatch):
    """Test the style in a CI environment."""
    monkeypatch.setenv("CI", "1")
    progress = ProgressTracker()
    assert progress.style == ProgressStyle.PLAIN


def test_rich_unavailable_progress_style(monkeypatch):
    """Test the fallback to PLAIN when Rich is unavailable."""
    monkeypatch.setattr(
        "rich.live.Live.start",
        lambda self: (_ for _ in ()).throw(LiveError("Live view not available")),
    )
    monkeypatch.setattr("rich.live.Live.stop", lambda self: None)
    progress = ProgressTracker()
    assert progress.style == ProgressStyle.PLAIN
