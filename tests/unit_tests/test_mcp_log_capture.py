# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for the MCP log-capture helper and stderr plumbing.

These tests validate that:

* The `_LogCapture` context manager opens / closes / cleans up the right files
  given `log_file_path` and `include_logs_in_response` combinations.
* `_stream_from_subprocess(log_file=...)` actually redirects subprocess stderr
  into the supplied file (this is the underlying primitive that the MCP layer
  wires through `Executor.execute`).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from airbyte._executors.base import _stream_from_subprocess
from airbyte.mcp.local import _LogCapture


def test_log_capture_no_capture_yields_no_file() -> None:
    """`_LogCapture` should not open a file when no path and no readback are requested."""
    with _LogCapture(log_file_path=None, include_in_response=False) as cap:
        assert cap.file is None
        assert cap.read_logs() == ""


def test_log_capture_explicit_path_writes_and_persists(tmp_path: Path) -> None:
    """Explicit `log_file_path` should be written to and kept on disk after exit."""
    log_path = tmp_path / "subdir" / "stderr.log"

    with _LogCapture(log_file_path=log_path, include_in_response=False) as cap:
        assert cap.file is not None
        cap.file.write("hello stderr\n")
        cap.file.flush()

    assert log_path.exists(), "log file should be persisted after the context exits"
    assert log_path.read_text(encoding="utf-8") == "hello stderr\n"


def test_log_capture_explicit_path_creates_parent_dirs(tmp_path: Path) -> None:
    """Parent directories should be auto-created."""
    log_path = tmp_path / "deeply" / "nested" / "stderr.log"

    with _LogCapture(log_file_path=log_path, include_in_response=False) as cap:
        assert cap.file is not None
        cap.file.write("nested\n")

    assert log_path.exists()
    assert log_path.read_text(encoding="utf-8") == "nested\n"


def test_log_capture_temp_path_when_response_requested() -> None:
    """When `include_in_response=True` without an explicit path, a temp file is used and removed."""
    with _LogCapture(log_file_path=None, include_in_response=True) as cap:
        assert cap.file is not None
        cap.file.write("temp logs\n")
        # Read inside the with-block, before cleanup happens.
        captured = cap.read_logs()
        temp_path: Path | None = cap._path  # noqa: SLF001  # Implementation detail under test

    assert captured == "temp logs\n"
    assert temp_path is not None
    assert not temp_path.exists(), "temp file should be removed after context exit"


def test_log_capture_explicit_path_with_readback(tmp_path: Path) -> None:
    """Explicit path + `include_in_response=True` should both persist file and return contents."""
    log_path = tmp_path / "stderr.log"

    with _LogCapture(log_file_path=log_path, include_in_response=True) as cap:
        assert cap.file is not None
        cap.file.write("dual capture\n")
        captured = cap.read_logs()

    assert captured == "dual capture\n"
    assert log_path.exists(), (
        "explicit path should persist even when readback is requested"
    )
    assert log_path.read_text(encoding="utf-8") == "dual capture\n"


def test_log_capture_overwrites_existing_file(tmp_path: Path) -> None:
    """Opening an existing log path should overwrite (write-mode), not append."""
    log_path = tmp_path / "stderr.log"
    log_path.write_text("previous contents\n", encoding="utf-8")

    with _LogCapture(log_file_path=log_path, include_in_response=False) as cap:
        assert cap.file is not None
        cap.file.write("fresh\n")

    assert log_path.read_text(encoding="utf-8") == "fresh\n"


def test_log_capture_read_without_capture_returns_empty_string() -> None:
    """`read_logs()` should be a safe no-op string when no capture was configured."""
    with _LogCapture(log_file_path=None, include_in_response=False) as cap:
        assert cap.read_logs() == ""


def test_stream_from_subprocess_redirects_stderr_to_log_file(tmp_path: Path) -> None:
    """`_stream_from_subprocess(log_file=...)` should redirect subprocess stderr to the file."""
    log_path = tmp_path / "subprocess-stderr.log"
    stderr_message = "this should land in the log file"
    stdout_message = "STDOUT_LINE"

    script = (
        "import sys; "
        f"sys.stderr.write({stderr_message!r} + chr(10)); "
        f"sys.stdout.write({stdout_message!r} + chr(10))"
    )

    with log_path.open("w", encoding="utf-8") as log_file:
        with _stream_from_subprocess(
            [sys.executable, "-c", script],
            log_file=log_file,
        ) as line_iter:
            stdout_lines = list(line_iter)

    assert any(stdout_message in line for line in stdout_lines)

    captured = log_path.read_text(encoding="utf-8")
    assert stderr_message in captured, (
        f"expected stderr_message in log file, got: {captured!r}"
    )


@pytest.mark.parametrize(
    ("log_file_path", "include_in_response", "expect_capture"),
    [
        pytest.param(None, False, False, id="no-capture"),
        pytest.param("explicit", False, True, id="explicit-path-no-readback"),
        pytest.param(None, True, True, id="temp-path-with-readback"),
        pytest.param("explicit", True, True, id="explicit-path-with-readback"),
    ],
)
def test_log_capture_file_attribute_matrix(
    tmp_path: Path,
    *,
    log_file_path: str | None,
    include_in_response: bool,
    expect_capture: bool,
) -> None:
    """Matrix test: `cap.file` is a writable handle if and only if capture is requested."""
    path: Path | None = tmp_path / "stderr.log" if log_file_path == "explicit" else None
    with _LogCapture(
        log_file_path=path, include_in_response=include_in_response
    ) as cap:
        if expect_capture:
            assert cap.file is not None
            cap.file.write("matrix\n")
        else:
            assert cap.file is None
