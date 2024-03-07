# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Batch handle class."""

from __future__ import annotations

from typing import IO, TYPE_CHECKING, Callable


if TYPE_CHECKING:
    from pathlib import Path


class BatchHandle:
    """A handle for a batch of records."""

    def __init__(
        self,
        stream_name: str,
        batch_id: str,
        files: list[Path],
        file_opener: Callable[[Path], IO[bytes]],
    ) -> None:
        """Initialize the batch handle."""
        self._stream_name = stream_name
        self._batch_id = batch_id
        self._files = files
        self._record_count = 0
        assert self._files, "A batch must have at least one file."
        self._open_file_writer: IO[bytes] = file_opener(self._files[0])

    @property
    def files(self) -> list[Path]:
        """Return the files."""
        return self._files

    @property
    def batch_id(self) -> str:
        """Return the batch ID."""
        return self._batch_id

    @property
    def stream_name(self) -> str:
        """Return the stream name."""
        return self._stream_name

    @property
    def record_count(self) -> int:
        """Return the record count."""
        return self._record_count

    def increment_record_count(self) -> None:
        """Increment the record count."""
        self._record_count += 1

    @property
    def open_file_writer(self) -> IO[bytes] | None:
        """Return the open file writer, if any, or None."""
        return self._open_file_writer

    def close_files(self) -> None:
        """Close the file writer."""
        if self.open_file_writer is None:
            return

        # with suppress(Exception):
        self.open_file_writer.close()

    def __del__(self) -> None:
        """Upon deletion, close the file writer."""
        self.close_files()
