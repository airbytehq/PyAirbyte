# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Batch handle class."""

from __future__ import annotations

from contextlib import suppress
from pathlib import Path  # noqa: TCH003  # Pydantic needs this import
from typing import IO, Any, Optional

from pydantic import BaseModel, Field, PrivateAttr


class BatchHandle(BaseModel):
    """A handle for a batch of records."""

    stream_name: str
    batch_id: str

    files: list[Path] = Field(default_factory=list)
    _open_file_writer: Optional[Any] = PrivateAttr(default=None)
    _record_count: int = PrivateAttr(default=0)

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
        if self._open_file_writer is None:
            return

        with suppress(Exception):
            self._open_file_writer.close()

    def __del__(self) -> None:
        """Upon deletion, close the file writer."""
        self.close_files()
