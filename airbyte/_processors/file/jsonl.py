# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A Parquet cache implementation."""

from __future__ import annotations

import gzip
import io
import json
import queue
import threading
from typing import IO, TYPE_CHECKING, Any, cast

import orjson
from overrides import overrides

from airbyte._processors.file.base import (
    FileWriterBase,
)


if TYPE_CHECKING:
    from pathlib import Path

    from airbyte.records import StreamRecord


class JsonlWriter(FileWriterBase):
    """A Jsonl cache implementation."""

    default_cache_file_suffix = ".jsonl.gz"
    prune_extra_fields = True

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: ANN401
        self._current_file_writer: IO[str] | None = None
        self._queue: queue.Queue[str] = queue.Queue()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._write_worker)
        self._thread.start()
        super().__init__(*args, **kwargs)

    @overrides
    def _open_new_file(
        self,
        file_path: Path,
    ) -> IO[str]:
        """Open a new file for writing."""
        # buffer_size = 1024 * 1024  # 1 MB

        gzip_file: gzip.GzipFile = gzip.open(
            file_path,
            mode="wb",
        )
        self._current_file_writer = io.TextIOWrapper(
            io.BufferedWriter(
                cast(io.RawIOBase, gzip_file),
                # buffer_size=buffer_size,
            ),
            encoding="utf-8",
        )
        return self._current_file_writer

    def _write_worker(self) -> None:
        while not self._stop_event.is_set() or not self._queue.empty():
            try:
                record = self._queue.get(timeout=1)
                assert self._current_file_writer is not None
                self._current_file_writer.write(record)
                self._queue.task_done()
            except queue.Empty:
                continue

    def close(self) -> None:
        """Close the writer and wait for the worker thread to finish."""
        self._queue.join()
        self._stop_event.set()
        self._thread.join()
        assert self._current_file_writer is not None
        self._current_file_writer.close()

    @overrides
    def _write_record_dict(
        self,
        record_dict: StreamRecord,
        open_file_writer: IO[str],
    ) -> None:
        # If the record is too nested, `orjson` will fail with error `TypeError: Recursion
        # limit reached`. If so, fall back to the slower `json.dumps`.
        try:
            line = orjson.dumps(record_dict).decode(encoding="utf-8") + "\n"
        except TypeError:
            # Using isoformat method for datetime serialization
            line = json.dumps(record_dict, default=lambda _: _.isoformat()) + "\n"
        else:
            # No exception occurred, so write the line to the file
            if open_file_writer is not self._current_file_writer:
                self._current_file_writer = open_file_writer

            self._queue.put(line)
