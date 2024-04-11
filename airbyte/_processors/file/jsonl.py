# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A Parquet cache implementation."""

from __future__ import annotations

import gzip
import json
from typing import IO, TYPE_CHECKING, cast

import orjson

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

    def _open_new_file(
        self,
        file_path: Path,
    ) -> IO[bytes]:
        """Open a new file for writing."""
        return cast(IO[bytes], gzip.open(file_path, "w"))

    def _write_record_dict(
        self,
        record_dict: StreamRecord,
        open_file_writer: gzip.GzipFile | IO[bytes],
    ) -> None:
        # If the record is too nested, `orjson` will fail with error `TypeError: Recursion
        # limit reached`. If so, fall back to the slower `json.dumps`.
        try:
            open_file_writer.write(orjson.dumps(record_dict) + b"\n")
        except TypeError:
            # Using isoformat method for datetime serialization
            open_file_writer.write(
                json.dumps(record_dict, default=lambda _: _.isoformat()).encode() + b"\n"
            )
