# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A Parquet cache implementation."""

from __future__ import annotations

import gzip
import json
from typing import IO, TYPE_CHECKING, cast

import orjson
import ulid

from airbyte._future_cdk.state_writers import StateWriterBase
from airbyte._processors.file.base import (
    FileWriterBase,
)


if TYPE_CHECKING:
    from pathlib import Path

    from airbyte.records import StreamRecord


def _get_state_file_path(cache_dir: Path, stream_name: str) -> Path:
    """Return the state file path for the given stream."""
    return cache_dir / f"{stream_name}_state.parquet"


def _get_records_file_path(cache_dir: Path, stream_name: str, batch_id: str) -> Path:
    """Return the records file path for the given stream and batch."""
    return cache_dir / f"{stream_name}_{batch_id}.parquet"


class IcebergStateWriter(StateWriterBase):
    """An Iceberg state writer implementation."""

    def __init__(self, cache_dir: Path) -> None:
        """Initialize the Iceberg state writer."""
        self._cache_dir = cache_dir

    def write_state(self, state_message: dict) -> None:
        """Write the state for the given stream."""
        state_file_path = self._get_state_file_path(stream_name)
        with open(state_file_path, "w") as state_file:
            json.dump(state, state_file)


class IcebergWriter(FileWriterBase):
    """An Iceberg file writer implementation."""

    default_cache_file_suffix = ".parquet"
    prune_extra_fields = True

    def get_state_writer(self) -> IcebergStateWriter:
        return IcebergStateWriter(self._cache_dir)

    def _open_new_file(
        self,
        file_path: Path,
    ) -> IO[bytes]:
        """Open a new file for writing."""
        return cast(IO[bytes], gzip.open(file_path, "w"))

    def _get_new_cache_file_path(
        self,
        stream_name: str,
        batch_id: str | None = None,  # ULID of the batch
    ) -> Path:
        """Return a new cache file path for the given stream."""
        batch_id = batch_id or str(ulid.ULID())
        target_dir = Path(self._cache_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        return target_dir / f"{stream_name}_{batch_id}{self.default_cache_file_suffix}"

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
