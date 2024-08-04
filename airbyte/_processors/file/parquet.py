# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A Parquet cache implementation."""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import IO, TYPE_CHECKING, Literal, cast

import orjson
import ulid
from overrides import overrides
from pydantic import PrivateAttr

from airbyte_protocol.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStateType,
)

from airbyte import exceptions as exc
from airbyte._future_cdk.state_writers import StateWriterBase
from airbyte._processors.file.base import (
    FileWriterBase,
)


if TYPE_CHECKING:
    from airbyte.records import StreamRecord


class ParquetWriter(FileWriterBase):
    """An Parquet file writer implementation."""

    default_cache_file_suffix = ".parquet"
    prune_extra_fields = True

    _state_writer: StateWriterBase | None = PrivateAttr(default=None)
    _buffered_records: dict[str, list[AirbyteMessage]] = PrivateAttr(default_factory=dict)

    def _get_records_file_path(
        self,
        cache_dir: Path,
        stream_name: str,
        batch_id: str,
    ) -> Path:
        """Return the records file path for the given stream and batch."""
        return cache_dir / f"{stream_name}_{batch_id}.records.parquet"

    @overrides
    def _open_new_file(
        self,
        file_path: Path,
    ) -> IO[str]:
        """Open a new file for writing."""
        return cast(IO[str], gzip.open(file_path, "w"))

    @overrides
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

    @overrides
    def _write_record_dict(
        self,
        record_dict: StreamRecord,
        open_file_writer: IO[str],
    ) -> None:
        # If the record is too nested, `orjson` will fail with error `TypeError: Recursion
        # limit reached`. If so, fall back to the slower `json.dumps`.
        try:
            open_file_writer.write(orjson.dumps(record_dict).decode("utf-8") + "\n")
        except TypeError:
            # Using isoformat method for datetime serialization
            open_file_writer.write(json.dumps(record_dict, default=lambda _: _.isoformat()) + "\n")
