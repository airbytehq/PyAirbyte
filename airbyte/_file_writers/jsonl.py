# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""A Parquet cache implementation."""
from __future__ import annotations

import gzip
from pathlib import Path
from typing import TYPE_CHECKING, cast

import orjson
import ulid
from overrides import overrides

from airbyte._file_writers.base import (
    FileWriterBase,
    FileWriterBatchHandle,
    FileWriterConfigBase,
)


if TYPE_CHECKING:
    import pyarrow as pa


class JsonlWriterConfig(FileWriterConfigBase):
    """Configuration for the Snowflake cache."""

    # Inherits `cache_dir` from base class


class JsonlWriter(FileWriterBase):
    """A Jsonl cache implementation."""

    config_class = JsonlWriterConfig

    def get_new_cache_file_path(
        self,
        stream_name: str,
        batch_id: str | None = None,  # ULID of the batch
    ) -> Path:
        """Return a new cache file path for the given stream."""
        batch_id = batch_id or str(ulid.ULID())
        config: JsonlWriterConfig = cast(JsonlWriterConfig, self.config)
        target_dir = Path(config.cache_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        return target_dir / f"{stream_name}_{batch_id}.jsonl.gz"

    @overrides
    def _write_batch(
        self,
        stream_name: str,
        batch_id: str,
        record_batch: pa.Table,
    ) -> FileWriterBatchHandle:
        """Process a record batch.

        Return the path to the cache file.
        """
        _ = batch_id  # unused
        output_file_path = self.get_new_cache_file_path(stream_name)

        with gzip.open(output_file_path, "w") as jsonl_file:
            for record in record_batch.to_pylist():
                jsonl_file.write(orjson.dumps(record) + b"\n")

        batch_handle = FileWriterBatchHandle()
        batch_handle.files.append(output_file_path)
        return batch_handle
