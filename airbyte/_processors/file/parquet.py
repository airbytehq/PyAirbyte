# Copyright (c) 2023 Airbyte, Inc., all rights reserved
"""A Parquet file writer implementation.

NOTE: Parquet is a strongly typed columnar storage format, which has known issues when applied to
variable schemas, schemas with indeterminate types, and schemas that have empty data nodes.
This implementation is deprecated for now in favor of jsonl.gz, and may be removed or revamped in
the future.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte import exceptions as exc
from airbyte._processors.file.base import FileWriterBase
from airbyte._util.text_util import lower_case_set


if TYPE_CHECKING:
    import pyarrow as pa


class ParquetWriter(FileWriterBase):
    """A Parquet cache implementation."""

    default_cache_file_suffix = ".parquet"

    def _get_missing_columns(
        self,
        stream_name: str,
        record_batch: pa.Table,  # TODO: Refactor to remove dependency on pyarrow
    ) -> list[str]:
        """Return a list of columns that are missing in the batch.

        The comparison is based on a case-insensitive comparison of the column names.
        """
        if not self.cache.processor._catalog_manager:
            raise exc.AirbyteLibInternalError(message="Catalog manager should exist but does not.")
        stream = self.cache.processor._catalog_manager.get_stream_config(stream_name)
        stream_property_names = stream.stream.json_schema["properties"].keys()
        return [
            col
            for col in stream_property_names
            if col.lower() not in lower_case_set(record_batch.schema.names)
        ]

    # TODO: Delete if not needed
    # @overrides
    # def _write_batch(
    #     self,
    #     stream_name: str,
    #     batch_id: str,
    #     record_batch: pa.Table,  # TODO: Refactor to remove dependency on pyarrow
    # ) -> BatchHandle:
    #     """Process a record batch.

    #     Return the path to the cache file.
    #     """
    #     _ = batch_id  # unused
    #     output_file_path = self._get_new_cache_file_path(stream_name)

    #     missing_columns = self._get_missing_columns(stream_name, record_batch)
    #     if missing_columns:
    #         # We need to append columns with the missing column name(s) and a null type
    #         null_array = cast(pa.Array, pa.array([None] * len(record_batch), type=pa.null()))
    #         for col in missing_columns:
    #             record_batch = record_batch.append_column(col, null_array)

    #     try:
    #         with parquet.ParquetWriter(output_file_path, schema=record_batch.schema) as writer:
    #             writer.write_table(record_batch)
    #     except Exception as e:
    #         raise exc.AirbyteLibInternalError(
    #             message=f"Failed to write record batch to Parquet file: {e}",
    #             context={
    #                 "stream_name": stream_name,
    #                 "batch_id": batch_id,
    #                 "output_file_path": output_file_path,
    #                 "schema": record_batch.schema,
    #                 "record_batch": record_batch,
    #             },
    #         ) from e

    #     batch_handle = BatchHandle()
    #     batch_handle.files.append(output_file_path)
    #     return batch_handle
