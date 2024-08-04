# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from textwrap import dedent, indent
from typing import TYPE_CHECKING

from airbyte_protocol.models import (
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStateType,
)

from airbyte import exceptions as exc
from airbyte._future_cdk.sql_processor import SqlConfig, SqlProcessorBase
from airbyte._future_cdk.state_writers import StateWriterBase
from airbyte._processors.file.parquet import ParquetWriter


if TYPE_CHECKING:
    from pathlib import Path


class IcebergConfig(SqlConfig):
    """A Iceberg configuration."""

    def __init__(self, db_path: str, schema_name: str) -> None:
        """Initialize the Iceberg configuration."""
        self.db_path = db_path
        self.schema_name = schema_name


class IcebergSqlProcessor(SqlProcessorBase):
    """A Iceberg SQL processor."""

    supports_merge_insert = False
    file_writer_class = ParquetWriter
    sql_config: IcebergConfig

    class IcebergStateWriter(StateWriterBase):
        """A state writer for the Parquet cache."""

        def __init__(self, iceberg_processor: IcebergSqlProcessor) -> None:
            self._iceberg_processor = iceberg_processor
            super().__init__()

        def _write_state(self, state: AirbyteRecordMessage) -> None:
            """Write the state to the cache."""
            self._iceberg_processor.write_state(state)

    @property
    def get_state_writer(self) -> StateWriterBase:
        if self._state_writer is None:
            self._state_writer = self.IcebergStateWriter(self)

        return self._state_writer

    def write_state(self, state: AirbyteStateMessage) -> None:
        """Write the state to the cache.

        Args:
            state (AirbyteStateMessage): The state to write.

        Implementation:
        - State messages are written a separate file.
        - Any pending records are written to the cache file and the cache file is closed.
        - For stream state messages, the matching stream batches are flushed and closed.
        - For global state, all batches are flushed and closed.
        """
        stream_names: list[str] = []
        if state.type == AirbyteStateType.STREAM:
            stream_names = [state.record.stream]
        if state.type == AirbyteStateType.GLOBAL:
            stream_names = list(self._buffered_records.keys())
        else:
            msg = f"Unexpected state type: {state.type}"
            raise exc.PyAirbyteInternalError(msg)

        for stream_name in stream_names:
            state_file_name = self.file_writer.get_active_batch(stream_name)
            self.file_writer.flush_active_batch(stream_name)
            self.file_writer._write_state_to_file(state)
            return

    def _write_files_to_new_table(
        self,
        files: list[Path],
        stream_name: str,
        batch_id: str,
    ) -> str:
        """Write file(s) to a new table.

        This involves registering the table in the Iceberg catalog, creating a manifest file,
        and registering the manifest file in the catalog.
        """
        temp_table_name = self._create_table_for_loading(
            stream_name=stream_name,
            batch_id=batch_id,
        )
        columns_list = list(self._get_sql_column_definitions(stream_name=stream_name).keys())
        columns_list_str = indent(
            "\n, ".join([self._quote_identifier(col) for col in columns_list]),
            "    ",
        )
        files_list = ", ".join([f"'{f!s}'" for f in files])
        columns_type_map = indent(
            "\n, ".join(
                [
                    self._quote_identifier(self.normalizer.normalize(prop_name))
                    + ': "'
                    + str(
                        self._get_sql_column_definitions(stream_name)[
                            self.normalizer.normalize(prop_name)
                        ]
                    )
                    + '"'
                    for prop_name in columns_list
                ]
            ),
            "    ",
        )
        insert_statement = dedent(
            f"""
            INSERT INTO {self.sql_config.schema_name}.{temp_table_name}
            (
                {columns_list_str}
            )
            SELECT
                {columns_list_str}
            FROM read_json_auto(
                [{files_list}],
                format = 'newline_delimited',
                union_by_name = true,
                columns = {{ { columns_type_map } }}
            )
            """
        )
        self._execute_sql(insert_statement)
        return temp_table_name
