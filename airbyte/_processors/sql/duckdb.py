# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A DuckDB implementation of the cache."""

from __future__ import annotations

import warnings
from pathlib import Path
from textwrap import dedent, indent
from typing import TYPE_CHECKING

from overrides import overrides

from airbyte._processors.file import JsonlWriter
from airbyte._processors.sql.base import SqlProcessorBase
from airbyte._util.telemetry import CacheTelemetryInfo


if TYPE_CHECKING:
    from airbyte.caches.duckdb import DuckDBCache


# Suppress warnings from DuckDB about reflection on indices.
# https://github.com/Mause/duckdb_engine/issues/905
warnings.filterwarnings(
    "ignore",
    message="duckdb-engine doesn't yet support reflection on indices",
)


class DuckDBSqlProcessor(SqlProcessorBase):
    """A DuckDB implementation of the cache.

    Jsonl is used for local file storage before bulk loading.
    Unlike the Snowflake implementation, we can't use the COPY command to load data
    so we insert as values instead.
    """

    supports_merge_insert = False
    file_writer_class = JsonlWriter
    cache: DuckDBCache

    @overrides
    def _setup(self) -> None:
        """Create the database parent folder if it doesn't yet exist."""
        if self.cache.db_path == ":memory:":
            return

        Path(self.cache.db_path).parent.mkdir(parents=True, exist_ok=True)

    @overrides
    def _ensure_compatible_table_schema(
        self,
        stream_name: str,
        *,
        raise_on_error: bool = True,
    ) -> bool:
        """Return true if the given table is compatible with the stream's schema.

        In addition to the base implementation, this also checks primary keys.
        """
        # call super
        if not super()._ensure_compatible_table_schema(
            stream_name=stream_name,
            raise_on_error=raise_on_error,
        ):
            return False

        # TODO: Add validation for primary keys after DuckDB adds support for primary key
        #       inspection: https://github.com/Mause/duckdb_engine/issues/594

        return True

    def _write_files_to_new_table(
        self,
        files: list[Path],
        stream_name: str,
        batch_id: str,
    ) -> str:
        """Write a file(s) to a new table.

        We use DuckDB's `read_parquet` function to efficiently read the files and insert
        them into the table in a single operation.

        Note: This implementation is fragile in regards to column ordering. However, since
        we are inserting into a temp table we have just created, there should be no
        drift between the table schema and the file schema.
        """
        temp_table_name = self._create_table_for_loading(
            stream_name=stream_name,
            batch_id=batch_id,
        )
        columns_list = list(self._get_sql_column_definitions(stream_name=stream_name).keys())
        columns_list_str = indent(
            "\n, ".join([self._quote_identifier(c) for c in columns_list]),
            "    ",
        )
        files_list = ", ".join([f"'{f!s}'" for f in files])
        columns_type_map = indent(
            "\n, ".join(
                [
                    f"{self._quote_identifier(c)}: "
                    f"{self._get_sql_column_definitions(stream_name)[c]!s}"
                    for c in columns_list
                ]
            ),
            "    ",
        )
        insert_statement = dedent(
            f"""
            INSERT INTO {self.cache.schema_name}.{temp_table_name}
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

    @overrides
    def _get_telemetry_info(self) -> CacheTelemetryInfo:
        return CacheTelemetryInfo("duckdb")
