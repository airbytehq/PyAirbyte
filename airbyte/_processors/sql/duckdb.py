# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A DuckDB implementation of the cache."""

from __future__ import annotations

import warnings
from pathlib import Path
from textwrap import dedent, indent
from typing import TYPE_CHECKING

from overrides import overrides

from airbyte._future_cdk import SqlProcessorBase
from airbyte._processors.file import JsonlWriter


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

    def _write_files_to_new_table(
        self,
        files: list[Path],
        stream_name: str,
        batch_id: str,
    ) -> str:
        """Write a file(s) to a new table.

        We use DuckDB native SQL functions to efficiently read the files and insert
        them into the table in a single operation.
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
