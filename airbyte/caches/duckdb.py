# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""A DuckDB implementation of the cache."""

from __future__ import annotations

import warnings
from pathlib import Path
from textwrap import dedent, indent
from typing import cast

from overrides import overrides
from pydantic import PrivateAttr

from airbyte._file_writers import ParquetWriter, ParquetWriterConfig
from airbyte.caches.base import SQLCacheBase, SQLCacheInstanceBase
from airbyte.telemetry import CacheTelemetryInfo


# Suppress warnings from DuckDB about reflection on indices.
# https://github.com/Mause/duckdb_engine/issues/905
warnings.filterwarnings(
    "ignore",
    message="duckdb-engine doesn't yet support reflection on indices",
)


class DuckDBCacheInstance(SQLCacheInstanceBase):
    """A DuckDB implementation of the cache.

    Parquet is used for local file storage before bulk loading.
    Unlike the Snowflake implementation, we can't use the COPY command to load data
    so we insert as values instead.
    """

    supports_merge_insert = False
    file_writer_class = ParquetWriter

    @overrides
    def _get_telemetry_info(self) -> CacheTelemetryInfo:
        return CacheTelemetryInfo("duckdb")

    @overrides
    def _setup(self) -> None:
        """Create the database parent folder if it doesn't yet exist."""
        config = cast(DuckDBCache, self.config)

        if config.db_path == ":memory:":
            return

        Path(config.db_path).parent.mkdir(parents=True, exist_ok=True)

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
        columns_list = [
            self._quote_identifier(c)
            for c in list(self._get_sql_column_definitions(stream_name).keys())
        ]
        columns_list_str = indent("\n, ".join(columns_list), "    ")
        files_list = ", ".join([f"'{f!s}'" for f in files])
        insert_statement = dedent(
            f"""
            INSERT INTO {self.config.schema_name}.{temp_table_name}
            (
                {columns_list_str}
            )
            SELECT
                {columns_list_str}
            FROM read_parquet(
                [{files_list}],
                union_by_name = true
            )
            """
        )
        self._execute_sql(insert_statement)
        return temp_table_name


class DuckDBCache(SQLCacheBase, ParquetWriterConfig):
    """A DuckDB cache.

    Also inherits config from the ParquetWriter, which is responsible for writing files to disk.
    """

    db_path: Path | str
    """Normally db_path is a Path object.

    There are some cases, such as when connecting to MotherDuck, where it could be a string that
    is not also a path, such as "md:" to connect the user's default MotherDuck DB.
    """
    schema_name: str = "main"
    """The name of the schema to write to. Defaults to "main"."""

    _sql_processor_class = PrivateAttr(DuckDBCacheInstance)

    @overrides
    def get_sql_alchemy_url(self) -> str:
        """Return the SQLAlchemy URL to use."""
        # return f"duckdb:///{self.db_path}?schema={self.schema_name}"
        return f"duckdb:///{self.db_path!s}"

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database."""
        if self.db_path == ":memory:":
            return "memory"

        # Return the file name without the extension
        return str(self.db_path).split("/")[-1].split(".")[0]
