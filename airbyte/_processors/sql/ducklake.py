# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""A DuckLake implementation of the cache, built on the DuckDB implementation."""

from __future__ import annotations

from pathlib import Path
from textwrap import indent
from typing import TYPE_CHECKING

from overrides import overrides
from sqlalchemy import text

from airbyte._processors.sql.duckdb import DuckDBSqlProcessor
from airbyte._writers.jsonl import JsonlWriter
from airbyte.constants import DEBUG_MODE


if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

    from airbyte.caches.ducklake import DuckLakeConfig


class DuckLakeSqlProcessor(DuckDBSqlProcessor):
    """A cache implementation for DuckLake table format."""

    supports_merge_insert = False  # Custom DELETE-then-INSERT implementation, below.
    file_writer_class = JsonlWriter
    sql_config: DuckLakeConfig

    @overrides
    def _setup(self) -> None:
        """Set up the DuckLake connection and attach the catalog."""
        super()._setup()

        data_path = Path(self.sql_config.data_path)
        data_path.mkdir(parents=True, exist_ok=True)

        self._install_ducklake_extension()
        self._attach_ducklake_catalog()

    def _install_ducklake_extension(self) -> None:
        """Install the DuckLake extension if not already installed."""
        self._execute_sql("INSTALL ducklake;")
        self._execute_sql("LOAD ducklake;")

    def _attach_ducklake_catalog(self) -> None:
        """Attach the DuckLake catalog to the DuckDB connection."""
        metadata_conn = self.sql_config.metadata_connection_string
        catalog_name = self.sql_config.catalog_name
        data_path = self.sql_config.data_path

        attach_sql = (
            f"ATTACH 'ducklake:{metadata_conn}' AS {catalog_name} (DATA_PATH '{data_path}')"
        )

        try:
            self._execute_sql(attach_sql)
        except Exception as ex:
            if (
                "already exists" not in str(ex).lower()
                and "already attached" not in str(ex).lower()
            ):
                raise

    @overrides
    def _init_connection_settings(self, connection: Connection) -> None:
        """Initialize connection settings for DuckLake.

        This ensures the DuckLake catalog is available for each new connection.
        """
        super()._init_connection_settings(connection)

        connection.execute(text("LOAD ducklake;"))

        metadata_conn = self.sql_config.metadata_connection_string
        catalog_name = self.sql_config.catalog_name
        data_path = self.sql_config.data_path

        attach_sql = f"ATTACH IF NOT EXISTS 'ducklake:{metadata_conn}' AS {catalog_name} (DATA_PATH '{data_path}')"
        connection.execute(text(attach_sql))
        connection.execute(text(f"USE {self.sql_config.get_database_name()}"))
        connection.execute(text(f"USE {self.sql_config.get_database_name()}.main"))

    def _emulated_merge_temp_table_to_final_table(
        self,
        stream_name: str,
        temp_table_name: str,
        final_table_name: str,
    ) -> None:
        """Emulate the merge operation using a series of SQL commands.

        This is a fallback implementation because DuckLake does not yet support
        MERGE or complex UPDATE commands directly.

        More information:
        - https://github.com/duckdb/ducklake/discussions/95
        """
        pk_columns: list[str] = self.catalog_provider.get_primary_keys(stream_name)
        pk_columns_str: str = ", ".join(pk_columns)
        all_incoming_columns: list[str] = list(
            self._get_sql_column_definitions(stream_name=stream_name).keys(),
        )
        all_incoming_columns_str: str = ",\n".join(all_incoming_columns)

        # Delete records in the final table that match the primary keys in the temp table
        delete_stmt = text(
            "\n".join([
                f"DELETE FROM {final_table_name} ",
                f"WHERE {pk_columns_str} IN ",
                f"(SELECT {pk_columns_str} FROM {temp_table_name})",
            ])
        )
        # Insert new records from the temp table into the final table
        insert_stmt = text(
            "\n".join([
                f"INSERT INTO {final_table_name} (",
                f"{indent(all_incoming_columns_str, '  ')}",
                ")",
                "SELECT ",
                f"{indent(all_incoming_columns_str, '  ')}",
                f"FROM {temp_table_name}",
            ])
        )

        if DEBUG_MODE:
            print(str(delete_stmt))
            print(str(insert_stmt))

        with self.get_sql_connection() as conn:
            conn.execute(delete_stmt)
            conn.execute(insert_stmt)

    def _do_checkpoint(
        self,
        connection: Connection | None = None,
    ) -> None:
        """No-op checkpoint.

        DuckLake does not support the `CHECKPOINT` operation.
        """
        pass
