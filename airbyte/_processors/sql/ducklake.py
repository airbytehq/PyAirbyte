
"""A DuckLake implementation of the cache, built on the DuckDB implementation."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from overrides import overrides

from airbyte._processors.sql.duckdb import DuckDBSqlProcessor
from airbyte._writers.jsonl import JsonlWriter


if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

    from airbyte.caches.ducklake import DuckLakeConfig


class DuckLakeSqlProcessor(DuckDBSqlProcessor):
    """A cache implementation for DuckLake table format."""

    supports_merge_insert = False
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
        try:
            self._execute_sql("INSTALL ducklake;")
            self._execute_sql("LOAD ducklake;")
        except Exception:
            pass

    def _attach_ducklake_catalog(self) -> None:
        """Attach the DuckLake catalog to the DuckDB connection."""
        metadata_conn = self.sql_config.metadata_connection_string
        catalog_name = self.sql_config.catalog_name
        data_path = self.sql_config.data_path

        attach_sql = (
            f"ATTACH 'ducklake:{metadata_conn}' AS {catalog_name} "
            f"(DATA_PATH '{data_path}')"
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

        try:
            connection.execute("LOAD ducklake;")

            metadata_conn = self.sql_config.metadata_connection_string
            catalog_name = self.sql_config.catalog_name
            data_path = self.sql_config.data_path

            attach_sql = (
                f"ATTACH 'ducklake:{metadata_conn}' AS {catalog_name} "
                f"(DATA_PATH '{data_path}')"
            )
            connection.execute(attach_sql)
            connection.execute(f"USE {self.catalog_name}"
        except Exception:
            pass
