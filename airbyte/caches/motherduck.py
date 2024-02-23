"""A cache implementation for the MotherDuck service, built on DuckDB."""
from __future__ import annotations

from overrides import overrides

from airbyte._processors.sql.motherduck import MotherDuckSqlProcessor
from airbyte.caches.duckdb import DuckDBCache


class MotherDuckCache(DuckDBCache):
    """Cache that uses MotherDuck for external persistent storage."""

    db_path = "md:"
    database: str
    api_key: str

    _sql_processor_class = MotherDuckSqlProcessor
    _sql_processor: MotherDuckSqlProcessor

    @overrides
    def get_sql_alchemy_url(self) -> str:
        """Return the SQLAlchemy URL to use."""
        return (
            f"duckdb:///md:{self.database}?motherduck_token={self.api_key}"
            # f"&schema={self.schema_name}"  # TODO: Debug why this doesn't work
        )

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database."""
        return self.database
