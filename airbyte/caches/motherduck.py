"""A cache implementation for the MotherDuck service, built on DuckDB."""
from __future__ import annotations

from overrides import overrides

from airbyte.caches.duckdb import DuckDBCacheBase, DuckDBCacheConfig


class MotherDuckCacheConfig(DuckDBCacheConfig):
    """Configuration for the MotherDuck cache."""

    db_path = "md:"
    database: str
    api_key: str

    @overrides
    def get_sql_alchemy_url(self) -> str:
        """Return the SQLAlchemy URL to use."""
        # return f"duckdb:///{self.db_path}?schema={self.schema_name}"
        return f"duckdb:///md:{self.api_key}@{self.database}"

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database."""
        return self.database


class MotherDuckCache(DuckDBCacheBase):
    """A cache implementation for the MotherDuck service, built on DuckDB."""

    config_class = MotherDuckCacheConfig
