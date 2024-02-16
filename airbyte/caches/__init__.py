"""Base module for all caches."""
from __future__ import annotations

from airbyte.caches.base import SQLCacheBase
from airbyte.caches.duckdb import DuckDBCache, DuckDBCacheConfig
from airbyte.caches.motherduck import MotherDuckCache, MotherDuckCacheConfig
from airbyte.caches.postgres import PostgresCache, PostgresCacheConfig
from airbyte.caches.snowflake import SnowflakeCacheConfig, SnowflakeSQLCache


# We export these classes for easy access: `airbyte.caches...`
__all__ = [
    "DuckDBCache",
    "DuckDBCacheConfig",
    "MotherDuckCache",
    "MotherDuckCacheConfig",
    "PostgresCache",
    "PostgresCacheConfig",
    "SQLCacheBase",
    "SnowflakeCacheConfig",
    "SnowflakeSQLCache",
]
