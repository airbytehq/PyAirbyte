"""Base module for all caches."""
from __future__ import annotations

from airbyte.caches.base import SQLCacheInstanceBase
from airbyte.caches.duckdb import DuckDBCacheInstance, DuckDBCache
from airbyte.caches.postgres import PostgresCache, PostgresCacheConfig
from airbyte.caches.snowflake import SnowflakeCacheConfig, SnowflakeSQLCache


# We export these classes for easy access: `airbyte.caches...`
__all__ = [
    "DuckDBCacheInstance",
    "DuckDBCache",
    "PostgresCache",
    "PostgresCacheConfig",
    "SQLCacheInstanceBase",
    "SnowflakeCacheConfig",
    "SnowflakeSQLCache",
]
