"""Base module for all caches."""
from __future__ import annotations

from airbyte.caches.base import SQLCacheConfigBase
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.postgres import PostgresCache, PostgresCacheConfig
from airbyte.caches.snowflake import SnowflakeCacheConfig, SnowflakeSQLCache


# We export these classes for easy access: `airbyte.caches...`
__all__ = [
    "DuckDBCache",
    "PostgresCache",
    "PostgresCacheConfig",
    "SQLCacheConfigBase",
    "SnowflakeCacheConfig",
    "SnowflakeSQLCache",
]
