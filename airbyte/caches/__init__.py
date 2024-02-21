"""Base module for all caches."""
from __future__ import annotations

from airbyte.caches.base import SQLCacheBase
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.postgres import PostgresCache
from airbyte.caches.snowflake import SnowflakeCache


# We export these classes for easy access: `airbyte.caches...`
__all__ = [
    "DuckDBCache",
    "PostgresCache",
    "SQLCacheBase",
    "SnowflakeCache",
]
