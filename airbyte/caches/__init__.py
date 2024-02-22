# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Base module for all caches."""
from __future__ import annotations

from airbyte.caches.base import CacheBase
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.factories import get_default_cache, new_local_cache
from airbyte.caches.postgres import PostgresCache
from airbyte.caches.snowflake import SnowflakeCache


# We export these classes for easy access: `airbyte.caches...`
__all__ = [
    # Factories
    "get_default_cache",
    "new_local_cache",
    # Classes
    "DuckDBCache",
    "PostgresCache",
    "CacheBase",
    "SnowflakeCache",
]
