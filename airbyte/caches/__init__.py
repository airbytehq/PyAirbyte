# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Base module for all caches."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte.caches.base import CacheBase
from airbyte.caches.bigquery import BigQueryCache
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.motherduck import MotherDuckCache
from airbyte.caches.postgres import PostgresCache
from airbyte.caches.snowflake import SnowflakeCache
from airbyte.caches.util import get_default_cache, new_local_cache


# Submodules imported here for documentation reasons: https://github.com/mitmproxy/pdoc/issues/757
if TYPE_CHECKING:
    # ruff: noqa: TC004
    from airbyte.caches import base, bigquery, duckdb, motherduck, postgres, snowflake, util

# We export these classes for easy access: `airbyte.caches...`
__all__ = [
    # Factories
    "get_default_cache",
    "new_local_cache",
    # Classes
    "BigQueryCache",
    "CacheBase",
    "DuckDBCache",
    "MotherDuckCache",
    "PostgresCache",
    "SnowflakeCache",
    # Submodules,
    "util",
    "bigquery",
    "duckdb",
    "motherduck",
    "postgres",
    "snowflake",
    "base",
]
