"""PyAirbyte brings Airbyte ELT to every Python developer.

.. include:: ../README.md

"""
from __future__ import annotations

from airbyte._factories.cache_factories import get_default_cache, new_local_cache
from airbyte._factories.connector_factories import get_source
from airbyte.caches import DuckDBCache, DuckDBCacheConfig
from airbyte.datasets import CachedDataset
from airbyte.registry import get_available_connectors
from airbyte.results import ReadResult
from airbyte.secrets import SecretSource, get_secret
from airbyte.source import Source


__all__ = [
    "CachedDataset",
    "DuckDBCache",
    "DuckDBCacheConfig",
    "get_available_connectors",
    "get_source",
    "get_default_cache",
    "get_secret",
    "new_local_cache",
    "ReadResult",
    "SecretSource",
    "Source",
]

__docformat__ = "google"
