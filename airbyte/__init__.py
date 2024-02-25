"""PyAirbyte brings Airbyte ELT to every Python developer.

.. include:: ../README.md

"""
from __future__ import annotations

from airbyte import caches, datasets, registry, secrets
from airbyte._factories.connector_factories import get_source
from airbyte.caches.bigquery import BigQueryCache
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.factories import get_default_cache, new_local_cache
from airbyte.datasets import CachedDataset
from airbyte.registry import get_available_connectors
from airbyte.results import ReadResult
from airbyte.secrets import SecretSource, get_secret
from airbyte.source import Source


__all__ = [
    # Modules
    "caches",
    "datasets",
    "registry",
    "secrets",
    # Factories
    "get_available_connectors",
    "get_default_cache",
    "get_secret",
    "get_source",
    "new_local_cache",
    # Classes
    "BigQueryCache",
    "CachedDataset",
    "DuckDBCache",
    "ReadResult",
    "SecretSource",
    "Source",
]

__docformat__ = "google"
