# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte brings Airbyte ELT to every Python developer.

.. include:: ../README.md

## API Reference

"""

from __future__ import annotations

from airbyte import (
    caches,
    cloud,
    datasets,
    documents,
    exceptions,  # noqa: ICN001  # No 'exc' alias for top-level module
    experimental,
    records,
    results,
    secrets,
    sources,
)
from airbyte.caches.bigquery import BigQueryCache
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.util import get_default_cache, new_local_cache
from airbyte.datasets import CachedDataset
from airbyte.records import StreamRecord
from airbyte.results import ReadResult
from airbyte.secrets import SecretSourceEnum, get_secret
from airbyte.sources import registry
from airbyte.sources.base import Source
from airbyte.sources.registry import get_available_connectors
from airbyte.sources.util import get_source


__all__ = [
    # Modules
    "cloud",
    "caches",
    "datasets",
    "documents",
    "exceptions",
    "experimental",
    "records",
    "registry",
    "results",
    "secrets",
    "sources",
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
    "SecretSourceEnum",
    "Source",
    "StreamRecord",
]

__docformat__ = "google"
