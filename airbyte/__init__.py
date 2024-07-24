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
    destinations,
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
from airbyte.destinations.base import Destination
from airbyte.destinations.util import get_destination
from airbyte.records import StreamRecord
from airbyte.results import ReadResult, WriteResult
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
    "destinations",
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
    "get_destination",
    "get_secret",
    "get_source",
    "new_local_cache",
    # Classes
    "BigQueryCache",
    "CachedDataset",
    "Destination",
    "DuckDBCache",
    "ReadResult",
    "SecretSourceEnum",
    "Source",
    "StreamRecord",
    "WriteResult",
]

__docformat__ = "google"
