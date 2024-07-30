# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte brings Airbyte ELT to every Python developer.

.. include:: ../README.md

## Reading Data

You can connect to hundreds of sources using the `get_source` method. You can then read
data from sources using `Source.read` method.

```python
from airbyte import get_source

source = get_source(
    "source-faker",
    config={},
)
read_result = source.read()

for record in read_result["users"].records:
    print(record)
```

For more information, see the `airbyte.sources` module.

## Writing Data to SQL Databases Using Caches

Data can be written to caches using a number of SQL-based cache implementations, including
Postgres, BigQuery, Snowflake, DuckDB, and MotherDuck. If you do not specify a cache, PyAirbyte
will automatically use a local DuckDB cache by default.

For more information, see the `airbyte.caches` module.

## Writing Data to Destination Connectors

Data can be written to destinations using the `Destination.write` method. You can connect to
destinations using the `get_destination` method. PyAirbyte supports all Airbyte destinations, but
Docker is required on your machine in order to run Java-based destinations.

When loading to a SQL database, we recommend using SQL cache (see above) instead of a destination.
This is because SQL caches are Python-native and therefor more portable when run from different
Python-based environments. Destinations in PyAirbyte are uniquely suited for loading to non-SQL
platforms and other reverse ETL-type use cases.

For more information, see the `airbyte.destinations` module.

## Secrets Management

PyAirbyte provides a secrets management system that allows you to securely store and retrieve
sensitive information.

The `get_secret` function retrieves secrets from a variety of sources, including environment
variables, local `.env` files, Google Colab secrets, and manual entry via `getpass`.

If you need to build your own secret manager, you can subclass the
`airbyte.secrets.CustomSecretManager` class.

For more information, see the `airbyte.secrets` module.

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
