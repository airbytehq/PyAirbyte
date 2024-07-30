# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte brings Airbyte ELT to every Python developer.

# PyAirbyte

PyAirbyte brings the power of Airbyte to every Python developer. PyAirbyte provides a set of
utilities to use Airbyte connectors in Python.

[![PyPI version](https://badge.fury.io/py/airbyte.svg)](https://badge.fury.io/py/airbyte)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/airbyte)](https://pypi.org/project/airbyte/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/airbyte)](https://pypi.org/project/airbyte/)
[![PyPI - Wheel](https://img.shields.io/pypi/wheel/airbyte)](https://pypi.org/project/airbyte/)
[![PyPI - Implementation](https://img.shields.io/pypi/implementation/airbyte)](https://pypi.org/project/airbyte/)
[![PyPI - Format](https://img.shields.io/pypi/format/airbyte)](https://pypi.org/project/airbyte/)
[![Star on GitHub](https://img.shields.io/github/stars/airbytehq/pyairbyte.svg?style=social&label=★%20on%20GitHub)](https://github.com/airbytehq/pyairbyte)

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

## Frequently asked Questions

**1. Does PyAirbyte replace Airbyte?**

No. PyAirbyte is a Python library that allows you to use Airbyte connectors in Python but it does
not have orchestration or scheduling capabilities, nor does is provide logging, alerting, or other
features for managing data pipelines in production. Airbyte is a full-fledged data integration
platform that provides connectors, orchestration, and scheduling capabilities.

**2. What is the PyAirbyte cache? Is it a destination?**

Yes and no. You can think of it as a built-in destination implementation, but we avoid the word
"destination" in our docs to prevent confusion with our certified destinations list
[here](https://docs.airbyte.com/integrations/destinations/).

**3. Does PyAirbyte work with data orchestration frameworks like Airflow, Dagster, and Snowpark,
etc.?**

Yes, it should. Please give it a try and report any problems you see. Also, drop us a note if works
for you!

**4. Can I use PyAirbyte to develop or test when developing Airbyte sources?**

Yes, you can. PyAirbyte makes it easy to test connectors in Python, and you can use it to develop
new local connectors as well as existing already-published ones.

**5. Can I develop traditional ETL pipelines with PyAirbyte?**

Yes. Just pick the cache type matching the destination - like SnowflakeCache for landing data in
Snowflake.

**6. Can PyAirbyte import a connector from a local directory that has python project files, or does

it have to be pip install**

Yes, PyAirbyte can use any local install that has a CLI - and will automatically find connectors b
name if they are on PATH.

## Contributing

To learn how you can contribute to PyAirbyte, please see our
[PyAirbyte Contributors Guide](./CONTRIBUTING.md).

## Changelog and Release Notes

For a version history and list of all changes, please see our
[GitHub Releases](https://github.com/airbytehq/PyAirbyte/releases) page.

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
