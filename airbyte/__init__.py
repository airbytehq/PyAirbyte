# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""***PyAirbyte brings the power of Airbyte to every Python developer.***

[![PyPI version](https://badge.fury.io/py/airbyte.svg)](https://badge.fury.io/py/airbyte)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/airbyte)](https://pypi.org/project/airbyte/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/airbyte)](https://pypi.org/project/airbyte/)
[![Star on GitHub](https://img.shields.io/github/stars/airbytehq/pyairbyte.svg?style=social&label=★%20on%20GitHub)](https://github.com/airbytehq/pyairbyte)

# Getting Started

## Reading Data

You can connect to any of [hundreds of sources](https://docs.airbyte.com/integrations/sources/)
using the `get_source` method. You can then read data from sources using `Source.read` method.

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

## Writing to SQL Caches

Data can be written to caches using a number of SQL-based cache implementations, including
Postgres, BigQuery, Snowflake, DuckDB, and MotherDuck. If you do not specify a cache, PyAirbyte
will automatically use a local DuckDB cache by default.

For more information, see the `airbyte.caches` module.

## Writing to Destination Connectors

Data can be written to destinations using the `Destination.write` method. You can connect to
destinations using the `get_destination` method. PyAirbyte supports all Airbyte destinations, but
Docker is required on your machine in order to run Java-based destinations.

**Note:** When loading to a SQL database, we recommend using SQL cache (where available,
[see above](#writing-to-sql-caches)) instead of a destination connector. This is because SQL caches
are Python-native and therefor more portable when run from different Python-based environments which
might not have Docker container support. Destinations in PyAirbyte are uniquely suited for loading
to non-SQL platforms such as vector stores and other reverse ETL-type use cases.

For more information, see the `airbyte.destinations` module and the full list of destination
connectors [here](https://docs.airbyte.com/integrations/destinations/).

# PyAirbyte API

## Importing as `ab`

Most examples in the PyAirbyte documentation use the `import airbyte as ab` convention. The `ab`
alias is recommended, making code more concise and readable. When getting started, this
also saves you from digging in submodules to find the classes and functions you need, since
frequently-used classes and functions are available at the top level of the `airbyte` module.

## Navigating the API

While many PyAirbyte classes and functions are available at the top level of the `airbyte` module,
you can also import classes and functions from submodules directly. For example, while you can
import the `Source` class from `airbyte`, you can also import it from the `sources` submodule like
this:

```python
from airbyte.sources import Source
```

Whether you import from the top level or from a submodule, the classes and functions are the same.
We expect that most users will import from the top level when getting started, and then import from
submodules when they are deploying more complex implementations.

For quick reference, top-Level modules are listed in the left sidebar of this page.

# Other Resources

- [PyAirbyte GitHub Readme](https://github.com/airbytehq/pyairbyte)
- [PyAirbyte Issue Tracker](https://github.com/airbytehq/pyairbyte/issues)
- [Frequently Asked Questions](https://github.com/airbytehq/PyAirbyte/blob/main/docs/faq.md)
- [PyAirbyte Contributors Guide](https://github.com/airbytehq/PyAirbyte/blob/main/docs/CONTRIBUTING.md)
- [GitHub Releases](https://github.com/airbytehq/PyAirbyte/releases)

----------------------

# API Reference

Below is a list of all classes, functions, and modules available in the top-level `airbyte`
module. (This is a long list!) If you are just starting out, we recommend beginning by selecting a
submodule to navigate to from the left sidebar or from the list below:

Each module
has its own documentation and code samples related to effectively using the related capabilities.

- **`airbyte.cloud`** - Working with Airbyte Cloud, including running jobs remotely.
- **`airbyte.caches`** - Working with caches, including how to inspect a cache and get data from it.
- **`airbyte.datasets`** - Working with datasets, including how to read from datasets and convert to
    other formats, such as Pandas, Arrow, and LLM Document formats.
- **`airbyte.destinations`** - Working with destinations, including how to write to Airbyte
    destinations connectors.
- **`airbyte.documents`** - Working with LLM documents, including how to convert records into
    document formats, for instance, when working with AI libraries like LangChain.
- **`airbyte.exceptions`** - Definitions of all exception and warning classes used in PyAirbyte.
- **`airbyte.experimental`** - Experimental features and utilities that do not yet have a stable
    API.
- **`airbyte.logs`** - Logging functionality and configuration.
- **`airbyte.records`** - Internal record handling classes.
- **`airbyte.results`** - Documents the classes returned when working with results from
    `Source.read` and `Destination.write`
- **`airbyte.secrets`** - Tools for managing secrets in PyAirbyte.
- **`airbyte.sources`** - Tools for creating and reading from Airbyte sources. This includes
    `airbyte.source.get_source` to declare a source, `airbyte.source.Source.read` for reading data,
    and `airbyte.source.Source.get_records()` to peek at records without caching or writing them
    directly.

----------------------

"""  # noqa: D415

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte.caches.bigquery import BigQueryCache
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.util import get_colab_cache, get_default_cache, new_local_cache
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


# Submodules imported here for documentation reasons: https://github.com/mitmproxy/pdoc/issues/757
if TYPE_CHECKING:
    # ruff: noqa: TC004  # imports used for more than type checking
    from airbyte import (
        caches,
        callbacks,
        cli,
        cloud,
        constants,
        datasets,
        destinations,
        documents,
        exceptions,  # noqa: ICN001  # No 'exc' alias for top-level module
        experimental,
        logs,
        records,
        results,
        secrets,
        sources,
    )


__all__ = [
    # Modules
    "caches",
    "callbacks",
    "cli",
    "cloud",
    "constants",
    "datasets",
    "destinations",
    "documents",
    "exceptions",
    "experimental",
    "logs",
    "records",
    "registry",
    "results",
    "secrets",
    "sources",
    # Factories
    "get_available_connectors",
    "get_colab_cache",
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
