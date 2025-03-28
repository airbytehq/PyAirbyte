# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A DuckDB implementation of the PyAirbyte cache.

## Usage Example

```python
from airbyte as ab
from airbyte.caches import DuckDBCache

cache = DuckDBCache(
    db_path="/path/to/my/duckdb-file",
    schema_name="myschema",
)
```
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, ClassVar

from airbyte_api.models import DestinationDuckdb
from duckdb_engine import DuckDBEngineWarning

from airbyte._processors.sql.duckdb import DuckDBConfig, DuckDBSqlProcessor
from airbyte.caches.base import CacheBase
from airbyte.destinations._translate_cache_to_dest import duckdb_cache_to_destination_configuration


if TYPE_CHECKING:
    from airbyte.shared.sql_processor import SqlProcessorBase


# Suppress warnings from DuckDB about reflection on indices.
# https://github.com/Mause/duckdb_engine/issues/905
warnings.filterwarnings(
    "ignore",
    message="duckdb-engine doesn't yet support reflection on indices",
    category=DuckDBEngineWarning,
)


class DuckDBCache(DuckDBConfig, CacheBase):
    """A DuckDB cache."""

    _sql_processor_class: ClassVar[type[SqlProcessorBase]] = DuckDBSqlProcessor

    paired_destination_name: ClassVar[str | None] = "destination-duckdb"
    paired_destination_config_class: ClassVar[type | None] = DestinationDuckdb

    @property
    def paired_destination_config(self) -> DestinationDuckdb:
        """Return a dictionary of destination configuration values."""
        return duckdb_cache_to_destination_configuration(cache=self)


# Expose the Cache class and also the Config class.
__all__ = [
    "DuckDBCache",
    "DuckDBConfig",
]
