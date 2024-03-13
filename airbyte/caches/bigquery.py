# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""A BigQuery implementation of the cache.

## Usage Example

```python
import airbyte as ab
from airbyte.caches import BigQueryCache

cache = BigQueryCache(
    project_name="myproject",
    dataset_name="mydataset",
    credentials_path="path/to/credentials.json",
)
```
"""

from __future__ import annotations

import urllib

from overrides import overrides

from airbyte._processors.sql.bigquery import BigQuerySqlProcessor
from airbyte.caches.base import (
    CacheBase,
)


class BigQueryCache(CacheBase):
    """The BigQuery cache implementation."""

    project_name: str
    """The name of the project to use. In BigQuery, this is equivalent to the database name."""

    dataset_name: str = "airbyte_raw"
    """The name of the dataset to use. In BigQuery, this is equivalent to the schema name."""

    credentials_path: str
    """The path to the credentials file to use."""

    _sql_processor_class: type[BigQuerySqlProcessor] = BigQuerySqlProcessor

    def __post_init__(self) -> None:
        """Initialize the BigQuery cache."""
        self.schema_name = self.dataset_name

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database. For BigQuery, this is the project name."""
        return self.project_name

    @overrides
    def get_sql_alchemy_url(self) -> str:
        """Return the SQLAlchemy URL to use."""
        credentials_path_encoded = urllib.parse.quote(self.credentials_path)
        return f"bigquery://{self.project_name!s}?credentials_path={credentials_path_encoded}"
