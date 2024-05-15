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

from pydantic import PrivateAttr

from airbyte._processors.sql.bigquery import BigQueryConfig, BigQuerySqlProcessor
from airbyte.caches.base import (
    CacheBase,
)


class BigQueryCache(BigQueryConfig, CacheBase):
    """The BigQuery cache implementation."""

    _sql_processor_class: type[BigQuerySqlProcessor] = PrivateAttr(default=BigQuerySqlProcessor)
