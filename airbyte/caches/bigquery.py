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
from airbyte.constants import DEFAULT_ARROW_MAX_CHUNK_SIZE


class BigQueryCache(BigQueryConfig, CacheBase):
    """The BigQuery cache implementation."""

    _sql_processor_class: type[BigQuerySqlProcessor] = PrivateAttr(default=BigQuerySqlProcessor)

    def get_arrow_dataset(
        self,
        stream_name: str,
        *,
        max_chunk_size: int = DEFAULT_ARROW_MAX_CHUNK_SIZE,
    ) -> None:
        # Raises a NotImplementedError as BigQuery doesn't support pd.read_sql_table
        raise NotImplementedError(
            "BigQuery doesn't currently support to_arrow"
            "Please consider using a different cache implementation for these functionalities."
        )
