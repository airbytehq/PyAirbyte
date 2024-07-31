# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A Postgres implementation of the PyAirbyte cache.

## Usage Example

```python
from airbyte as ab
from airbyte.caches import PostgresCache

cache = PostgresCache(
    host="myhost",
    port=5432,
    username="myusername",
    password=ab.get_secret("POSTGRES_PASSWORD"),
    database="mydatabase",
)
```
"""

from __future__ import annotations

from pydantic import PrivateAttr

from airbyte._processors.sql.postgres import PostgresConfig, PostgresSqlProcessor
from airbyte.caches.base import CacheBase


class PostgresCache(PostgresConfig, CacheBase):
    """Configuration for the Postgres cache.

    Also inherits config from the JsonlWriter, which is responsible for writing files to disk.
    """

    _sql_processor_class = PrivateAttr(default=PostgresSqlProcessor)


# Expose the Cache class and also the Config class.
__all__ = [
    "PostgresCache",
    "PostgresConfig",
]
