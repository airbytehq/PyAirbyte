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

from overrides import overrides

from airbyte._processors.sql.postgres import PostgresSqlProcessor
from airbyte.caches.base import CacheBase


class PostgresCache(CacheBase):
    """Configuration for the Postgres cache.

    Also inherits config from the JsonlWriter, which is responsible for writing files to disk.
    """

    host: str
    port: int
    username: str
    password: str
    database: str

    _sql_processor_class = PostgresSqlProcessor

    @overrides
    def get_sql_alchemy_url(self) -> str:
        """Return the SQLAlchemy URL to use."""
        return f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database."""
        return self.database
