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

from typing import TYPE_CHECKING, ClassVar

from airbyte_api.models import DestinationPostgres

from airbyte._processors.sql.postgres import PostgresConfig, PostgresSqlProcessor
from airbyte.caches.base import CacheBase
from airbyte.destinations._translate_cache_to_dest import (
    postgres_cache_to_destination_configuration,
)


if TYPE_CHECKING:
    from airbyte.shared.sql_processor import SqlProcessorBase


class PostgresCache(PostgresConfig, CacheBase):
    """Configuration for the Postgres cache.

    Also inherits config from the JsonlWriter, which is responsible for writing files to disk.
    """

    _sql_processor_class: ClassVar[type[SqlProcessorBase]] = PostgresSqlProcessor

    paired_destination_name: ClassVar[str | None] = "destination-bigquery"
    paired_destination_config_class: ClassVar[type | None] = DestinationPostgres

    @property
    def paired_destination_config(self) -> DestinationPostgres:
        """Return a dictionary of destination configuration values."""
        return postgres_cache_to_destination_configuration(cache=self)

    def clone_as_cloud_destination_config(self) -> DestinationPostgres:
        """Return a DestinationPostgres instance with the same configuration."""
        return DestinationPostgres(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )


# Expose the Cache class and also the Config class.
__all__ = [
    "PostgresCache",
    "PostgresConfig",
]
