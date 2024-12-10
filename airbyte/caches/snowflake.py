# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A Snowflake implementation of the PyAirbyte cache.

## Usage Example

```python
from airbyte as ab
from airbyte.caches import SnowflakeCache

cache = SnowflakeCache(
    account="myaccount",
    username="myusername",
    password=ab.get_secret("SNOWFLAKE_PASSWORD"),
    warehouse="mywarehouse",
    database="mydatabase",
    role="myrole",
    schema_name="myschema",
)
```
"""

from __future__ import annotations

from typing import ClassVar

from airbyte_api.models import DestinationSnowflake

from airbyte._processors.sql.snowflake import SnowflakeConfig, SnowflakeSqlProcessor
from airbyte.caches.base import CacheBase
from airbyte.destinations._translate_cache_to_dest import (
    snowflake_cache_to_destination_configuration,
)
from airbyte.shared.sql_processor import RecordDedupeMode, SqlProcessorBase


class SnowflakeCache(SnowflakeConfig, CacheBase):
    """Configuration for the Snowflake cache."""

    dedupe_mode: RecordDedupeMode = RecordDedupeMode.APPEND

    _sql_processor_class: ClassVar[type[SqlProcessorBase]] = SnowflakeSqlProcessor

    paired_destination_name: ClassVar[str | None] = "destination-bigquery"
    paired_destination_config_class: ClassVar[type | None] = DestinationSnowflake

    @property
    def paired_destination_config(self) -> DestinationSnowflake:
        """Return a dictionary of destination configuration values."""
        return snowflake_cache_to_destination_configuration(cache=self)


# Expose the Cache class and also the Config class.
__all__ = [
    "SnowflakeCache",
    "SnowflakeConfig",
]
