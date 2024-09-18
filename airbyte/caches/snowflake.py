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

from pydantic import PrivateAttr

from airbyte._processors.sql.snowflake import SnowflakeConfig, SnowflakeSqlProcessor
from airbyte.caches.base import CacheBase
from airbyte.shared.sql_processor import RecordDedupeMode


class SnowflakeCache(SnowflakeConfig, CacheBase):
    """Configuration for the Snowflake cache."""

    dedupe_mode: RecordDedupeMode = RecordDedupeMode.APPEND

    _sql_processor_class = PrivateAttr(default=SnowflakeSqlProcessor)


# Expose the Cache class and also the Config class.
__all__ = [
    "SnowflakeCache",
    "SnowflakeConfig",
]
