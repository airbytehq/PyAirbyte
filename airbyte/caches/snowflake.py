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

from overrides import overrides
from snowflake.sqlalchemy import URL

from airbyte._processors.sql.base import RecordDedupeMode
from airbyte._processors.sql.snowflake import SnowflakeSqlProcessor
from airbyte.caches.base import CacheBase
from airbyte.secrets import SecretString


class SnowflakeCache(CacheBase):
    """Configuration for the Snowflake cache."""

    account: str
    username: str
    password: SecretString
    warehouse: str
    database: str
    role: str

    dedupe_mode = RecordDedupeMode.APPEND

    _sql_processor_class = SnowflakeSqlProcessor

    # Already defined in base class:
    # schema_name: str

    @overrides
    def get_sql_alchemy_url(self) -> SecretString:
        """Return the SQLAlchemy URL to use."""
        return SecretString(
            URL(
                account=self.account,
                user=self.username,
                password=self.password,
                database=self.database,
                warehouse=self.warehouse,
                schema=self.schema_name,
                role=self.role,
            )
        )

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database."""
        return self.database
