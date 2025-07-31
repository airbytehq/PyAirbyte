# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A Snowflake implementation of the PyAirbyte cache.

## Usage Example

# Password connection:

```python
from airbyte as ab
from airbyte.caches import SnowflakeCache

cache = SnowflakeCache(
    account="myaccount",
    username="myusername",
    password=ab.get_secret("SNOWFLAKE_PASSWORD"), # optional
    warehouse="mywarehouse",
    database="mydatabase",
    role="myrole",
    schema_name="myschema",
)
```

# Private key connection:

```python
from airbyte as ab
from airbyte.caches import SnowflakeCache

cache = SnowflakeCache(
    account="myaccount",
    username="myusername",
    private_key=ab.get_secret("SNOWFLAKE_PRIVATE_KEY"),
    private_key_passphrase=ab.get_secret("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"), # optional
    warehouse="mywarehouse",
    database="mydatabase",
    role="myrole",
    schema_name="myschema",
)
```

# Private key path connection:

```python
from airbyte as ab
from airbyte.caches import SnowflakeCache

cache = SnowflakeCache(
    account="myaccount",
    username="myusername",
    private_key_path="path/to/my/private_key.pem",
    private_key_passphrase=ab.get_secret("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"), # optional
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
from airbyte.lakes import LakeStorage
from airbyte.secrets.util import get_secret
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

    def unload_stream_to_lake(
        self,
        stream_name: str,
        lake_store: LakeStorage,
    ) -> None:
        """Unload a single stream to the lake store.

        This generic implementation delegates to the `lake_store` and passes
        an Arrow dataset to the lake store object.

        Subclasses can override this method to provide a faster
        unload implementation.
        """
        sql_table = self.streams[stream_name].to_sql_table()
        table_name = sql_table.name
        aws_access_key_id = get_secret("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = get_secret("AWS_SECRET_ACCESS_KEY")
        unload_statement = "\n".join([
            f"COPY INTO '{lake_store.get_stream_root_uri(stream_name)}'",
            f"FROM {table_name}",
            "CREDENTIALS=(",
            f"  AWS_KEY_ID='{aws_access_key_id}'",
            f"  AWS_SECRET_KEY='{aws_secret_access_key}'",
            ")",
            "FILE_FORMAT = (TYPE = 'PARQUET')",
            "OVERWRITE = TRUE",
        ])
        self.execute_sql(unload_statement)

        # To get the manifest data:
        # self.query_sql("RESULT_SCAN(LAST_QUERY_ID())")

    def load_stream_from_lake(
        self,
        stream_name: str,
        lake_store: LakeStorage,
        *,
        zero_copy: bool = False,
    ) -> None:
        """Load a single stream from the lake store.

        This generic implementation delegates to the `lake_store` and passes
        an Arrow dataset to the lake store object.

        Subclasses can override this method to provide a faster
        unload implementation.
        """
        sql_table = self.streams[stream_name].to_sql_table()
        table_name = sql_table.name
        aws_access_key_id = get_secret(AWS_ACCESS_KEY_ID)
        aws_secret_access_key = get_secret("AWS_SECRET_ACCESS_KEY")
        if zero_copy:
            # Zero-copy loading is not yet supported in Snowflake.
            raise NotImplementedError("Zero-copy loading is not yet supported in Snowflake.")

        load_statement = "\n".join([
            f"COPY INTO {table_name}",
            f"FROM '{lake_store.get_stream_root_uri(stream_name)}'",
            "CREDENTIALS=(",
            f"  AWS_KEY_ID='{aws_access_key_id}'",
            f"  AWS_SECRET_KEY='{aws_secret_access_key}'",
            ")",
            "FILE_FORMAT = (TYPE = 'PARQUET')",
        ])
        self.execute_sql(load_statement)


# Expose the Cache class and also the Config class.
__all__ = [
    "SnowflakeCache",
    "SnowflakeConfig",
]
