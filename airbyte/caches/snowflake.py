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

from typing import TYPE_CHECKING, ClassVar

from airbyte_api.models import DestinationSnowflake

from airbyte._processors.sql.snowflake import SnowflakeConfig, SnowflakeSqlProcessor
from airbyte.caches.base import CacheBase
from airbyte.destinations._translate_cache_to_dest import (
    snowflake_cache_to_destination_configuration,
)


if TYPE_CHECKING:
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
        *,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ) -> None:
        """Unload a single stream to the lake store using Snowflake COPY INTO.

        This implementation uses Snowflake's COPY INTO command to unload data
        directly to S3 in Parquet format with managed artifacts for optimal performance.

        Args:
            stream_name: The name of the stream to unload.
            lake_store: The lake store to unload to.
            aws_access_key_id: AWS access key ID. If not provided, will try to get from secrets.
            aws_secret_access_key: AWS secret access key. If not provided, will try to get from secrets.
        """
        sql_table = self.streams[stream_name].to_sql_table()
        table_name = sql_table.name
        
        if aws_access_key_id is None:
            aws_access_key_id = get_secret("AWS_ACCESS_KEY_ID")
        if aws_secret_access_key is None:
            aws_secret_access_key = get_secret("AWS_SECRET_ACCESS_KEY")

        artifact_prefix = lake_store.get_artifact_prefix()
        file_format_name = f"{artifact_prefix}PARQUET_FORMAT"
        create_format_sql = f"""
            CREATE FILE FORMAT IF NOT EXISTS {file_format_name}
            TYPE = PARQUET
            COMPRESSION = SNAPPY
        """
        self.execute_sql(create_format_sql)

        stage_name = f"{artifact_prefix}STAGE"
        create_stage_sql = f"""
            CREATE OR REPLACE STAGE {stage_name}
            URL = '{lake_store.root_storage_uri}'
            CREDENTIALS = (
                AWS_KEY_ID = '{aws_access_key_id}'
                AWS_SECRET_KEY = '{aws_secret_access_key}'
            )
            FILE_FORMAT = {file_format_name}
        """
        self.execute_sql(create_stage_sql)

        unload_statement = f"""
            COPY INTO @{stage_name}/{stream_name}/
            FROM {self._read_processor.sql_config.schema_name}.{table_name}
            FILE_FORMAT = {file_format_name}
            OVERWRITE = TRUE
        """
        self.execute_sql(unload_statement)

    def unload_table_to_lake(
        self,
        table_name: str,
        lake_store: LakeStorage,
        *,
        db_name: str | None = None,
        schema_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ) -> None:
        """Unload an arbitrary table to the lake store using Snowflake COPY INTO.

        This implementation uses Snowflake's COPY INTO command to unload data
        directly to S3 in Parquet format with managed artifacts for optimal performance.
        Unlike unload_stream_to_lake(), this method works with any table and doesn't
        require a stream mapping.

        Args:
            table_name: The name of the table to unload.
            lake_store: The lake store to unload to.
            db_name: Database name. If provided, schema_name must also be provided.
            schema_name: Schema name. If not provided, uses the cache's default schema.
            aws_access_key_id: AWS access key ID. If not provided, will try to get from secrets.
            aws_secret_access_key: AWS secret access key. If not provided, will try to get from secrets.

        Raises:
            ValueError: If db_name is provided but schema_name is not.
        """
        if db_name is not None and schema_name is None:
            raise ValueError("If db_name is provided, schema_name must also be provided.")

        if aws_access_key_id is None:
            aws_access_key_id = get_secret("AWS_ACCESS_KEY_ID")
        if aws_secret_access_key is None:
            aws_secret_access_key = get_secret("AWS_SECRET_ACCESS_KEY")

        if db_name is not None and schema_name is not None:
            qualified_table_name = f"{db_name}.{schema_name}.{table_name}"
        elif schema_name is not None:
            qualified_table_name = f"{schema_name}.{table_name}"
        else:
            qualified_table_name = f"{self._read_processor.sql_config.schema_name}.{table_name}"

        artifact_prefix = lake_store.get_artifact_prefix()
        file_format_name = f"{artifact_prefix}PARQUET_FORMAT"
        create_format_sql = f"""
            CREATE FILE FORMAT IF NOT EXISTS {file_format_name}
            TYPE = PARQUET
            COMPRESSION = SNAPPY
        """
        self.execute_sql(create_format_sql)

        stage_name = f"{artifact_prefix}STAGE"
        create_stage_sql = f"""
            CREATE OR REPLACE STAGE {stage_name}
            URL = '{lake_store.root_storage_uri}'
            CREDENTIALS = (
                AWS_KEY_ID = '{aws_access_key_id}'
                AWS_SECRET_KEY = '{aws_secret_access_key}'
            )
            FILE_FORMAT = {file_format_name}
        """
        self.execute_sql(create_stage_sql)

        unload_statement = f"""
            COPY INTO @{stage_name}/{table_name}/
            FROM {qualified_table_name}
            FILE_FORMAT = {file_format_name}
            OVERWRITE = TRUE
        """
        self.execute_sql(unload_statement)

    def load_stream_from_lake(
        self,
        stream_name: str,
        lake_store: LakeStorage,
        *,
        zero_copy: bool = False,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ) -> None:
        """Load a single stream from the lake store using Snowflake COPY INTO.

        This implementation uses Snowflake's COPY INTO command to load data
        directly from S3 in Parquet format with managed artifacts for optimal performance.

        Args:
            stream_name: The name of the stream to load.
            lake_store: The lake store to load from.
            zero_copy: Whether to use zero-copy loading. If True, the data will be
                loaded without copying it to the cache. This is useful for large datasets
                that don't need to be stored in the cache.
            aws_access_key_id: AWS access key ID. If not provided, will try to get from secrets.
            aws_secret_access_key: AWS secret access key. If not provided, will try to get from secrets.
        """
        sql_table = self.streams[stream_name].to_sql_table()
        table_name = sql_table.name

        if zero_copy:
            raise NotImplementedError("Zero-copy loading is not yet supported in Snowflake.")

        if aws_access_key_id is None:
            aws_access_key_id = get_secret("AWS_ACCESS_KEY_ID")
        if aws_secret_access_key is None:
            aws_secret_access_key = get_secret("AWS_SECRET_ACCESS_KEY")

        artifact_prefix = lake_store.get_artifact_prefix()
        file_format_name = f"{artifact_prefix}PARQUET_FORMAT"
        create_format_sql = f"""
            CREATE FILE FORMAT IF NOT EXISTS {file_format_name}
            TYPE = PARQUET
            COMPRESSION = SNAPPY
        """
        self.execute_sql(create_format_sql)

        stage_name = f"{artifact_prefix}STAGE"
        create_stage_sql = f"""
            CREATE OR REPLACE STAGE {stage_name}
            URL = '{lake_store.root_storage_uri}'
            CREDENTIALS = (
                AWS_KEY_ID = '{aws_access_key_id}'
                AWS_SECRET_KEY = '{aws_secret_access_key}'
            )
            FILE_FORMAT = {file_format_name}
        """
        self.execute_sql(create_stage_sql)

        load_statement = f"""
            COPY INTO {self._read_processor.sql_config.schema_name}.{table_name}
            FROM @{stage_name}/{stream_name}/
            FILE_FORMAT = {file_format_name}
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = FALSE
        """
        self.execute_sql(load_statement)


# Expose the Cache class and also the Config class.
__all__ = [
    "SnowflakeCache",
    "SnowflakeConfig",
]
