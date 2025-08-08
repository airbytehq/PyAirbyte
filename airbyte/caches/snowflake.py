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
from typing_extensions import override

from airbyte._processors.sql.snowflake import SnowflakeConfig, SnowflakeSqlProcessor
from airbyte.caches.base import CacheBase
from airbyte.destinations._translate_cache_to_dest import (
    snowflake_cache_to_destination_configuration,
)
from airbyte.lakes import FastUnloadResult
from airbyte.shared.sql_processor import RecordDedupeMode, SqlProcessorBase


if TYPE_CHECKING:
    from airbyte.lakes import LakeStorage


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

    def _get_lake_artifact_prefix(self, lake_store: LakeStorage) -> str:
        """Get the artifact prefix for this lake storage."""
        return f"AIRBYTE_LAKE_{lake_store.short_name.upper()}_"

    def _get_lake_file_format_name(self, lake_store: LakeStorage) -> str:
        """Get the file_format name."""
        artifact_prefix = self._get_lake_artifact_prefix(lake_store)
        return f"{artifact_prefix}PARQUET_FORMAT"

    def _get_lake_stage_name(self, lake_store: LakeStorage) -> str:
        """Get the stage name."""
        artifact_prefix = self._get_lake_artifact_prefix(lake_store)
        return f"{artifact_prefix}STAGE"

    def _setup_lake_artifacts(
        self,
        lake_store: LakeStorage,
    ) -> None:
        if not hasattr(lake_store, "aws_access_key_id"):
            raise NotImplementedError(
                "Snowflake lake operations currently only support S3 lake storage"
            )

        qualified_prefix = (
            f"{self.database}.{self.schema_name}" if self.database else self.schema_name
        )
        file_format_name = self._get_lake_file_format_name(lake_store)
        stage_name = self._get_lake_stage_name(lake_store)

        create_format_sql = f"""
            CREATE FILE FORMAT IF NOT EXISTS {qualified_prefix}.{file_format_name}
            TYPE = PARQUET
            COMPRESSION = SNAPPY
        """
        self.execute_sql(create_format_sql)

        create_stage_sql = f"""
            CREATE STAGE IF NOT EXISTS {qualified_prefix}.{stage_name}
            URL = '{lake_store.root_storage_uri}'
            CREDENTIALS = (
                AWS_KEY_ID = '{lake_store.aws_access_key_id}'
                AWS_SECRET_KEY = '{lake_store.aws_secret_access_key}'
            )
            FILE_FORMAT = {qualified_prefix}.{file_format_name}
        """
        self.execute_sql(create_stage_sql)

    @override
    def fast_unload_table(
        self,
        table_name: str,
        lake_store: LakeStorage,
        lake_path_prefix: str,
        *,
        stream_name: str | None = None,
        db_name: str | None = None,
        schema_name: str | None = None,
    ) -> FastUnloadResult:
        """Unload an arbitrary table to the lake store using Snowflake COPY INTO.

        This implementation uses Snowflake's COPY INTO command to unload data
        directly to S3 in Parquet format with managed artifacts for optimal performance.
        Unlike fast_unload_stream(), this method works with any table and doesn't
        require a stream mapping.

        Raises:
            ValueError: If db_name is provided but schema_name is not.
        """
        if db_name is not None and schema_name is None:
            raise ValueError("If db_name is provided, schema_name must also be provided.")

        qualified_prefix = (
            f"{self.database}.{self.schema_name}" if self.database else self.schema_name
        )
        file_format_name = self._get_lake_file_format_name(lake_store)
        stage_name = self._get_lake_stage_name(lake_store)

        if db_name is not None and schema_name is not None:
            qualified_table_name = f"{db_name}.{schema_name}.{table_name}"
        elif schema_name is not None:
            qualified_table_name = f"{schema_name}.{table_name}"
        else:
            qualified_table_name = f"{self._read_processor.sql_config.schema_name}.{table_name}"

        self._setup_lake_artifacts(lake_store)

        unload_statement = f"""
            COPY INTO @{qualified_prefix}.{stage_name}/{lake_path_prefix}/
            FROM {qualified_table_name}
            FILE_FORMAT = {qualified_prefix}.{file_format_name}
            OVERWRITE = TRUE
        """
        self.execute_sql(unload_statement)
        return FastUnloadResult(
            stream_name=stream_name,
            table_name=table_name,
            lake_store=lake_store,
            lake_path_prefix=lake_path_prefix,
        )

    @override
    def fast_load_table(
        self,
        table_name: str,
        lake_store: LakeStorage,
        lake_path_prefix: str,
        *,
        db_name: str | None = None,
        schema_name: str | None = None,
        zero_copy: bool = False,
    ) -> None:
        """Load a single stream from the lake store using Snowflake COPY INTO.

        This implementation uses Snowflake's COPY INTO command to load data
        directly from S3 in Parquet format with managed artifacts for optimal performance.
        """
        if zero_copy:
            raise NotImplementedError("Zero-copy loading is not yet supported in Snowflake.")

        if db_name is not None and schema_name is None:
            raise ValueError("If db_name is provided, schema_name must also be provided.")

        qualified_prefix = (
            f"{self.database}.{self.schema_name}" if self.database else self.schema_name
        )
        file_format_name = self._get_lake_file_format_name(lake_store)
        stage_name = self._get_lake_stage_name(lake_store)

        if db_name is not None and schema_name is not None:
            qualified_table_name = f"{db_name}.{schema_name}.{table_name}"
        elif schema_name is not None:
            qualified_table_name = f"{schema_name}.{table_name}"
        else:
            qualified_table_name = f"{self._read_processor.sql_config.schema_name}.{table_name}"

        qualified_prefix = (
            f"{self.database}.{self.schema_name}" if self.database else self.schema_name
        )
        file_format_name = self._get_lake_file_format_name(lake_store)
        stage_name = self._get_lake_stage_name(lake_store)

        self._setup_lake_artifacts(lake_store)

        load_statement = f"""
            COPY INTO {qualified_table_name}
            FROM @{qualified_prefix}.{stage_name}/{lake_path_prefix}/
            FILE_FORMAT = {qualified_prefix}.{file_format_name}
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = FALSE
        """
        self.execute_sql(load_statement)


# Expose the Cache class and also the Config class.
__all__ = [
    "SnowflakeCache",
    "SnowflakeConfig",
]
