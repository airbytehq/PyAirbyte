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

from typing import TYPE_CHECKING, ClassVar, NoReturn

from airbyte_api.models import DestinationBigquery

from airbyte._processors.sql.bigquery import BigQueryConfig, BigQuerySqlProcessor
from airbyte.caches.base import (
    CacheBase,
)
from airbyte.constants import DEFAULT_ARROW_MAX_CHUNK_SIZE
from airbyte.destinations._translate_cache_to_dest import (
    bigquery_cache_to_destination_configuration,
)


if TYPE_CHECKING:
    from airbyte.lakes import LakeStorage


if TYPE_CHECKING:
    from airbyte.shared.sql_processor import SqlProcessorBase


class BigQueryCache(BigQueryConfig, CacheBase):
    """The BigQuery cache implementation."""

    _sql_processor_class: ClassVar[type[SqlProcessorBase]] = BigQuerySqlProcessor

    paired_destination_name: ClassVar[str | None] = "destination-bigquery"
    paired_destination_config_class: ClassVar[type | None] = DestinationBigquery

    @property
    def paired_destination_config(self) -> DestinationBigquery:
        """Return a dictionary of destination configuration values."""
        return bigquery_cache_to_destination_configuration(cache=self)

    def get_arrow_dataset(
        self,
        stream_name: str,
        *,
        max_chunk_size: int = DEFAULT_ARROW_MAX_CHUNK_SIZE,
    ) -> NoReturn:
        """Raises NotImplementedError; BigQuery doesn't support `pd.read_sql_table`.

        See: https://github.com/airbytehq/PyAirbyte/issues/165
        """
        raise NotImplementedError(
            "BigQuery doesn't currently support to_arrow"
            "Please consider using a different cache implementation for these functionalities."
        )

    def unload_stream_to_lake(
        self,
        stream_name: str,
        lake_store: LakeStorage,
    ) -> None:
        """Unload a single stream to the lake store using BigQuery EXPORT DATA.

        This implementation uses BigQuery's native EXPORT DATA functionality
        to write directly to GCS, bypassing the Arrow dataset limitation.
        """
        sql_table = self.streams[stream_name].to_sql_table()
        table_name = sql_table.name

        if not hasattr(lake_store, "bucket_name"):
            raise NotImplementedError("BigQuery unload currently only supports GCS lake storage")

        export_uri = f"{lake_store.get_stream_root_uri(stream_name)}*.parquet"

        export_statement = f"""
            EXPORT DATA OPTIONS(
                uri='{export_uri}',
                format='PARQUET',
                overwrite=true
            ) AS
            SELECT * FROM {self._read_processor.sql_config.schema_name}.{table_name}
        """

        self.execute_sql(export_statement)

    def load_stream_from_lake(
        self,
        stream_name: str,
        lake_store: LakeStorage,
        *,
        zero_copy: bool = False,  # noqa: ARG002
    ) -> None:
        """Load a single stream from the lake store using BigQuery LOAD DATA.

        This implementation uses BigQuery's native LOAD DATA functionality
        to read directly from GCS, bypassing the Arrow dataset limitation.
        """
        sql_table = self.streams[stream_name].to_sql_table()
        table_name = sql_table.name

        if not hasattr(lake_store, "bucket_name"):
            raise NotImplementedError("BigQuery load currently only supports GCS lake storage")

        source_uri = f"{lake_store.get_stream_root_uri(stream_name)}*.parquet"

        load_statement = f"""
            LOAD DATA INTO {self._read_processor.sql_config.schema_name}.{table_name}
            FROM FILES (
                format = 'PARQUET',
                uris = ['{source_uri}']
            )
        """

        self.execute_sql(load_statement)


# Expose the Cache class and also the Config class.
__all__ = [
    "BigQueryCache",
    "BigQueryConfig",
]
