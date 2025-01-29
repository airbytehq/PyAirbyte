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

import pandas as pd
import pandas_gbq
from airbyte_api.models import DestinationBigquery
from google.oauth2.service_account import Credentials

from airbyte._processors.sql.bigquery import BigQueryConfig, BigQuerySqlProcessor
from airbyte.caches.base import (
    CacheBase,
)
from airbyte.destinations._translate_cache_to_dest import (
    bigquery_cache_to_destination_configuration,
)


if TYPE_CHECKING:
    from collections.abc import Iterator

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

    def _read_to_pandas_dataframe(
        self,
        table_name: str,
        chunksize: int | None = None,
        **kwargs,
    ) -> pd.DataFrame | Iterator[pd.DataFrame]:
        # Pop unused kwargs, maybe not the best way to do this
        kwargs.pop("con", None)
        kwargs.pop("schema", None)

        # Read the table using pandas_gbq
        credentials = Credentials.from_service_account_file(self.credentials_path)
        result = pandas_gbq.read_gbq(
            f"{self.project_name}.{self.dataset_name}.{table_name}",
            project_id=self.project_name,
            credentials=credentials,
            **kwargs,
        )

        # Cast result to DataFrame if it's not already a DataFrame
        if not isinstance(result, pd.DataFrame):
            result = pd.DataFrame(result)

        # Return chunks as iterator if chunksize is provided
        if chunksize is not None:
            return (result[i : i + chunksize] for i in range(0, len(result), chunksize))

        return result


# Expose the Cache class and also the Config class.
__all__ = [
    "BigQueryCache",
    "BigQueryConfig",
]
