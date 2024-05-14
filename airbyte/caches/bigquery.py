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

from typing import TYPE_CHECKING, Any, Optional

from overrides import overrides
from pydantic import root_validator
from sqlalchemy.engine import make_url

from airbyte._processors.sql.bigquery import BigQuerySqlProcessor
from airbyte.caches.base import (
    CacheBase,
)


if TYPE_CHECKING:
    from sqlalchemy.engine.url import URL


class BigQueryCache(CacheBase):
    """The BigQuery cache implementation."""

    project_name: str
    """The name of the project to use. In BigQuery, this is equivalent to the database name."""

    dataset_name: str = "airbyte_raw"
    """The name of the dataset to use. In BigQuery, this is equivalent to the schema name."""

    credentials_path: Optional[str] = None
    """The path to the credentials file to use.
    If not passed, falls back to the default inferred from the environment."""

    _sql_processor_class: type[BigQuerySqlProcessor] = BigQuerySqlProcessor

    @root_validator(pre=True)
    @classmethod
    def set_schema_name(cls, values: dict[str, Any]) -> dict[str, Any]:
        dataset_name = values.get("dataset_name")
        if dataset_name is None:
            raise ValueError("dataset_name must be defined")  # noqa: TRY003
        values["schema_name"] = dataset_name
        return values

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database. For BigQuery, this is the project name."""
        return self.project_name

    @overrides
    def get_sql_alchemy_url(self) -> str:
        """Return the SQLAlchemy URL to use."""
        url: URL = make_url(f"bigquery://{self.project_name!s}")
        if self.credentials_path:
            url = url.update_query_dict({"credentials_path": self.credentials_path})

        return str(url)
