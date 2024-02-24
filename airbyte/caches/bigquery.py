# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""A BigQuery implementation of the cache."""

from __future__ import annotations

import urllib

from overrides import overrides

from airbyte._processors.sql.bigquery import BigQuerySqlProcessor
from airbyte.caches.base import (
    CacheBase,
)


class BigQueryCache(CacheBase):
    """The BigQuery cache implementation."""

    project_name: str
    dataset_name: str = "airbyte_raw"
    credentials_path: str

    _sql_processor_class: type[BigQuerySqlProcessor] = BigQuerySqlProcessor

    def __post_init__(self) -> None:
        """Initialize the BigQuery cache."""
        self.schema_name = self.dataset_name

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database. For BigQuery, this is the project name."""
        return self.project_name

    @overrides
    def get_sql_alchemy_url(self) -> str:
        """Return the SQLAlchemy URL to use."""
        credentials_path_encoded = urllib.parse.quote(self.credentials_path)
        return f"bigquery://{self.project_name!s}?credentials_path={credentials_path_encoded}"
