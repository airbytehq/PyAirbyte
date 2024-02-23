# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

"""A BigQuery implementation of the cache."""

import urllib

from overrides import overrides

from airbyte._processors.sql.bigquery import BigQuerySqlProcessor
from airbyte.caches.base import (
    CacheBase,
)


class BigQueryCache(CacheBase):
    """The BigQuery cache implementation."""

    project_name: str
    credentials_path: str

    @overrides
    @property
    def database_name(self) -> str:
        """Return the name of the database. For BigQuery, this is the schema/dataset name."""
        return self.project_name

    _sql_processor: BigQuerySqlProcessor
    _sql_processor_class: type[BigQuerySqlProcessor] = BigQuerySqlProcessor

    @overrides
    def get_sql_alchemy_url(self) -> str:
        """Return the SQLAlchemy URL to use."""
        credentials_path_encoded = urllib.parse.quote(self.credentials_path)
        return f"bigquery://{self.project_name!s}?credentials_path={credentials_path_encoded}"

    def get_database_name(self) -> str:
        """Return the name of the database. For BigQuery, this is the schema/dataset name."""
        return self.schema_name
