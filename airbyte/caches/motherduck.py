# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""A MotherDuck implementation of the PyAirbyte cache, built on DuckDB.

## Usage Example

```python
from airbyte as ab
from airbyte.caches import MotherDuckCache

cache = MotherDuckCache(
    database="mydatabase",
    schema_name="myschema",
    api_key=ab.get_secret("MOTHERDUCK_API_KEY"),
)
"""

from __future__ import annotations

import warnings

from duckdb_engine import DuckDBEngineWarning
from overrides import overrides
from pydantic import Field, PrivateAttr

from airbyte._processors.sql.duckdb import DuckDBConfig
from airbyte._processors.sql.motherduck import MotherDuckSqlProcessor
from airbyte.caches.duckdb import DuckDBCache
from airbyte.secrets import SecretString


class MotherDuckConfig(DuckDBConfig):
    """Configuration for the MotherDuck cache."""

    database: str = Field()
    api_key: SecretString = Field()
    db_path: str = Field(default="md:")

    @overrides
    def get_sql_alchemy_url(self) -> SecretString:
        """Return the SQLAlchemy URL to use."""
        # Suppress warnings from DuckDB about reflection on indices.
        # https://github.com/Mause/duckdb_engine/issues/905
        warnings.filterwarnings(
            "ignore",
            message="duckdb-engine doesn't yet support reflection on indices",
            category=DuckDBEngineWarning,
        )

        return SecretString(
            f"duckdb:///md:{self.database}?motherduck_token={self.api_key}"
            # f"&schema={self.schema_name}"  # TODO: Debug why this doesn't work
        )

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database."""
        return self.database


class MotherDuckCache(MotherDuckConfig, DuckDBCache):
    """Cache that uses MotherDuck for external persistent storage."""

    _sql_processor_class: type[MotherDuckSqlProcessor] = PrivateAttr(default=MotherDuckSqlProcessor)
