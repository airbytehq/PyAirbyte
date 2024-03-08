# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A DuckDB implementation of the PyAirbyte cache.

## Usage Example

```python
from airbyte as ab
from airbyte.caches import DuckDBCache

cache = DuckDBCache(
    db_path="/path/to/my/database.duckdb",
    schema_name="myschema",
)
"""

from __future__ import annotations

import warnings
from pathlib import Path  # noqa: TCH003  # Used in Pydantic init
from typing import Union

from overrides import overrides

from airbyte._processors.sql.duckdb import DuckDBSqlProcessor
from airbyte.caches.base import CacheBase


# Suppress warnings from DuckDB about reflection on indices.
# https://github.com/Mause/duckdb_engine/issues/905
warnings.filterwarnings(
    "ignore",
    message="duckdb-engine doesn't yet support reflection on indices",
)


class DuckDBCache(CacheBase):
    """A DuckDB cache."""

    db_path: Union[Path, str]
    """Normally db_path is a Path object.

    The database name will be inferred from the file name. For example, given a `db_path` of
    `/path/to/my/my_db.duckdb`, the database name is `my_db`.
    """

    schema_name: str = "main"
    """The name of the schema to write to. Defaults to "main"."""

    _sql_processor_class = DuckDBSqlProcessor

    @overrides
    def get_sql_alchemy_url(self) -> str:
        """Return the SQLAlchemy URL to use."""
        # return f"duckdb:///{self.db_path}?schema={self.schema_name}"
        return f"duckdb:///{self.db_path!s}"

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database."""
        if self.db_path == ":memory:":
            return "memory"

        # Return the file name without the extension
        return str(self.db_path).split("/")[-1].split(".")[0]
