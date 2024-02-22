# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A DuckDB implementation of the cache."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

from overrides import overrides

from airbyte._processors.sql.duckdb import DuckDBSqlProcessor
from airbyte.caches.base import CacheBase


if TYPE_CHECKING:
    from pathlib import Path


# Suppress warnings from DuckDB about reflection on indices.
# https://github.com/Mause/duckdb_engine/issues/905
warnings.filterwarnings(
    "ignore",
    message="duckdb-engine doesn't yet support reflection on indices",
)


class DuckDBCache(CacheBase):
    """A DuckDB cache."""

    db_path: Path | str
    """Normally db_path is a Path object.

    There are some cases, such as when connecting to MotherDuck, where it could be a string that
    is not also a path, such as "md:" to connect the user's default MotherDuck DB.
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
