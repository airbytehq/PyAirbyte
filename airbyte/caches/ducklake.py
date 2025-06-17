# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

"""A DuckLake implementation of the PyAirbyte cache, built on DuckDB.

```python
from airbyte as ab
from airbyte.caches import DuckLakeCache

cache = DuckLakeCache(
    metadata_connection_string="sqlite:./metadata.db",
    data_path="./ducklake_data/",
    catalog_name="my_catalog",
    schema_name="myschema",
)
```
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from overrides import overrides
from pydantic import Field

from airbyte._processors.sql.duckdb import DuckDBConfig, DuckDBSqlProcessor
from airbyte.caches.duckdb import DuckDBCache
from airbyte.destinations._translate_cache_to_dest import (
    duckdb_cache_to_destination_configuration,
)
from airbyte.secrets import SecretString


if TYPE_CHECKING:
    from airbyte.shared.sql_processor import SqlProcessorBase


class DuckLakeConfig(DuckDBConfig):
    """Configuration for the DuckLake cache."""

    metadata_connection_string: str = Field(default="sqlite:metadata.db")
    """Connection string for DuckLake metadata database.

    This is the part after the 'ducklake:' prefix in the ATTACH statement.
    Examples:
    - "sqlite:./metadata.db" for SQLite
    - "postgres:dbname=postgres" for PostgreSQL
    """

    data_path: Path | str = Field(default="data")
    """Root path for storing Parquet data files.
    
    Examples (Cloud Storage):
    - `s3://my-bucket/my-data`

    For testing purposes, you can also use local file storage for testing.
    Note that these will not be portable to cloud-based runners:

    Examples (local files):
    - `/path/to/my/data-root`
    - `./relative/path/to/data-root/`
    
    If not specified, defaults to local storage: `self.cach_dir / 'data'`.
    """

    catalog_name: str = Field(default="ducklake_catalog")
    """Name for the attached DuckLake catalog."""

    storage_credentials: dict | None = Field(default=None)
    """Optional dictionary of storage credentials for accessing data files.
    
    NOTE: This is not yet supported.
    """


    def __init__(self, **data) -> None:
        """Initialize DuckLakeConfig with cache_dir-relative defaults."""
        if "db_path" not in data:
            data["db_path"] = "ducklake.db"

        super().__init__(**data)

        if not Path(self.db_path).is_absolute():
            self.db_path = self.cache_dir / self.db_path

        if self.metadata_connection_string.startswith("sqlite:"):
            db_path = self.metadata_connection_string[7:]
            if not Path(db_path).is_absolute():
                self.metadata_connection_string = f"sqlite:{self.cache_dir / db_path}"

        if not Path(self.data_path).is_absolute():
            self.data_path = self.cache_dir / self.data_path

    @overrides
    def get_sql_alchemy_url(self) -> SecretString:
        """Return the SQLAlchemy URL to use.

        For DuckLake, we use the standard DuckDB connection but will attach
        the DuckLake catalog during connection initialization.
        """
        return SecretString(f"duckdb:///{self.db_path!s}")

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database."""
        if self.db_path == ":memory:":
            return "memory"

        split_on = "\\" if "\\" in str(self.db_path) else "/"

        return str(self.db_path).split(sep=split_on)[-1].split(".")[0]


class DuckLakeCache(DuckLakeConfig, DuckDBCache):
    """Cache that uses DuckLake table format for data storage."""

    _sql_processor_class: ClassVar[type[SqlProcessorBase]] = DuckDBSqlProcessor


__all__ = [
    "DuckLakeCache",
    "DuckLakeConfig",
]
