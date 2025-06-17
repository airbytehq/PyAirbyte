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
from typing import TYPE_CHECKING, Any, ClassVar

from overrides import overrides
from pydantic import Field

from airbyte._processors.sql.duckdb import DuckDBConfig
from airbyte.caches.duckdb import DuckDBCache
from airbyte.secrets import SecretString


if TYPE_CHECKING:
    from airbyte.shared.sql_processor import SqlProcessorBase

from airbyte._processors.sql.ducklake import DuckLakeSqlProcessor


class DuckLakeConfig(DuckDBConfig):
    """Configuration for the DuckLake cache."""

    db_path: Path | str = Field(default="ducklake-dummy-db.duckdb")
    """Path to the DuckDB database file.

    We don't store any data here.
    """

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

    If not specified, defaults to local storage: `self.cache_dir / 'data'`.
    """

    catalog_name: str = Field(default="ducklake_catalog")
    """Name for the attached DuckLake catalog."""

    storage_credentials: dict | None = Field(default=None)
    """Optional dictionary of storage credentials for accessing data files.

    NOTE: This is not yet supported.
    """

    @overrides
    def get_sql_alchemy_url(self) -> SecretString:
        """Return the SQLAlchemy URL to use.

        For DuckLake, we use the standard DuckDB connection but will attach
        the DuckLake catalog during connection initialization.
        """
        return SecretString(f"duckdb:///{self.db_path!s}")

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database.

        For DuckLake, this should be the catalog name since that's the logical
        database that users will interact with, not the underlying DuckDB file.
        """
        return self.catalog_name


class DuckLakeCache(DuckLakeConfig, DuckDBCache):
    """Cache that uses DuckLake table format for data storage."""

    _sql_processor_class: ClassVar[type[SqlProcessorBase]] = DuckLakeSqlProcessor

    def model_post_init(self, __context: dict[str, Any] | None, /) -> None:
        """Initialize paths relative to cache_dir after model creation."""
        super().model_post_init(__context)

        if not Path(self.db_path).is_absolute():
            self.db_path = self.cache_dir / self.db_path

        if self.metadata_connection_string.startswith("sqlite:"):
            db_path = self.metadata_connection_string[7:]
            if not Path(db_path).is_absolute():
                self.metadata_connection_string = f"sqlite:{self.cache_dir / db_path}"

        if not Path(self.data_path).is_absolute():
            self.data_path = self.cache_dir / self.data_path


__all__ = [
    "DuckLakeCache",
    "DuckLakeConfig",
]
