# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""A Postgres implementation of the cache."""

from __future__ import annotations

from overrides import overrides

from airbyte._file_writers import JsonlWriter, JsonlWriterConfig
from airbyte.caches.base import SQLCacheBase, SQLCacheInstanceBase
from airbyte.telemetry import CacheTelemetryInfo


class PostgresCacheInstance(SQLCacheInstanceBase):
    """A Postgres implementation of the cache.

    Jsonl is used for local file storage before bulk loading.
    Unlike the Snowflake implementation, we can't use the COPY command to load data
    so we insert as values instead.

    TOOD: Add optimized bulk load path for Postgres. Could use an alternate file writer
    or another import method. (Relatively low priority, since for now it works fine as-is.)
    """

    file_writer_class = JsonlWriter
    supports_merge_insert = False  # TODO: Add native implementation for merge insert

    @overrides
    def _get_telemetry_info(self) -> CacheTelemetryInfo:
        return CacheTelemetryInfo("postgres")


class PostgresCache(SQLCacheBase, JsonlWriterConfig):
    """Configuration for the Postgres cache.

    Also inherits config from the JsonlWriter, which is responsible for writing files to disk.
    """

    host: str
    port: int
    username: str
    password: str
    database: str

    _sql_processor_class = PostgresCacheInstance

    @overrides
    def get_sql_alchemy_url(self) -> str:
        """Return the SQLAlchemy URL to use."""
        return f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database."""
        return self.database
