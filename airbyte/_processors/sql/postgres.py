# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A Postgres implementation of the cache."""

from __future__ import annotations

from typing import Union

from overrides import overrides

from airbyte._future_cdk.sql_processor import SqlConfig, SqlProcessorBase
from airbyte._processors.file import JsonlWriter
from airbyte.secrets.base import SecretString


class PostgresConfig(SqlConfig):
    """Configuration for the Postgres cache.

    Also inherits config from the JsonlWriter, which is responsible for writing files to disk.
    """

    host: str
    port: int
    database: str
    username: str
    password: Union[SecretString, str]

    @overrides
    def get_sql_alchemy_url(self) -> SecretString:
        """Return the SQLAlchemy URL to use."""
        return SecretString(
            f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        )

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database."""
        return self.database


class PostgresSqlProcessor(SqlProcessorBase):
    """A Postgres implementation of the cache.

    Jsonl is used for local file storage before bulk loading.
    Unlike the Snowflake implementation, we can't use the COPY command to load data
    so we insert as values instead.

    TODO: Add optimized bulk load path for Postgres. Could use an alternate file writer
    or another import method. (Relatively low priority, since for now it works fine as-is.)
    """

    supports_merge_insert = False  # TODO: Add native implementation for merge insert
    file_writer_class = JsonlWriter
    sql_config: PostgresConfig
