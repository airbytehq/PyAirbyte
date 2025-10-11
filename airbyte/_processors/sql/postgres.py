# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A Postgres implementation of the cache."""

from __future__ import annotations

import functools

from overrides import overrides
from sqlalchemy.dialects.postgresql import JSONB

from airbyte._util.name_normalizers import LowerCaseNormalizer
from airbyte._writers.jsonl import JsonlWriter
from airbyte.secrets.base import SecretString
from airbyte.shared.sql_processor import SqlConfig, SqlProcessorBase
from airbyte.types import SQLTypeConverter


class PostgresConfig(SqlConfig):
    """Configuration for the Postgres cache.

    Also inherits config from the JsonlWriter, which is responsible for writing files to disk.
    """

    host: str
    port: int
    database: str
    username: str
    password: SecretString | str

    @overrides
    def get_sql_alchemy_url(self) -> SecretString:
        """Return the SQLAlchemy URL to use."""
        return SecretString(
            f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        )

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database."""
        return self.database


class PostgresTypeConverter(SQLTypeConverter):
    """A Postgres-specific type converter that uses JSONB for JSON data."""

    @classmethod
    def get_json_type(cls) -> JSONB:
        """Return JSONB type for Postgres instead of generic JSON."""
        return JSONB()


class PostgresNormalizer(LowerCaseNormalizer):
    """A name normalizer for Postgres.

    Postgres has specific field name length limits:
    - Tables names are limited to 63 characters.
    - Column names are limited to 63 characters.

    The postgres normalizer inherits from the default LowerCaseNormalizer class, and
    additionally truncates column and table names to 63 characters.
    """

    @staticmethod
    @functools.cache
    def normalize(name: str) -> str:
        """Normalize the name, truncating to 63 characters."""
        return LowerCaseNormalizer.normalize(name)[:63]


class PostgresSqlProcessor(SqlProcessorBase):
    """A Postgres implementation of the cache.

    Jsonl is used for local file storage before bulk loading.
    Unlike the Snowflake implementation, we can't use the COPY command to load data
    so we insert as values instead.

    TODO: Add optimized bulk load path for Postgres. Could use an alternate file writer
    or another import method. (Relatively low priority, since for now it works fine as-is.)
    """

    supports_merge_insert = False
    file_writer_class = JsonlWriter
    sql_config: PostgresConfig

    normalizer = PostgresNormalizer
    """A Postgres-specific name normalizer for table and column name normalization."""

    type_converter_class = PostgresTypeConverter
    """A Postgres-specific type converter that uses JSONB for JSON data."""
