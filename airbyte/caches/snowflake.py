# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A Snowflake implementation of the cache."""

from __future__ import annotations

from overrides import overrides
from snowflake.sqlalchemy import URL

from airbyte._processors.sql.base import RecordDedupeMode
from airbyte._processors.sql.snowflake import SnowflakeSQLSqlProcessor
from airbyte.caches.base import CacheBase


class SnowflakeCache(CacheBase):
    """Configuration for the Snowflake cache."""

    account: str
    username: str
    password: str
    warehouse: str
    database: str
    role: str

    dedupe_mode = RecordDedupeMode.APPEND

    _sql_processor_class = SnowflakeSQLSqlProcessor

    # Already defined in base class:
    # schema_name: str

    @overrides
    def get_sql_alchemy_url(self) -> str:
        """Return the SQLAlchemy URL to use."""
        return str(
            URL(
                account=self.account,
                user=self.username,
                password=self.password,
                database=self.database,
                warehouse=self.warehouse,
                schema=self.schema_name,
                role=self.role,
            )
        )

    @overrides
    def get_database_name(self) -> str:
        """Return the name of the database."""
        return self.database
