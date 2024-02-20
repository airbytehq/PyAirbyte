# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A Generic SQL Cache implementation."""
from __future__ import annotations

from overrides import overrides

from airbyte.caches.base import SQLCacheBase


class GenericSQLCacheConfig(SQLCacheBase):
    """Allows configuring 'sql_alchemy_url' directly."""

    sql_alchemy_url: str

    @overrides
    def get_sql_alchemy_url(self) -> str:
        """Returns a SQL Alchemy URL."""
        return self.sql_alchemy_url
