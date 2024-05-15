# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A Generic SQL Cache implementation."""

from __future__ import annotations

from overrides import overrides

from airbyte.caches.base import CacheBase
from airbyte.secrets.base import SecretString


class GenericSQLCacheConfig(CacheBase):
    """Allows configuring 'sql_alchemy_url' directly."""

    sql_alchemy_url: SecretString | str

    @overrides
    def get_sql_alchemy_url(self) -> SecretString:
        """Returns a SQL Alchemy URL."""
        return SecretString(self.sql_alchemy_url)
