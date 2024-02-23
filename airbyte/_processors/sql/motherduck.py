# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A MotherDuck implementation of the cache, built on the DuckDB implementation."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

from overrides import overrides

from airbyte._processors.file import JsonlWriter
from airbyte._processors.sql.duckdb import DuckDBSqlProcessor


if TYPE_CHECKING:
    from airbyte.caches.motherduck import MotherDuckCache


# Suppress warnings from DuckDB about reflection on indices.
# https://github.com/Mause/duckdb_engine/issues/905
warnings.filterwarnings(
    "ignore",
    message="duckdb-engine doesn't yet support reflection on indices",
)


class MotherDuckSqlProcessor(DuckDBSqlProcessor):
    """A cache implementation for MotherDuck."""

    supports_merge_insert = False
    file_writer_class = JsonlWriter
    cache: MotherDuckCache

    @overrides
    def _setup(self) -> None:
        """Do any necessary setup, if applicable.

        Note: The DuckDB parent class requires pre-creation of local directory structure. We
        don't need to do that here so we override the method be a no-op.
        """
        # No setup to do and no need to pre-create local file storage.
        pass
