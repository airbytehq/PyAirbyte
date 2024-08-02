# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from airbyte._future_cdk.sql_processor import SqlProcessorBase


class IcebergSqlProcessor(SqlProcessorBase):
    """A Iceberg SQL processor."""

    def __init__(self, db_path: str, schema_name: str) -> None:
        """Initialize the Iceberg SQL processor."""
        super().__init__(db_path=db_path, schema_name=schema_name)
