# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A Postgres implementation of the cache."""

from __future__ import annotations

from airbyte._processors.file import JsonlWriter
from airbyte._processors.sql.base import SqlProcessorBase


class PostgresSqlProcessor(SqlProcessorBase):
    """A Postgres implementation of the cache.

    Jsonl is used for local file storage before bulk loading.
    Unlike the Snowflake implementation, we can't use the COPY command to load data
    so we insert as values instead.

    TODO: Add optimized bulk load path for Postgres. Could use an alternate file writer
    or another import method. (Relatively low priority, since for now it works fine as-is.)
    """

    file_writer_class = JsonlWriter
    supports_merge_insert = False  # TODO: Add native implementation for merge insert
