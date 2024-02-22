# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""File processors."""

from __future__ import annotations

from .base import FileWriterBase, FileWriterBatchHandle
from .jsonl import JsonlWriter
from .parquet import ParquetWriter


__all__ = [
    "FileWriterBatchHandle",
    "FileWriterBase",
    "JsonlWriter",
    "ParquetWriter",
]
