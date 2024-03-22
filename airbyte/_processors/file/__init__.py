# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""File processors."""

from __future__ import annotations

from airbyte._batch_handles import BatchHandle
from airbyte._processors.file.base import FileWriterBase
from airbyte._processors.file.jsonl import JsonlWriter


__all__ = [
    "BatchHandle",
    "FileWriterBase",
    "JsonlWriter",
]
