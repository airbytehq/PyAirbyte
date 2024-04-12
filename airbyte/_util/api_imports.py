# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Imported classes from the Airbyte API.

Any classes that are imported from the Airbyte API should be imported here.
This allows for easy access to these classes in other modules, especially
for type hinting purposes.
"""
# Ignore import sorting in this file. Manual grouping is more important.
# ruff: noqa: I001

from __future__ import annotations

# Internal-Use Classes

# These classes are used internally to cache API responses.
from airbyte_api.models import (
    ConnectionResponse,
    JobResponse,
)

# Public-Use Classes

# This class is used to represent the status of a job. It may be used in
# type hints for public functions that return a job status.
from airbyte_api.models import JobStatusEnum  # Alias not needed


__all__: list[str] = [
    "ConnectionResponse",
    "JobResponse",
    "JobStatusEnum",
]
