# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Useful constants for working with Airbyte Cloud features in PyAirbyte."""

from __future__ import annotations

from airbyte._util.api_imports import JobStatusEnum


FINAL_STATUSES: set[JobStatusEnum] = {
    JobStatusEnum.SUCCEEDED,
    JobStatusEnum.FAILED,
    JobStatusEnum.CANCELLED,
}
"""The set of `.JobStatusEnum` strings that indicate a sync job has completed."""

FAILED_STATUSES: set[JobStatusEnum] = {
    JobStatusEnum.FAILED,
    JobStatusEnum.CANCELLED,
}
"""The set of `.JobStatusEnum` strings that indicate a sync job has failed."""

READABLE_DESTINATION_TYPES: set[str] = {
    "bigquery",
    "snowflake",
}
"""List of Airbyte Cloud destinations that PyAirbyte is able to read from."""
