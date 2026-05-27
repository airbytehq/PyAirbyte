# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Datetime parsing helpers."""

from __future__ import annotations

from datetime import datetime, timezone


def parse_datetime(value: str | int) -> datetime:
    """Parse an ISO datetime string or Unix timestamp as a timezone-aware datetime."""
    if isinstance(value, int) or (
        isinstance(value, str)
        and (value.isdigit() or (value.startswith("-") and value[1:].isdigit()))
    ):
        return datetime.fromtimestamp(int(value), tz=timezone.utc)

    if not isinstance(value, str):
        raise TypeError(f"Could not parse datetime string: {value}")

    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed
