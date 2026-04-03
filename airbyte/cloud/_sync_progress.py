# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Sync progress estimation for datetime-cursor-based incremental streams.

This module provides functions to estimate per-stream sync progress using
a wall-clock time heuristic:

```
progress = (cursor_dt - sync_start_time) / (now - sync_start_time)
```

Where:
- `cursor_dt` is the latest committed cursor value parsed as a datetime.
- `sync_start_time` is the wall-clock time when the sync job started.
- `now` is the current UTC time.

This heuristic works well for real-time incremental syncs where cursor
timestamps advance roughly with wall-clock time. It may be inaccurate
for historical backfills where the cursor covers a different time range
than the actual sync duration (progress will clamp to 0% in that case).

Only streams with datetime-based cursors are supported. Non-datetime
cursors (integers, opaque tokens, etc.) are skipped.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from airbyte.cloud._connection_state import (
    ConnectionStateResponse,
    StreamState,
    _get_stream_list,
)


logger = logging.getLogger(__name__)


def _try_parse_datetime_cursor(value: str) -> datetime | None:
    """Attempt to parse a string as a datetime via ISO 8601.

    Returns `None` if the value is numeric or cannot be parsed.
    """
    # Fast rejection: if it looks like a pure integer or float, skip it
    stripped = value.strip()
    if not stripped:
        return None

    try:
        float(stripped)
    except ValueError:
        pass
    else:
        return None  # It's a number, not a datetime

    # Try ISO 8601 parsing (handles most Airbyte cursor formats)
    # Normalize trailing "Z" to "+00:00" for Python 3.10 compatibility
    normalized = stripped[:-1] + "+00:00" if stripped.endswith(("Z", "z")) else stripped
    try:
        dt = datetime.fromisoformat(normalized)
    except (ValueError, TypeError):
        pass
    else:
        # Ensure timezone-aware (assume UTC if naive)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    return None


def _extract_cursor_field_from_catalog(
    catalog: dict[str, Any],
    stream_name: str,
    stream_namespace: str | None,
) -> str | None:
    """Extract the cursor field name for a stream from the configured catalog.

    Returns the cursor field name, or `None` if the stream is not found
    or does not have a cursor field configured.
    """
    streams = catalog.get("streams", [])
    for stream_entry in streams:
        config = stream_entry.get("config", {})
        stream_info = stream_entry.get("stream", {})

        entry_name = stream_info.get("name") or config.get("aliasName")
        entry_namespace = stream_info.get("namespace")

        # Normalize namespace comparison (None and "" are equivalent)
        if entry_name != stream_name:
            continue
        if (entry_namespace or None) != (stream_namespace or None):
            continue

        # cursor_field may be a list of path segments or a plain string
        cursor_field = config.get("cursorField")
        if isinstance(cursor_field, str) and cursor_field:
            return cursor_field
        if isinstance(cursor_field, list) and cursor_field:
            return ".".join(str(segment) for segment in cursor_field)

    return None


def _find_cursor_value_in_state(
    stream_state: dict[str, Any] | None,
    cursor_field: str | None,
) -> str | None:
    """Find a cursor value in a stream state blob.

    If `cursor_field` is provided, looks for it directly. Otherwise,
    attempts common cursor field names and heuristics.

    Returns the cursor value as a string, or `None` if not found.
    """
    if not stream_state:
        return None

    # If we know the cursor field, look for it directly (supports dot-delimited paths)
    if cursor_field:
        current: Any = stream_state
        for segment in cursor_field.split("."):
            if isinstance(current, dict) and segment in current:
                current = current[segment]
            else:
                current = None
                break
        if current is not None:
            return str(current)

    # Try common cursor field names as fallback
    common_cursor_names = ["cursor", "updated_at", "date", "timestamp"]
    for name in common_cursor_names:
        if name in stream_state:
            val = stream_state[name]
            if val is not None:
                return str(val)

    # Last resort: check for a single string value that parses as datetime
    string_values = [str(v) for v in stream_state.values() if v is not None and isinstance(v, str)]
    datetime_values = [(v, _try_parse_datetime_cursor(v)) for v in string_values]
    parsed = [(v, dt) for v, dt in datetime_values if dt is not None]
    if len(parsed) == 1:
        return parsed[0][0]

    return None


def compute_stream_progress(
    state_data: dict[str, Any],
    catalog_data: dict[str, Any] | None,
    sync_start_time: datetime,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Compute per-stream sync progress for datetime-cursor-based streams.

    Uses a wall-clock time heuristic:
    `progress = (cursor_dt - sync_start_time) / (now - sync_start_time)`.
    This assumes cursor timestamps advance roughly with wall-clock time,
    which works well for real-time incremental syncs but may be inaccurate
    for historical backfills where the cursor covers a different time range
    than the actual sync duration.

    Progress is `None` for streams without datetime cursors or without
    state. Values are clamped to [0.0, 1.0].
    """
    if now is None:
        now = datetime.now(timezone.utc)

    # Ensure times are timezone-aware
    if sync_start_time.tzinfo is None:
        sync_start_time = sync_start_time.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    try:
        state = ConnectionStateResponse(**state_data)
        streams: list[StreamState] = _get_stream_list(state)
    except (ValidationError, TypeError, KeyError):
        logger.warning("Failed to parse connection state data; returning empty progress.")
        streams = []

    results: list[dict[str, Any]] = []
    for stream in streams:
        stream_name = stream.stream_descriptor.name
        stream_namespace = stream.stream_descriptor.namespace

        # Look up cursor field from catalog
        cursor_field: str | None = None
        if catalog_data:
            cursor_field = _extract_cursor_field_from_catalog(
                catalog_data, stream_name, stream_namespace
            )

        # Find cursor value in state
        cursor_value_str = _find_cursor_value_in_state(stream.stream_state, cursor_field)

        entry: dict[str, Any] = {
            "stream_name": stream_name,
            "stream_namespace": stream_namespace,
            "cursor_field": cursor_field,
            "cursor_value": cursor_value_str,
            "progress_pct": None,
            "reason": None,
        }

        if cursor_value_str is None:
            entry["reason"] = "No cursor value found in state."
            results.append(entry)
            continue

        cursor_dt = _try_parse_datetime_cursor(cursor_value_str)
        if cursor_dt is None:
            entry["reason"] = (
                f"Cursor value '{cursor_value_str}' is not a recognized datetime format."
            )
            results.append(entry)
            continue

        # Calculate progress: (cursor - sync_start) / (now - sync_start)
        denominator = (now - sync_start_time).total_seconds()
        if denominator <= 0:
            entry["reason"] = "Sync start time is not before current time."
            results.append(entry)
            continue

        numerator = (cursor_dt - sync_start_time).total_seconds()

        # Clamp to [0.0, 1.0]
        progress = max(0.0, min(1.0, numerator / denominator))
        entry["progress_pct"] = round(progress, 4)

        results.append(entry)

    return results
