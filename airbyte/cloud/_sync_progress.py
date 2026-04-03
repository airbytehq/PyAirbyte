# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Sync progress estimation for datetime-cursor-based incremental streams.

This module provides functions to estimate per-stream sync progress by
comparing the current cursor value against a known previous bookmark
and the current time (`now`), which serves as the estimated sync target.

Formula (when previous bookmark is available):

    progress = (cursor_dt - previous_bookmark_dt) / (now - previous_bookmark_dt)

Where:

- `previous_bookmark_dt` is the cursor value from the last completed sync.
- `cursor_dt` is the latest committed cursor value parsed as a datetime.
- `now` is the current UTC time (estimated completion target).

Because the Airbyte state API returns only the current (advancing)
cursor, the previous bookmark is not directly available from a single
snapshot.  Callers should supply `previous_state_data` (the state
from the previous completed sync) when available.  When it is not
supplied, the module falls back to using `sync_start_time` as the
range anchor, which works for real-time incremental syncs but yields
`progress_pct = None` for historical back-fills where the cursor is
behind `sync_start_time`.

Each per-stream result always includes the raw factors that went into
the estimate so callers can compute their own progress or track it
across multiple calls:

- `cursor_value` / `cursor_datetime` -- the current cursor position
- `previous_cursor_value` / `previous_cursor_datetime` -- the
  baseline, if known
- `target_datetime` -- the estimated target (`now`)
- `sync_start_time` -- the wall-clock job start

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


def _build_previous_cursor_map(
    previous_state_data: dict[str, Any],
    catalog_data: dict[str, Any] | None,
) -> dict[tuple[str, str | None], str | None]:
    """Build a map of `(stream_name, namespace)` to previous cursor value.

    Parses the previous state snapshot and extracts cursor values for
    each stream, returning a lookup dict.
    """
    result: dict[tuple[str, str | None], str | None] = {}
    try:
        prev_state = ConnectionStateResponse(**previous_state_data)
        prev_streams: list[StreamState] = _get_stream_list(prev_state)
    except (ValidationError, TypeError, KeyError):
        return result

    for stream in prev_streams:
        s_name = stream.stream_descriptor.name
        s_ns = stream.stream_descriptor.namespace

        cursor_field: str | None = None
        if catalog_data:
            cursor_field = _extract_cursor_field_from_catalog(catalog_data, s_name, s_ns)

        cursor_val = _find_cursor_value_in_state(stream.stream_state, cursor_field)
        result[s_name, s_ns] = cursor_val

    return result


def _compute_single_stream_progress(
    stream: StreamState,
    catalog_data: dict[str, Any] | None,
    sync_start_time: datetime,
    now: datetime,
    prev_cursor_map: dict[tuple[str, str | None], str | None],
) -> dict[str, Any]:
    """Compute progress for a single stream.

    Returns a dict containing raw factors and the computed `progress_pct`.
    """
    stream_name = stream.stream_descriptor.name
    stream_namespace = stream.stream_descriptor.namespace

    # Look up cursor field from catalog
    cursor_field: str | None = None
    if catalog_data:
        cursor_field = _extract_cursor_field_from_catalog(
            catalog_data, stream_name, stream_namespace
        )

    # Find cursor value in current (advancing) state
    cursor_value_str = _find_cursor_value_in_state(stream.stream_state, cursor_field)

    # Find previous cursor value
    prev_cursor_str: str | None = prev_cursor_map.get((stream_name, stream_namespace))
    prev_cursor_dt: datetime | None = None
    if prev_cursor_str:
        prev_cursor_dt = _try_parse_datetime_cursor(prev_cursor_str)

    entry: dict[str, Any] = {
        "stream_name": stream_name,
        "stream_namespace": stream_namespace,
        "cursor_field": cursor_field,
        "cursor_value": cursor_value_str,
        "cursor_datetime": None,
        "previous_cursor_value": prev_cursor_str,
        "previous_cursor_datetime": prev_cursor_dt.isoformat() if prev_cursor_dt else None,
        "sync_start_time": sync_start_time.isoformat(),
        "target_datetime": now.isoformat(),
        "progress_pct": None,
        "reason": None,
    }

    if cursor_value_str is None:
        entry["reason"] = "No cursor value found in state."
        return entry

    cursor_dt = _try_parse_datetime_cursor(cursor_value_str)
    if cursor_dt is None:
        entry["reason"] = f"Cursor value '{cursor_value_str}' is not a recognized datetime format."
        return entry

    entry["cursor_datetime"] = cursor_dt.isoformat()

    # Determine the range anchor (baseline).
    # Prefer previous bookmark; fall back to sync_start_time.
    range_start: datetime = prev_cursor_dt if prev_cursor_dt is not None else sync_start_time

    denominator = (now - range_start).total_seconds()
    if denominator <= 0:
        entry["reason"] = "Range anchor is not before target time."
        return entry

    numerator = (cursor_dt - range_start).total_seconds()

    if numerator < 0:
        _set_negative_progress_reason(entry, prev_cursor_dt)
        return entry

    # Clamp to [0.0, 1.0]
    entry["progress_pct"] = round(max(0.0, min(1.0, numerator / denominator)), 4)
    return entry


def _set_negative_progress_reason(
    entry: dict[str, Any],
    prev_cursor_dt: datetime | None,
) -> None:
    """Set progress and reason when cursor is behind the range anchor."""
    if prev_cursor_dt is not None:
        entry["progress_pct"] = 0.0
        entry["reason"] = "Cursor has not advanced past the previous bookmark."
    else:
        entry["progress_pct"] = None
        entry["reason"] = (
            "Cursor is behind sync start time (historical back-fill). "
            "Previous bookmark not available; progress indeterminate. "
            "Use the raw factor fields to compute progress across "
            "multiple calls."
        )


def compute_stream_progress(
    state_data: dict[str, Any],
    catalog_data: dict[str, Any] | None,
    sync_start_time: datetime,
    now: datetime | None = None,
    previous_state_data: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Compute per-stream sync progress for datetime-cursor-based streams.

    Returns a list of per-stream dicts, each containing the raw factors
    that went into the progress estimate as well as the computed
    `progress_pct` (or `None` when indeterminate).

    Progress formula (when previous bookmark is available):

        progress = (cursor_dt - previous_bookmark_dt) / (now - previous_bookmark_dt)

    When `previous_state_data` is not supplied, falls back to
    `sync_start_time` as the range anchor:

        progress = (cursor_dt - sync_start_time) / (now - sync_start_time)

    This fallback works for real-time incremental syncs where cursors
    advance near wall-clock time, but yields `progress_pct = None`
    for historical back-fills where the cursor is behind
    `sync_start_time`.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    # Ensure times are timezone-aware
    if sync_start_time.tzinfo is None:
        sync_start_time = sync_start_time.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    # Build previous cursor lookup if previous state was provided
    prev_cursor_map: dict[tuple[str, str | None], str | None] = {}
    if previous_state_data:
        prev_cursor_map = _build_previous_cursor_map(previous_state_data, catalog_data)

    try:
        state = ConnectionStateResponse(**state_data)
        streams: list[StreamState] = _get_stream_list(state)
    except (ValidationError, TypeError, KeyError):
        logger.warning("Failed to parse connection state data; returning empty progress.")
        streams = []

    return [
        _compute_single_stream_progress(
            stream=stream,
            catalog_data=catalog_data,
            sync_start_time=sync_start_time,
            now=now,
            prev_cursor_map=prev_cursor_map,
        )
        for stream in streams
    ]
