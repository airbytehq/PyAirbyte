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

from airbyte_cdk.utils.datetime_helpers import ab_datetime_try_parse

from airbyte.cloud._connection_state import (
    ConnectionStateResponse,
    StreamState,
    _get_stream_list,
)


logger = logging.getLogger(__name__)


def _try_parse_datetime_cursor(value: str) -> datetime | None:
    """Attempt to parse a string as a datetime.

    Delegates to the CDK's `ab_datetime_try_parse` for the actual parsing.
    Returns `None` if the value is numeric or cannot be parsed.
    """
    stripped = value.strip()
    if not stripped:
        return None

    # Reject pure numeric strings — the CDK parser interprets them as
    # epoch timestamps, but cursor values like "12345" are opaque tokens.
    try:
        float(stripped)
    except ValueError:
        pass
    else:
        return None

    dt = ab_datetime_try_parse(stripped)
    if dt is None:
        return None

    # Ensure timezone-aware (assume UTC if naive)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _extract_cursor_field_from_catalog(
    catalog: dict[str, Any],
    stream_name: str,
    stream_namespace: str | None,
) -> str | None:
    """Extract the cursor field name for a stream from the configured catalog.

    Returns the cursor field name, or `None` if the stream is not found
    or does not have a cursor field configured.
    """
    # Handle both raw catalog ({"streams": [...]}) and full connection
    # response ({"syncCatalog": {"streams": [...]}}) from the Config API.
    streams = catalog.get("streams", [])
    if not streams and "syncCatalog" in catalog:
        streams = catalog["syncCatalog"].get("streams", [])

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

    Requires `cursor_field` (from the configured catalog) to locate the
    cursor.  Returns `None` when the cursor field is unknown or absent.
    """
    if not stream_state:
        return None

    if not cursor_field:
        return None

    # Traverse dot-delimited paths (e.g. "metadata.updated_at")
    current: Any = stream_state
    for segment in cursor_field.split("."):
        if isinstance(current, dict) and segment in current:
            current = current[segment]
        else:
            return None

    if current is not None:
        return str(current)

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
    if not isinstance(previous_state_data, dict):
        return result

    prev_state = ConnectionStateResponse(**previous_state_data)
    prev_streams: list[StreamState] = _get_stream_list(prev_state)

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
    first_seen_cursors: dict[tuple[str, str | None], str] | None = None,
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

    # Resolve the first-observed cursor for this stream (tier 2 baseline).
    first_seen_dt: datetime | None = None
    if first_seen_cursors:
        first_seen_val = first_seen_cursors.get((stream_name, stream_namespace))
        if first_seen_val is not None:
            first_seen_dt = _try_parse_datetime_cursor(first_seen_val)

    _compute_progress_pct(
        entry=entry,
        cursor_dt=cursor_dt,
        prev_cursor_dt=prev_cursor_dt,
        first_seen_dt=first_seen_dt,
        sync_start_time=sync_start_time,
        now=now,
    )
    return entry


def _compute_progress_pct(
    *,
    entry: dict[str, Any],
    cursor_dt: datetime,
    prev_cursor_dt: datetime | None,
    first_seen_dt: datetime | None,
    sync_start_time: datetime,
    now: datetime,
) -> None:
    """Populate `progress_pct` and `reason` on `entry` in-place.

    Handles two modes:

    1. *Historical backfill* — the cursor is at or behind the previous
       bookmark (e.g. GA4 re-processing from an earlier date).  Progress is
       measured as `(cursor - first_seen) / (prev_bookmark - first_seen)`.

    2. *Standard forward progress* — the cursor advances beyond the
       baseline toward `now`.  Progress is
       `(cursor - range_start) / (now - range_start)`.
    """
    # --- Historical backfill path ---
    if (
        prev_cursor_dt is not None
        and cursor_dt <= prev_cursor_dt
        and first_seen_dt is not None
        and first_seen_dt < prev_cursor_dt
    ):
        denominator = (prev_cursor_dt - first_seen_dt).total_seconds()
        if denominator <= 0:
            entry["reason"] = "Range anchor is not before target time."
            return
        numerator = (cursor_dt - first_seen_dt).total_seconds()
        entry["progress_pct"] = round(max(0.0, min(1.0, numerator / denominator)), 4)
        if entry["progress_pct"] == 0.0:
            entry["reason"] = "Cursor has not yet advanced past the first observed value."
        return

    # --- Standard forward-progress path ---
    range_start: datetime | None = prev_cursor_dt
    if range_start is None and first_seen_dt is not None:
        range_start = first_seen_dt
    if range_start is None:
        range_start = sync_start_time

    denominator = (now - range_start).total_seconds()
    if denominator <= 0:
        entry["reason"] = "Range anchor is not before target time."
        return

    numerator = (cursor_dt - range_start).total_seconds()
    if numerator < 0:
        _set_negative_progress_reason(entry, prev_cursor_dt)
        return

    entry["progress_pct"] = round(max(0.0, min(1.0, numerator / denominator)), 4)


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
    first_seen_cursors: dict[tuple[str, str | None], str] | None = None,
) -> list[dict[str, Any]]:
    """Compute per-stream sync progress for datetime-cursor-based streams.

    Returns a list of per-stream dicts, each containing the raw factors
    that went into the progress estimate as well as the computed
    `progress_pct` (or `None` when indeterminate).

    Progress formula (when previous bookmark is available):

        progress = (cursor_dt - previous_bookmark_dt) / (now - previous_bookmark_dt)

    Baseline selection uses a tiered fallback:

    1. Previous completed sync's cursor from `previous_state_data`.
    2. First-observed cursor during polling (`first_seen_cursors`).
    3. `sync_start_time` (wall-clock job start).

    Tier 3 works for real-time incremental syncs where cursors advance
    near wall-clock time, but yields `progress_pct = None` for
    historical back-fills where the cursor is behind `sync_start_time`.
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

    if not isinstance(state_data, dict):
        logger.warning(
            "Expected dict for state_data, got %s; returning empty progress.",
            type(state_data).__name__,
        )
        return []

    state = ConnectionStateResponse(**state_data)
    streams: list[StreamState] = _get_stream_list(state)

    return [
        _compute_single_stream_progress(
            stream=stream,
            catalog_data=catalog_data,
            sync_start_time=sync_start_time,
            now=now,
            prev_cursor_map=prev_cursor_map,
            first_seen_cursors=first_seen_cursors,
        )
        for stream in streams
    ]
