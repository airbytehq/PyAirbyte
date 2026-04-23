# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Local-sync progress tracking via direct observation of state messages.

When PyAirbyte runs a sync locally (`Source.read()`, or `Source` -> `Destination`
via `tally_pending_writes` / `tally_confirmed_writes`), it acts as the in-process
intermediary that buffers every `AirbyteMessage` on its way from the source to the
cache/destination. This gives PyAirbyte the ability to directly observe both:

- **Source-side cursors**: state messages emitted by the source as it advances
  through its records (tracked in `ProgressTracker.tally_records_read`).
- **Destination-committed cursors**: state messages acknowledged by the
  destination after batches are committed (tracked in
  `ProgressTracker.tally_confirmed_writes`).

This module provides small helpers used by `airbyte.progress.ProgressTracker`
to extract cursor values from state messages, compute a simple progress
percentage for datetime cursors, and serialize per-stream progress snapshots
for JSONL audit logging.

This is intentionally distinct from `airbyte.cloud._sync_progress`, which
reconstructs progress from snapshots returned by the Airbyte Platform Config
API. The local-sync path has direct access to every state message and does
not require an external API call.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from airbyte_cdk.utils.datetime_helpers import ab_datetime_try_parse


if TYPE_CHECKING:
    from airbyte_protocol.models import AirbyteStateMessage


# Field names commonly used as datetime cursors by Airbyte connectors.
# Checked in order when multiple keys exist in `stream_state`.
_COMMON_CURSOR_FIELDS: tuple[str, ...] = (
    "updatedAt",
    "updated_at",
    "createdAt",
    "created_at",
    "timestamp",
    "cursor",
    "date",
    "modified",
    "modified_at",
    "last_modified",
    "lastModified",
)


def _try_parse_datetime_cursor(value: str) -> datetime | None:
    """Attempt to parse a string as a datetime.

    Delegates to the CDK's `ab_datetime_try_parse` and rejects pure numeric
    strings (which the CDK parser would otherwise interpret as epoch timestamps).
    Returns `None` when the value cannot be parsed as a datetime.
    """
    stripped = value.strip()
    if not stripped:
        return None

    try:
        float(stripped)
    except ValueError:
        pass
    else:
        return None

    dt = ab_datetime_try_parse(stripped)
    if dt is None:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _normalize_stream_state(stream_state: object) -> dict[str, Any] | None:
    """Normalize a `stream_state` value to a plain `dict`.

    The Airbyte protocol models typically represent `stream_state` as an
    `AirbyteStateBlob` (a Pydantic model with `extra="allow"`) rather than a
    raw dict.  This helper coerces either form into a dict so downstream
    cursor extraction can treat them uniformly.  Returns `None` when the
    value cannot be represented as a dict.
    """
    if stream_state is None:
        return None
    if isinstance(stream_state, dict):
        return stream_state
    if hasattr(stream_state, "model_dump"):
        dumped = stream_state.model_dump()
        if isinstance(dumped, dict):
            return dumped
    return None


_MAX_STATE_RECURSION_DEPTH = 4


def _find_known_cursor(
    state_dict: dict[str, Any],
    depth: int,
) -> tuple[str | None, str | None]:
    """Depth-first search for a `_COMMON_CURSOR_FIELDS` key in nested dicts.

    Some connectors (notably `source-github`) nest per-stream state under a
    partition key (e.g. the repo name), so the cursor lives at
    `stream_state[<repo>][updated_at]` rather than `stream_state[updated_at]`.
    This helper walks up to `_MAX_STATE_RECURSION_DEPTH` levels, checking
    known cursor field names at each level before descending. Non-`dict` and
    `None` values are skipped.
    """
    for candidate in _COMMON_CURSOR_FIELDS:
        if candidate in state_dict:
            raw = state_dict[candidate]
            if raw is not None and not isinstance(raw, dict):
                return candidate, str(raw)

    if depth >= _MAX_STATE_RECURSION_DEPTH:
        return None, None

    for raw in state_dict.values():
        nested = _normalize_stream_state(raw)
        if not nested:
            continue
        cursor_field, cursor_value = _find_known_cursor(nested, depth + 1)
        if cursor_field is not None:
            return cursor_field, cursor_value

    return None, None


def _find_datetime_fallback(
    state_dict: dict[str, Any],
    depth: int,
) -> tuple[str | None, str | None]:
    """Depth-first search for the first datetime-parseable scalar value.

    Used only when no well-known cursor field name (`_COMMON_CURSOR_FIELDS`)
    is present anywhere in the state tree. Recurses up to
    `_MAX_STATE_RECURSION_DEPTH` levels into nested dicts.
    """
    for key, raw in state_dict.items():
        if raw is None:
            continue
        if isinstance(raw, (str, int, float)):
            value = str(raw)
            if _try_parse_datetime_cursor(value) is not None:
                return key, value

    if depth >= _MAX_STATE_RECURSION_DEPTH:
        return None, None

    for raw in state_dict.values():
        nested = _normalize_stream_state(raw)
        if not nested:
            continue
        cursor_field, cursor_value = _find_datetime_fallback(nested, depth + 1)
        if cursor_field is not None:
            return cursor_field, cursor_value

    return None, None


def _extract_cursor_from_stream_state(
    stream_state: object,
) -> tuple[str | None, str | None]:
    """Return `(cursor_field, cursor_value)` from a `stream_state` blob.

    Recursively searches nested state dicts so per-partition state (e.g.
    `source-github`'s `{"<repo>": {"updated_at": "..."}}`) is handled.

    The search prefers well-known cursor field names (`updatedAt`,
    `created_at`, …) at any depth up to `_MAX_STATE_RECURSION_DEPTH`. If
    none of those are present anywhere in the tree, falls back to the first
    datetime-parseable scalar. Returns `(None, None)` when no usable cursor
    can be extracted.
    """
    state_dict = _normalize_stream_state(stream_state)
    if not state_dict:
        return None, None

    cursor_field, cursor_value = _find_known_cursor(state_dict, depth=0)
    if cursor_field is not None:
        return cursor_field, cursor_value

    return _find_datetime_fallback(state_dict, depth=0)


def extract_cursor_from_state_message(
    state_message: AirbyteStateMessage,
) -> tuple[str | None, str | None, str | None]:
    """Return `(stream_name, cursor_field, cursor_value)` from a state message.

    Handles `STREAM`-type state messages. `GLOBAL` and `LEGACY` state
    messages are not per-stream and return `(None, None, None)` -- callers
    should fall back to other tracking strategies for those.
    """
    stream = getattr(state_message, "stream", None)
    if stream is None:
        return None, None, None

    descriptor = getattr(stream, "stream_descriptor", None)
    if descriptor is None:
        return None, None, None

    stream_name = getattr(descriptor, "name", None)
    if not stream_name:
        return None, None, None

    cursor_field, cursor_value = _extract_cursor_from_stream_state(
        getattr(stream, "stream_state", None)
    )
    return stream_name, cursor_field, cursor_value


def compute_stream_progress_pct(
    *,
    baseline_cursor: str | None,
    latest_cursor: str | None,
    now: datetime | None = None,
) -> float | None:
    """Compute a progress percentage for a single stream's datetime cursor.

    The formula is:

        progress = (latest - baseline) / (now - baseline)

    Returns a value clamped to `[0.0, 1.0]`, or `None` when either cursor is
    missing, cannot be parsed as a datetime, or when the denominator is not
    positive (e.g. a historical backfill where `now` equals the baseline).
    """
    if baseline_cursor is None or latest_cursor is None:
        return None

    baseline_dt = _try_parse_datetime_cursor(baseline_cursor)
    latest_dt = _try_parse_datetime_cursor(latest_cursor)
    if baseline_dt is None or latest_dt is None:
        return None

    now_dt = now or datetime.now(timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)

    denominator = (now_dt - baseline_dt).total_seconds()
    if denominator <= 0:
        return None

    numerator = (latest_dt - baseline_dt).total_seconds()
    if numerator < 0:
        return 0.0

    return round(max(0.0, min(1.0, numerator / denominator)), 4)
