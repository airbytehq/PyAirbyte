# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for `airbyte.cli.smoke_test_source.source.SourceSmokeTest`.

Covers:

- The incremental cursor coalesce rule (state > start_date > Jan 1 UTC).
- STATE emission triggered by `batch_size`, `partition_by`, and end-of-stream.
- Deduplication when triggers coincide.
- `check()` validation for new config fields.
- Non-regression for full-refresh scenarios.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest
from airbyte_cdk.models import (
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    Status,
    StreamDescriptor,
    SyncMode,
    Type,
)

from airbyte.cli.smoke_test_source._scenarios import (
    _partition_bucket,
    iter_incremental_scenario_events,
)
from airbyte.cli.smoke_test_source.source import (
    SourceSmokeTest,
    _default_cursor_start,
    _parse_cursor_step,
    _parse_start_date,
    _state_cursor_for_stream,
)


_LOGGER = logging.getLogger("airbyte.test")
_STREAM = "incremental_batch_stream"


def _source() -> SourceSmokeTest:
    return SourceSmokeTest()


def _configured_catalog(
    sync_mode: SyncMode = SyncMode.incremental,
    stream_name: str = _STREAM,
) -> ConfiguredAirbyteCatalog:
    src = _source()
    # `incremental_batch_stream` is `high_volume: True`, so opt into the
    # slow-stream set explicitly when building the test catalog.
    catalog = src.discover(_LOGGER, {"all_slow_streams": True})
    stream = next(s for s in catalog.streams if s.name == stream_name)
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=stream,
                sync_mode=sync_mode,
                destination_sync_mode=DestinationSyncMode.append,
                cursor_field=["updated_at"]
                if sync_mode == SyncMode.incremental
                else None,
            )
        ],
    )


def _state_for(stream_name: str, updated_at: str) -> list[AirbyteStateMessage]:
    return [
        AirbyteStateMessage(
            type=AirbyteStateType.STREAM,
            stream=AirbyteStreamState(
                stream_descriptor=StreamDescriptor(name=stream_name, namespace=None),
                stream_state=AirbyteStateBlob(updated_at=updated_at),
            ),
        ),
    ]


def _records_and_states(messages: list) -> tuple[list, list]:
    return (
        [m for m in messages if m.type == Type.RECORD],
        [m for m in messages if m.type == Type.STATE],
    )


def _state_cursor(msg) -> str:
    return getattr(msg.state.stream.stream_state, "updated_at")


# ---------------------------------------------------------------------------
# Spec / discover
# ---------------------------------------------------------------------------


def test_spec_includes_incremental_config_fields():
    spec = _source().spec(_LOGGER)
    props = spec.connectionSpecification["properties"]
    for key in (
        "start_date",
        "batch_size",
        "batch_count",
        "partition_by",
        "cursor_step",
    ):
        assert key in props, f"spec missing {key!r}"


def test_discover_marks_incremental_stream_correctly():
    catalog = _source().discover(_LOGGER, {"all_slow_streams": True})
    incr = next(s for s in catalog.streams if s.name == _STREAM)
    assert SyncMode.incremental in incr.supported_sync_modes
    assert SyncMode.full_refresh in incr.supported_sync_modes
    assert incr.source_defined_cursor is True
    assert incr.default_cursor_field == ["updated_at"]


def test_discover_leaves_full_refresh_scenarios_unchanged():
    catalog = _source().discover(_LOGGER, {})
    basic = next(s for s in catalog.streams if s.name == "basic_types")
    assert basic.supported_sync_modes == [SyncMode.full_refresh]
    assert basic.source_defined_cursor is False


# ---------------------------------------------------------------------------
# Cursor coalesce
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(None, None, id="none"),
        pytest.param("", None, id="empty_string"),
        pytest.param(
            "2024-06-15",
            datetime(2024, 6, 15, tzinfo=timezone.utc),
            id="date_only",
        ),
        pytest.param(
            "2024-06-15T12:34:56Z",
            datetime(2024, 6, 15, 12, 34, 56, tzinfo=timezone.utc),
            id="datetime_z",
        ),
        pytest.param(
            "2024-06-15T12:34:56+02:00",
            datetime(2024, 6, 15, 10, 34, 56, tzinfo=timezone.utc),
            id="datetime_offset_normalized_to_utc",
        ),
    ],
)
def test_parse_start_date(value, expected):
    assert _parse_start_date(value) == expected


def test_parse_start_date_rejects_garbage():
    with pytest.raises(ValueError):
        _parse_start_date("not-a-date-at-all")


@pytest.mark.parametrize(
    ("value", "expected_seconds"),
    [
        pytest.param(None, 1, id="default"),
        pytest.param(30, 30, id="int_seconds"),
        pytest.param("30", 30, id="numeric_string"),
        pytest.param("PT30S", 30, id="iso_seconds"),
        pytest.param("PT1H", 3600, id="iso_hours"),
        pytest.param("P1D", 86400, id="iso_days"),
        pytest.param("P1DT6H", 86400 + 6 * 3600, id="iso_days_hours"),
    ],
)
def test_parse_cursor_step(value, expected_seconds):
    assert _parse_cursor_step(value) == timedelta(seconds=expected_seconds)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(0, id="zero_seconds"),
        pytest.param(-5, id="negative_seconds"),
        pytest.param("PT", id="empty_duration"),
        pytest.param("PTbadS", id="garbage_duration"),
    ],
)
def test_parse_cursor_step_rejects_invalid_value(value):
    with pytest.raises(ValueError):
        _parse_cursor_step(value)


def test_parse_cursor_step_rejects_bool_as_type_error():
    with pytest.raises(TypeError):
        _parse_cursor_step(True)


def test_default_cursor_start_is_jan_1_current_utc_year():
    # Sample the SUT first so a Dec 31 23:59:59 -> Jan 1 boundary can't make
    # `now.year` disagree with the year the SUT observed.
    result = _default_cursor_start()
    now = datetime.now(tz=timezone.utc)
    assert result in {
        datetime(now.year, 1, 1, tzinfo=timezone.utc),
        datetime(now.year - 1, 1, 1, tzinfo=timezone.utc),
    }


def test_state_cursor_returns_none_when_no_state():
    assert _state_cursor_for_stream(None, _STREAM, None, "updated_at") is None
    assert _state_cursor_for_stream([], _STREAM, None, "updated_at") is None


def test_state_cursor_returns_parsed_value_when_present():
    state = _state_for(_STREAM, "2030-06-15T12:00:00Z")
    result = _state_cursor_for_stream(state, _STREAM, None, "updated_at")
    assert result == datetime(2030, 6, 15, 12, 0, 0, tzinfo=timezone.utc)


def test_state_cursor_returns_none_when_stream_mismatch():
    state = _state_for("other_stream", "2030-06-15T12:00:00Z")
    assert _state_cursor_for_stream(state, _STREAM, None, "updated_at") is None


# ---------------------------------------------------------------------------
# Incremental iterator
# ---------------------------------------------------------------------------


def test_iter_emits_size_triggered_states_plus_terminal():
    events = list(
        iter_incremental_scenario_events(
            {"name": _STREAM},
            cursor_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            cursor_step=timedelta(hours=1),
            batch_size=5,
            batch_count=3,
        )
    )
    records = [e for e in events if e["kind"] == "record"]
    states = [e for e in events if e["kind"] == "state"]
    assert len(records) == 15
    assert len(states) == 3
    assert [s["cursor"] for s in states] == [
        "2024-01-01T05:00:00Z",
        "2024-01-01T10:00:00Z",
        "2024-01-01T15:00:00Z",
    ]


def test_iter_emits_partition_boundary_states():
    events = list(
        iter_incremental_scenario_events(
            {"name": _STREAM},
            cursor_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            cursor_step=timedelta(hours=6),
            batch_size=4,
            batch_count=3,
            partition_by="day",
        )
    )
    records = [e for e in events if e["kind"] == "record"]
    states = [e for e in events if e["kind"] == "state"]
    assert len(records) == 12
    assert [s["cursor"] for s in states] == [
        "2024-01-02T00:00:00Z",
        "2024-01-03T00:00:00Z",
        "2024-01-04T00:00:00Z",
    ]


def test_iter_dedupes_coincident_triggers():
    events = list(
        iter_incremental_scenario_events(
            {"name": _STREAM},
            cursor_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            cursor_step=timedelta(hours=12),
            batch_size=2,
            batch_count=2,
            partition_by="day",
        )
    )
    states = [e for e in events if e["kind"] == "state"]
    assert [s["cursor"] for s in states] == [
        "2024-01-02T00:00:00Z",
        "2024-01-03T00:00:00Z",
    ]


def test_iter_emits_terminal_state_when_no_trigger_at_end():
    events = list(
        iter_incremental_scenario_events(
            {"name": _STREAM},
            cursor_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            cursor_step=timedelta(hours=1),
            batch_size=10,
            batch_count=None,
            max_records=3,
        )
    )
    states = [e for e in events if e["kind"] == "state"]
    assert len(states) == 1
    assert states[0]["cursor"] == "2024-01-01T03:00:00Z"


@pytest.mark.parametrize(
    ("ts", "grain", "expected"),
    [
        pytest.param(
            datetime(2024, 6, 15, 12, tzinfo=timezone.utc),
            "day",
            (2024, 6, 15),
            id="day",
        ),
        pytest.param(
            datetime(2024, 6, 15, 12, tzinfo=timezone.utc),
            "week",
            datetime(2024, 6, 15).isocalendar()[:2],
            id="week",
        ),
        pytest.param(
            datetime(2024, 6, 15, 12, tzinfo=timezone.utc),
            "month",
            (2024, 6),
            id="month",
        ),
    ],
)
def test_partition_bucket(ts, grain, expected):
    assert _partition_bucket(ts, grain) == tuple(expected)


# ---------------------------------------------------------------------------
# read() end-to-end
# ---------------------------------------------------------------------------


def test_read_incremental_uses_start_date_when_no_state():
    src = _source()
    catalog = _configured_catalog(SyncMode.incremental)
    config = {
        "scenario_filter": [_STREAM],
        "all_fast_streams": False,
        "start_date": "2024-01-01T00:00:00Z",
        "batch_size": 2,
        "batch_count": 1,
        "cursor_step": 3600,
    }
    records, states = _records_and_states(list(src.read(_LOGGER, config, catalog)))
    assert len(records) == 2
    assert records[0].record.data["updated_at"] == "2024-01-01T01:00:00Z"
    assert records[-1].record.data["updated_at"] == "2024-01-01T02:00:00Z"
    assert len(states) == 1
    assert _state_cursor(states[0]) == "2024-01-01T02:00:00Z"


def test_read_incremental_state_overrides_start_date():
    src = _source()
    catalog = _configured_catalog(SyncMode.incremental)
    state = _state_for(_STREAM, "2030-06-15T12:00:00Z")
    config = {
        "scenario_filter": [_STREAM],
        "all_fast_streams": False,
        "start_date": "2024-01-01T00:00:00Z",
        "batch_size": 2,
        "batch_count": 1,
        "cursor_step": 3600,
    }
    records, _ = _records_and_states(
        list(src.read(_LOGGER, config, catalog, state=state))
    )
    assert records[0].record.data["updated_at"] == "2030-06-15T13:00:00Z"


def test_read_incremental_defaults_to_jan_1_current_utc_year():
    src = _source()
    catalog = _configured_catalog(SyncMode.incremental)
    config = {
        "scenario_filter": [_STREAM],
        "all_fast_streams": False,
        "batch_size": 1,
        "batch_count": 1,
        "cursor_step": 3600,
    }
    records, _ = _records_and_states(list(src.read(_LOGGER, config, catalog)))
    now_year = datetime.now(tz=timezone.utc).year
    # Tolerate a Dec 31 -> Jan 1 flip between the SUT call and this sample.
    assert records[0].record.data["updated_at"] in {
        f"{now_year}-01-01T01:00:00Z",
        f"{now_year - 1}-01-01T01:00:00Z",
    }


def test_read_full_refresh_scenario_unchanged_by_incremental_config():
    src = _source()
    catalog = _configured_catalog(SyncMode.full_refresh, stream_name="basic_types")
    config = {
        "start_date": "2024-01-01T00:00:00Z",
        "batch_size": 5,
        "batch_count": 3,
        "partition_by": "day",
        "cursor_step": 3600,
    }
    records, states = _records_and_states(list(src.read(_LOGGER, config, catalog)))
    assert len(records) == 3
    assert states == []


# ---------------------------------------------------------------------------
# check()
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "config",
    [
        pytest.param({}, id="empty"),
        pytest.param({"start_date": "2024-01-01"}, id="valid_start_date"),
        pytest.param({"batch_size": 10, "batch_count": 5}, id="valid_batches"),
        pytest.param({"partition_by": "day"}, id="valid_partition"),
        pytest.param({"cursor_step": "PT1H"}, id="valid_cursor_step_iso"),
        pytest.param({"cursor_step": 60}, id="valid_cursor_step_int"),
    ],
)
def test_check_accepts_valid_configs(config):
    result = _source().check(_LOGGER, config)
    assert result.status == Status.SUCCEEDED


@pytest.mark.parametrize(
    "config",
    [
        pytest.param({"start_date": "garbage-value-xyz"}, id="bad_start_date"),
        pytest.param({"batch_size": 0}, id="zero_batch_size"),
        pytest.param({"batch_count": -1}, id="negative_batch_count"),
        pytest.param({"partition_by": "year"}, id="bad_partition_grain"),
        pytest.param({"cursor_step": "PTbadS"}, id="bad_cursor_step"),
        pytest.param({"batch_size": True}, id="batch_size_bool_true"),
        pytest.param({"batch_count": True}, id="batch_count_bool_true"),
        pytest.param({"batch_count": 5}, id="batch_count_without_batch_size"),
        pytest.param({"start_date": "2024-13-45"}, id="invalid_10char_date"),
    ],
)
def test_check_rejects_invalid_configs(config):
    result = _source().check(_LOGGER, config)
    assert result.status == Status.FAILED
    assert result.message
