# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Smoke test source for destination regression testing.

This source generates synthetic test data covering common edge cases
that break destinations: type variations, null handling, naming edge cases,
schema variations, and batch size variations.

Predefined scenarios are always available. Additional scenarios can be
injected dynamically via the ``custom_scenarios`` config field.

.. warning::
    This module is experimental and subject to change without notice.
    The APIs and behavior may be modified or removed in future versions.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateBlob,
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStream,
    AirbyteStreamState,
    AirbyteStreamStatus,
    AirbyteStreamStatusTraceMessage,
    AirbyteTraceMessage,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification,
    Status,
    StreamDescriptor,
    SyncMode,
    TraceType,
    Type,
)
from airbyte_cdk.sources.source import Source

from airbyte.cli.smoke_test_source._scenarios import (
    _DEFAULT_LARGE_BATCH_COUNT,
    PREDEFINED_SCENARIOS,
    PartitionGrain,
    get_scenario_records,
    iter_incremental_scenario_events,
)


if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


_VALID_PARTITION_GRAINS: tuple[str, ...] = ("day", "week", "month")

_ISO_DATE_LENGTH = 10

_ISO_DURATION_RE = re.compile(
    r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$",
)


logger = logging.getLogger("airbyte")


def _build_streams_from_scenarios(
    scenarios: list[dict[str, Any]],
    namespace: str | None = None,
) -> list[AirbyteStream]:
    """Build AirbyteStream objects from scenario definitions.

    Scenarios with `incremental=True` advertise both `full_refresh` and
    `incremental` sync modes, set `source_defined_cursor=True`, and expose
    the scenario's `cursor_field` as `default_cursor_field`.
    """
    streams: list[AirbyteStream] = []
    for scenario in scenarios:
        is_incremental = bool(scenario.get("incremental"))
        sync_modes = [SyncMode.full_refresh]
        if is_incremental:
            sync_modes.append(SyncMode.incremental)
        stream_kwargs: dict[str, Any] = {
            "name": scenario["name"],
            "namespace": namespace,
            "json_schema": scenario["json_schema"],
            "supported_sync_modes": sync_modes,
            "source_defined_cursor": is_incremental,
            "source_defined_primary_key": scenario.get("primary_key"),
        }
        if is_incremental and scenario.get("cursor_field"):
            stream_kwargs["default_cursor_field"] = list(scenario["cursor_field"])
        streams.append(AirbyteStream(**stream_kwargs))
    return streams


def _parse_start_date(value: Any) -> datetime | None:  # noqa: ANN401
    """Parse `start_date` config into a UTC datetime, or `None` if unset.

    Accepts ISO-8601 date (`YYYY-MM-DD`) or datetime strings. Naive inputs
    are assumed to be UTC.
    """
    if value is None or value == "":  # noqa: PLC1901
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            if len(text) == _ISO_DATE_LENGTH:
                try:
                    parsed_date = date.fromisoformat(text)
                except ValueError as date_exc:
                    raise ValueError(f"Invalid `start_date`: {value!r}") from date_exc
                parsed = datetime(
                    parsed_date.year,
                    parsed_date.month,
                    parsed_date.day,
                    tzinfo=timezone.utc,
                )
            else:
                raise ValueError(f"Invalid `start_date`: {value!r}") from exc
        dt = parsed
    else:
        raise TypeError(f"Invalid `start_date` type: {type(value).__name__}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_cursor_step(value: Any) -> timedelta:  # noqa: ANN401
    """Parse `cursor_step` config into a positive `timedelta`.

    Accepts numeric seconds, numeric strings, or ISO-8601 duration strings
    (subset: `PnDTnHnMnS`). Defaults to 1 second when unset.
    """
    if value is None or value == "":  # noqa: PLC1901
        return timedelta(seconds=1)
    if isinstance(value, bool):
        raise TypeError("`cursor_step` must be a number of seconds or ISO-8601 duration.")
    if isinstance(value, (int, float)):
        seconds = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if text.startswith("P"):
            match = _ISO_DURATION_RE.match(text)
            if not match or match.group(0) == "P" or not any(match.groups()):
                raise ValueError(f"Invalid ISO-8601 duration for `cursor_step`: {value!r}")
            days = int(match.group(1) or 0)
            hours = int(match.group(2) or 0)
            minutes = int(match.group(3) or 0)
            secs = float(match.group(4) or 0)
            seconds = days * 86400 + hours * 3600 + minutes * 60 + secs
        else:
            try:
                seconds = float(text)
            except ValueError as exc:
                raise ValueError(f"Invalid `cursor_step`: {value!r}") from exc
    else:
        raise TypeError(f"Invalid `cursor_step` type: {type(value).__name__}")
    if seconds <= 0:
        raise ValueError("`cursor_step` must be a positive duration.")
    return timedelta(seconds=seconds)


def _default_cursor_start() -> datetime:
    """Return Jan 1 of the current UTC year at `00:00:00Z`."""
    now = datetime.now(tz=timezone.utc)
    return datetime(now.year, 1, 1, tzinfo=timezone.utc)


def _coalesce_partition_grain(raw: Any) -> PartitionGrain | None:  # noqa: ANN401
    """Return `raw` as a valid `PartitionGrain`, or `None` when unset/invalid.

    Invalid values fall through to `None` here; `check()` rejects them upfront.
    """
    return raw if raw in _VALID_PARTITION_GRAINS else None


def _state_cursor_for_stream(
    state: list[AirbyteStateMessage] | None,
    stream_name: str,
    namespace: str | None,
    cursor_field: str,
) -> datetime | None:
    """Extract the `cursor_field` value from per-stream state, if present."""
    if not state:
        return None
    for msg in state:
        stream_state = getattr(msg, "stream", None)
        if not stream_state:
            continue
        descriptor = getattr(stream_state, "stream_descriptor", None)
        if descriptor is None or descriptor.name != stream_name:
            continue
        if getattr(descriptor, "namespace", None) != namespace:
            continue
        blob = getattr(stream_state, "stream_state", None)
        if blob is None:
            continue
        if isinstance(blob, dict):
            raw = blob.get(cursor_field)
        else:
            raw = getattr(blob, cursor_field, None)
        if raw is None:
            continue
        return _parse_start_date(raw)
    return None


class SourceSmokeTest(Source):
    """Smoke test source for destination regression testing.

    Generates synthetic data across predefined scenarios that cover
    common destination failure patterns. Supports dynamic injection
    of additional scenarios via the ``custom_scenarios`` config field.
    """

    def spec(
        self,
        logger: logging.Logger,  # noqa: ARG002
    ) -> ConnectorSpecification:
        """Return the connector specification."""
        return ConnectorSpecification(
            documentationUrl="https://docs.airbyte.com/integrations/sources/smoke-test",
            connectionSpecification={
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "Smoke Test Source Spec",
                "type": "object",
                "required": [],
                "properties": {
                    "custom_scenarios": {
                        "type": "array",
                        "title": "Custom Test Scenarios",
                        "description": (
                            "Additional test scenarios to inject "
                            "at runtime. Each scenario defines a "
                            "stream name, JSON schema, and records."
                        ),
                        "items": {
                            "type": "object",
                            "required": [
                                "name",
                                "json_schema",
                            ],
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "description": "Stream name for this scenario.",
                                },
                                "description": {
                                    "type": "string",
                                    "description": ("Human-readable description of this scenario."),
                                },
                                "json_schema": {
                                    "type": "object",
                                    "description": "JSON schema for the stream.",
                                },
                                "records": {
                                    "type": "array",
                                    "description": "Records to emit for this stream.",
                                    "items": {"type": "object"},
                                },
                                "primary_key": {
                                    "type": ["array", "null"],
                                    "description": (
                                        "Primary key definition (list of key paths) or null."
                                    ),
                                    "items": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                    },
                                },
                            },
                        },
                        "default": [],
                    },
                    "large_batch_record_count": {
                        "type": "integer",
                        "title": "Large Batch Record Count",
                        "description": (
                            "Number of records to generate for "
                            "the large_batch_stream scenario. "
                            "Set to 0 to emit no records for this stream."
                        ),
                        "default": 1000,
                    },
                    "all_fast_streams": {
                        "type": "boolean",
                        "title": "All Fast Streams",
                        "description": ("Include all fast (non-high-volume) predefined streams."),
                        "default": True,
                    },
                    "all_slow_streams": {
                        "type": "boolean",
                        "title": "All Slow Streams",
                        "description": (
                            "Include all slow (high-volume) streams "
                            "such as large_batch_stream. These are "
                            "excluded by default to avoid incurring "
                            "the cost of large record sets."
                        ),
                        "default": False,
                    },
                    "scenario_filter": {
                        "type": "array",
                        "title": "Scenario Filter",
                        "description": (
                            "Specific scenario names to include. "
                            "These are unioned with the boolean-driven "
                            "sets (deduped). If omitted or empty, "
                            "only the boolean flags control selection."
                        ),
                        "items": {"type": "string"},
                        "default": [],
                    },
                    "namespace": {
                        "type": ["string", "null"],
                        "title": "Namespace",
                        "description": (
                            "Namespace (schema/database) to set on all "
                            "streams. When provided, the destination will "
                            "write data into this namespace."
                        ),
                        "default": None,
                    },
                    "start_date": {
                        "type": ["string", "null"],
                        "title": "Start Date",
                        "description": (
                            "Lower bound for incremental streams' cursor. "
                            "Accepts an ISO-8601 date (`YYYY-MM-DD`) or a "
                            "date-time (`YYYY-MM-DDTHH:MM:SSZ`). When omitted "
                            "and no state is provided, the cursor starts at "
                            "Jan 1 (00:00:00 UTC) of the current year."
                        ),
                        "default": None,
                    },
                    "batch_size": {
                        "type": ["integer", "null"],
                        "title": "Batch Size",
                        "minimum": 1,
                        "description": (
                            "Emit a STATE message after every `batch_size` "
                            "records per incremental stream. Leave unset to "
                            "disable size-based checkpointing."
                        ),
                        "default": None,
                    },
                    "batch_count": {
                        "type": ["integer", "null"],
                        "title": "Batch Count",
                        "minimum": 1,
                        "description": (
                            "When paired with `batch_size`, stop each "
                            "incremental stream after this many batches. "
                            "Total records emitted = batch_size * batch_count."
                        ),
                        "default": None,
                    },
                    "partition_by": {
                        "type": ["string", "null"],
                        "title": "Partition By",
                        "enum": ["day", "week", "month"],
                        "description": (
                            "Emit a STATE message at each partition boundary "
                            "(UTC) of the cursor for incremental streams."
                        ),
                        "default": None,
                    },
                    "cursor_step": {
                        "type": ["string", "number", "null"],
                        "title": "Cursor Step",
                        "description": (
                            "Spacing between synthetic records for "
                            "incremental streams, as a number of seconds or "
                            "an ISO-8601 duration (e.g. `PT1S`, `PT1H`, "
                            "`P1D`). Defaults to 1 second."
                        ),
                        "default": None,
                    },
                },
            },
        )

    def _get_all_scenarios(
        self,
        config: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        """Combine predefined and custom scenarios.

        Selection logic:
        1. Boolean flags control groups: ``all_fast_streams``
           (default true) enables non-high-volume scenarios,
           ``all_slow_streams`` (default false) enables
           high-volume scenarios.
        2. ``scenario_filter`` names are unioned with the boolean sets.
        3. Custom scenarios are always included.
        4. The final list is deduplicated by name.
        """
        include_default = config.get("all_fast_streams", True)
        include_high_volume = config.get("all_slow_streams", False)
        raw_scenario_filter = config.get("scenario_filter", [])
        scenario_filter: list[str] = (
            [name for name in raw_scenario_filter if isinstance(name, str)]
            if isinstance(raw_scenario_filter, list)
            else []
        )
        explicit_names: set[str] = set(scenario_filter)

        large_batch_count = config.get(
            "large_batch_record_count",
            _DEFAULT_LARGE_BATCH_COUNT,
        )

        scenarios: list[dict[str, Any]] = []
        seen_names: set[str] = set()

        for scenario in PREDEFINED_SCENARIOS:
            name = scenario["name"]
            is_high_volume = scenario.get("high_volume", False)

            included_by_flag = (include_high_volume and is_high_volume) or (
                include_default and not is_high_volume
            )
            if not included_by_flag and name not in explicit_names:
                continue

            s = dict(scenario)
            if name == "large_batch_stream" and large_batch_count != _DEFAULT_LARGE_BATCH_COUNT:
                s["record_count"] = large_batch_count

            if name not in seen_names:
                scenarios.append(s)
                seen_names.add(name)

        raw_custom = config.get("custom_scenarios", [])
        custom = raw_custom if isinstance(raw_custom, list) else []
        if custom:
            for cs in custom:
                if not isinstance(cs, dict):
                    continue
                name = cs.get("name", "")
                if not name or not cs.get("json_schema"):
                    continue
                if name not in seen_names:
                    scenarios.append(
                        {
                            "name": name,
                            "description": cs.get(
                                "description",
                                "Custom injected scenario",
                            ),
                            "json_schema": cs["json_schema"],
                            "primary_key": cs.get("primary_key"),
                            "records": cs.get("records", []),
                        }
                    )
                    seen_names.add(name)

        return scenarios

    @staticmethod
    def _validate_custom_scenarios(
        scenarios: list[Any],
    ) -> str | None:
        """Validate custom scenario entries, returning an error message or None."""
        for i, scenario in enumerate(scenarios):
            if not isinstance(scenario, dict):
                return f"Custom scenario at index {i} must be an object."
            if not scenario.get("name"):
                return f"Custom scenario at index {i} is missing 'name'."
            if not isinstance(scenario.get("json_schema"), dict):
                return (
                    f"Custom scenario '{scenario['name']}' must provide 'json_schema' as an object."
                )
            if "records" in scenario:
                if not isinstance(scenario["records"], list):
                    return (
                        f"Custom scenario '{scenario['name']}' has invalid 'records': "
                        "expected an array of objects."
                    )
                for j, record in enumerate(scenario["records"]):
                    if not isinstance(record, dict):
                        return (
                            f"Custom scenario '{scenario['name']}' record at index {j} "
                            "must be an object."
                        )
        return None

    @staticmethod
    def _validate_incremental_config(  # noqa: PLR0911
        config: Mapping[str, Any],
    ) -> str | None:
        """Validate `start_date`, `batch_size`, `batch_count`, `partition_by`, `cursor_step`."""
        try:
            _parse_start_date(config.get("start_date"))
        except (ValueError, TypeError) as exc:
            return str(exc)
        try:
            _parse_cursor_step(config.get("cursor_step"))
        except (ValueError, TypeError) as exc:
            return str(exc)

        for key in ("batch_size", "batch_count"):
            value = config.get(key)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, int):
                return f"`{key}` must be a positive integer."
            if value < 1:
                return f"`{key}` must be >= 1."

        if config.get("batch_count") is not None and config.get("batch_size") is None:
            return "`batch_count` requires `batch_size` to also be set."

        partition_by = config.get("partition_by")
        if partition_by is not None and partition_by not in _VALID_PARTITION_GRAINS:
            return (
                f"`partition_by` must be one of {list(_VALID_PARTITION_GRAINS)} "
                f"or null, got {partition_by!r}."
            )
        return None

    def check(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
    ) -> AirbyteConnectionStatus:
        """Validate the configuration."""
        raw_custom = config.get("custom_scenarios", [])
        if not isinstance(raw_custom, list):
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message="'custom_scenarios' must be an array of objects.",
            )

        error = self._validate_custom_scenarios(raw_custom)
        if error:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=error,
            )

        incremental_error = self._validate_incremental_config(config)
        if incremental_error:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=incremental_error,
            )

        scenarios = self._get_all_scenarios(config)
        if not scenarios:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message="No scenarios available. Check scenario_filter config.",
            )

        logger.info(f"Smoke test source check passed with {len(scenarios)} scenarios.")
        return AirbyteConnectionStatus(status=Status.SUCCEEDED)

    def discover(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
    ) -> AirbyteCatalog:
        """Return the catalog with all test scenario streams."""
        scenarios = self._get_all_scenarios(config)
        namespace = config.get("namespace")
        streams = _build_streams_from_scenarios(scenarios, namespace=namespace)
        logger.info(f"Discovered {len(streams)} smoke test streams.")
        return AirbyteCatalog(streams=streams)

    def _stream_status_message(
        self,
        stream_name: str,
        status: AirbyteStreamStatus,
        namespace: str | None = None,
    ) -> AirbyteMessage:
        """Build an AirbyteMessage containing a stream status trace."""
        return AirbyteMessage(
            type=Type.TRACE,
            trace=AirbyteTraceMessage(
                type=TraceType.STREAM_STATUS,
                emitted_at=time.time() * 1000,
                stream_status=AirbyteStreamStatusTraceMessage(
                    stream_descriptor=StreamDescriptor(
                        name=stream_name,
                        namespace=namespace,
                    ),
                    status=status,
                ),
            ),
        )

    @staticmethod
    def _build_state_message(
        stream_name: str,
        namespace: str | None,
        cursor_field: str,
        cursor_value: str,
    ) -> AirbyteMessage:
        """Build a per-stream STATE message carrying `{cursor_field: cursor_value}`."""
        return AirbyteMessage(
            type=Type.STATE,
            state=AirbyteStateMessage(
                type=AirbyteStateType.STREAM,
                stream=AirbyteStreamState(
                    stream_descriptor=StreamDescriptor(
                        name=stream_name,
                        namespace=namespace,
                    ),
                    stream_state=AirbyteStateBlob(**{cursor_field: cursor_value}),
                ),
            ),
        )

    def _emit_record(
        self,
        stream_name: str,
        namespace: str | None,
        data: dict[str, Any],
        now_ms: int,
    ) -> AirbyteMessage:
        """Build a RECORD message for `stream_name`."""
        return AirbyteMessage(
            type=Type.RECORD,
            record=AirbyteRecordMessage(
                stream=stream_name,
                namespace=namespace,
                data=data,
                emitted_at=now_ms,
            ),
        )

    def _emit_incremental(  # noqa: PLR0913
        self,
        *,
        scenario: dict[str, Any],
        stream_name: str,
        namespace: str | None,
        state: list[AirbyteStateMessage] | None,
        start_date: datetime | None,
        cursor_step: timedelta,
        batch_size: int | None,
        batch_count: int | None,
        partition_by: PartitionGrain | None,
        now_ms: int,
        logger: logging.Logger,
    ) -> Iterable[AirbyteMessage]:
        """Emit RECORD and STATE messages for an incremental-sync stream."""
        cursor_field_list = scenario.get("cursor_field") or ["updated_at"]
        cursor_field = cursor_field_list[0]
        cursor_start = (
            _state_cursor_for_stream(state, stream_name, namespace, cursor_field)
            or start_date
            or _default_cursor_start()
        )
        record_count = 0
        state_count = 0
        for event in iter_incremental_scenario_events(
            scenario,
            cursor_start=cursor_start,
            cursor_step=cursor_step,
            batch_size=batch_size,
            batch_count=batch_count,
            partition_by=partition_by,
        ):
            if event["kind"] == "record":
                record_count += 1
                yield self._emit_record(stream_name, namespace, event["data"], now_ms)
            else:
                state_count += 1
                yield self._build_state_message(
                    stream_name,
                    namespace,
                    cursor_field,
                    event["cursor"],
                )
        logger.info(
            f"Emitted {record_count} records and {state_count} "
            f"STATE messages for incremental stream '{stream_name}' "
            f"(cursor_start={cursor_start.isoformat()})."
        )

    def _emit_incremental_as_full_refresh(  # noqa: PLR0913
        self,
        *,
        scenario: dict[str, Any],
        stream_name: str,
        namespace: str | None,
        start_date: datetime | None,
        cursor_step: timedelta,
        batch_size: int | None,
        batch_count: int | None,
        now_ms: int,
        logger: logging.Logger,
    ) -> Iterable[AirbyteMessage]:
        """Emit RECORDs (no STATE) for an incremental scenario read in full-refresh mode.

        Total record count still honors `batch_size * batch_count` when both are
        set; otherwise it falls back to the scenario's `record_count` or the
        generator's default cap. Only STATE emission is suppressed in this mode.

        Note: when `start_date` is not configured, the cursor origin defaults
        to Jan 1 of the current UTC year, so `updated_at` values shift across
        year boundaries even though record `id` / `category` / `value` remain
        deterministic.
        """
        cursor_start = start_date or _default_cursor_start()
        count = 0
        for event in iter_incremental_scenario_events(
            scenario,
            cursor_start=cursor_start,
            cursor_step=cursor_step,
            batch_size=batch_size,
            batch_count=batch_count,
            partition_by=None,
        ):
            if event["kind"] != "record":
                continue
            count += 1
            yield self._emit_record(stream_name, namespace, event["data"], now_ms)
        logger.info(f"Emitted {count} records for full-refresh stream '{stream_name}'.")

    def _emit_full_refresh(
        self,
        *,
        scenario: dict[str, Any],
        stream_name: str,
        namespace: str | None,
        now_ms: int,
        logger: logging.Logger,
    ) -> Iterable[AirbyteMessage]:
        """Emit RECORDs for a classic full-refresh scenario (inline or simple generator)."""
        records = get_scenario_records(scenario)
        logger.info(f"Emitting {len(records)} records for stream '{stream_name}'.")
        for record in records:
            yield self._emit_record(stream_name, namespace, record, now_ms)

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: list[AirbyteStateMessage] | None = None,
    ) -> Iterable[AirbyteMessage]:
        """Read records from selected smoke test streams.

        Streams configured for `SyncMode.incremental` that map to a scenario
        with `incremental=True` honor `start_date`, `cursor_step`, and the
        checkpoint triggers (`batch_size` / `batch_count` / `partition_by`),
        and emit STATE messages accordingly. All other streams use the
        existing full-refresh behavior.
        """
        scenarios = self._get_all_scenarios(config)
        scenario_map = {s["name"]: s for s in scenarios}
        namespace = config.get("namespace")

        start_date = _parse_start_date(config.get("start_date"))
        cursor_step = _parse_cursor_step(config.get("cursor_step"))
        batch_size = config.get("batch_size")
        batch_count = config.get("batch_count")
        partition_by = _coalesce_partition_grain(config.get("partition_by"))

        now_ms = int(time.time() * 1000)

        for configured in catalog.streams:
            stream_name = configured.stream.name
            scenario = scenario_map.get(stream_name)
            if not scenario:
                logger.warning(f"Stream '{stream_name}' not found in scenarios, skipping.")
                continue

            yield self._stream_status_message(
                stream_name,
                AirbyteStreamStatus.STARTED,
                namespace=namespace,
            )
            yield self._stream_status_message(
                stream_name,
                AirbyteStreamStatus.RUNNING,
                namespace=namespace,
            )

            is_incremental_run = configured.sync_mode == SyncMode.incremental and bool(
                scenario.get("incremental")
            )

            if is_incremental_run:
                yield from self._emit_incremental(
                    scenario=scenario,
                    stream_name=stream_name,
                    namespace=namespace,
                    state=state,
                    start_date=start_date,
                    cursor_step=cursor_step,
                    batch_size=batch_size,
                    batch_count=batch_count,
                    partition_by=partition_by,
                    now_ms=now_ms,
                    logger=logger,
                )
            elif scenario.get("record_generator") == "incremental_batch":
                yield from self._emit_incremental_as_full_refresh(
                    scenario=scenario,
                    stream_name=stream_name,
                    namespace=namespace,
                    start_date=start_date,
                    cursor_step=cursor_step,
                    batch_size=batch_size,
                    batch_count=batch_count,
                    now_ms=now_ms,
                    logger=logger,
                )
            else:
                yield from self._emit_full_refresh(
                    scenario=scenario,
                    stream_name=stream_name,
                    namespace=namespace,
                    now_ms=now_ms,
                    logger=logger,
                )

            yield self._stream_status_message(
                stream_name,
                AirbyteStreamStatus.COMPLETE,
                namespace=namespace,
            )
