# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Smoke test source for destination regression testing.

This source generates synthetic test data covering common edge cases
that break destinations: type variations, null handling, naming edge cases,
schema variations, and batch size variations.

Predefined scenarios are always available. Additional scenarios can be
injected dynamically via the ``custom_scenarios`` config field.
"""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING, Any

from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification,
    Status,
    SyncMode,
    Type,
)
from airbyte_cdk.sources.source import Source


if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


logger = logging.getLogger("airbyte")


PREDEFINED_SCENARIOS: list[dict[str, Any]] = [
    {
        "name": "basic_types",
        "description": "Covers fundamental column types: string, integer, number, boolean.",
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "amount": {"type": "number"},
                "is_active": {"type": "boolean"},
            },
        },
        "primary_key": [["id"]],
        "records": [
            {"id": 1, "name": "Alice", "amount": 100.50, "is_active": True},
            {"id": 2, "name": "Bob", "amount": 0.0, "is_active": False},
            {"id": 3, "name": "", "amount": -99.99, "is_active": True},
        ],
    },
    {
        "name": "timestamp_types",
        "description": "Covers date and timestamp formats including ISO 8601 variations.",
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "created_date": {"type": "string", "format": "date"},
                "updated_at": {"type": "string", "format": "date-time"},
                "epoch_seconds": {"type": "integer"},
            },
        },
        "primary_key": [["id"]],
        "records": [
            {
                "id": 1,
                "created_date": "2024-01-15",
                "updated_at": "2024-01-15T10:30:00Z",
                "epoch_seconds": 1705312200,
            },
            {
                "id": 2,
                "created_date": "1970-01-01",
                "updated_at": "1970-01-01T00:00:00+00:00",
                "epoch_seconds": 0,
            },
            {
                "id": 3,
                "created_date": "2099-12-31",
                "updated_at": "2099-12-31T23:59:59.999999Z",
                "epoch_seconds": 4102444799,
            },
        ],
    },
    {
        "name": "large_decimals_and_numbers",
        "description": "Tests handling of very large numbers, high precision decimals, and boundary values.",
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "big_integer": {"type": "integer"},
                "precise_decimal": {"type": "number"},
                "small_decimal": {"type": "number"},
            },
        },
        "primary_key": [["id"]],
        "records": [
            {
                "id": 1,
                "big_integer": 9999999999999999,
                "precise_decimal": 3.141592653589793,
                "small_decimal": 0.000001,
            },
            {
                "id": 2,
                "big_integer": -9999999999999999,
                "precise_decimal": -0.1,
                "small_decimal": 1e-10,
            },
            {
                "id": 3,
                "big_integer": 0,
                "precise_decimal": 99999999.99999999,
                "small_decimal": 0.0,
            },
        ],
    },
    {
        "name": "nested_json_objects",
        "description": "Tests nested object and array handling in destination columns.",
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "metadata": {
                    "type": "object",
                    "properties": {
                        "source": {"type": "string"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                    },
                },
                "nested_deep": {
                    "type": "object",
                    "properties": {
                        "level1": {
                            "type": "object",
                            "properties": {
                                "level2": {
                                    "type": "object",
                                    "properties": {
                                        "value": {"type": "string"},
                                    },
                                },
                            },
                        },
                    },
                },
                "items_array": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "sku": {"type": "string"},
                            "qty": {"type": "integer"},
                        },
                    },
                },
            },
        },
        "primary_key": [["id"]],
        "records": [
            {
                "id": 1,
                "metadata": {"source": "api", "tags": ["a", "b", "c"]},
                "nested_deep": {"level1": {"level2": {"value": "deep"}}},
                "items_array": [{"sku": "ABC", "qty": 10}],
            },
            {
                "id": 2,
                "metadata": {"source": "manual", "tags": []},
                "nested_deep": {"level1": {"level2": {"value": ""}}},
                "items_array": [],
            },
        ],
    },
    {
        "name": "null_handling",
        "description": "Tests null values across all column types and patterns.",
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "nullable_string": {"type": ["null", "string"]},
                "nullable_integer": {"type": ["null", "integer"]},
                "nullable_number": {"type": ["null", "number"]},
                "nullable_boolean": {"type": ["null", "boolean"]},
                "nullable_object": {
                    "type": ["null", "object"],
                    "properties": {"key": {"type": "string"}},
                },
                "always_null": {"type": ["null", "string"]},
            },
        },
        "primary_key": [["id"]],
        "records": [
            {
                "id": 1,
                "nullable_string": "present",
                "nullable_integer": 42,
                "nullable_number": 3.14,
                "nullable_boolean": True,
                "nullable_object": {"key": "val"},
                "always_null": None,
            },
            {
                "id": 2,
                "nullable_string": None,
                "nullable_integer": None,
                "nullable_number": None,
                "nullable_boolean": None,
                "nullable_object": None,
                "always_null": None,
            },
            {
                "id": 3,
                "nullable_string": "",
                "nullable_integer": 0,
                "nullable_number": 0.0,
                "nullable_boolean": False,
                "nullable_object": {},
                "always_null": None,
            },
        ],
    },
    {
        "name": "column_naming_edge_cases",
        "description": "Tests special characters, casing, and reserved words in column names.",
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "CamelCaseColumn": {"type": "string"},
                "ALLCAPS": {"type": "string"},
                "snake_case_column": {"type": "string"},
                "column-with-dashes": {"type": "string"},
                "column.with.dots": {"type": "string"},
                "column with spaces": {"type": "string"},
                "select": {"type": "string"},
                "from": {"type": "string"},
                "order": {"type": "string"},
                "group": {"type": "string"},
            },
        },
        "primary_key": [["id"]],
        "records": [
            {
                "id": 1,
                "CamelCaseColumn": "camel",
                "ALLCAPS": "caps",
                "snake_case_column": "snake",
                "column-with-dashes": "dashes",
                "column.with.dots": "dots",
                "column with spaces": "spaces",
                "select": "reserved_select",
                "from": "reserved_from",
                "order": "reserved_order",
                "group": "reserved_group",
            },
        ],
    },
    {
        "name": "table_naming_edge_cases",
        "description": "Stream with special characters in the name to test table naming.",
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "value": {"type": "string"},
            },
        },
        "primary_key": [["id"]],
        "records": [
            {"id": 1, "value": "table_name_test"},
        ],
    },
    {
        "name": "CamelCaseStreamName",
        "description": "Stream with CamelCase name to test case handling.",
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "value": {"type": "string"},
            },
        },
        "primary_key": [["id"]],
        "records": [
            {"id": 1, "value": "camel_case_stream_test"},
        ],
    },
    {
        "name": "wide_table_50_columns",
        "description": "Tests a wide table with 50 columns.",
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                **{"id": {"type": "integer"}},
                **{f"col_{i:03d}": {"type": "string"} for i in range(1, 50)},
            },
        },
        "primary_key": [["id"]],
        "records": [
            {"id": 1, **{f"col_{i:03d}": f"val_{i}" for i in range(1, 50)}},
            {"id": 2, **{f"col_{i:03d}": None for i in range(1, 50)}},
        ],
    },
    {
        "name": "empty_stream",
        "description": "A stream that emits zero records, testing empty dataset handling.",
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "value": {"type": "string"},
            },
        },
        "primary_key": [["id"]],
        "records": [],
    },
    {
        "name": "single_record_stream",
        "description": "A stream with exactly one record.",
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "value": {"type": "string"},
            },
        },
        "primary_key": [["id"]],
        "records": [
            {"id": 1, "value": "only_record"},
        ],
    },
    {
        "name": "large_batch_stream",
        "description": "A stream that generates a configurable number of records for batch testing.",
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "value": {"type": "number"},
                "category": {"type": "string"},
            },
        },
        "primary_key": [["id"]],
        "record_count": 1000,
        "record_generator": "large_batch",
    },
    {
        "name": "unicode_and_special_strings",
        "description": "Tests unicode characters, emoji, escape sequences, and special string values.",
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "unicode_text": {"type": "string"},
                "special_chars": {"type": "string"},
            },
        },
        "primary_key": [["id"]],
        "records": [
            {"id": 1, "unicode_text": "Hello World", "special_chars": "line1\nline2\ttab"},
            {"id": 2, "unicode_text": "Caf\u00e9 na\u00efve r\u00e9sum\u00e9", "special_chars": 'quote"inside'},
            {"id": 3, "unicode_text": "\u4f60\u597d\u4e16\u754c", "special_chars": "back\\slash"},
            {"id": 4, "unicode_text": "\u0410\u0411\u0412\u0413", "special_chars": ""},
        ],
    },
    {
        "name": "schema_with_no_primary_key",
        "description": "A stream without a primary key, testing append-only behavior.",
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "event_id": {"type": "string"},
                "event_type": {"type": "string"},
                "payload": {"type": "string"},
            },
        },
        "primary_key": None,
        "records": [
            {"event_id": "evt_001", "event_type": "click", "payload": "{}"},
            {"event_id": "evt_001", "event_type": "click", "payload": "{}"},
            {"event_id": "evt_002", "event_type": "view", "payload": '{"page": "home"}'},
        ],
    },
    {
        "name": "long_column_names",
        "description": "Tests handling of very long column names that may exceed database limits.",
        "json_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "a_very_long_column_name_that_exceeds_typical_database_limits_and_should_be_truncated_or_handled_gracefully_by_the_destination": {
                    "type": "string",
                },
                "another_extremely_verbose_column_name_designed_to_test_the_absolute_maximum_length_that_any_reasonable_database_would_support": {
                    "type": "string",
                },
            },
        },
        "primary_key": [["id"]],
        "records": [
            {
                "id": 1,
                "a_very_long_column_name_that_exceeds_typical_database_limits_and_should_be_truncated_or_handled_gracefully_by_the_destination": "long_col_1",
                "another_extremely_verbose_column_name_designed_to_test_the_absolute_maximum_length_that_any_reasonable_database_would_support": "long_col_2",
            },
        ],
    },
]


def _generate_large_batch_records(scenario: dict[str, Any]) -> list[dict[str, Any]]:
    """Generate records for the large_batch_stream scenario."""
    count = scenario.get("record_count", 1000)
    categories = ["cat_a", "cat_b", "cat_c", "cat_d", "cat_e"]
    return [
        {
            "id": i,
            "name": f"record_{i:06d}",
            "value": float(i) * 1.1,
            "category": categories[i % len(categories)],
        }
        for i in range(1, count + 1)
    ]


def _get_scenario_records(scenario: dict[str, Any]) -> list[dict[str, Any]]:
    """Get records for a scenario, using generator if specified."""
    if scenario.get("record_generator") == "large_batch":
        return _generate_large_batch_records(scenario)
    return scenario.get("records", [])


def _build_streams_from_scenarios(
    scenarios: list[dict[str, Any]],
) -> list[AirbyteStream]:
    """Build AirbyteStream objects from scenario definitions."""
    streams: list[AirbyteStream] = []
    for scenario in scenarios:
        streams.append(
            AirbyteStream(
                name=scenario["name"],
                json_schema=scenario["json_schema"],
                supported_sync_modes=[SyncMode.full_refresh],
                source_defined_cursor=False,
                source_defined_primary_key=scenario.get("primary_key"),
            )
        )
    return streams


class SourceSmokeTest(Source):
    """Smoke test source for destination regression testing.

    Generates synthetic data across predefined scenarios that cover
    common destination failure patterns. Supports dynamic injection
    of additional scenarios via the ``custom_scenarios`` config field.
    """

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:  # noqa: ARG002
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
                            "Additional test scenarios to inject at runtime. "
                            "Each scenario defines a stream name, JSON schema, and records."
                        ),
                        "items": {
                            "type": "object",
                            "required": ["name", "json_schema", "records"],
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "description": "Stream name for this scenario.",
                                },
                                "description": {
                                    "type": "string",
                                    "description": "Human-readable description of this scenario.",
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
                                    "description": "Primary key definition (list of key paths) or null.",
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
                            "Number of records to generate for the large_batch_stream scenario. "
                            "Set to 0 to skip this stream."
                        ),
                        "default": 1000,
                    },
                    "scenario_filter": {
                        "type": "array",
                        "title": "Scenario Filter",
                        "description": (
                            "If provided, only emit these scenario names. "
                            "Empty or absent means emit all scenarios."
                        ),
                        "items": {"type": "string"},
                        "default": [],
                    },
                },
            },
        )

    def _get_all_scenarios(self, config: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Combine predefined and custom scenarios, applying config overrides."""
        scenarios: list[dict[str, Any]] = []

        large_batch_count = config.get("large_batch_record_count", 1000)

        for scenario in PREDEFINED_SCENARIOS:
            s = dict(scenario)
            if s["name"] == "large_batch_stream" and large_batch_count != 1000:
                s["record_count"] = large_batch_count
            scenarios.append(s)

        custom = config.get("custom_scenarios", [])
        if custom:
            for custom_scenario in custom:
                scenarios.append({
                    "name": custom_scenario["name"],
                    "description": custom_scenario.get("description", "Custom injected scenario"),
                    "json_schema": custom_scenario["json_schema"],
                    "primary_key": custom_scenario.get("primary_key"),
                    "records": custom_scenario.get("records", []),
                })

        scenario_filter = config.get("scenario_filter", [])
        if scenario_filter:
            scenarios = [s for s in scenarios if s["name"] in scenario_filter]

        return scenarios

    def check(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
    ) -> AirbyteConnectionStatus:
        """Validate the configuration."""
        scenarios = self._get_all_scenarios(config)
        if not scenarios:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message="No scenarios available. Check scenario_filter config.",
            )

        custom = config.get("custom_scenarios", [])
        for i, scenario in enumerate(custom):
            if not scenario.get("name"):
                return AirbyteConnectionStatus(
                    status=Status.FAILED,
                    message=f"Custom scenario at index {i} is missing 'name'.",
                )
            if not scenario.get("json_schema"):
                return AirbyteConnectionStatus(
                    status=Status.FAILED,
                    message=f"Custom scenario '{scenario['name']}' is missing 'json_schema'.",
                )

        logger.info(f"Smoke test source check passed with {len(scenarios)} scenarios.")
        return AirbyteConnectionStatus(status=Status.SUCCEEDED)

    def discover(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
    ) -> AirbyteCatalog:
        """Return the catalog with all available test scenario streams."""
        scenarios = self._get_all_scenarios(config)
        streams = _build_streams_from_scenarios(scenarios)
        logger.info(f"Discovered {len(streams)} smoke test streams.")
        return AirbyteCatalog(streams=streams)

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: list[Any] | None = None,  # noqa: ARG002
    ) -> Iterable[AirbyteMessage]:
        """Read records from selected smoke test streams."""
        selected_streams = {stream.stream.name for stream in catalog.streams}
        scenarios = self._get_all_scenarios(config)
        scenario_map = {s["name"]: s for s in scenarios}
        now_ms = int(time.time() * 1000)

        for stream_name in selected_streams:
            scenario = scenario_map.get(stream_name)
            if not scenario:
                logger.warning(f"Stream '{stream_name}' not found in scenarios, skipping.")
                continue

            records = _get_scenario_records(scenario)
            logger.info(f"Emitting {len(records)} records for stream '{stream_name}'.")

            for record in records:
                yield AirbyteMessage(
                    type=Type.RECORD,
                    record=AirbyteRecordMessage(
                        stream=stream_name,
                        data=record,
                        emitted_at=now_ms,
                    ),
                )
