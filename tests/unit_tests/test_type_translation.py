# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import pytest
from sqlalchemy import types
from airbyte.types import SQLTypeConverter, _get_airbyte_type


@pytest.mark.parametrize(
    "json_schema_property_def, expected_sql_type",
    [
        ({"type": "string"}, types.VARCHAR),
        ({"type": ["boolean", "null"]}, types.BOOLEAN),
        ({"type": ["null", "boolean"]}, types.BOOLEAN),
        ({"type": "string"}, types.VARCHAR),
        ({"type": ["null", "string"]}, types.VARCHAR),
        ({"type": "boolean"}, types.BOOLEAN),
        ({"type": "string", "format": "date"}, types.DATE),
        ({"type": ["null", "string"]}, types.VARCHAR),
        ({"type": ["null", "boolean"]}, types.BOOLEAN),
        ({"type": ["null", "number"]}, types.DECIMAL),
        (
            {
                "type": "string",
                "format": "date-time",
                "airbyte_type": "timestamp_without_timezone",
            },
            types.TIMESTAMP,
        ),
        (
            {
                "type": "string",
                "format": "date-time",
                "airbyte_type": "timestamp_with_timezone",
            },
            types.TIMESTAMP,
        ),
        (
            {
                "type": "string",
                "format": "time",
                "airbyte_type": "time_without_timezone",
            },
            types.TIME,
        ),
        (
            {"type": "string", "format": "time", "airbyte_type": "time_with_timezone"},
            types.TIME,
        ),
        ({"type": "integer"}, types.BIGINT),
        ({"type": "number", "airbyte_type": "integer"}, types.BIGINT),
        ({"type": "number"}, types.DECIMAL),
        ({"type": "array", "items": {"type": "object"}}, types.JSON),
        ({"type": ["null", "array"], "items": {"type": "object"}}, types.JSON),
        ({"type": "object", "properties": {}}, types.JSON),
        ({"type": ["null", "object"], "properties": {}}, types.JSON),
        # Malformed JSON schema seen in the wild:
        ({"type": "array", "items": {}}, types.JSON),
        ({"type": ["null", "array"], "items": {"items": {}}}, types.JSON),
    ],
)
def test_to_sql_type(json_schema_property_def, expected_sql_type):
    converter = SQLTypeConverter()
    sql_type = converter.to_sql_type(json_schema_property_def)
    assert isinstance(sql_type, expected_sql_type)


@pytest.mark.parametrize(
    "json_schema_property_def, expected_airbyte_type",
    [
        ({"type": "string"}, "string"),
        ({"type": ["boolean", "null"]}, "boolean"),
        ({"type": ["null", "boolean"]}, "boolean"),
        ({"type": "string"}, "string"),
        ({"type": ["null", "string"]}, "string"),
        ({"type": "boolean"}, "boolean"),
        ({"type": "string", "format": "date"}, "date"),
        (
            {
                "type": "string",
                "format": "date-time",
                "airbyte_type": "timestamp_without_timezone",
            },
            "timestamp_without_timezone",
        ),
        (
            {
                "type": "string",
                "format": "date-time",
                "airbyte_type": "timestamp_with_timezone",
            },
            "timestamp_with_timezone",
        ),
        (
            {
                "type": "string",
                "format": "time",
                "airbyte_type": "time_without_timezone",
            },
            "time_without_timezone",
        ),
        (
            {"type": "string", "format": "time", "airbyte_type": "time_with_timezone"},
            "time_with_timezone",
        ),
        ({"type": "integer"}, "integer"),
        ({"type": "number", "airbyte_type": "integer"}, "integer"),
        ({"type": "number"}, "number"),
        # Array type:
        ({"type": "array"}, "array"),
        ({"type": "array", "items": {"type": "object"}}, "array"),
        ({"type": ["null", "array"], "items": {"type": "object"}}, "array"),
        # Object type:
        ({"type": "object"}, "object"),
        # Malformed JSON schema seen in the wild:
        ({"type": "array", "items": {"items": {}}}, "array"),
        ({"type": ["null", "array"], "items": {"items": {}}}, "array"),
    ],
)
def test_to_airbyte_type(json_schema_property_def, expected_airbyte_type):
    airbyte_type, _ = _get_airbyte_type(json_schema_property_def)
    assert airbyte_type == expected_airbyte_type


@pytest.mark.parametrize(
    "json_schema_property_def, expected_airbyte_type, expected_airbyte_subtype",
    [
        ({"type": "string"}, "string", None),
        ({"type": "number"}, "number", None),
        ({"type": "array"}, "array", None),
        ({"type": "object"}, "object", None),
        ({"type": "array", "items": {"type": ["null", "string"]}}, "array", "string"),
        ({"type": "array", "items": {"type": ["boolean"]}}, "array", "boolean"),
        # Malformed JSON schema seen in the wild:
        ({"type": "array", "items": {"items": {}}}, "array", None),
        ({"type": ["null", "array"], "items": {"items": {}}}, "array", None),
    ],
)
def test_to_airbyte_subtype(
    json_schema_property_def,
    expected_airbyte_type,
    expected_airbyte_subtype,
):
    airbyte_type, subtype = _get_airbyte_type(json_schema_property_def)
    assert airbyte_type == expected_airbyte_type
    assert subtype == expected_airbyte_subtype
