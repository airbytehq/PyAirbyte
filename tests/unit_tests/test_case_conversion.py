# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for generic camelCase ↔ snake_case key conversion helpers."""

from __future__ import annotations

import pytest

from airbyte.cloud._case_conversion import (
    _camel_to_snake,
    _snake_to_camel,
    camel_to_snake_keys,
    snake_to_camel_keys,
)


@pytest.mark.parametrize(
    "camel,snake",
    [
        pytest.param("streamDescriptor", "stream_descriptor", id="two_word"),
        pytest.param("jsonSchema", "json_schema", id="abbreviation_prefix"),
        pytest.param("syncMode", "sync_mode", id="simple"),
        pytest.param("destinationSyncMode", "destination_sync_mode", id="three_word"),
        pytest.param(
            "sourceDefinedPrimaryKey", "source_defined_primary_key", id="four_word"
        ),
        pytest.param("name", "name", id="single_word"),
        pytest.param("type", "type", id="single_word_type"),
        pytest.param("stateType", "state_type", id="state_type"),
        pytest.param("connectionId", "connection_id", id="connection_id"),
        pytest.param("sharedState", "shared_state", id="shared_state"),
        pytest.param("streamStates", "stream_states", id="stream_states"),
        pytest.param("cursorField", "cursor_field", id="cursor_field"),
        pytest.param("primaryKey", "primary_key", id="primary_key"),
    ],
)
def test_camel_to_snake(camel: str, snake: str) -> None:
    assert _camel_to_snake(camel) == snake


@pytest.mark.parametrize(
    "snake,camel",
    [
        pytest.param("stream_descriptor", "streamDescriptor", id="two_word"),
        pytest.param("json_schema", "jsonSchema", id="abbreviation_prefix"),
        pytest.param("sync_mode", "syncMode", id="simple"),
        pytest.param("destination_sync_mode", "destinationSyncMode", id="three_word"),
        pytest.param(
            "source_defined_primary_key", "sourceDefinedPrimaryKey", id="four_word"
        ),
        pytest.param("name", "name", id="single_word"),
        pytest.param("type", "type", id="single_word_type"),
        pytest.param("state_type", "stateType", id="state_type"),
        pytest.param("connection_id", "connectionId", id="connection_id"),
        pytest.param("shared_state", "sharedState", id="shared_state"),
        pytest.param("stream_states", "streamStates", id="stream_states"),
        pytest.param("cursor_field", "cursorField", id="cursor_field"),
        pytest.param("primary_key", "primaryKey", id="primary_key"),
    ],
)
def test_snake_to_camel(snake: str, camel: str) -> None:
    assert _snake_to_camel(snake) == camel


@pytest.mark.parametrize(
    "camel,snake",
    [
        pytest.param("streamDescriptor", "stream_descriptor", id="two_word"),
        pytest.param("jsonSchema", "json_schema", id="abbreviation"),
        pytest.param("name", "name", id="identity"),
    ],
)
def test_round_trip_camel_snake_camel(camel: str, snake: str) -> None:
    assert _camel_to_snake(camel) == snake
    assert _snake_to_camel(snake) == camel


def test_camel_to_snake_keys_shallow() -> None:
    data = {
        "streamDescriptor": {"name": "users", "namespace": "public"},
        "streamState": {"cursor": "2024-01-01"},
    }
    result = camel_to_snake_keys(data)
    assert result == {
        "stream_descriptor": {"name": "users", "namespace": "public"},
        "stream_state": {"cursor": "2024-01-01"},
    }
    # Verify values are NOT recursed into (same object identity)
    assert result["stream_descriptor"] is data["streamDescriptor"]
    assert result["stream_state"] is data["streamState"]


def test_snake_to_camel_keys_shallow() -> None:
    data = {
        "stream_descriptor": {"name": "users", "namespace": "public"},
        "stream_state": {"cursor": "2024-01-01"},
    }
    result = snake_to_camel_keys(data)
    assert result == {
        "streamDescriptor": {"name": "users", "namespace": "public"},
        "streamState": {"cursor": "2024-01-01"},
    }
    # Verify values are NOT recursed into (same object identity)
    assert result["streamDescriptor"] is data["stream_descriptor"]
    assert result["streamState"] is data["stream_state"]


def test_camel_to_snake_keys_empty_dict() -> None:
    assert camel_to_snake_keys({}) == {}


def test_snake_to_camel_keys_empty_dict() -> None:
    assert snake_to_camel_keys({}) == {}


def test_opaque_values_preserved_by_shallow_conversion() -> None:
    """Opaque blobs with snake_case keys are NOT converted by `snake_to_camel_keys`."""
    opaque_blob = {"updated_at": "2024-01-01", "cursor_field": "id"}
    data = {"stream_state": opaque_blob}
    result = snake_to_camel_keys(data)
    # Key is converted, but value is the same object (not recursed into)
    assert result["streamState"] is opaque_blob
