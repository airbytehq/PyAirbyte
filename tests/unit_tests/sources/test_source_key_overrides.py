# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
from airbyte.sources.base import Source
from airbyte_protocol.models import (
    AirbyteCatalog,
    AirbyteStream,
)


@pytest.fixture
def mock_stream():
    """Create a mock AirbyteStream for testing."""
    stream = Mock(spec=AirbyteStream)
    stream.name = "test_stream"
    stream.source_defined_primary_key = [["original_pk"]]
    stream.source_defined_cursor = True
    stream.default_cursor_field = ["original_cursor"]

    return stream


@pytest.fixture
def mock_catalog(mock_stream):
    """Create a mock AirbyteCatalog for testing."""
    catalog = Mock(spec=AirbyteCatalog)
    catalog.streams = [mock_stream]
    return catalog


def test_source_init_with_overrides():
    """Test that overrides are set when provided to the constructor."""
    cursor_overrides = {"stream1": "cursor1", "stream2": "cursor2"}
    pk_overrides = {"stream1": "pk1", "stream2": ["pk2a", "pk2b"]}

    with patch.object(Source, "_discover", return_value=Mock()):
        with patch.object(Source, "set_cursor_keys") as mock_set_cursor:
            with patch.object(Source, "set_primary_keys") as mock_set_pk:
                source = Source(
                    executor=Mock(),
                    name="test-source",
                    cursor_key_overrides=cursor_overrides,
                    primary_key_overrides=pk_overrides,
                )

                mock_set_cursor.assert_called_once_with(**cursor_overrides)
                mock_set_pk.assert_called_once_with(**pk_overrides)


@pytest.mark.parametrize(
    "cursor_overrides,primary_key_overrides",
    [
        (None, None),
        ({"stream1": "cursor1"}, None),
        (None, {"stream1": "pk1"}),
        ({"stream1": "cursor1"}, {"stream1": "pk1"}),
    ],
)
def test_source_init_with_different_override_combinations(
    cursor_overrides, primary_key_overrides
):
    """Test that the Source initializes correctly with different combinations of overrides."""
    with patch.object(Source, "_discover", return_value=Mock()):
        source = Source(
            executor=Mock(),
            name="test-source",
            cursor_key_overrides=cursor_overrides,
            primary_key_overrides=primary_key_overrides,
        )

        if cursor_overrides:
            assert source._cursor_key_overrides == cursor_overrides
        else:
            assert source._cursor_key_overrides == {}

        if primary_key_overrides:
            expected_pk_overrides = {
                k: v if isinstance(v, list) else [v]
                for k, v in primary_key_overrides.items()
            }
            assert source._primary_key_overrides == expected_pk_overrides
        else:
            assert source._primary_key_overrides == {}


def test_set_cursor_keys():
    """Test that set_cursor_keys properly updates the cursor key overrides."""
    with patch.object(Source, "_discover", return_value=Mock()):
        source = Source(executor=Mock(), name="test-source")

        source.set_cursor_keys(kwargs={"stream1": "cursor1"})
        assert source._cursor_key_overrides == {"stream1": "cursor1"}

        source.set_cursor_keys(kwargs={"stream2": "cursor2"})
        assert source._cursor_key_overrides == {
            "stream1": "cursor1",
            "stream2": "cursor2",
        }

        source.set_cursor_keys(kwargs={"stream1": "new_cursor1"})
        assert source._cursor_key_overrides == {
            "stream1": "new_cursor1",
            "stream2": "cursor2",
        }


def test_set_cursor_key():
    """Test that set_cursor_key properly updates a single cursor key override."""
    with patch.object(Source, "_discover", return_value=Mock()):
        source = Source(executor=Mock(), name="test-source")

        source.set_cursor_key("stream1", "cursor1")
        assert source._cursor_key_overrides == {"stream1": "cursor1"}

        source.set_cursor_key("stream2", "cursor2")
        assert source._cursor_key_overrides == {
            "stream1": "cursor1",
            "stream2": "cursor2",
        }

        source.set_cursor_key("stream1", "new_cursor1")
        assert source._cursor_key_overrides == {
            "stream1": "new_cursor1",
            "stream2": "cursor2",
        }


@pytest.mark.parametrize(
    "input_keys,expected_output",
    [
        ({"stream1": "pk1"}, {"stream1": ["pk1"]}),
        ({"stream1": ["pk1"]}, {"stream1": ["pk1"]}),
        ({"stream1": ["pk1", "pk2"]}, {"stream1": ["pk1", "pk2"]}),
        (
            {"stream1": "pk1", "stream2": ["pk2a", "pk2b"]},
            {"stream1": ["pk1"], "stream2": ["pk2a", "pk2b"]},
        ),
    ],
)
def test_set_primary_keys(input_keys, expected_output):
    """Test that set_primary_keys properly converts and updates the primary key overrides."""
    with patch.object(Source, "_discover", return_value=Mock()):
        source = Source(executor=Mock(), name="test-source")

        source.set_primary_keys(kwargs=input_keys)

        assert source._primary_key_overrides == expected_output

        update_keys = {"stream3": "pk3"}
        expected_after_update = expected_output.copy()
        expected_after_update["stream3"] = ["pk3"]

        source.set_primary_keys(kwargs=update_keys)
        assert source._primary_key_overrides == expected_after_update


@pytest.mark.parametrize(
    "input_key,expected_output",
    [
        ("pk1", ["pk1"]),
        (["pk1"], ["pk1"]),
        (["pk1", "pk2"], ["pk1", "pk2"]),
    ],
)
def test_set_primary_key(input_key, expected_output):
    """Test that set_primary_key properly converts and updates a single primary key override."""
    with patch.object(Source, "_discover", return_value=Mock()):
        source = Source(executor=Mock(), name="test-source")

        source.set_primary_key("stream1", input_key)

        assert source._primary_key_overrides == {"stream1": expected_output}


def test_get_configured_catalog_with_overrides(mock_catalog, mock_stream):
    """Test that get_configured_catalog correctly applies overrides."""
    with patch.object(Source, "_discover", return_value=mock_catalog):
        source = Source(
            executor=Mock(),
            name="test-source",
            cursor_key_overrides={"test_stream": "custom_cursor"},
            primary_key_overrides={"test_stream": "custom_pk"},
        )

        catalog = source.get_configured_catalog()

        assert len(catalog.streams) == 1
        configured_stream = catalog.streams[0]
        assert configured_stream.cursor_field == ["custom_cursor"]
        assert configured_stream.primary_key == [["custom_pk"]]


def test_get_configured_catalog_without_overrides(mock_catalog, mock_stream):
    """Test that get_configured_catalog uses source-defined keys when no overrides exist."""
    with patch.object(Source, "_discover", return_value=mock_catalog):
        source = Source(executor=Mock(), name="test-source")

        catalog = source.get_configured_catalog()

        assert len(catalog.streams) == 1
        configured_stream = catalog.streams[0]
        assert configured_stream.cursor_field == ["original_cursor"]
        assert configured_stream.primary_key == [["original_pk"]]


def test_get_configured_catalog_composite_primary_key(mock_catalog, mock_stream):
    """Test that get_configured_catalog correctly handles composite primary keys."""
    mock_stream.source_defined_primary_key = [["pk1", "pk2"]]
    with patch.object(Source, "_discover", return_value=mock_catalog):
        source = Source(
            executor=Mock(),
            name="test-source",
            primary_key_overrides={"test_stream": ["custom_pk1", "custom_pk2"]},
        )

        catalog = source.get_configured_catalog()

        assert len(catalog.streams) == 1
        configured_stream = catalog.streams[0]
        assert configured_stream.primary_key == [["custom_pk1", "custom_pk2"]]


@pytest.mark.parametrize(
    "cursor_override,pk_override,expected_cursor,expected_pk",
    [
        (None, None, ["original_cursor"], [["original_pk"]]),
        ("custom_cursor", None, ["custom_cursor"], [["original_pk"]]),
        (None, "custom_pk", ["original_cursor"], [["custom_pk"]]),
        ("custom_cursor", "custom_pk", ["custom_cursor"], [["custom_pk"]]),
        (None, ["pk1", "pk2"], ["original_cursor"], [["pk1", "pk2"]]),
    ],
)
def test_get_configured_catalog_parametrized(
    mock_catalog,
    mock_stream,
    cursor_override,
    pk_override,
    expected_cursor,
    expected_pk,
):
    """Test various combinations of cursor and primary key overrides."""
    cursor_key_overrides = {"test_stream": cursor_override} if cursor_override else None
    primary_key_overrides = {"test_stream": pk_override} if pk_override else None

    with patch.object(Source, "_discover", return_value=mock_catalog):
        source = Source(
            executor=Mock(),
            name="test-source",
            cursor_key_overrides=cursor_key_overrides,
            primary_key_overrides=primary_key_overrides,
        )

        catalog = source.get_configured_catalog()

        assert len(catalog.streams) == 1
        configured_stream = catalog.streams[0]
        assert configured_stream.cursor_field == expected_cursor
        assert configured_stream.primary_key == expected_pk
