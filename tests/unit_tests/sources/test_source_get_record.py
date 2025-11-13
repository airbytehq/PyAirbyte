# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
from airbyte import exceptions as exc
from airbyte._executors.declarative import DeclarativeExecutor
from airbyte.sources.base import Source
from airbyte_protocol.models import (
    AirbyteCatalog,
    AirbyteStream,
)


@pytest.fixture
def mock_stream_with_pk():
    """Create a mock AirbyteStream with a primary key."""
    stream = Mock(spec=AirbyteStream)
    stream.name = "users"
    stream.source_defined_primary_key = [["id"]]
    stream.default_cursor_field = []
    return stream


@pytest.fixture
def mock_stream_no_pk():
    """Create a mock AirbyteStream without a primary key."""
    stream = Mock(spec=AirbyteStream)
    stream.name = "events"
    stream.source_defined_primary_key = []
    stream.default_cursor_field = []
    return stream


@pytest.fixture
def mock_stream_composite_pk():
    """Create a mock AirbyteStream with a composite primary key."""
    stream = Mock(spec=AirbyteStream)
    stream.name = "orders"
    stream.source_defined_primary_key = [["company_id"], ["order_id"]]
    stream.default_cursor_field = []
    return stream


@pytest.fixture
def mock_catalog_with_pk(mock_stream_with_pk):
    """Create a mock AirbyteCatalog with a stream that has a primary key."""
    catalog = Mock(spec=AirbyteCatalog)
    catalog.streams = [mock_stream_with_pk]
    return catalog


@pytest.fixture
def mock_catalog_no_pk(mock_stream_no_pk):
    """Create a mock AirbyteCatalog with a stream that has no primary key."""
    catalog = Mock(spec=AirbyteCatalog)
    catalog.streams = [mock_stream_no_pk]
    return catalog


@pytest.fixture
def mock_catalog_composite_pk(mock_stream_composite_pk):
    """Create a mock AirbyteCatalog with a stream that has a composite primary key."""
    catalog = Mock(spec=AirbyteCatalog)
    catalog.streams = [mock_stream_composite_pk]
    return catalog


@pytest.fixture
def mock_declarative_executor():
    """Create a mock DeclarativeExecutor."""
    executor = Mock(spec=DeclarativeExecutor)
    executor.fetch_record = Mock(return_value={"id": "123", "name": "Test User"})
    return executor


@pytest.fixture
def mock_non_declarative_executor():
    """Create a mock non-declarative executor."""
    from airbyte._executors.base import Executor
    
    executor = Mock(spec=Executor)
    return executor


def test_get_record_with_string_pk_value(mock_catalog_with_pk, mock_declarative_executor):
    """Test get_record with a simple string primary key value."""
    with patch.object(Source, "_discover", return_value=mock_catalog_with_pk):
        source = Source(executor=mock_declarative_executor, name="test-source")
        
        result = source.get_record("users", pk_value="123")
        
        assert result == {"id": "123", "name": "Test User"}
        mock_declarative_executor.fetch_record.assert_called_once_with(
            stream_name="users",
            pk_value="123",
            config=None,
        )


def test_get_record_with_dict_pk_value_valid(mock_catalog_with_pk, mock_declarative_executor):
    """Test get_record with a dict primary key value that matches the stream's PK."""
    with patch.object(Source, "_discover", return_value=mock_catalog_with_pk):
        source = Source(executor=mock_declarative_executor, name="test-source")
        
        result = source.get_record("users", pk_value={"id": "123"})
        
        assert result == {"id": "123", "name": "Test User"}
        mock_declarative_executor.fetch_record.assert_called_once_with(
            stream_name="users",
            pk_value="123",
            config=None,
        )


def test_get_record_with_dict_pk_value_multiple_entries(
    mock_catalog_with_pk, mock_declarative_executor
):
    """Test get_record with a dict that has multiple entries (should fail)."""
    with patch.object(Source, "_discover", return_value=mock_catalog_with_pk):
        source = Source(executor=mock_declarative_executor, name="test-source")
        
        with pytest.raises(exc.PyAirbyteInputError) as exc_info:
            source.get_record("users", pk_value={"id": "123", "name": "test"})
        
        assert "exactly one entry" in str(exc_info.value)


def test_get_record_with_dict_pk_value_wrong_key(
    mock_catalog_with_pk, mock_declarative_executor
):
    """Test get_record with a dict where the key doesn't match the stream's PK."""
    with patch.object(Source, "_discover", return_value=mock_catalog_with_pk):
        source = Source(executor=mock_declarative_executor, name="test-source")
        
        with pytest.raises(exc.PyAirbyteInputError) as exc_info:
            source.get_record("users", pk_value={"user_id": "123"})
        
        assert "does not match" in str(exc_info.value)
        assert "Expected Key" in str(exc_info.value)


def test_get_record_with_non_declarative_executor(
    mock_catalog_with_pk, mock_non_declarative_executor
):
    """Test get_record with a non-declarative executor (should raise NotImplementedError)."""
    with patch.object(Source, "_discover", return_value=mock_catalog_with_pk):
        source = Source(executor=mock_non_declarative_executor, name="test-source")
        
        with pytest.raises(NotImplementedError) as exc_info:
            source.get_record("users", pk_value="123")
        
        assert "only supported for declarative sources" in str(exc_info.value)


def test_get_record_with_no_primary_key(mock_catalog_no_pk, mock_declarative_executor):
    """Test get_record with a stream that has no primary key."""
    with patch.object(Source, "_discover", return_value=mock_catalog_no_pk):
        source = Source(executor=mock_declarative_executor, name="test-source")
        
        with pytest.raises(NotImplementedError) as exc_info:
            source.get_record("events", pk_value="123")
        
        assert "does not have a primary key" in str(exc_info.value)


def test_get_record_with_composite_primary_key(
    mock_catalog_composite_pk, mock_declarative_executor
):
    """Test get_record with a stream that has a composite primary key."""
    with patch.object(Source, "_discover", return_value=mock_catalog_composite_pk):
        source = Source(executor=mock_declarative_executor, name="test-source")
        
        with pytest.raises(NotImplementedError) as exc_info:
            source.get_record("orders", pk_value="123")
        
        assert "composite primary key" in str(exc_info.value)


def test_get_record_with_invalid_stream_name(
    mock_catalog_with_pk, mock_declarative_executor
):
    """Test get_record with an invalid stream name."""
    with patch.object(Source, "_discover", return_value=mock_catalog_with_pk):
        source = Source(executor=mock_declarative_executor, name="test-source")
        
        with pytest.raises(exc.PyAirbyteInputError) as exc_info:
            source.get_record("nonexistent_stream", pk_value="123")
        
        assert "does not exist" in str(exc_info.value)


def test_get_record_with_primary_key_override(
    mock_catalog_with_pk, mock_declarative_executor
):
    """Test get_record with a primary key override."""
    with patch.object(Source, "_discover", return_value=mock_catalog_with_pk):
        source = Source(
            executor=mock_declarative_executor,
            name="test-source",
            primary_key_overrides={"users": "custom_id"},
        )
        
        result = source.get_record("users", pk_value={"custom_id": "456"})
        
        assert result == {"id": "123", "name": "Test User"}
        mock_declarative_executor.fetch_record.assert_called_once_with(
            stream_name="users",
            pk_value="456",
            config=None,
        )


def test_get_record_with_config(mock_catalog_with_pk, mock_declarative_executor):
    """Test get_record passes config to the executor."""
    with patch.object(Source, "_discover", return_value=mock_catalog_with_pk):
        source = Source(
            executor=mock_declarative_executor,
            name="test-source",
            config={"api_key": "test_key"},
        )
        
        result = source.get_record("users", pk_value="123")
        
        assert result == {"id": "123", "name": "Test User"}
        mock_declarative_executor.fetch_record.assert_called_once_with(
            stream_name="users",
            pk_value="123",
            config={"api_key": "test_key"},
        )


def test_get_record_propagates_exceptions_from_executor(
    mock_catalog_with_pk, mock_declarative_executor
):
    """Test that exceptions from the executor are propagated."""
    test_exception = ValueError("Record with primary key 999 not found")
    mock_declarative_executor.fetch_record.side_effect = test_exception
    
    with patch.object(Source, "_discover", return_value=mock_catalog_with_pk):
        source = Source(executor=mock_declarative_executor, name="test-source")
        
        with pytest.raises(ValueError) as exc_info:
            source.get_record("users", pk_value="999")
        
        assert "999" in str(exc_info.value)


def test_get_record_with_integer_pk_value(mock_catalog_with_pk, mock_declarative_executor):
    """Test get_record with an integer primary key value (should be converted to string)."""
    with patch.object(Source, "_discover", return_value=mock_catalog_with_pk):
        source = Source(executor=mock_declarative_executor, name="test-source")
        
        result = source.get_record("users", pk_value=123)
        
        assert result == {"id": "123", "name": "Test User"}
        mock_declarative_executor.fetch_record.assert_called_once_with(
            stream_name="users",
            pk_value="123",
            config=None,
        )


def test_get_record_with_dict_integer_value(mock_catalog_with_pk, mock_declarative_executor):
    """Test get_record with a dict containing an integer value (should be converted to string)."""
    with patch.object(Source, "_discover", return_value=mock_catalog_with_pk):
        source = Source(executor=mock_declarative_executor, name="test-source")
        
        result = source.get_record("users", pk_value={"id": 456})
        
        assert result == {"id": "123", "name": "Test User"}
        mock_declarative_executor.fetch_record.assert_called_once_with(
            stream_name="users",
            pk_value="456",
            config=None,
        )


def test_get_stream_primary_key_helper(mock_catalog_with_pk):
    """Test the _get_stream_primary_key helper method."""
    with patch.object(Source, "_discover", return_value=mock_catalog_with_pk):
        source = Source(executor=Mock(), name="test-source")
        
        pk = source._get_stream_primary_key("users")
        
        assert pk == ["id"]


def test_normalize_and_validate_pk_with_string(mock_catalog_with_pk):
    """Test the _normalize_and_validate_pk helper with a string value."""
    with patch.object(Source, "_discover", return_value=mock_catalog_with_pk):
        source = Source(executor=Mock(), name="test-source")
        
        result = source._normalize_and_validate_pk("users", "123")
        
        assert result == "123"


def test_normalize_and_validate_pk_with_valid_dict(mock_catalog_with_pk):
    """Test the _normalize_and_validate_pk helper with a valid dict."""
    with patch.object(Source, "_discover", return_value=mock_catalog_with_pk):
        source = Source(executor=Mock(), name="test-source")
        
        result = source._normalize_and_validate_pk("users", {"id": "123"})
        
        assert result == "123"
