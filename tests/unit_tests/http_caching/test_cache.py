"""Tests for the HTTP caching module."""

import tempfile
from pathlib import Path

import pytest

from airbyte.http_caching.cache import AirbyteConnectorCache, HttpCacheMode
from airbyte.http_caching.modes import SerializationFormat


def test_cache_initialization() -> None:
    """Test that the cache can be initialized with default values."""
    cache = AirbyteConnectorCache()
    assert cache.mode == HttpCacheMode.READ_WRITE
    assert cache.cache_dir.name == ".airbyte-http-cache"
    assert cache.serialization_format == SerializationFormat.NATIVE


def test_cache_with_custom_dir() -> None:
    """Test that the cache can be initialized with a custom directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache = AirbyteConnectorCache(cache_dir=temp_dir)
        assert cache.mode == HttpCacheMode.READ_WRITE
        assert cache.cache_dir == Path(temp_dir)


def test_cache_with_read_dir() -> None:
    """Test that the cache can be initialized with separate read directory."""
    with tempfile.TemporaryDirectory() as write_dir:
        with tempfile.TemporaryDirectory() as read_dir:
            cache = AirbyteConnectorCache(cache_dir=write_dir, read_dir=read_dir)
            assert cache.mode == HttpCacheMode.READ_WRITE
            assert cache.cache_dir == Path(write_dir)
            assert cache.read_dir == Path(read_dir)


def test_cache_with_custom_mode() -> None:
    """Test that the cache can be initialized with a custom mode."""
    cache = AirbyteConnectorCache(mode=HttpCacheMode.READ_ONLY)
    assert cache.mode == HttpCacheMode.READ_ONLY

    cache = AirbyteConnectorCache(mode=HttpCacheMode.WRITE_ONLY)
    assert cache.mode == HttpCacheMode.WRITE_ONLY

    cache = AirbyteConnectorCache(mode=HttpCacheMode.READ_ONLY_FAIL_ON_MISS)
    assert cache.mode == HttpCacheMode.READ_ONLY_FAIL_ON_MISS


def test_cache_with_string_mode() -> None:
    """Test that the cache can be initialized with a string mode."""
    cache = AirbyteConnectorCache(mode="read_only")
    assert cache.mode == HttpCacheMode.READ_ONLY

    cache = AirbyteConnectorCache(mode="write_only")
    assert cache.mode == HttpCacheMode.WRITE_ONLY

    cache = AirbyteConnectorCache(mode="read_write")
    assert cache.mode == HttpCacheMode.READ_WRITE

    cache = AirbyteConnectorCache(mode="read_only_fail_on_miss")
    assert cache.mode == HttpCacheMode.READ_ONLY_FAIL_ON_MISS


def test_cache_with_invalid_mode() -> None:
    """Test that the cache raises an error with an invalid mode."""
    with pytest.raises(ValueError):
        AirbyteConnectorCache(mode="invalid_mode")


def test_cache_with_serialization_format() -> None:
    """Test that the cache can be initialized with a custom serialization format."""
    cache = AirbyteConnectorCache(serialization_format=SerializationFormat.JSON)
    assert cache.serialization_format == SerializationFormat.JSON

    cache = AirbyteConnectorCache(serialization_format="json")
    assert cache.serialization_format == SerializationFormat.JSON

    cache = AirbyteConnectorCache(serialization_format="native")
    assert cache.serialization_format == SerializationFormat.NATIVE


def test_cache_with_invalid_serialization_format() -> None:
    """Test that the cache raises an error with an invalid serialization format."""
    with pytest.raises(ValueError):
        AirbyteConnectorCache(serialization_format="invalid_format")
