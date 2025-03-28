# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from pathlib import Path


from airbyte.caches.base import CacheBase
from airbyte.caches.duckdb import DuckDBCache

UNIT_TEST_DB_PATH: Path = Path(".cache") / "unit_tests" / "test_db.duckdb"


def test_duck_db_cache_config_initialization():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH, schema_name="test_schema")
    assert config.db_path == Path(UNIT_TEST_DB_PATH)
    assert config.schema_name == "test_schema"


def test_duck_db_cache_config_default_schema_name():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH)
    assert config.schema_name == "main"


def test_get_sql_alchemy_url():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH, schema_name="test_schema")
    assert config.get_sql_alchemy_url() == f"duckdb:///{UNIT_TEST_DB_PATH}"


def test_get_sql_alchemy_url_with_default_schema_name():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH)
    assert config.get_sql_alchemy_url() == f"duckdb:///{UNIT_TEST_DB_PATH}"


def test_duck_db_cache_config_inheritance():
    assert issubclass(DuckDBCache, CacheBase)


def test_duck_db_cache_config_get_sql_alchemy_url():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH, schema_name="test_schema")
    assert config.get_sql_alchemy_url() == f"duckdb:///{UNIT_TEST_DB_PATH}"


def test_duck_db_cache_config_get_database_name():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH, schema_name="test_schema")
    assert config.get_database_name() == "test_db"


def test_duck_db_cache_base_inheritance():
    assert issubclass(DuckDBCache, CacheBase)


def test_duck_db_cache_config_get_sql_alchemy_url_with_default_schema_name():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH)
    assert config.get_sql_alchemy_url() == f"duckdb:///{UNIT_TEST_DB_PATH}"


def test_duck_db_cache_config_get_database_name_with_default_schema_name():
    config = DuckDBCache(db_path=UNIT_TEST_DB_PATH)
    assert config.get_database_name() == "test_db"


def test_duck_db_cache_config_inheritance_from_sql_cache_config_base():
    assert issubclass(DuckDBCache, CacheBase)


def test_create_source_tables(mocker):
    """Test that the create_source_tables method correctly creates tables based on the source's catalog."""
    # Import here to avoid circular imports
    from airbyte_protocol.models import (
        ConfiguredAirbyteCatalog,
        ConfiguredAirbyteStream,
    )

    # Create a proper ConfiguredAirbyteCatalog for mocking
    stream1 = ConfiguredAirbyteStream(
        stream={
            "name": "stream1",
            "json_schema": {},
            "supported_sync_modes": ["full_refresh"],
        },
        sync_mode="full_refresh",
        destination_sync_mode="overwrite",
    )
    stream2 = ConfiguredAirbyteStream(
        stream={
            "name": "stream2",
            "json_schema": {},
            "supported_sync_modes": ["full_refresh"],
        },
        sync_mode="full_refresh",
        destination_sync_mode="overwrite",
    )
    catalog = ConfiguredAirbyteCatalog(streams=[stream1, stream2])

    # Mock the catalog provider
    mock_catalog_provider = mocker.Mock()
    mock_catalog_provider.stream_names = ["stream1", "stream2"]
    mocker.patch(
        "airbyte.shared.catalog_providers.CatalogProvider",
        return_value=mock_catalog_provider,
    )

    # Mock a source with configured catalog and selected streams
    mock_source = mocker.Mock()
    mock_source.get_configured_catalog.return_value = catalog
    mock_source.get_selected_streams.return_value = ["stream1"]

    # Create a DuckDBCache instance with mocked processor
    cache = DuckDBCache(db_path=UNIT_TEST_DB_PATH)

    # Mock the processor property
    mock_processor = mocker.Mock()
    mocker.patch.object(
        DuckDBCache, "processor", mocker.PropertyMock(return_value=mock_processor)
    )

    # Test with default (None) stream parameter - should use source's selected streams
    cache.create_source_tables(mock_source)

    # Verify the correct methods were called
    mock_source.get_selected_streams.assert_called_once()
    mock_source.get_configured_catalog.assert_called_once_with(streams=["stream1"])
    mock_processor._ensure_schema_exists.assert_called_once()
    assert mock_processor._ensure_final_table_exists.call_count == 2

    # Reset mocks
    mock_source.reset_mock()
    mock_processor.reset_mock()

    # Test with explicit stream list
    cache.create_source_tables(mock_source, streams=["stream2"])

    # Verify the correct methods were called
    mock_source.get_selected_streams.assert_not_called()
    mock_source.get_configured_catalog.assert_called_once_with(streams=["stream2"])
    mock_processor._ensure_schema_exists.assert_called_once()
    assert mock_processor._ensure_final_table_exists.call_count == 2
