# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Unit tests for the IcebergCache implementation."""

from __future__ import annotations

from pathlib import Path

import pytest

from airbyte._processors.sql.iceberg import (
    IcebergConfig,
    IcebergProcessor,
    IcebergTypeConverter,
)
from airbyte.caches.base import CacheBase
from airbyte.caches.iceberg import IcebergCache


UNIT_TEST_WAREHOUSE_PATH: Path = Path(".cache") / "unit_tests" / "iceberg_warehouse"


def test_iceberg_config_initialization() -> None:
    """Test basic IcebergConfig initialization with custom values."""
    config = IcebergConfig(
        warehouse_path=UNIT_TEST_WAREHOUSE_PATH,
        namespace="test_namespace",
    )
    assert config.warehouse_path == UNIT_TEST_WAREHOUSE_PATH
    assert config.namespace == "test_namespace"
    assert config.catalog_type == "sql"
    assert config.catalog_name == "default"


def test_iceberg_config_defaults() -> None:
    """Test IcebergConfig default values."""
    config = IcebergConfig(warehouse_path=UNIT_TEST_WAREHOUSE_PATH)
    assert config.namespace == "airbyte_raw"
    # schema_name is always "main" for SQLite compatibility (internal tables)
    # namespace is used for Iceberg table organization instead
    assert config.schema_name == "main"
    assert config.catalog_type == "sql"


def test_iceberg_config_get_database_name() -> None:
    """Test get_database_name returns the namespace."""
    config = IcebergConfig(
        warehouse_path=UNIT_TEST_WAREHOUSE_PATH,
        namespace="my_namespace",
    )
    assert config.get_database_name() == "my_namespace"


@pytest.mark.parametrize(
    "catalog_type,catalog_uri,expected_url",
    [
        pytest.param(
            "sql",
            "sqlite:///test.db",
            "sqlite:///test.db",
            id="sql_catalog_returns_sqlite_uri",
        ),
        pytest.param(
            "rest",
            "https://example.com/catalog",
            "iceberg://",
            id="rest_catalog_returns_placeholder",
        ),
    ],
)
def test_iceberg_config_get_sql_alchemy_url(
    catalog_type: str,
    catalog_uri: str,
    expected_url: str,
) -> None:
    """Test get_sql_alchemy_url for different catalog types."""
    config = IcebergConfig(
        warehouse_path=UNIT_TEST_WAREHOUSE_PATH,
        catalog_type=catalog_type,
        catalog_uri=catalog_uri,
    )
    assert expected_url in str(config.get_sql_alchemy_url())


@pytest.mark.parametrize(
    "json_schema,expected_type_name",
    [
        pytest.param({"type": "string"}, "StringType", id="string"),
        pytest.param({"type": "integer"}, "LongType", id="integer"),
        pytest.param({"type": "number"}, "DoubleType", id="number"),
        pytest.param({"type": "boolean"}, "BooleanType", id="boolean"),
        pytest.param({"type": "object"}, "StringType", id="object_as_json_string"),
        pytest.param({"type": "array"}, "StringType", id="array_as_json_string"),
        pytest.param({"type": ["string", "null"]}, "StringType", id="nullable_string"),
        pytest.param({"type": ["integer", "null"]}, "LongType", id="nullable_integer"),
        pytest.param(
            {"type": "string", "format": "date-time"},
            "TimestamptzType",
            id="datetime_format",
        ),
        pytest.param(
            {"type": "string", "format": "datetime"},
            "TimestamptzType",
            id="datetime_format_alt",
        ),
    ],
)
def test_iceberg_type_converter(json_schema: dict, expected_type_name: str) -> None:
    """Test IcebergTypeConverter converts JSON schema types to Iceberg types."""
    converter = IcebergTypeConverter()
    result = converter.json_schema_to_iceberg_type(json_schema)
    assert result.__class__.__name__ == expected_type_name


def test_iceberg_cache_inheritance() -> None:
    """Test that IcebergCache inherits from expected base classes."""
    assert issubclass(IcebergCache, CacheBase)
    assert issubclass(IcebergCache, IcebergConfig)


def test_iceberg_cache_processor_class() -> None:
    """Test that IcebergCache uses IcebergProcessor."""
    assert IcebergCache._sql_processor_class == IcebergProcessor


def test_iceberg_cache_no_paired_destination() -> None:
    """Test that IcebergCache has no paired destination (yet)."""
    assert IcebergCache.paired_destination_name is None
    assert IcebergCache.paired_destination_config_class is None


def test_iceberg_processor_merge_insert_support() -> None:
    """Test that IcebergProcessor doesn't use SQL merge (uses Iceberg's own semantics)."""
    assert IcebergProcessor.supports_merge_insert is False
