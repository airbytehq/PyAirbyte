# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Unit tests specific to BigQuery caches."""

from __future__ import annotations

import pytest
from sqlalchemy import types as sqlalchemy_types

import airbyte as ab
from airbyte._processors.sql.bigquery import BigQueryTypeConverter
from airbyte._util import text_util


@pytest.mark.requires_creds
def test_bigquery_props(
    new_bigquery_cache: ab.BigQueryCache,
) -> None:
    """Test that the BigQueryCache properties are set correctly."""
    # assert new_bigquery_cache.credentials_path.endswith(".json")
    assert new_bigquery_cache.dataset_name == new_bigquery_cache.schema_name, (
        "Dataset name should be the same as schema name."
    )
    assert new_bigquery_cache.schema_name != "airbyte_raw", (
        "Schema name should not be the default value."
    )

    assert new_bigquery_cache.get_database_name() == new_bigquery_cache.project_name, (
        "Database name should be the same as project name."
    )


@pytest.mark.requires_creds
def test_decimal_type_conversion(
    new_bigquery_cache: ab.BigQueryCache,
) -> None:
    """Test that DECIMAL(38,9) types are correctly converted to BigQuery NUMERIC types."""
    table_name = f"test_decimal_{text_util.generate_random_suffix()}"

    try:
        # Verify type conversion
        converter = BigQueryTypeConverter()
        converted_type = converter.to_sql_type({"type": "number", "format": "decimal"})

        # Check that the converted type is a NUMERIC type with correct precision and scale
        assert isinstance(converted_type, sqlalchemy_types.Numeric), (
            "DECIMAL type should be converted to NUMERIC"
        )
        assert converted_type.precision == 38, "Precision should be 38"
        assert converted_type.scale == 9, "Scale should be 9"

        # Ensure schema exists before creating table
        new_bigquery_cache._ensure_schema_exists()

        # Create a test table with a DECIMAL column
        sql = f"""
        CREATE TABLE {new_bigquery_cache.schema_name}.{table_name} (
            id INT64,
            amount NUMERIC(38, 9)
        )
        """
        new_bigquery_cache.execute_sql(sql)

        # Insert test data
        sql = f"""
        INSERT INTO {new_bigquery_cache.schema_name}.{table_name} (id, amount)
        VALUES (1, 123.456789)
        """
        new_bigquery_cache.execute_sql(sql)

        # Verify we can read the data back
        sql = f"SELECT amount FROM {new_bigquery_cache.schema_name}.{table_name} WHERE id = 1"
        result = new_bigquery_cache.execute_sql(sql).fetchone()
        assert result is not None, "Should be able to read NUMERIC data"
        assert isinstance(result[0], (float, int, str)), (
            "NUMERIC data should be readable"
        )

    finally:
        # Clean up
        cleanup_sql = (
            f"DROP TABLE IF EXISTS {new_bigquery_cache.schema_name}.{table_name}"
        )
        new_bigquery_cache.execute_sql(cleanup_sql)
