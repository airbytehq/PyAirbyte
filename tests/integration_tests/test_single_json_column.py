"""Integration tests for the single JSON column feature with PostgreSQL and DuckDB."""

from __future__ import annotations

import json
import airbyte as ab
import psycopg
import pytest
from airbyte import exceptions as exc
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.postgres import PostgresCache
from airbyte.constants import AB_DATA_COLUMN, AB_EXTRACTED_AT_COLUMN, AB_META_COLUMN, AB_RAW_ID_COLUMN
from sqlalchemy import text


@pytest.fixture(scope="module", autouse=True)
def autouse_source_test_installation(source_test_installation) -> None:
    """Install source-test connector for testing."""
    return


@pytest.fixture(scope="function", autouse=True)
def autouse_source_test_registry(source_test_registry) -> None:
    """Enable the test registry for source-test connector."""
    return


def test_postgres_single_json_column_structure(
    new_postgres_cache: PostgresCache,
) -> None:
    """Test that single JSON column mode creates the correct table structure.

    This test verifies:
    1. Table has only 4 columns: _airbyte_data + 3 internal columns
    2. Primary keys are inside _airbyte_data (not separate columns)
    3. Data can be written and read back correctly
    4. _airbyte_data uses JSONB type (Postgres-specific)
    """
    # Enable single JSON column mode
    new_postgres_cache.use_single_json_column = True

    # Get source-test connector
    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()

    # Write data to cache using append strategy
    # Note: Single JSON column mode only supports APPEND strategy because
    # primary keys are inside the JSON column, not as separate columns
    result = source.read(new_postgres_cache, write_strategy="append")

    # Verify data was written
    assert result.processed_records > 0

    # Connect directly to Postgres to inspect table structure
    pg_url = (
        f"postgresql://{new_postgres_cache.username}:{new_postgres_cache.password}"
        f"@{new_postgres_cache.host}:{new_postgres_cache.port}/{new_postgres_cache.database}"
    )

    with psycopg.connect(pg_url) as conn:
        with conn.cursor() as cur:
            # Get all stream names from the result
            for stream_name in result.streams.keys():
                # Use processor's normalizer to get the correct table name
                normalized_stream_name = new_postgres_cache.processor.normalizer.normalize(stream_name)
                table_name = f"{new_postgres_cache.table_prefix}{normalized_stream_name}"

                # Get column names and types
                cur.execute(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (new_postgres_cache.schema_name, table_name),
                )
                columns = cur.fetchall()
                column_names = [col[0] for col in columns]
                column_types = {col[0]: col[1] for col in columns}

                # Verify we have exactly 4 columns
                assert len(column_names) == 4, (
                    f"Expected 4 columns, got {len(column_names)}: {column_names}"
                )

                # Verify the expected columns exist
                expected_columns = {
                    AB_DATA_COLUMN,
                    AB_RAW_ID_COLUMN,
                    AB_EXTRACTED_AT_COLUMN,
                    AB_META_COLUMN,
                }
                assert set(column_names) == expected_columns, (
                    f"Column mismatch. Expected: {expected_columns}, Got: {set(column_names)}"
                )

                # Verify _airbyte_data is JSONB type
                assert column_types[AB_DATA_COLUMN] == "jsonb", (
                    f"Expected _airbyte_data to be JSONB, got {column_types[AB_DATA_COLUMN]}"
                )

                # Read some data and verify it's in JSON format
                cur.execute(
                    f"""
                    SELECT {AB_DATA_COLUMN}
                    FROM {new_postgres_cache.schema_name}.{table_name}
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row:
                    json_data = row[0]
                    # Verify it's a dict (psycopg2 automatically parses JSONB)
                    assert isinstance(json_data, dict), (
                        f"Expected _airbyte_data to contain dict, got {type(json_data)}"
                    )
                    # Verify it contains some data
                    assert len(json_data) > 0, (
                        "_airbyte_data should contain data properties"
                    )

    # Verify we can read data back through PyAirbyte's API
    for stream_name in result.streams.keys():
        dataset = new_postgres_cache[stream_name]
        df = dataset.to_pandas()

        # Skip empty streams (e.g., always-empty-stream)
        if len(df) == 0:
            continue

        # Verify DataFrame has the _airbyte_data column
        assert AB_DATA_COLUMN in df.columns, (
            f"Expected {AB_DATA_COLUMN} in DataFrame columns"
        )

        # Verify it contains data
        assert len(df) > 0, f"Expected data in stream {stream_name}"


def test_duckdb_single_json_column_structure(
    new_duckdb_cache: DuckDBCache,
) -> None:
    """Test that single JSON column mode creates the correct table structure in DuckDB.

    This test verifies:
    1. Table has only 4 columns: _airbyte_data + 3 internal columns
    2. Primary keys are inside _airbyte_data (not separate columns)
    3. Data can be written and read back correctly
    4. _airbyte_data uses JSON type
    """
    new_duckdb_cache.use_single_json_column = True

    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()

    result = source.read(new_duckdb_cache, write_strategy="append")

    assert result.processed_records > 0

    # Use the cache's existing SQL connection to inspect table structure
    with new_duckdb_cache.processor.get_sql_connection() as conn:
        for stream_name in result.streams.keys():
            normalized_stream_name = new_duckdb_cache.processor.normalizer.normalize(stream_name)
            table_name = f"{new_duckdb_cache.table_prefix}{normalized_stream_name}"

            columns_query = text(
                f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{new_duckdb_cache.schema_name}'
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
                """
            )
            result_proxy = conn.execute(columns_query)
            columns = result_proxy.fetchall()
            column_names = [col[0] for col in columns]
            column_types = {col[0]: col[1] for col in columns}

            assert len(column_names) == 4, (
                f"Expected 4 columns, got {len(column_names)}: {column_names}"
            )

            expected_columns = {
                AB_DATA_COLUMN,
                AB_RAW_ID_COLUMN,
                AB_EXTRACTED_AT_COLUMN,
                AB_META_COLUMN,
            }
            assert set(column_names) == expected_columns, (
                f"Column mismatch. Expected: {expected_columns}, Got: {set(column_names)}"
            )

            assert column_types[AB_DATA_COLUMN] == "JSON", (
                f"Expected _airbyte_data to be JSON, got {column_types[AB_DATA_COLUMN]}"
            )

            data_query = text(
                f"""
                SELECT {AB_DATA_COLUMN}
                FROM {new_duckdb_cache.schema_name}.{table_name}
                LIMIT 1
                """
            )
            result_proxy = conn.execute(data_query)
            rows = result_proxy.fetchall()

            if rows:
                json_data = rows[0][0]
                if isinstance(json_data, str):
                    json_data = json.loads(json_data)

                assert isinstance(json_data, dict), (
                    f"Expected _airbyte_data to contain dict, got {type(json_data)}"
                )
                assert len(json_data) > 0, "_airbyte_data should contain data properties"

    for stream_name in result.streams.keys():
        dataset = new_duckdb_cache[stream_name]
        df = dataset.to_pandas()

        # Skip empty streams
        if len(df) == 0:
            continue

        assert AB_DATA_COLUMN in df.columns, (
            f"Expected {AB_DATA_COLUMN} in DataFrame columns"
        )

        assert len(df) > 0, f"Expected data in stream {stream_name}"


def test_duckdb_single_json_column_rejects_non_append_strategies(
    new_duckdb_cache: DuckDBCache,
) -> None:
    """Test that single JSON column mode rejects non-append write strategies in DuckDB.

    Single JSON column mode is only compatible with APPEND strategy because
    merge/replace strategies require primary keys as separate columns.
    """
    # Enable single JSON column mode
    new_duckdb_cache.use_single_json_column = True

    # Get source-test connector
    source = ab.get_source("source-test", config={"apiKey": "test"})
    source.select_all_streams()

    # Verify that MERGE strategy is rejected
    with pytest.raises(exc.PyAirbyteInputError) as excinfo:
        source.read(new_duckdb_cache, write_strategy="merge")

    assert "use_single_json_column=True" in str(excinfo.value)
    assert "APPEND" in str(excinfo.value)

    # Verify that REPLACE strategy is rejected
    with pytest.raises(exc.PyAirbyteInputError) as excinfo:
        source.read(new_duckdb_cache, write_strategy="replace", force_full_refresh=True)

    assert "use_single_json_column=True" in str(excinfo.value)
    assert "APPEND" in str(excinfo.value)
