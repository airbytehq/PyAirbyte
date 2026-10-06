# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Cloud destinations for Airbyte."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from airbyte.exceptions import AirbyteLibInputError
from airbyte.secrets.base import SecretString


if TYPE_CHECKING:
    from collections.abc import Callable

    from airbyte.caches.base import CacheBase
    from airbyte.caches.bigquery import BigQueryCache
    from airbyte.caches.duckdb import DuckDBCache
    from airbyte.caches.motherduck import MotherDuckCache
    from airbyte.caches.postgres import PostgresCache
    from airbyte.caches.snowflake import SnowflakeCache


SNOWFLAKE_PASSWORD_SECRET_NAME = "SNOWFLAKE_PASSWORD"


def cache_to_destination_configuration(
    cache: CacheBase,
) -> dict[str, Any]:
    """Get the destination configuration from the cache."""
    conversion_fn_map: dict[str, Callable[[Any], dict[str, Any]]] = {
        "BigQueryCache": bigquery_cache_to_destination_configuration,
        "bigquery": bigquery_cache_to_destination_configuration,
        "DuckDBCache": duckdb_cache_to_destination_configuration,
        "duckdb": duckdb_cache_to_destination_configuration,
        "MotherDuckCache": motherduck_cache_to_destination_configuration,
        "motherduck": motherduck_cache_to_destination_configuration,
        "PostgresCache": postgres_cache_to_destination_configuration,
        "postgres": postgres_cache_to_destination_configuration,
        "SnowflakeCache": snowflake_cache_to_destination_configuration,
        "snowflake": snowflake_cache_to_destination_configuration,
    }
    cache_class_name = cache.__class__.__name__
    if cache_class_name not in conversion_fn_map:
        raise AirbyteLibInputError(
            message=(
                "Cannot convert cache type to destination configuration. "
                f"Cache type {cache_class_name} not supported. "
                f"Supported cache types: {list(conversion_fn_map.keys())}"
            ),
        )

    conversion_fn = conversion_fn_map[cache_class_name]
    return conversion_fn(cache)


def duckdb_cache_to_destination_configuration(
    cache: DuckDBCache,
) -> dict[str, Any]:
    """Get the destination configuration from the DuckDB cache."""
    return {
        "destination_path": str(cache.db_path),
        "destinationType": "duckdb",
        "schema": cache.schema_name,
    }


def motherduck_cache_to_destination_configuration(
    cache: MotherDuckCache,
) -> dict[str, Any]:
    """Get the destination configuration from the DuckDB cache."""
    return {
        "destination_path": cache.db_path,
        "destinationType": "duckdb",
        "motherduck_api_key": cache.api_key,
        "schema": cache.schema_name,
    }


def postgres_cache_to_destination_configuration(
    cache: PostgresCache,
) -> dict[str, Any]:
    """Get the destination configuration from the Postgres cache."""
    return {
        "database": cache.database,
        "host": cache.host,
        "username": cache.username,
        "destinationType": "postgres",
        "disable_type_dedupe": False,
        "drop_cascade": False,
        "password": cache.password,
        "port": cache.port,
        "schema": cache.schema_name,
        "ssl": False,
        "unconstrained_number": False,
    }


def snowflake_cache_to_destination_configuration(
    cache: SnowflakeCache,
) -> dict[str, Any]:
    """Get the destination configuration from the Snowflake cache."""
    return {
        "host": f"{cache.account}.snowflakecomputing.com",
        "database": cache.get_database_name().upper(),
        "schema": cache.schema_name.upper(),
        "warehouse": cache.warehouse,
        "role": cache.role,
        "username": cache.username,
        "credentials": {
            "password": cache.password,
            "auth_type": "Username and Password",
        },
        "destinationType": "snowflake",
        "disable_type_dedupe": False,
        "retention_period_days": 1,
        "use_merge_for_upsert": False,
    }


def bigquery_cache_to_destination_configuration(
    cache: BigQueryCache,
) -> dict[str, Any]:
    """Get the destination configuration from the BigQuery cache."""
    credentials_json: str | None = (
        SecretString(Path(cache.credentials_path).read_text(encoding="utf-8"))
        if cache.credentials_path
        else None
    )

    return {
        "project_id": cache.project_name,
        "dataset_id": cache.dataset_name,
        "dataset_location": cache.dataset_location,
        "cdc_deletion_mode": "Hard delete",
        "credentials_json": credentials_json,
        "destinationType": "bigquery",
        "disable_type_dedupe": False,
        "loading_method": {"method": "Standard"},
    }
