# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Cloud destinations for Airbyte."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from airbyte_api.models import (
    BatchedStandardInserts,
    DestinationBigquery,
    DestinationDuckdb,
    DestinationPostgres,
    DestinationSnowflake,
    UsernameAndPassword,
)

from airbyte.secrets.base import SecretString


if TYPE_CHECKING:
    from collections.abc import Callable

    from airbyte._util import api_util
    from airbyte.caches.base import CacheBase
    from airbyte.caches.bigquery import BigQueryCache
    from airbyte.caches.duckdb import DuckDBCache
    from airbyte.caches.motherduck import MotherDuckCache
    from airbyte.caches.postgres import PostgresCache
    from airbyte.caches.snowflake import SnowflakeCache


SNOWFLAKE_PASSWORD_SECRET_NAME = "SNOWFLAKE_PASSWORD"


def cache_to_destination_configuration(
    cache: CacheBase,
) -> api_util.DestinationConfiguration:
    """Get the destination configuration from the cache."""
    conversion_fn_map: dict[str, Callable[[Any], api_util.DestinationConfiguration]] = {
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
        raise ValueError(
            "Cannot convert cache type to destination configuration. "
            f"Cache type {cache_class_name} not supported. "
            f"Supported cache types: {list(conversion_fn_map.keys())}"
        )

    conversion_fn = conversion_fn_map[cache_class_name]
    return conversion_fn(cache)


def duckdb_cache_to_destination_configuration(
    cache: DuckDBCache,
) -> DestinationDuckdb:
    """Get the destination configuration from the DuckDB cache."""
    return DestinationDuckdb(
        destination_path=str(cache.db_path),
        schema=cache.schema_name,
    )


def motherduck_cache_to_destination_configuration(
    cache: MotherDuckCache,
) -> DestinationDuckdb:
    """Get the destination configuration from the DuckDB cache."""
    return DestinationDuckdb(
        destination_path=cache.db_path,
        schema=cache.schema_name,
        motherduck_api_key=cache.api_key,
    )


def postgres_cache_to_destination_configuration(
    cache: PostgresCache,
) -> DestinationPostgres:
    """Get the destination configuration from the Postgres cache."""
    return DestinationPostgres(
        database=cache.database,
        host=cache.host,
        password=cache.password,
        port=cache.port,
        schema=cache.schema_name,
        username=cache.username,
    )


def snowflake_cache_to_destination_configuration(
    cache: SnowflakeCache,
) -> DestinationSnowflake:
    """Get the destination configuration from the Snowflake cache."""
    return DestinationSnowflake(
        host=f"{cache.account}.snowflakecomputing.com",
        database=cache.get_database_name().upper(),
        schema=cache.schema_name.upper(),
        warehouse=cache.warehouse,
        role=cache.role,
        username=cache.username,
        credentials=UsernameAndPassword(
            password=cache.password,
        ),
    )


def bigquery_cache_to_destination_configuration(
    cache: BigQueryCache,
) -> DestinationBigquery:
    """Get the destination configuration from the BigQuery cache."""
    credentials_json: str | None = (
        SecretString(Path(cache.credentials_path).read_text(encoding="utf-8"))
        if cache.credentials_path
        else None
    )
    return DestinationBigquery(
        project_id=cache.project_name,
        dataset_id=cache.dataset_name,
        dataset_location="US",
        credentials_json=credentials_json,
        loading_method=BatchedStandardInserts(),
    )
