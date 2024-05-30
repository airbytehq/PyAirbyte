# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Cloud destinations for Airbyte."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from airbyte_api.models import (
    DestinationBigquery,
    DestinationDuckdb,
    DestinationPostgres,
    DestinationSnowflake,
    StandardInserts,
    UsernameAndPassword,
)

from airbyte.caches import (
    BigQueryCache,
    DuckDBCache,
    MotherDuckCache,
    PostgresCache,
    SnowflakeCache,
)
from airbyte.secrets import get_secret


if TYPE_CHECKING:
    from collections.abc import Callable

    from airbyte.caches.base import CacheBase


SNOWFLAKE_PASSWORD_SECRET_NAME = "SNOWFLAKE_PASSWORD"


def get_destination_config_from_cache(
    cache: CacheBase,
) -> dict[str, str]:
    """Get the destination configuration from the cache."""
    conversion_fn_map: dict[str, Callable[[Any], dict[str, str]]] = {
        "BigQueryCache": get_bigquery_destination_config,
        "DuckDBCache": get_duckdb_destination_config,
        "MotherDuckCache": get_motherduck_destination_config,
        "PostgresCache": get_postgres_destination_config,
        "SnowflakeCache": get_snowflake_destination_config,
    }
    cache_class_name = cache.__class__.__name__
    if cache_class_name not in conversion_fn_map:
        raise ValueError(  # noqa: TRY003
            "Cannot convert cache type to destination configuration. Cache type not supported. ",
            f"Supported cache types: {list(conversion_fn_map.keys())}",
        )

    conversion_fn = conversion_fn_map[cache_class_name]
    return conversion_fn(cache)


def get_duckdb_destination_config(
    cache: DuckDBCache,
) -> dict[str, str]:
    """Get the destination configuration from the DuckDB cache."""
    return DestinationDuckdb(
        destination_path=cache.db_path,
        schema=cache.schema_name,
    ).to_dict()


def get_motherduck_destination_config(
    cache: MotherDuckCache,
) -> dict[str, str]:
    """Get the destination configuration from the DuckDB cache."""
    return DestinationDuckdb(
        destination_path=cache.db_path,
        schema=cache.schema_name,
        motherduck_api_key=cache.api_key,
    ).to_dict()


def get_postgres_destination_config(
    cache: PostgresCache,
) -> dict[str, str]:
    """Get the destination configuration from the Postgres cache."""
    return DestinationPostgres(
        database=cache.database,
        host=cache.host,
        password=cache.password,
        port=cache.port,
        schema=cache.schema_name,
        username=cache.username,
    ).to_dict()


def get_snowflake_destination_config(
    cache: SnowflakeCache,
) -> dict[str, str]:
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
    ).to_dict()


def get_bigquery_destination_config(
    cache: BigQueryCache,
) -> dict[str, str]:
    """Get the destination configuration from the BigQuery cache."""
    credentials_json: str | None = (
        Path(cache.credentials_path).read_text(encoding="utf-8") if cache.credentials_path else None
    )
    destination = DestinationBigquery(
        project_id=cache.project_name,
        dataset_id=cache.dataset_name,
        dataset_location="US",
        credentials_json=credentials_json,
        loading_method=StandardInserts,
    )
    return destination.to_dict()


def create_bigquery_cache(
    destination_configuration: DestinationBigquery,
) -> BigQueryCache:
    """Create a new BigQuery cache from the destination configuration."""
    credentials_path = get_secret("BIGQUERY_CREDENTIALS_PATH")
    return BigQueryCache(
        project_name=destination_configuration.project_id,
        dataset_name=destination_configuration.dataset_id,
        credentials_path=credentials_path,
    )


def create_duckdb_cache(
    destination_configuration: DestinationDuckdb,
) -> DuckDBCache:
    """Create a new DuckDB cache from the destination configuration."""
    return DuckDBCache(
        db_path=destination_configuration.destination_path,
        schema_name=destination_configuration.schema,
    )


def create_motherduck_cache(
    destination_configuration: DestinationDuckdb,
) -> MotherDuckCache:
    """Create a new DuckDB cache from the destination configuration."""
    return MotherDuckCache(
        database=destination_configuration.destination_path,
        schema_name=destination_configuration.schema,
        api_key=destination_configuration.motherduck_api_key,
    )


def create_postgres_cache(
    destination_configuration: DestinationPostgres,
) -> PostgresCache:
    """Create a new Postgres cache from the destination configuration."""
    port: int = int(destination_configuration.port) if "port" in destination_configuration else 5432
    return PostgresCache(
        database=destination_configuration.database,
        host=destination_configuration.host,
        password=destination_configuration.password,
        port=port,
        schema_name=destination_configuration.schema,
        username=destination_configuration.username,
    )


def create_snowflake_cache(
    destination_configuration: DestinationSnowflake,
    password_secret_name: str = SNOWFLAKE_PASSWORD_SECRET_NAME,
) -> SnowflakeCache:
    """Create a new Snowflake cache from the destination configuration."""
    return SnowflakeCache(
        account=destination_configuration.host.split(".snowflakecomputing")[0],
        database=destination_configuration.database,
        schema_name=destination_configuration.schema,
        warehouse=destination_configuration.warehouse,
        role=destination_configuration.role,
        username=destination_configuration.username,
        password=get_secret(password_secret_name),
    )


def create_cache_from_destination_config(
    destination_configuration: DestinationBigquery
    | DestinationDuckdb
    | DestinationPostgres
    | DestinationSnowflake,
) -> CacheBase:
    """Create a new cache from the destination."""
    conversion_fn_map: dict[str, Callable[[dict[str, str]], CacheBase]] = {
        "DestinationBigquery": create_bigquery_cache,
        "DestinationDuckdb": create_duckdb_cache,
        "DestinationPostgres": create_postgres_cache,
        "DestinationSnowflake": create_snowflake_cache,
    }
    destination_class_name = type(destination_configuration).__name__
    if destination_class_name not in conversion_fn_map:
        raise ValueError(  # noqa: TRY003
            "Cannot convert destination configuration to cache. Destination type not supported. ",
            f"Supported destination types: {list(conversion_fn_map.keys())}",
        )

    conversion_fn = conversion_fn_map[destination_class_name]
    return conversion_fn(destination_configuration)
