# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Cloud destinations for Airbyte."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airbyte_api.models import (
    DestinationBigquery,
    DestinationDuckdb,
    DestinationPostgres,
    DestinationSnowflake,
)

from airbyte.caches.base import CacheBase
from airbyte.caches.bigquery import BigQueryCache
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.motherduck import MotherDuckCache
from airbyte.caches.postgres import PostgresCache
from airbyte.caches.snowflake import SnowflakeCache
from airbyte.secrets import get_secret
from airbyte.secrets.base import SecretString


if TYPE_CHECKING:
    from collections.abc import Callable

    from airbyte.caches.base import CacheBase


SNOWFLAKE_PASSWORD_SECRET_NAME = "SNOWFLAKE_PASSWORD"

DestinationConfiguration = (
    DestinationBigquery | DestinationDuckdb | DestinationPostgres | DestinationSnowflake
)


def destination_to_cache(
    destination_configuration: DestinationConfiguration | dict[str, Any],
) -> CacheBase:
    """Get the destination configuration from the cache."""
    conversion_fn_map: dict[str, Callable[[Any], CacheBase]] = {
        "bigquery": biqquery_destination_to_cache,
        "duckdb": duckdb_destination_to_cache,
        "motherduck": motherduck_destination_to_cache,
        "postgres": postgres_destination_to_cache,
        "snowflake": snowflake_destination_to_cache,
    }
    if isinstance(destination_configuration, dict):
        try:
            destination_type = (
                destination_configuration.get("DESTINATION_TYPE")
                or destination_configuration["destinationType"]
            )
            if hasattr(destination_configuration, "value"):
                destination_type = destination_type.value
            else:
                destination_type = str(destination_type)
        except KeyError as ex:
            raise ValueError(
                f"Missing 'destinationType' in keys {list(destination_configuration.keys())}."
            ) from ex
    else:
        destination_type = str(destination_configuration.DESTINATION_TYPE)

    conversion_fn = conversion_fn_map[destination_type]
    return conversion_fn(destination_configuration)


def biqquery_destination_to_cache(
    destination_configuration: DestinationBigquery | dict[str, Any],
) -> BigQueryCache:
    """Create a new BigQuery cache from the destination configuration."""
    credentials_path = get_secret("BIGQUERY_CREDENTIALS_PATH")
    if isinstance(destination_configuration, dict):
        destination_configuration = DestinationBigquery(**destination_configuration)

    return BigQueryCache(
        project_name=destination_configuration.project_id,
        dataset_name=destination_configuration.dataset_id,
        credentials_path=credentials_path,
    )


def duckdb_destination_to_cache(
    destination_configuration: DestinationDuckdb,
) -> DuckDBCache:
    """Create a new DuckDB cache from the destination configuration."""
    return DuckDBCache(
        db_path=destination_configuration.destination_path,
        schema_name=destination_configuration.schema or "main",
    )


def motherduck_destination_to_cache(
    destination_configuration: DestinationDuckdb,
) -> MotherDuckCache:
    """Create a new DuckDB cache from the destination configuration."""
    if not destination_configuration.motherduck_api_key:
        raise ValueError("MotherDuck API key is required for MotherDuck cache.")

    return MotherDuckCache(
        database=destination_configuration.destination_path,
        schema_name=destination_configuration.schema or "main",
        api_key=SecretString(destination_configuration.motherduck_api_key),
    )


def postgres_destination_to_cache(
    destination_configuration: DestinationPostgres,
) -> PostgresCache:
    """Create a new Postgres cache from the destination configuration."""
    port: int = int(destination_configuration.port) if destination_configuration.port else 5432
    if not destination_configuration.password:
        raise ValueError("Password is required for Postgres cache.")

    return PostgresCache(
        database=destination_configuration.database,
        host=destination_configuration.host,
        password=destination_configuration.password,
        port=port,
        schema_name=destination_configuration.schema or "public",
        username=destination_configuration.username,
    )


def snowflake_destination_to_cache(
    destination_configuration: DestinationSnowflake | dict[str, Any],
    password_secret_name: str = SNOWFLAKE_PASSWORD_SECRET_NAME,
) -> SnowflakeCache:
    """Create a new Snowflake cache from the destination configuration."""
    if isinstance(destination_configuration, dict):
        destination_configuration = DestinationSnowflake(**destination_configuration)

    return SnowflakeCache(
        account=destination_configuration.host.split(".snowflakecomputing")[0],
        database=destination_configuration.database,
        schema_name=destination_configuration.schema,
        warehouse=destination_configuration.warehouse,
        role=destination_configuration.role,
        username=destination_configuration.username,
        password=get_secret(password_secret_name),
    )
