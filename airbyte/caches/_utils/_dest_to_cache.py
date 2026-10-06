# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Cloud destinations for Airbyte."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from airbyte.caches.base import CacheBase
from airbyte.caches.bigquery import BigQueryCache
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.motherduck import MotherDuckCache
from airbyte.caches.postgres import PostgresCache
from airbyte.caches.snowflake import SnowflakeCache
from airbyte.exceptions import AirbyteLibInputError, AirbyteLibSecretNotFoundError
from airbyte.secrets import get_secret
from airbyte.secrets.base import SecretString


if TYPE_CHECKING:
    from collections.abc import Callable

    from airbyte.caches.base import CacheBase


SNOWFLAKE_PASSWORD_SECRET_NAME = "SNOWFLAKE_PASSWORD"


_SUPPORTED_DESTINATION_TYPES: set[str] = {
    "bigquery",
    "duckdb",
    "motherduck",
    "postgres",
    "snowflake",
}


def get_supported_destination_types() -> set[str]:
    """Return the set of destination type identifiers that have cache support."""
    return _SUPPORTED_DESTINATION_TYPES


def _required_key(
    destination_configuration: dict[str, Any],
    key: str,
) -> Any:  # noqa: ANN401
    """Read a required key from a destination configuration dict."""
    value = destination_configuration.get(key)
    if value is None:
        raise AirbyteLibInputError(
            message=f"Destination configuration is missing required key '{key}'.",
            context={"destination_configuration": destination_configuration},
        )
    return value


def destination_to_cache(
    destination_configuration: dict[str, Any] | BaseModel,
    *,
    schema_name: str | None = None,
) -> CacheBase:
    """Get the destination configuration from the cache."""
    conversion_fn_map: dict[str, Callable[[dict[str, Any]], CacheBase]] = {
        "bigquery": bigquery_destination_to_cache,
        "duckdb": duckdb_destination_to_cache,
        "motherduck": motherduck_destination_to_cache,
        "postgres": postgres_destination_to_cache,
        "snowflake": snowflake_destination_to_cache,
    }
    if isinstance(destination_configuration, BaseModel):
        destination_configuration = destination_configuration.model_dump(mode="json", by_alias=True)

    if isinstance(destination_configuration, dict):
        destination_type = destination_configuration.get(
            "DESTINATION_TYPE"
        ) or destination_configuration.get("destinationType")
        if destination_type is None:
            raise AirbyteLibInputError(
                message=(
                    "Missing 'destinationType' in keys "
                    f"{list(destination_configuration.keys())}."
                ),
            ) from None
        if hasattr(destination_type, "value"):
            destination_type = destination_type.value
        elif hasattr(destination_type, "_value_"):
            destination_type = destination_type._value_
        else:
            destination_type = str(destination_type)
    else:
        raise AirbyteLibInputError(
            message=(
                "Destination configuration must be a dict or a model, "
                f"got {type(destination_configuration).__name__}."
            ),
        )

    if destination_type not in conversion_fn_map:
        raise AirbyteLibInputError(
            message=(
                "Cannot convert destination to a cache configuration. "
                f"Destination type {destination_type} not supported. "
                f"Supported cache types: {list(conversion_fn_map.keys())}"
            ),
        )

    conversion_fn = conversion_fn_map[destination_type]
    cache = conversion_fn(destination_configuration)
    if schema_name is not None:
        cache.schema_name = schema_name
        # Force engine re-creation so the schema_translate_map picks up
        # the overridden schema_name (the engine is lazily cached during
        # __init__ with the original schema from the destination config).
        cache.processor.sql_config.dispose_engine()
    return cache


def bigquery_destination_to_cache(
    destination_configuration: dict[str, Any],
) -> BigQueryCache:
    """Create a new BigQuery cache from the destination configuration.

    We may have to inject credentials, because they are obfuscated when config
    is returned from the REST API.

    When the destination config contains a plaintext `credentials_json` field
    (the local `Destination.get_sql_cache()` path), the JSON is written to a
    temporary file and used directly.  Otherwise we fall back to the
    `BIGQUERY_CREDENTIALS_PATH` secret/env-var (the cloud API path).
    """
    raw_credentials_json: str | None = destination_configuration.get("credentials_json")

    if raw_credentials_json and "****" not in raw_credentials_json:
        # Plaintext credentials from a local destination config — write to
        # a temp file so BigQueryCache can use it as a credentials path.
        # The file is intentionally *not* deleted on close because the
        # cache needs the path to remain valid after this function returns.
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".json", prefix="bq_creds_")
        Path(tmp_path).write_text(raw_credentials_json, encoding="utf-8")
        # Close the file descriptor opened by mkstemp (write_text uses its own).
        os.close(tmp_fd)
        credentials_path = tmp_path
    else:
        credentials_path = get_secret("BIGQUERY_CREDENTIALS_PATH")

    return BigQueryCache(
        project_name=_required_key(destination_configuration, "project_id"),
        dataset_name=_required_key(destination_configuration, "dataset_id"),
        credentials_path=credentials_path,
        dataset_location=destination_configuration.get("dataset_location"),
    )


def duckdb_destination_to_cache(
    destination_configuration: dict[str, Any],
) -> DuckDBCache:
    """Create a new DuckDB cache from the destination configuration."""
    db_path = _required_key(destination_configuration, "destination_path")

    # The DuckDB destination Docker container mounts a host directory to
    # `/local` inside the container.  Paths written as `/local/foo.duckdb`
    # actually live at `<project_dir>/destination-duckdb/foo.duckdb` on the
    # host.  Resolve the host-side path so the cache can open the file.
    if db_path.startswith(("/local/", "/local\\")):
        from airbyte.constants import DEFAULT_PROJECT_DIR  # noqa: PLC0415

        host_path = str(DEFAULT_PROJECT_DIR / "destination-duckdb" / db_path[len("/local/") :])
        db_path = host_path

    return DuckDBCache(
        db_path=db_path,
        schema_name=destination_configuration.get("schema") or "main",
    )


def motherduck_destination_to_cache(
    destination_configuration: dict[str, Any],
) -> MotherDuckCache:
    """Create a new MotherDuck cache from the destination configuration."""
    if not destination_configuration.get("motherduck_api_key"):
        raise AirbyteLibInputError(message="MotherDuck API key is required for MotherDuck cache.")

    return MotherDuckCache(
        database=_required_key(destination_configuration, "destination_path"),
        schema_name=destination_configuration.get("schema") or "main",
        api_key=SecretString(destination_configuration["motherduck_api_key"]),
    )


def postgres_destination_to_cache(
    destination_configuration: dict[str, Any],
) -> PostgresCache:
    """Create a new Postgres cache from the destination configuration."""
    port: int = int(destination_configuration.get("port") or 5432)
    if not destination_configuration.get("password"):
        raise AirbyteLibInputError(message="Password is required for Postgres cache.")

    return PostgresCache(
        database=_required_key(destination_configuration, "database"),
        host=_required_key(destination_configuration, "host"),
        password=destination_configuration["password"],
        port=port,
        schema_name=destination_configuration.get("schema") or "public",
        username=_required_key(destination_configuration, "username"),
    )


def snowflake_destination_to_cache(
    destination_configuration: dict[str, Any],
    password_secret_name: str = SNOWFLAKE_PASSWORD_SECRET_NAME,
) -> SnowflakeCache:
    """Create a new Snowflake cache from the destination configuration.

    We may have to inject credentials, because they are obfuscated when config
    is returned from the REST API.
    """
    snowflake_password: str | None = None
    credentials = destination_configuration.get("credentials") or {}
    if isinstance(credentials, dict) and isinstance(credentials.get("password"), str):
        destination_password = str(credentials["password"])
        if "****" in destination_password:
            try:
                snowflake_password = get_secret(password_secret_name)
            except ValueError as ex:
                raise AirbyteLibSecretNotFoundError(
                    "Password is required for Snowflake cache, but it was not available."
                ) from ex
        else:
            # The password is a plaintext value (e.g. from a local
            # Destination's hydrated config).  Use it directly instead
            # of treating it as a secret name to look up.
            snowflake_password = destination_password
    else:
        snowflake_password = get_secret(password_secret_name)

    return SnowflakeCache(
        account=_required_key(destination_configuration, "host").split(".snowflakecomputing")[0],
        database=_required_key(destination_configuration, "database"),
        schema_name=_required_key(destination_configuration, "schema"),
        warehouse=_required_key(destination_configuration, "warehouse"),
        role=_required_key(destination_configuration, "role"),
        username=_required_key(destination_configuration, "username"),
        password=snowflake_password,
    )
