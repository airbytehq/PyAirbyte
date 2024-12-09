# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Fixtures for integration tests."""

from __future__ import annotations

from contextlib import suppress
from typing import Any, Generator

import airbyte as ab
import pytest
from airbyte._util import meta, text_util
from airbyte._util.temp_files import as_temp_files
from airbyte.caches.base import CacheBase
from airbyte.caches.bigquery import BigQueryCache
from airbyte.caches.motherduck import MotherDuckCache
from airbyte.caches.snowflake import SnowflakeCache
from airbyte.destinations.base import Destination
from airbyte.secrets import GoogleGSMSecretManager, SecretHandle
from sqlalchemy import create_engine, text

AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"


@pytest.fixture(scope="session")
def ci_secret_manager() -> GoogleGSMSecretManager:
    secret = ab.get_secret("GCP_GSM_CREDENTIALS")
    if not secret or secret.is_empty():
        pytest.skip("GCP_GSM_CREDENTIALS secret not found.")

    return GoogleGSMSecretManager(
        project=AIRBYTE_INTERNAL_GCP_PROJECT,
        credentials_json=ab.get_secret("GCP_GSM_CREDENTIALS"),
    )


def get_connector_config(self, connector_name: str, index: int = 0) -> dict | None:
    """Retrieve the connector configuration from GSM."""
    gsm_secrets_manager = GoogleGSMSecretManager(
        project=AIRBYTE_INTERNAL_GCP_PROJECT,
        credentials_json=ab.get_secret("GCP_GSM_CREDENTIALS"),
    )
    first_secret: SecretHandle = next(
        gsm_secrets_manager.fetch_connector_secrets(
            connector_name=connector_name,
        ),
        None,
    )

    print(f"Found '{connector_name}' credential secret '${first_secret.secret_name}'.")
    return first_secret.get_value().parse_json()


@pytest.fixture(scope="session")
def motherduck_secrets(ci_secret_manager: GoogleGSMSecretManager) -> dict:
    return ci_secret_manager.get_secret(
        "SECRET_DESTINATION_DUCKDB__MOTHERDUCK__CREDS",
    ).parse_json()


@pytest.fixture
def new_motherduck_cache(
    motherduck_secrets,
) -> MotherDuckCache:
    return MotherDuckCache(
        database="my_db",  # TODO: Use a dedicated DB for testing
        schema_name=f"test_deleteme_{text_util.generate_random_suffix()}",
        api_key=motherduck_secrets["motherduck_api_key"],
    )


@pytest.fixture(scope="session")
def new_snowflake_destination_config(ci_secret_manager: GoogleGSMSecretManager) -> dict:
    config = ci_secret_manager.get_secret(
        "AIRBYTE_LIB_SNOWFLAKE_CREDS",
    ).parse_json()
    config["schema"] = f"test_deleteme_{text_util.generate_random_suffix()}"
    return config


@pytest.fixture
def new_snowflake_cache(
    new_snowflake_destination_config: dict[str, Any],
) -> Generator[SnowflakeCache, Any, None]:
    cache = SnowflakeCache(
        account=new_snowflake_destination_config["account"],
        username=new_snowflake_destination_config["username"],
        password=new_snowflake_destination_config["password"],
        database=new_snowflake_destination_config["database"],
        warehouse=new_snowflake_destination_config["warehouse"],
        role=new_snowflake_destination_config["role"],
        schema_name=new_snowflake_destination_config["schema"],
    )
    sqlalchemy_url = cache.get_sql_alchemy_url()

    yield cache

    engine = create_engine(
        sqlalchemy_url,
        future=True,
    )
    with engine.connect() as connection:
        connection.execute(
            text(f"DROP SCHEMA IF EXISTS {cache.schema_name}"),
        )


@pytest.fixture
def new_bigquery_destination_config(
    ci_secret_manager: GoogleGSMSecretManager,
) -> dict[str, Any]:
    dest_bigquery_config = ci_secret_manager.get_secret(
        "SECRET_DESTINATION-BIGQUERY_CREDENTIALS__CREDS"
    ).parse_json()
    dest_bigquery_config["dataset_id"] = (
        f"test_deleteme_{text_util.generate_random_suffix()}"
    )
    return dest_bigquery_config


@pytest.fixture
def new_bigquery_destination(
    new_bigquery_destination_config: dict[str, Any],
) -> Destination:
    dest_config = new_bigquery_destination_config.copy()
    _ = dest_config.pop("destinationType", None)
    return ab.get_destination(
        "destination-bigquery",
        config=dest_config,
        install_if_missing=False,
    )


@pytest.fixture
def new_bigquery_cache(
    new_bigquery_destination_config: dict[str, Any],
) -> Generator[BigQueryCache, Any, None]:
    credentials_json = new_bigquery_destination_config["credentials_json"]
    with as_temp_files([credentials_json]) as (credentials_path,):
        cache = BigQueryCache(
            credentials_path=credentials_path,
            project_name=new_bigquery_destination_config["project_id"],
            dataset_name=new_bigquery_destination_config["dataset_id"],
        )
        yield cache

        url = cache.get_sql_alchemy_url()
        engine = create_engine(
            url,
            future=True,
        )
        with suppress(Exception):
            with engine.begin() as connection:
                connection.execute(text(f"DROP SCHEMA IF EXISTS {cache.schema_name}"))


@pytest.fixture(scope="function")
def new_generic_cache(request) -> CacheBase:
    """This is a placeholder fixture that will be overridden by pytest_generate_tests()."""
    return request.getfixturevalue(request.param)


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Override default pytest behavior, parameterizing our tests based on the available cache types.

    This is useful for running the same tests with different cache types, to ensure that the tests
    can pass across all cache types.
    """
    all_cache_type_fixtures: dict[str, str] = {
        # Ordered by priority (fastest first)
        "DuckDB": "new_duckdb_cache",
        "Postgres": "new_postgres_cache",
        "BigQuery": "new_bigquery_cache",
        "Snowflake": "new_snowflake_cache",
    }
    if meta.is_windows():
        # Postgres tests require Linux containers
        all_cache_type_fixtures.pop("Postgres")

    if "new_generic_cache" in metafunc.fixturenames:
        metafunc.parametrize(
            "new_generic_cache",
            all_cache_type_fixtures.values(),
            ids=all_cache_type_fixtures.keys(),
            indirect=True,
            scope="function",
        )
