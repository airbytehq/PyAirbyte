# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Fixtures for integration tests."""

from __future__ import annotations
from contextlib import suppress
import os

import pytest
import ulid
from sqlalchemy import create_engine

from airbyte._util import meta
from airbyte.caches.base import CacheBase
from airbyte.caches.bigquery import BigQueryCache
from airbyte.caches.motherduck import MotherDuckCache
from airbyte.caches.snowflake import SnowflakeCache
from airbyte.secrets import GoogleGSMSecretManager, SecretHandle
from airbyte._util.temp_files import as_temp_files

import airbyte as ab


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
        database="integration_tests_deleteany",
        schema_name=f"test_deleteme_{str(ulid.ULID()).lower()[-6:]}",
        api_key=motherduck_secrets["motherduck_api_key"],
    )


@pytest.fixture(scope="session")
def snowflake_creds(ci_secret_manager: GoogleGSMSecretManager) -> dict:
    return ci_secret_manager.get_secret(
        "AIRBYTE_LIB_SNOWFLAKE_CREDS",
    ).parse_json()


@pytest.fixture
def new_snowflake_cache(snowflake_creds: dict):
    config = SnowflakeCache(
        account=snowflake_creds["account"],
        username=snowflake_creds["username"],
        password=snowflake_creds["password"],
        database=snowflake_creds["database"],
        warehouse=snowflake_creds["warehouse"],
        role=snowflake_creds["role"],
        schema_name=f"test{str(ulid.ULID()).lower()[-6:]}",
    )
    sqlalchemy_url = config.get_sql_alchemy_url()

    yield config

    engine = create_engine(config.get_sql_alchemy_url())
    with engine.begin() as connection:
        connection.execute(f"DROP SCHEMA IF EXISTS {config.schema_name}")


@pytest.fixture
def new_bigquery_cache(ci_secret_manager: GoogleGSMSecretManager):
    dest_bigquery_config = ci_secret_manager.get_secret(
        "SECRET_DESTINATION-BIGQUERY_CREDENTIALS__CREDS"
    ).parse_json()

    dataset_name = f"test_deleteme_{str(ulid.ULID()).lower()[-6:]}"
    credentials_json = dest_bigquery_config["credentials_json"]
    with as_temp_files([credentials_json]) as (credentials_path,):
        cache = BigQueryCache(
            credentials_path=credentials_path,
            project_name=dest_bigquery_config["project_id"],
            dataset_name=dataset_name,
        )
        yield cache

        url = cache.get_sql_alchemy_url()
        engine = create_engine(url)
        with suppress(Exception):
            with engine.begin() as connection:
                connection.execute(f"DROP SCHEMA IF EXISTS {cache.schema_name}")


@pytest.fixture(autouse=True, scope="session")
def bigquery_credentials_file(ci_secret_manager: GoogleGSMSecretManager):
    dest_bigquery_config = ci_secret_manager.get_secret(
        secret_name="SECRET_DESTINATION-BIGQUERY_CREDENTIALS__CREDS"
    ).parse_json()

    credentials_json = dest_bigquery_config["credentials_json"]
    with as_temp_files(files_contents=[credentials_json]) as (credentials_path,):
        os.environ["BIGQUERY_CREDENTIALS_PATH"] = credentials_path

        yield

    return


@pytest.fixture(autouse=True, scope="session")
def with_snowflake_password_env_var(snowflake_creds: dict):
    os.environ["SNOWFLAKE_PASSWORD"] = snowflake_creds["password"]

    yield

    return


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
