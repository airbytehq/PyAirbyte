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
from airbyte.secrets import CustomSecretManager, GoogleGSMSecretManager, SecretHandle
from airbyte._util.temp_files import as_temp_files

import airbyte as ab

AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"


@pytest.mark.requires_creds
@pytest.fixture(scope="session")
def ci_secret_manager() -> GoogleGSMSecretManager:
    return GoogleGSMSecretManager(
        project=AIRBYTE_INTERNAL_GCP_PROJECT,
        credentials_json=ab.get_secret("GCP_GSM_CREDENTIALS"),
    )


def get_connector_config(self, connector_name: str, index: int = 0) -> dict | None:
    """Retrieve the connector configuration from GSM."""
    gcp_gsm_credentials = ab.get_secret("GCP_GSM_CREDENTIALS")
    gsm_secrets_manager = GoogleGSMSecretManager(
        project=AIRBYTE_INTERNAL_GCP_PROJECT,
        credentials_json=ab.get_secret("GCP_GSM_CREDENTIALS"),
    )
    first_secret: SecretHandle = next(gsm_secrets_manager.fetch_secrets(
        # https://cloud.google.com/secret-manager/docs/filtering
        filter_string=f"labels.connector={connector_name}"
    ), None)

    print(
        f"Found '{connector_name}' credential secret ${first_secret.secret_name}."
    )
    return first_secret.get_value().parse_json()


class AirbyteIntegrationTestSecretManager(CustomSecretManager):
    """Custom secret manager for Airbyte integration tests.

    This class is used to auto-retrieve needed secrets from GSM.
    """
    auto_register = True
    replace_existing = False
    as_backup = True

    def get_secret(
        self,
        secret_name: str,
        *,
        required: bool = False,
    ) -> str | None:
        """This method attempts to find matching properties within the integration test config.

        If `required` is `True`, this method will raise an exception if the secret is not found.
        Otherwise, it will return None.
        """
        system_name = secret_name.split("_")[0].lower()
        property_name = "_".join(secret_name.split("_")[1:]).lower()

        mapping = {
            "snowflake": "destination-snowflake",
            "bigquery": "destination-bigquery",
            "postgres": "destination-postgres",
            "duckdb": "destination-duckdb",
        }
        if system_name not in mapping:
            return None

        connector_name = mapping[system_name]
        connector_config = self.get_connector_config(connector_name)
        if "credentials" in connector_config:
            if property_name in connector_config["credentials"]:
                return connector_config["credentials"][property_name]

        if property_name in connector_config:
            return connector_config[property_name]

        if not required:
            return None

        raise KeyError(
            f"Property '{property_name}' not found in '{connector_name}' connector config. "
            f"\nAvailable config keys: {', '.join(connector_config.keys())} "
            f"\nAvailable 'credential' keys: {', '.join(connector_config.get('credentials', {}).keys())} "
        )


@pytest.fixture(autouse=True, scope="session")
def airbyte_integration_test_secrets_manager() -> AirbyteIntegrationTestSecretManager:
    """Create a new instance of the custom secret manager."""

    return AirbyteIntegrationTestSecretManager()


@pytest.fixture
def new_motherduck_cache(
    airbyte_integration_test_secrets_manager: AirbyteIntegrationTestSecretManager,
) -> MotherDuckCache:
    config = airbyte_integration_test_secrets_manager.get_connector_config(
        connector_name="destination-duckdb",
    )
    return MotherDuckCache(
        database="integration_tests_deleteany",
        schema_name=f"test_deleteme_{str(ulid.ULID()).lower()[-6:]}",
        api_key=config["motherduck_api_key"],
    )


@pytest.fixture
def new_snowflake_cache(ci_secret_manager: GoogleGSMSecretManager):
    secret = ci_secret_manager.get_secret(
        "AIRBYTE_LIB_SNOWFLAKE_CREDS",
    ).parse_json()

    config = SnowflakeCache(
        account=secret["account"],
        username=secret["username"],
        password=secret["password"],
        database=secret["database"],
        warehouse=secret["warehouse"],
        role=secret["role"],
        schema_name=f"test{str(ulid.ULID()).lower()[-6:]}",
    )
    sqlalchemy_url = config.get_sql_alchemy_url()

    yield config

    engine = create_engine(config.get_sql_alchemy_url())
    with engine.begin() as connection:
        connection.execute(f"DROP SCHEMA IF EXISTS {config.schema_name}")


@pytest.fixture
@pytest.mark.requires_creds
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


@pytest.mark.requires_creds
@pytest.fixture(autouse=True, scope="session")
def bigquery_credentials_file(ci_secret_manager: GoogleGSMSecretManager):
    dest_bigquery_config = ci_secret_manager.get_secret(
        secret_name="SECRET_DESTINATION-BIGQUERY_CREDENTIALS__CREDS"
    ).parse_json()

    credentials_json = dest_bigquery_config["credentials_json"]
    with as_temp_files([credentials_json]) as (credentials_path,):
        os.environ["BIGQUERY_CREDENTIALS_PATH"] = credentials_path

        yield

    return


@pytest.fixture(scope="function")
def new_motherduck_cache() -> MotherDuckCache:
    return MotherDuckCache(
        api_key=ab.get_secret("MOTHERDUCK_API_KEY"),
        schema_name=f"test{str(ulid.ULID()).lower()[-6:]}",
        database="integration_tests_deleteany",
    )


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
