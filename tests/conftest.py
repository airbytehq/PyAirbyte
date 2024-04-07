# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Global pytest fixtures."""
from __future__ import annotations

from contextlib import suppress
import json
import logging
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
from ci_credentials import RemoteSecret, get_connector_secrets
from requests.exceptions import HTTPError

import ulid
from airbyte._util.google_secrets import get_gcp_secret
from airbyte._util.meta import is_windows
from airbyte.caches.base import CacheBase
from airbyte.caches.bigquery import BigQueryCache
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.motherduck import MotherDuckCache
from airbyte.caches.snowflake import SnowflakeCache

import docker
import psycopg2 as psycopg
import pytest
from _pytest.nodes import Item
from sqlalchemy import create_engine

from airbyte.caches import PostgresCache
from airbyte._executor import _get_bin_dir
from airbyte.caches.util import new_local_cache
from airbyte.secrets import CustomSecretManager
from airbyte.sources.base import as_temp_files

import airbyte as ab

logger = logging.getLogger(__name__)


PYTEST_POSTGRES_IMAGE = "postgres:13"
PYTEST_POSTGRES_CONTAINER = "postgres_pytest_container"
PYTEST_POSTGRES_PORT = 5432

LOCAL_TEST_REGISTRY_URL = "./tests/integration_tests/fixtures/registry.json"

AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"


def get_ci_secret(
    secret_name,
    project_name: str = AIRBYTE_INTERNAL_GCP_PROJECT,
) -> str:
    return get_gcp_secret(project_name=project_name, secret_name=secret_name)


def get_ci_secret_json(
    secret_name,
    project_name: str = AIRBYTE_INTERNAL_GCP_PROJECT,
) -> dict:
    return json.loads(get_ci_secret(secret_name=secret_name, project_name=project_name))


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


    def get_connector_config(self, connector_name: str, index: int = 0) -> dict | None:
        assert connector_name is not None and connector_name != "all", \
            "We can only retrieve one connector config at a time."

        gcp_gsm_credentials = ab.get_secret("GCP_GSM_CREDENTIALS")
        secrets: list[RemoteSecret] = []
        secrets, _ = get_connector_secrets(
            connector_name=connector_name,
            gcp_gsm_credentials=gcp_gsm_credentials,
            disable_masking=True,
        )

        if len(secrets) > 1:
            print(
                f"Found {len(secrets)} secrets for connector '{connector_name}'."
            )
        else:
            print(
                f"Found '{connector_name}' credentials."
            )

        if index >= len(secrets):
            raise IndexError(f"Index {index} is out of range for connector '{connector_name}'.")

        return secrets[index].value_dict


@pytest.fixture(autouse=True, scope="session")
def airbyte_integration_test_secrets_manager() -> AirbyteIntegrationTestSecretManager:
    """Create a new instance of the custom secret manager."""

    return AirbyteIntegrationTestSecretManager()


def pytest_collection_modifyitems(items: list[Item]) -> None:
    """Override default pytest behavior, sorting our tests in a sensible execution order.

    In general, we want faster tests to run first, so that we can get feedback faster.

    Running lint tests first is helpful because they are fast and can catch typos and other errors.

    Otherwise tests are run based on an alpha-based natural sort, where 'unit' tests run after
    'integration' tests because 'u' comes after 'i' alphabetically.
    """
    def test_priority(item: Item) -> int:
        if item.get_closest_marker(name="slow"):
            return 9  # slow tests have the lowest priority
        elif "lint_tests" in str(item.fspath):
            return 1  # lint tests have high priority
        elif "unit_tests" in str(item.fspath):
            return 2  # unit tests have highest priority
        elif "docs_tests" in str(item.fspath):
            return 3  # doc tests have medium priority
        elif "integration_tests" in str(item.fspath):
            return 4  # integration tests have the lowest priority
        else:
            return 5  # all other tests have lower priority

    # Sort the items list in-place based on the test_priority function
    items.sort(key=test_priority)

    for item in items:
        # Skip tests that require Docker if Docker is not available (including on Windows).
        if "new_postgres_cache" in item.fixturenames or "postgres_cache" in item.fixturenames:
            if True or not is_docker_available():
                item.add_marker(pytest.mark.skip(reason="Skipping tests (Docker not available)"))

        # Every test in the cloud directory is slow abd requires credentials
        if "integration_tests/cloud" in str(item.fspath):
            item.add_marker(pytest.mark.slow)
            item.add_marker(pytest.mark.requires_creds)


def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


@pytest.fixture(scope="session", autouse=True)
def remove_postgres_container():
    client = docker.from_env()
    if is_port_in_use(PYTEST_POSTGRES_PORT):
        try:
            container = client.containers.get(
                PYTEST_POSTGRES_CONTAINER,
            )
            container.stop()
            container.remove()
        except docker.errors.NotFound:
            pass  # Container not found, nothing to do.


def test_pg_connection(host) -> bool:
    pg_url = f"postgresql://postgres:postgres@{host}:{PYTEST_POSTGRES_PORT}/postgres"

    max_attempts = 120
    for attempt in range(max_attempts):
        try:
            conn = psycopg.connect(pg_url)
            conn.close()
            return True
        except psycopg.OperationalError:
            logger.info(f"Waiting for postgres to start (attempt {attempt + 1}/{max_attempts})")
            time.sleep(1.0)

    else:
        return False


def is_docker_available():
    if is_windows():
        # Linux containers are not supported on Windows CI runners
        return False
    try:
        _ = docker.from_env()
        return True
    except docker.errors.DockerException:
        return False


@pytest.fixture(scope="session")
def new_postgres_cache():
    """Fixture to return a fresh Postgres cache.

    Each test that uses this fixture will get a unique table prefix.
    """
    client = docker.from_env()
    try:
        client.images.get(PYTEST_POSTGRES_IMAGE)
    except (docker.errors.ImageNotFound, HTTPError):
        # Pull the image if it doesn't exist, to avoid failing our sleep timer
        # if the image needs to download on-demand.
        client.images.pull(PYTEST_POSTGRES_IMAGE)

    try:
        previous_container = client.containers.get(PYTEST_POSTGRES_CONTAINER)
        previous_container.remove()
    except docker.errors.NotFound:
        pass

    postgres_is_running = False
    postgres = client.containers.run(
        image=PYTEST_POSTGRES_IMAGE,
        name=PYTEST_POSTGRES_CONTAINER,
        environment={"POSTGRES_USER": "postgres", "POSTGRES_PASSWORD": "postgres", "POSTGRES_DB": "postgres"},
        ports={"5432/tcp": PYTEST_POSTGRES_PORT},
        detach=True,
    )

    attempts = 10
    while not postgres_is_running and attempts > 0:
        try:
            postgres.reload()
            postgres_is_running = postgres.status == "running"
        except docker.errors.NotFound:
            attempts -= 1
            time.sleep(3)
    if not postgres_is_running:
        raise Exception(f"Failed to start the PostgreSQL container. Status: {postgres.status}.")

    final_host = None
    if host := os.environ.get("DOCKER_HOST_NAME"):
        final_host = host if test_pg_connection(host) else None
    else:
    # Try to connect to the database using localhost and the docker host IP
        for host in ["127.0.0.1", "localhost", "host.docker.internal", "172.17.0.1"]:
            if test_pg_connection(host):
                final_host = host
                break

    if final_host is None:
        raise Exception(f"Failed to connect to the PostgreSQL database on host {host}.")

    config = PostgresCache(
        host=final_host,
        port=PYTEST_POSTGRES_PORT,
        username="postgres",
        password="postgres",
        database="postgres",
        schema_name="public",

        # TODO: Move this to schema name when we support it (breaks as of 2024-01-31):
        table_prefix=f"test{str(ulid.ULID())[-6:]}_",
    )
    yield config

    # Stop and remove the container after the tests are done
    postgres.stop()
    postgres.remove()


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
def new_snowflake_cache():
    secret = get_ci_secret_json(
        "AIRBYTE_LIB_SNOWFLAKE_CREDS",
    )
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


@pytest.fixture(autouse=True, scope="session")
def with_bigquery_credentials_path_env_var():
    dest_bigquery_config = get_ci_secret_json(
        secret_name="SECRET_DESTINATION-BIGQUERY_CREDENTIALS__CREDS"
    )
    credentials_json = dest_bigquery_config["credentials_json"]

    with as_temp_files([credentials_json]) as (credentials_path,):
        os.environ["BIGQUERY_CREDENTIALS_PATH"] = credentials_path

        yield

    return


@pytest.fixture
@pytest.mark.requires_creds
def new_bigquery_cache():
    dest_bigquery_config = get_ci_secret_json(
        "SECRET_DESTINATION-BIGQUERY_CREDENTIALS__CREDS"
    )

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


@pytest.fixture(autouse=True)
def source_test_registry(monkeypatch):
    """
    Set environment variables for the test source.

    These are applied to this test file only.

    This means the normal registry is not usable. Expect AirbyteConnectorNotRegisteredError for
    other connectors.
    """
    env_vars = {
        "AIRBYTE_LOCAL_REGISTRY": LOCAL_TEST_REGISTRY_URL,
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)


@pytest.fixture(autouse=True)
def do_not_track(monkeypatch):
    """
    Set environment variables for the test source.

    These are applied to this test file only.
    """
    env_vars = {
        "DO_NOT_TRACK": "true"
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)


@pytest.fixture(scope="package")
def source_test_installation():
    """
    Prepare test environment. This will pre-install the test source from the fixtures array and set
    the environment variable to use the local json file as registry.
    """
    venv_dir = ".venv-source-test"
    if os.path.exists(venv_dir):
        shutil.rmtree(venv_dir)

    subprocess.run(["python", "-m", "venv", venv_dir], check=True)
    pip_path = str(_get_bin_dir(Path(venv_dir)) / "pip")
    subprocess.run([pip_path, "install", "-e", "./tests/integration_tests/fixtures/source-test"], check=True)

    yield

    shutil.rmtree(venv_dir)


@pytest.fixture(scope="function")
def new_duckdb_cache() -> DuckDBCache:
    return new_local_cache()


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
    if is_windows():
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
