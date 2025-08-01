# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Global pytest fixtures."""

from __future__ import annotations

import logging
import os
import shutil
import socket
import sys
import time
import warnings
from pathlib import Path

import docker
import psycopg
import pytest
from _pytest.nodes import Item
from requests.exceptions import HTTPError

import airbyte
from airbyte._executors.util import get_connector_executor
from airbyte._util import text_util
from airbyte._util.meta import is_windows
from airbyte.caches import PostgresCache
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.util import new_local_cache


logger = logging.getLogger(__name__)


PYTEST_POSTGRES_IMAGE = "postgres:13"
PYTEST_POSTGRES_CONTAINER = "postgres_pytest_container"
PYTEST_POSTGRES_PORT = 5432

LOCAL_TEST_REGISTRY_URL = "./tests/integration_tests/fixtures/registry.json"


def pytest_configure(config):
    if sys.platform.startswith("win"):
        # Ignore specific warnings only on Windows
        warnings.filterwarnings(
            "ignore",
            category=pytest.PytestUnraisableExceptionWarning,
        )
        warnings.filterwarnings(
            "ignore",
            category=ResourceWarning,
            message="unclosed file",
        )


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
        if (
            "new_postgres_cache" in item.fixturenames
            or "postgres_cache" in item.fixturenames
            or "source_docker_faker_seed_a" in item.fixturenames
            or "source_docker_faker_seed_b" in item.fixturenames
            or "new_duckdb_destination_executor" in item.fixturenames
            or "e2e_test_destination" in item.fixturenames
        ):
            if not is_docker_available():
                item.add_marker(
                    pytest.mark.skip(reason="Skipping tests (Docker not available)")
                )

        # Every test in the cloud directory is slow abd requires credentials
        if "integration_tests/cloud" in str(item.fspath):
            item.add_marker(pytest.mark.slow)
            item.add_marker(pytest.mark.requires_creds)

        if "super_slow" in item.keywords:
            # Super slow tests are also slow
            item.add_marker("slow")


def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


@pytest.fixture(scope="session", autouse=True)
def remove_postgres_container():
    if is_port_in_use(PYTEST_POSTGRES_PORT):
        try:
            client = docker.from_env()
            container = client.containers.get(
                PYTEST_POSTGRES_CONTAINER,
            )
            container.stop()
            container.remove()
        except docker.errors.NotFound:
            pass  # Container not found, nothing to do.
        except docker.errors.DockerException:
            pass  # Docker not running, nothing to do.


def test_pg_connection(host) -> bool:
    pg_url = f"postgresql://postgres:postgres@{host}:{PYTEST_POSTGRES_PORT}/postgres"

    max_attempts = 120
    for attempt in range(max_attempts):
        try:
            conn = psycopg.connect(pg_url)
            conn.close()
            return True
        except psycopg.OperationalError:
            logger.info(
                f"Waiting for postgres to start (attempt {attempt + 1}/{max_attempts})"
            )
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
def new_postgres_db():
    """Fixture to start a new PostgreSQL container for testing.

    This fixture will start a new PostgreSQL container before the tests run and stop it after the
    tests are done. The host of the PostgreSQL database will be returned to the tests.
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
        environment={
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "postgres",
            "POSTGRES_DB": "postgres",
        },
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
        raise Exception(
            f"Failed to start the PostgreSQL container. Status: {postgres.status}."
        )

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

    yield final_host

    # Stop and remove the container after the tests are done
    postgres.stop()
    postgres.remove()


@pytest.fixture(scope="function")
def new_postgres_cache(new_postgres_db: str):
    """Fixture to return a fresh Postgres cache.

    Each test that uses this fixture will get a unique table prefix.
    """
    config = PostgresCache(
        host=new_postgres_db,
        port=PYTEST_POSTGRES_PORT,
        username="postgres",
        password="postgres",
        database="postgres",
        schema_name="public",
        # TODO: Move this to schema name when we support it (breaks as of 2024-01-31):
        table_prefix=f"test{text_util.generate_random_suffix()}_",
    )
    yield config


@pytest.fixture(autouse=False)
def source_test_registry(monkeypatch):
    """Mock the registry to return our custom registry containing the 'source-test' connector.

    This means the normal registry is not usable. Expect AirbyteConnectorNotRegisteredError for
    other connectors.
    """

    # Define the mock function
    def mock_get_registry_cache():
        return LOCAL_TEST_REGISTRY_URL

    # Replace _get_registry_url() with the mock function
    monkeypatch.setattr(
        airbyte.sources.registry, "_get_registry_url", mock_get_registry_cache
    )

    # reset the registry cache
    airbyte.sources.registry.__cache = None

    yield

    # reset the registry cache (clean up)
    airbyte.sources.registry.__cache = None


@pytest.fixture(autouse=True)
def do_not_track(monkeypatch):
    """Set environment variables for the test source.

    These are applied to this test file only.
    """
    env_vars = {"DO_NOT_TRACK": "true"}
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)


@pytest.fixture(
    scope="session", params=[True, False], ids=["uv_enabled", "uv_disabled"]
)
def source_test_installation(request):
    """Test fixture for sample source installation.

    Prepare test environment. This will pre-install the test source from the fixtures array and set
    the environment variable to use the local json file as registry.

    Parametrized to test both uv-enabled and uv-disabled installation methods.
    """
    use_uv = request.param

    if not use_uv:
        os.environ["AIRBYTE_NO_UV"] = "1"

    venv_dir = ".venv-source-test"
    if Path(venv_dir).exists():
        shutil.rmtree(venv_dir)

    try:
        executor = get_connector_executor(
            name="source-test",
            pip_url="./tests/integration_tests/fixtures/source-test",
            install_root=Path.cwd(),
            install_if_missing=True,
        )

        yield executor

    finally:
        if Path(venv_dir).exists():
            shutil.rmtree(venv_dir)
        if not use_uv and "AIRBYTE_NO_UV" in os.environ:
            del os.environ["AIRBYTE_NO_UV"]


@pytest.fixture(scope="function")
def new_duckdb_cache() -> DuckDBCache:
    return new_local_cache()
