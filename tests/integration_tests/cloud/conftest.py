# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Fixtures for Cloud Workspace integration tests."""
from __future__ import annotations

from enum import auto
import os
from pathlib import Path
import sys
import pytest
from airbyte._util.api_util import CLOUD_API_ROOT
from dotenv import dotenv_values
from airbyte._executor import _get_bin_dir
from airbyte.caches.base import CacheBase
from airbyte.cloud import CloudWorkspace


ENV_AIRBYTE_API_KEY = "AIRBYTE_CLOUD_API_KEY"
ENV_AIRBYTE_API_WORKSPACE_ID = "AIRBYTE_CLOUD_API_WORKSPACE_ID"
ENV_MOTHERDUCK_API_KEY = "MOTHERDUCK_API_KEY"


@pytest.fixture(autouse=True)
def add_venv_bin_to_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch the PATH to include the virtual environment's bin directory."""
    # Get the path to the bin directory of the virtual environment
    venv_bin_path = str(_get_bin_dir(Path(sys.prefix)))

    # Add the bin directory to the PATH
    new_path = f"{venv_bin_path}{os.pathsep}{os.environ['PATH']}"
    monkeypatch.setenv('PATH', new_path)


@pytest.fixture
def workspace_id() -> str:
    return os.environ[ENV_AIRBYTE_API_WORKSPACE_ID]


@pytest.fixture
def api_root() -> str:
    return CLOUD_API_ROOT


@pytest.fixture
def api_key() -> str:
    dotenv_vars: dict[str, str | None] = dotenv_values()
    if ENV_AIRBYTE_API_KEY in dotenv_vars:
        return dotenv_vars[ENV_AIRBYTE_API_KEY]

    if ENV_AIRBYTE_API_KEY not in os.environ:
        raise ValueError(f"Please set the '{ENV_AIRBYTE_API_KEY}' environment variable.")

    return os.environ[ENV_AIRBYTE_API_KEY]


@pytest.mark.requires_creds
@pytest.fixture(autouse=True, scope="session")
def bigquery_credentials_file():
    dest_bigquery_config = get_ci_secret_json(
        secret_name="SECRET_DESTINATION-BIGQUERY_CREDENTIALS__CREDS"
    )
    credentials_json = dest_bigquery_config["credentials_json"]

    with as_temp_files([credentials_json]) as (credentials_path,):
        os.environ["BIGQUERY_CREDENTIALS_PATH"] = credentials_path

        yield

    return


@pytest.fixture
def motherduck_api_key() -> str:
    dotenv_vars: dict[str, str | None] = dotenv_values()
    if ENV_MOTHERDUCK_API_KEY in dotenv_vars:
        return dotenv_vars[ENV_MOTHERDUCK_API_KEY]

    if ENV_MOTHERDUCK_API_KEY not in os.environ:
        raise ValueError(f"Please set the '{ENV_MOTHERDUCK_API_KEY}' environment variable.")

    return os.environ[ENV_MOTHERDUCK_API_KEY]


@pytest.fixture
def cloud_workspace(
    workspace_id: str,
    api_key: str,
    api_root: str,
) -> CloudWorkspace:
    return CloudWorkspace(
        workspace_id=workspace_id,
        api_key=api_key,
        api_root=api_root,
    )


@pytest.fixture
def workspace_id() -> str:
    return os.environ[ENV_AIRBYTE_API_WORKSPACE_ID]


@pytest.fixture
def api_root() -> str:
    return CLOUD_API_ROOT


@pytest.fixture
def api_key() -> str:
    dotenv_vars: dict[str, str | None] = dotenv_values()
    if ENV_AIRBYTE_API_KEY in dotenv_vars:
        return dotenv_vars[ENV_AIRBYTE_API_KEY]

    if ENV_AIRBYTE_API_KEY not in os.environ:
        raise ValueError(f"Please set the {ENV_AIRBYTE_API_KEY} environment variable.")

    return os.environ[ENV_AIRBYTE_API_KEY]


@pytest.fixture(scope="function")
def new_deployable_cache(request) -> CacheBase:
    """This is a placeholder fixture that will be overridden by pytest_generate_tests()."""
    return request.getfixturevalue(request.param)


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Override default pytest behavior, parameterizing our tests based on the available cache types.

    This is useful for running the same tests with different cache types, to ensure that the tests
    can pass across all cache types.
    """
    deployable_cache_fixtures: dict[str, str] = {
        # Ordered by priority (fastest first)
        # "MotherDuck": "new_motherduck_cache",
        # "Postgres": "new_remote_postgres_cache",
        "BigQuery": "new_bigquery_cache",
        "Snowflake": "new_snowflake_cache",
    }

    if "new_deployable_cache" in metafunc.fixturenames:
        metafunc.parametrize(
            "new_deployable_cache",
            deployable_cache_fixtures.values(),
            ids=deployable_cache_fixtures.keys(),
            indirect=True,
            scope="function",
        )
