# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Fixtures for Cloud Workspace integration tests."""
from __future__ import annotations

from enum import auto
import os
from pathlib import Path
import sys
import pytest
from airbyte.cloud._api_util import CLOUD_API_ROOT
from dotenv import dotenv_values
from airbyte._executor import _get_bin_dir
from airbyte.caches.base import CacheBase
from airbyte.cloud import CloudWorkspace
from airbyte._util.temp_files import as_temp_files
from airbyte.secrets.base import SecretString
from airbyte.secrets.google_gsm import GoogleGSMSecretManager


AIRBYTE_CLOUD_WORKSPACE_ID = "19d7a891-8e0e-40ac-8a8c-5faf8d11e47c"

ENV_MOTHERDUCK_API_KEY = "PYAIRBYTE_MOTHERDUCK_API_KEY"
AIRBYTE_CLOUD_API_KEY_SECRET_NAME = "PYAIRBYTE_CLOUD_INTEROP_API_KEY"


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
    return AIRBYTE_CLOUD_WORKSPACE_ID


@pytest.fixture
def airbyte_cloud_api_root() -> str:
    return CLOUD_API_ROOT


@pytest.fixture
def airbyte_cloud_api_key(ci_secret_manager: GoogleGSMSecretManager) -> SecretString:
    secret: SecretString | None = ci_secret_manager.get_secret(AIRBYTE_CLOUD_API_KEY_SECRET_NAME)
    assert secret, f"Secret '{AIRBYTE_CLOUD_API_KEY_SECRET_NAME}' not found."
    return secret


@pytest.fixture
def motherduck_api_key(motherduck_secrets: dict) -> SecretString:
    return SecretString(motherduck_secrets["motherduck_api_key"])


@pytest.fixture
def cloud_workspace(
    workspace_id: str,
    airbyte_cloud_api_key: SecretString,
    airbyte_cloud_api_root: str,
) -> CloudWorkspace:
    return CloudWorkspace(
        workspace_id=workspace_id,
        api_key=airbyte_cloud_api_key,
        api_root=airbyte_cloud_api_root,
    )


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
