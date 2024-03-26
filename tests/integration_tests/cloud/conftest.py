# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Fixtures for Cloud Workspace integration tests."""
from __future__ import annotations

import os
from pathlib import Path
import sys
import pytest
from airbyte._util.api_util import CLOUD_API_ROOT
from dotenv import dotenv_values
from airbyte._executor import _get_bin_dir
from airbyte.cloud import CloudWorkspace


ENV_AIRBYTE_API_KEY = "AIRBYTE_API_KEY"
ENV_AIRBYTE_API_WORKSPACE_ID = "AIRBYTE_API_WORKSPACE_ID"
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
        raise ValueError("Please set the AIRBYTE_API_KEY environment variable.")

    return os.environ[ENV_AIRBYTE_API_KEY]


@pytest.fixture
def motherduck_api_key() -> str:
    dotenv_vars: dict[str, str | None] = dotenv_values()
    if ENV_MOTHERDUCK_API_KEY in dotenv_vars:
        return dotenv_vars[ENV_MOTHERDUCK_API_KEY]

    if ENV_MOTHERDUCK_API_KEY not in os.environ:
        raise ValueError("Please set the AIRBYTE_API_KEY environment variable.")

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
        raise ValueError("Please set the AIRBYTE_API_KEY environment variable.")

    return os.environ[ENV_AIRBYTE_API_KEY]


@pytest.fixture
def motherduck_api_key() -> str:
    dotenv_vars: dict[str, str | None] = dotenv_values()
    if ENV_MOTHERDUCK_API_KEY in dotenv_vars:
        return dotenv_vars[ENV_MOTHERDUCK_API_KEY]

    if ENV_MOTHERDUCK_API_KEY not in os.environ:
        raise ValueError("Please set the AIRBYTE_API_KEY environment variable.")

    return os.environ[ENV_MOTHERDUCK_API_KEY]
