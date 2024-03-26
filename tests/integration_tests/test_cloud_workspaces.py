# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Cloud Workspace integration tests.

These tests are designed to be run against a running instance of the Airbyte API.
"""
from __future__ import annotations
import os
from pathlib import Path
import sys

from dotenv import dotenv_values
import pytest

import airbyte as ab
from airbyte.caches import MotherDuckCache
from airbyte.cloud import CloudWorkspace
from airbyte._executor import _get_bin_dir
from airbyte._util.api_util import CLOUD_API_ROOT, delete_destination

ENV_AIRBYTE_API_KEY = "AIRBYTE_API_KEY"
ENV_AIRBYTE_API_WORKSPACE_ID = "AIRBYTE_API_WORKSPACE_ID"
ENV_MOTHERDUCK_API_KEY = "MOTHERDUCK_API_KEY"


@pytest.fixture(autouse=True)
def add_venv_bin_to_path(monkeypatch):
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


def test_deploy_source(
    cloud_workspace: CloudWorkspace,
) -> None:
    """Test deploying a source to a workspace."""
    source = ab.get_source(
        "source-faker",
        local_executable="source-faker",
        config={"count": 100},
        install_if_missing=False,
    )
    source.check()
    source_id: str = cloud_workspace.deploy_source(source)

    cloud_workspace.delete_source(source=source_id)


def test_deploy_cache_as_destination(
    workspace_id: str,
    api_key: str,
    motherduck_api_key: str,
) -> None:
    """Test deploying a cache to a workspace as a destination."""
    workspace = CloudWorkspace(
        workspace_id=workspace_id,
        api_key=api_key,
    )

    cache = MotherDuckCache(
        api_key=motherduck_api_key,
        database="temp",
        schema_name="public",
    )
    destination_id: str = workspace.deploy_cache_as_destination(cache=cache)
    workspace.delete_destination(destination_id=destination_id)


def test_deploy_connection(
    workspace_id: str,
    api_key: str,
    motherduck_api_key: str,
) -> None:
    """Test deploying a source and cache to a workspace as a new connection."""
    workspace = CloudWorkspace(
        workspace_id=workspace_id,
        api_key=api_key,
    )

    source = ab.get_source(
        "source-faker",
        local_executable="source-faker",
        config={"count": 100},
        install_if_missing=False,
    )
    source.check()

    cache = MotherDuckCache(
        api_key=motherduck_api_key,
        database="temp",
        schema_name="public",
    )

    connection_id: str = workspace.deploy_connection(source=source, cache=cache)
    workspace.delete_connection(connection_id=connection_id)

@pytest.mark.skip(reason="This test is not yet complete. It is hanging currently.")
def test_deploy_and_run_connection(
    workspace_id: str,
    api_key: str,
    motherduck_api_key: str,
) -> None:
    """Test deploying a source and cache to a workspace as a new connection."""
    workspace = CloudWorkspace(
        workspace_id=workspace_id,
        api_key=api_key,
    )

    source = ab.get_source(
        "source-faker",
        local_executable="source-faker",
        config={"count": 100},
        install_if_missing=False,
    )
    source.check()

    cache = MotherDuckCache(
        api_key=motherduck_api_key,
        database="temp",
        schema_name="public",
    )

    connection_id: str = workspace.deploy_connection(source=source, cache=cache)
    sync_result = workspace.run_sync(connection_id=connection_id)

    workspace.delete_connection(connection_id=connection_id)
