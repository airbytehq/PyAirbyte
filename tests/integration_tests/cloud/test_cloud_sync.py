# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Cloud Workspace integration tests.

These tests are designed to be run against a running instance of the Airbyte API.
"""
from __future__ import annotations

import pytest

import airbyte as ab
from airbyte.caches import MotherDuckCache
from airbyte.cloud import CloudWorkspace


@pytest.fixture
def pre_created_connection_id() -> str:
    return "80857d37-1f21-4500-a802-f5ac08d1a3dd"


def test_run_connection(
    cloud_workspace: CloudWorkspace,
    pre_created_connection_id: str,
):
    """Test running a connection."""
    sync_result = cloud_workspace.run_sync(connection_id=pre_created_connection_id)
    _ = sync_result


@pytest.mark.skip(reason="This test is not yet complete. It is hanging currently.")
def test_deploy_and_run_connection(
    cloud_workspace: CloudWorkspace,
    motherduck_api_key: str,
) -> None:
    """Test deploying a source and cache to a workspace as a new connection."""
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

    connection_id: str = cloud_workspace.deploy_connection(source=source, cache=cache)
    sync_result = cloud_workspace.run_sync(connection_id=connection_id)
    _ = sync_result

    cache = sync_result.get_sql_cache()
    assert cache.stream_names
    assert cache.streams["users"].to_pandas()

    cloud_workspace.delete_connection(connection_id=connection_id)
