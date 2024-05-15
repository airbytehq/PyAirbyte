# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Cloud Workspace integration tests.

These tests are designed to be run against a running instance of the Airbyte API.
"""

from __future__ import annotations

import airbyte as ab
import pytest
from airbyte.caches import MotherDuckCache
from airbyte.cloud import CloudWorkspace
from airbyte.cloud.sync_results import SyncResult


@pytest.fixture
def pre_created_connection_id() -> str:
    return "80857d37-1f21-4500-a802-f5ac08d1a3dd"


@pytest.mark.super_slow
def test_run_connection(
    cloud_workspace: CloudWorkspace,
    pre_created_connection_id: str,
) -> None:
    """Test running a connection."""
    sync_result: SyncResult = cloud_workspace.run_sync(
        connection_id=pre_created_connection_id
    )
    assert sync_result.is_job_complete()
    assert sync_result.stream_names


@pytest.mark.super_slow
def test_get_previous_sync_result(
    cloud_workspace: CloudWorkspace,
    pre_created_connection_id: str,
) -> None:
    """Test running a connection."""
    sync_result: SyncResult = cloud_workspace.get_previous_sync_logs(
        connection_id=pre_created_connection_id,
    )[0]
    assert sync_result.is_job_complete()
    assert sync_result.get_job_status()
    assert sync_result.stream_names


@pytest.mark.super_slow
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

    connection_id: str = cloud_workspace._deploy_connection(source=source, cache=cache)
    sync_result = cloud_workspace.run_sync(connection_id=connection_id)
    _ = sync_result

    cache = sync_result.get_sql_cache()
    assert cache.stream_names
    assert cache.streams["users"].to_pandas()

    cloud_workspace._permanently_delete_connection(connection_id=connection_id)
