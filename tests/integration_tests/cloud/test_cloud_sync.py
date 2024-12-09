# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Cloud Workspace integration tests.

These tests are designed to be run against a running instance of the Airbyte API.
"""

from __future__ import annotations
from dataclasses import asdict

import airbyte as ab
import pytest
from airbyte._util import text_util
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
    sync_result: SyncResult = cloud_workspace.get_connection(
        pre_created_connection_id
    ).run_sync()
    assert sync_result.is_job_complete()
    assert sync_result.stream_names


@pytest.mark.super_slow
def test_get_previous_sync_result(
    cloud_workspace: CloudWorkspace,
    pre_created_connection_id: str,
) -> None:
    """Test running a connection."""
    sync_result: SyncResult = cloud_workspace.get_connection(
        connection_id=pre_created_connection_id,
    ).get_previous_sync_logs()[0]
    assert sync_result.is_job_complete()
    assert sync_result.get_job_status()
    assert sync_result.stream_names


@pytest.mark.super_slow
# @pytest.mark.skip(reason="This test is not yet complete. It is hanging currently.")
def test_deploy_and_run_connection(
    cloud_workspace: CloudWorkspace,
    new_deployable_destination,
) -> None:
    """Test deploying a source and cache to a workspace as a new connection."""
    source = ab.get_source(
        "source-faker",
        config={"count": 100},
    )
    cloud_source = cloud_workspace.deploy_source(
        name=f"test-source-{text_util.generate_random_suffix()}",
        source=source,
    )
    if not isinstance(new_deployable_destination, dict):
        new_deployable_destination = asdict(new_deployable_destination)

    cloud_destination = cloud_workspace.deploy_destination(
        name=f"test-destination-{text_util.generate_random_suffix()}",
        destination=new_deployable_destination,
    )
    connection = cloud_workspace.deploy_connection(
        connection_name=f"test-connection-{text_util.generate_random_suffix()}",
        source=cloud_source,
        destination=cloud_destination,
    )
    sync_result = connection.run_sync()
    _ = sync_result

    cache = sync_result.get_sql_cache()
    assert list(cache.streams.keys())
    assert cache.streams["users"].to_pandas()

    cloud_workspace.permanently_delete_connection(connection)
