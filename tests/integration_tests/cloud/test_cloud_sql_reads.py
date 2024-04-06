# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Integration tests for reading from cache."""
from __future__ import annotations
from contextlib import suppress

import pytest
import sqlalchemy
from sqlalchemy.engine.base import Engine

import airbyte as ab
from airbyte import cloud
from airbyte.cloud._sync_results import SyncResult


@pytest.fixture
def deployable_source() -> ab.Source:
    return ab.get_source(
        "source-faker",
        local_executable="source-faker",
        config={"count": 100},
        install_if_missing=False,
    )


@pytest.fixture
def deployed_connection_id() -> str:
    return "c7b4d838-a612-495a-9d91-a14e477add51"


@pytest.fixture
def previous_job_run_id() -> str:
    return "10136196"


def test_deploy_and_run_and_read(
    cloud_workspace: cloud.CloudWorkspace,
    new_deployable_cache: ab.BigQueryCache | ab.SnowflakeCache,
    deployable_source: ab.Source,
) -> None:
    """Test reading from a cache."""

    # Deploy source, destination, and connection:
    source_id = cloud_workspace.deploy_source(source=deployable_source)
    destination_id = cloud_workspace.deploy_cache_as_destination(cache=new_deployable_cache)
    connection_id = cloud_workspace.deploy_connection(
        source=source_id,
        destination=destination_id,
    )

    # Run sync and get result:
    sync_result: SyncResult = cloud_workspace.run_sync(connection_id=connection_id)

    # Test sync result:
    assert sync_result.is_job_complete()

    # TODO: Remove this after Destination bug is resolved:
    #       https://github.com/airbytehq/airbyte/issues/36875
    sync_result: SyncResult = cloud_workspace.run_sync(connection_id=connection_id)

    # Test sync result:
    assert sync_result.is_job_complete()

    # TODO: Rebuild streams property from connection's configured streams API endpoint
    # assert sync_result.stream_names == ["users", "products", "purchases"]

    dataset: ab.CachedDataset = sync_result.get_dataset(stream_name="users")
    assert dataset.stream_name == "users"
    data_as_list = list(dataset)
    assert len(data_as_list) == 100

    # Cleanup
    with suppress(Exception):
        cloud_workspace.delete_connection(
            connection_id=connection_id,
            delete_source=True,
            delete_destination=True,
        )
    with suppress(Exception):
        cloud_workspace.delete_source(source_id=source_id)
    with suppress(Exception):
        cloud_workspace.delete_destination(destination_id=destination_id)


def test_read_from_deployed_connection(
    cloud_workspace: cloud.CloudWorkspace,
    deployed_connection_id: str,
) -> None:
    """Test reading from a cache."""
    # Run sync and get result:
    sync_result: SyncResult = cloud_workspace.get_sync_result(connection_id=deployed_connection_id)

    # Test sync result:
    assert sync_result.is_job_complete()

    cache = sync_result.get_sql_cache()
    sqlalchemy_url = cache.get_sql_alchemy_url()
    engine: Engine = sync_result.get_sql_engine()
    # assert sync_result.stream_names == ["users", "products", "purchases"]

    dataset: ab.CachedDataset = sync_result.get_dataset(stream_name="users")
    assert dataset.stream_name == "users"
    data_as_list = list(dataset)
    assert len(data_as_list) == 100

    pandas_df = dataset.to_pandas()
    assert pandas_df.shape == (100, 20)
    for col in pandas_df.columns:
        # Check that no values are null
        assert pandas_df[col].notnull().all()


def test_read_from_previous_job(
    cloud_workspace: cloud.CloudWorkspace,
    deployed_connection_id: str,
    previous_job_run_id: str,
) -> None:
    """Test reading from a cache."""
    # Run sync and get result:
    sync_result: SyncResult = cloud_workspace.get_sync_result(
        connection_id=deployed_connection_id,
        job_id=previous_job_run_id,
    )

    # Test sync result:
    assert sync_result.is_job_complete()

    cache = sync_result.get_sql_cache()
    sqlalchemy_url = cache.get_sql_alchemy_url()
    engine: Engine = sync_result.get_sql_engine()
    # assert sync_result.stream_names == ["users", "products", "purchases"]

    dataset: ab.CachedDataset = sync_result.get_dataset(stream_name="users")
    assert dataset.stream_name == "users"
    data_as_list = list(dataset)
    assert len(data_as_list) == 100

    pandas_df = dataset.to_pandas()
    assert pandas_df.shape == (100, 20)
    for col in pandas_df.columns:
        # Check that no values are null
        assert pandas_df[col].notnull().all()
