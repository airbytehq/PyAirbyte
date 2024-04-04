# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Integration tests for reading from cache."""
from __future__ import annotations
from contextlib import suppress

import pytest

import airbyte as ab
from airbyte import cloud
from airbyte.cloud._sync_results import SyncResult
from tests.conftest import new_bigquery_cache


@pytest.fixture
def deployable_cache(new_bigquery_cache) -> ab.BigQueryCache | ab.SnowflakeCache:
    # TODO: Add Snowflake here as well
    return new_bigquery_cache


@pytest.fixture
def deployable_source() -> ab.Source:
    return ab.get_source(
        "source-faker",
        local_executable="source-faker",
        config={"count": 100},
        install_if_missing=False,
    )


def test_read_cache(
    cloud_workspace: cloud.CloudWorkspace,
    deployable_cache: ab.BigQueryCache | ab.SnowflakeCache,
    deployable_source: ab.Source,
) -> None:
    """Test reading from a cache."""

    # Deploy source, destination, and connection:
    source_id = cloud_workspace.deploy_source(source=deployable_source)
    destination_id = cloud_workspace.deploy_cache_as_destination(cache=deployable_cache)
    connection_id = cloud_workspace.deploy_connection(
        source=source_id,
        destination=destination_id,
    )

    # Run sync and get result:
    sync_result: SyncResult = cloud_workspace.run_sync(connection_id=connection_id)

    # Test sync result:
    assert sync_result.success
    assert sync_result.stream_names == ["users", "products", "purchases"]
    dataset: ab.CachedDataset = sync_result.get_dataset("users")
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
