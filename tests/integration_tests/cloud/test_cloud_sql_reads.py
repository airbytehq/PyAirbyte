# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Integration tests for reading from cache."""

from __future__ import annotations

from contextlib import suppress

import pandas as pd
import pytest
from sqlalchemy.engine.base import Engine

import airbyte as ab
from airbyte import cloud
from airbyte.cloud.sync_results import SyncResult


@pytest.fixture
def deployable_source() -> ab.Source:
    return ab.get_source(
        "source-faker",
        local_executable="source-faker",
        config={"count": 100},
        install_if_missing=False,
    )


@pytest.fixture
def previous_job_run_id() -> str:
    return "10136196"


@pytest.mark.super_slow
def test_deploy_and_run_and_read(
    cloud_workspace: cloud.CloudWorkspace,
    new_deployable_cache: ab.BigQueryCache | ab.SnowflakeCache,
    deployable_source: ab.Source,
) -> None:
    """Test reading from a cache."""

    # Deploy source, destination, and connection:
    source_id = cloud_workspace.deploy_source(source=deployable_source)
    destination_id = cloud_workspace.deploy_cache_as_destination(
        cache=new_deployable_cache
    )
    connection: cloud.CloudConnection = cloud_workspace.deploy_connection(
        source=deployable_source,
        cache=new_deployable_cache,
        table_prefix=new_deployable_cache.table_prefix,
        selected_streams=deployable_source.get_selected_streams(),
    )

    # Run sync and get result:
    sync_result: SyncResult = connection.run_sync()

    # TODO: Remove this second run after Destination bug is resolved:
    #       https://github.com/airbytehq/airbyte/issues/36875
    sync_result: SyncResult = connection.run_sync()

    # Check sync result:
    assert sync_result.is_job_complete()
    assert set(sync_result.stream_names) == set(["users", "products", "purchases"])

    dataset: ab.CachedDataset = sync_result.get_dataset(stream_name="users")
    assert dataset.stream_name == "users"
    data_as_list = list(dataset)
    assert len(data_as_list) == 100

    # Cleanup
    with suppress(Exception):
        cloud_workspace.permanently_delete_connection(
            connection_id=connection.connection_id,
            delete_source=True,
            delete_destination=True,
        )
    with suppress(Exception):
        cloud_workspace.permanently_delete_source(source_id=source_id)
    with suppress(Exception):
        cloud_workspace.permanently_delete_destination(destination_id=destination_id)


@pytest.mark.parametrize(
    "deployed_connection_id",
    [
        pytest.param("c7b4d838-a612-495a-9d91-a14e477add51", id="Faker->Snowflake"),
        pytest.param("0e1d6b32-b8e3-4b68-91a3-3a314599c782", id="Faker->BigQuery"),
        pytest.param(
            "", id="Faker->Postgres", marks=pytest.mark.skip(reason="Not yet supported")
        ),
        pytest.param(
            "",
            id="Faker->MotherDuck",
            marks=pytest.mark.skip(reason="Not yet supported"),
        ),
    ],
)
def test_read_from_deployed_connection(
    cloud_workspace: cloud.CloudWorkspace,
    deployed_connection_id: str,
) -> None:
    """Test reading from a cache."""
    # Run sync and get result:
    sync_result: SyncResult = cloud_workspace.get_sync_result(
        connection_id=deployed_connection_id
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

    # TODO: Fails on BigQuery: https://github.com/airbytehq/PyAirbyte/issues/165
    # pandas_df = dataset.to_pandas()

    pandas_df = pd.DataFrame(data_as_list)

    assert pandas_df.shape == (100, 20)

    # Check that no values are null
    for col in pandas_df.columns:
        assert pandas_df[col].notnull().all()


@pytest.mark.parametrize(
    "deployed_connection_id",
    [
        pytest.param("c7b4d838-a612-495a-9d91-a14e477add51", id="Faker->Snowflake"),
        pytest.param("0e1d6b32-b8e3-4b68-91a3-3a314599c782", id="Faker->BigQuery"),
        pytest.param(
            "", id="Faker->Postgres", marks=pytest.mark.skip(reason="Not yet supported")
        ),
        pytest.param(
            "",
            id="Faker->MotherDuck",
            marks=pytest.mark.skip(reason="Not yet supported"),
        ),
    ],
)
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

    assert "users" in sync_result.stream_names
    dataset: ab.CachedDataset = sync_result.get_dataset(stream_name="users")
    assert dataset.stream_name == "users"
    data_as_list = list(dataset)
    assert len(data_as_list) == 100

    # TODO: Fails on BigQuery: https://github.com/airbytehq/PyAirbyte/issues/165
    # pandas_df = dataset.to_pandas()

    pandas_df = pd.DataFrame(data_as_list)

    assert pandas_df.shape == (100, 20)
    for col in pandas_df.columns:
        # Check that no values are null
        assert pandas_df[col].notnull().all()
