# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Integration tests for reading from cache."""

from __future__ import annotations


import airbyte as ab
import pandas as pd
import pytest
from airbyte import cloud
from airbyte.cloud.sync_results import SyncResult
from sqlalchemy.engine.base import Engine


@pytest.fixture
def deployable_source() -> ab.Source:
    return ab.get_source(
        "source-faker",
        config={"count": 100},
    )


@pytest.fixture
def previous_job_run_id() -> int:
    return 10136196


# This test is redundant with the other tests, and auto-retries in Cloud make the test untenable
# for us to have multiple tests running the sync operation for now.
# @pytest.mark.super_slow
# def test_deploy_and_run_and_read(
#     cloud_workspace: cloud.CloudWorkspace,
#     deployable_dummy_destination: ab.Destination,
#     deployable_dummy_source: ab.Source,
# ) -> None:
#     """Test reading from a cache."""

#     # Deploy source, destination, and connection:
#     cloud_source: CloudSource = cloud_workspace.deploy_source(
#         name="test-source",
#         source=deployable_dummy_source,
#         random_name_suffix=True,
#     )
#     cloud_destination = cloud_workspace.deploy_destination(
#         name="test-destination",
#         destination=deployable_dummy_destination,
#         random_name_suffix=True,
#     )
#     connection: cloud.CloudConnection = cloud_workspace.deploy_connection(
#         connection_name="test-connection-{text_util.generate_random_suffix()}",
#         source=cloud_source,
#         destination=cloud_destination,
#         selected_streams=deployable_source.get_selected_streams(),
#     )

#     # Run sync and get result:
#     sync_result: SyncResult = connection.run_sync()

#     # TODO: Remove this second run after Destination bug is resolved:
#     #       https://github.com/airbytehq/airbyte/issues/36875
#     sync_result: SyncResult = connection.run_sync()

#     # Check sync result:
#     assert sync_result.is_job_complete()
#     assert set(sync_result.stream_names) == set(["users", "products", "purchases"])

#     dataset: ab.CachedDataset = sync_result.get_dataset(stream_name="users")
#     assert dataset.stream_name == "users"
#     data_as_list = list(dataset)
#     assert len(data_as_list) == 100

#     # Cleanup
#     with suppress(Exception):
#         cloud_workspace.permanently_delete_connection(
#             connection=connection,
#             cascade_delete_source=True,
#             cascade_delete_destination=True,
#         )
#     with suppress(Exception):
#         cloud_workspace.permanently_delete_source(source=cloud_source)
#     with suppress(Exception):
#         cloud_workspace.permanently_delete_destination(destination=cloud_destination)


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
    with_snowflake_password_env_var,
    with_bigquery_credentials_env_vars,
) -> None:
    """Test reading from a cache."""
    # Run sync and get result:
    sync_result = cloud_workspace.get_connection(
        connection_id=deployed_connection_id
    ).get_sync_result()

    # Test sync result:
    assert sync_result
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
def test_translate_cloud_job_to_sql_cache(
    cloud_workspace: cloud.CloudWorkspace,
    deployed_connection_id: str,
    previous_job_run_id: int,
    with_bigquery_credentials_env_vars,
    with_snowflake_password_env_var,
) -> None:
    """Test reading from a cache."""
    # Run sync and get result:
    sync_result: SyncResult | None = cloud_workspace.get_connection(
        connection_id=deployed_connection_id
    ).get_sync_result(
        job_id=previous_job_run_id,
    )
    assert sync_result, f"Failed to get sync result for job {previous_job_run_id}"

    # Test sync result:
    assert sync_result.is_job_complete()

    cache = sync_result.get_sql_cache()
    sqlalchemy_url = cache.get_sql_alchemy_url()
    engine: Engine = sync_result.get_sql_engine()


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
    previous_job_run_id: int,
    with_bigquery_credentials_env_vars,
    with_snowflake_password_env_var,
) -> None:
    """Test reading from a cache."""
    # Run sync and get result:
    sync_result: SyncResult | None = cloud_workspace.get_connection(
        connection_id=deployed_connection_id
    ).get_sync_result(
        job_id=previous_job_run_id,
    )
    assert sync_result, f"Failed to get sync result for job {previous_job_run_id}"

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
