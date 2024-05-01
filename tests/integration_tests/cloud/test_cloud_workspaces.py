# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Cloud Workspace integration tests.

These tests are designed to be run against a running instance of the Airbyte API.
"""

from __future__ import annotations

from dataclasses import asdict

import airbyte as ab
from airbyte import cloud
from airbyte import exceptions as exc
from airbyte.caches import MotherDuckCache
from airbyte.cloud import CloudWorkspace
from airbyte.cloud.connections import CloudConnection
from pytest import raises


def test_deploy_source(
    cloud_workspace: CloudWorkspace,
) -> None:
    """Test deploying a source to a workspace."""
    local_source: ab.Source = ab.get_source(
        "source-faker",
        local_executable="source-faker",
        config={"count": 100, "seed": 123},
        install_if_missing=False,
    )
    local_source.check()

    # Deploy source:
    source_connector: cloud.CloudConnector = cloud_workspace.deploy_source(
        source=local_source,
        name="My Faker Source (DELETEME)",  # Used in deduplication and idempotency
    )
    assert source_connector.name == "My Faker Source (DELETEME)"
    assert "count" in list(asdict(source_connector.configuration).keys())
    assert asdict(source_connector.configuration)["count"] == 100
    assert asdict(source_connector.configuration)["seed"] == 123

    with raises(exc.PyAirbyteResourceConflictError):
        # Deploy source again (should fail):
        cloud_workspace.deploy_source(
            source=local_source,
            name_key="My Faker Source",  # Used in deduplication and idempotency
            update_existing=False,  # Fail since source already exists
        )

    # Change config and deploy source again (should succeed):
    local_source.set_config({"count": 200})
    source_connector: cloud.CloudConnector = cloud_workspace.deploy_source(
        source=local_source,
        name_key="My Faker Source",  # Used in deduplication and idempotency
        update_existing=True,  # Update existing source
    )

    # Partially update the configuration (merging with config from previous deployment):
    source_connector.update_configuration(
        {"count": 300},
        merge=True,
    )
    assert source_connector.configuration["count"] == 300
    assert source_connector.configuration["seed"] == 123

    # Fully replace the configuration:
    source_connector.update_configuration(
        {"count": 300},
        merge=False,
    )
    assert "count" in list(asdict(source_connector.configuration).keys())
    assert asdict(source_connector.configuration)["count"] == 300
    assert "seed" not in source_connector.configuration

    # Delete the deployed source connector:
    source_connector.permanently_delete_connector()


def test_deploy_cache_as_destination(
    cloud_workspace: CloudWorkspace,
    motherduck_api_key: str,
) -> None:
    """Test deploying a cache to a workspace as a destination."""
    cache = MotherDuckCache(
        api_key=motherduck_api_key,
        database="temp",
        schema_name="public",
    )
    destination_id: str = cloud_workspace.deploy_cache_as_destination(
        cache=cache,
        name="My MotherDuck Destination (DELETEME)",  # Used in deduplication and idempotency
    )
    cloud_workspace.permanently_delete_destination(destination=destination_id)


def test_deploy_connection(
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
        table_prefix="abc_deleteme_",
        # table_suffix="",  # Suffix not supported in CloudConnection
    )
    source_connector: cloud.CloudConnector = cloud_workspace.deploy_source(
        source=source,
        name="My Faker Source (DELETEME)",  # Used in deduplication and idempotency
    )
    destination_connector: cloud.CloudConnector = cloud_workspace.deploy_cache_as_destination(
        cache=cache,
        name="My MotherDuck Destination (DELETEME)",  # Used in deduplication and idempotency
    )

    connection: CloudConnection = cloud_workspace.deploy_connection(
        name="My Connection (DELETEME)",  # Used in deduplication and idempotency
        source=source_connector,
        destination=destination_connector,
        table_prefix=cache.table_prefix,
    )
    assert set(connection.stream_names) == set(["users", "products", "purchases"])
    assert connection.table_prefix == "abc_deleteme_"
    # assert connection.table_suffix == ""  # Suffix not supported in CloudConnection
    cloud_workspace.permanently_delete_connection(
        connection=connection,
        delete_source=True,
        delete_destination=True,
    )
