# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Cloud Workspace integration tests.

These tests are designed to be run against a running instance of the Airbyte API.
"""
from __future__ import annotations

import airbyte as ab
from airbyte.caches import MotherDuckCache
from airbyte.cloud import CloudWorkspace
from airbyte.cloud.connections import CloudConnection


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
    source_id: str = cloud_workspace._deploy_source(source)

    cloud_workspace.permanently_delete_source(source=source_id)


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
    destination_id: str = cloud_workspace._deploy_cache_as_destination(cache=cache)
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

    connection: CloudConnection = cloud_workspace._deploy_connection(
        source=source,
        cache=cache,
    )
    assert set(connection.stream_names) == set(["users", "products", "purchases"])
    assert connection.table_prefix == "abc_deleteme_"
    # assert connection.table_suffix == ""  # Suffix not supported in CloudConnection
    cloud_workspace.permanently_delete_connection(
        connection=connection,
        delete_source=True,
        delete_destination=True,
    )
