# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Cloud Workspace integration tests.

These tests are designed to be run against a running instance of the Airbyte API.
"""
from __future__ import annotations

import airbyte as ab
from airbyte.caches import MotherDuckCache
from airbyte.cloud import CloudWorkspace


ENV_AIRBYTE_API_KEY = "AIRBYTE_API_KEY"
ENV_AIRBYTE_API_WORKSPACE_ID = "AIRBYTE_API_WORKSPACE_ID"
ENV_MOTHERDUCK_API_KEY = "MOTHERDUCK_API_KEY"


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
    cloud_workspace: CloudWorkspace,
    motherduck_api_key: str,
) -> None:
    """Test deploying a cache to a workspace as a destination."""
    cache = MotherDuckCache(
        api_key=motherduck_api_key,
        database="temp",
        schema_name="public",
    )
    destination_id: str = cloud_workspace.deploy_cache_as_destination(cache=cache)
    cloud_workspace.delete_destination(destination_id=destination_id)


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
    )

    connection_id: str = cloud_workspace.deploy_connection(source=source, cache=cache)
    cloud_workspace.delete_connection(connection_id=connection_id)
