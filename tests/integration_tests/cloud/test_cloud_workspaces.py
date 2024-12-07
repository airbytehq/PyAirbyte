# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Cloud Workspace integration tests.

These tests are designed to be run against a running instance of the Airbyte API.
"""

from __future__ import annotations

import airbyte as ab
import pytest
from airbyte.cloud import CloudWorkspace
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.connectors import CloudSource
from airbyte.destinations.base import Destination
from airbyte.secrets.base import SecretString


def test_deploy_source(
    cloud_workspace: CloudWorkspace,
) -> None:
    """Test deploying a source to a workspace."""
    source = ab.get_source(
        "source-faker",
        config={"count": 100},
    )
    source.check()
    cloud_source: CloudSource = cloud_workspace.deploy_source(
        name="test-source",
        source=source,
    )

    cloud_workspace.permanently_delete_source(cloud_source)


@pytest.mark.skip("This test is flaky/failing and needs to be fixed.")
def test_deploy_connection(
    cloud_workspace: CloudWorkspace,
    motherduck_api_key: str,
) -> None:
    """Test deploying a source and cache to a workspace as a new connection."""
    source = ab.get_source(
        "source-faker",
        config={"count": 100},
    )
    source.check()

    destination = DestinationMotherDuck(
        motherduck_api_key=SecretString(motherduck_api_key),
        database="new_db",
        schema_name="public",
        table_prefix="abc_deleteme_",
    )
    destination.check()

    cloud_source = cloud_workspace.deploy_source(
        name="test-source",
        source=source,
    )
    cloud_destination = cloud_workspace.deploy_destination(
        name="test-destination",
        destination=destination,
    )

    connection: CloudConnection = cloud_workspace.deploy_connection(
        connection_name="test-connection",
        source=cloud_source,
        destination=cloud_destination,
    )
    assert set(connection.stream_names) == set(["users", "products", "purchases"])
    assert connection.table_prefix == "abc_deleteme_"
    cloud_workspace.permanently_delete_connection(
        connection=connection,
        delete_source=True,
        delete_destination=True,
    )
