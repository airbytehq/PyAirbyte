# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Integration tests which test CRUD operations on the Airbyte API.

These tests are designed to be run against a running instance of the Airbyte API.
"""

from __future__ import annotations

import ulid

from airbyte.cloud import _api_util
from airbyte_api.models import SourceFaker, DestinationDuckdb


def test_create_and_delete_source(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    airbyte_cloud_api_key: str,
) -> None:
    new_resource_name = "deleteme-source-faker" + str(ulid.ULID()).lower()[-6:]
    source_config = SourceFaker()
    source = _api_util.create_source(
        name=new_resource_name,
        api_root=airbyte_cloud_api_root,
        api_key=airbyte_cloud_api_key,
        workspace_id=workspace_id,
        config=source_config,
    )
    assert source.name == new_resource_name
    assert source.source_type == "faker"
    assert source.source_id

    _api_util.delete_source(
        source_id=source.source_id,
        api_root=airbyte_cloud_api_root,
        api_key=airbyte_cloud_api_key,
        workspace_id=workspace_id,
    )


def test_create_and_delete_destination(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    airbyte_cloud_api_key: str,
    motherduck_api_key: str,
) -> None:
    new_resource_name = "deleteme-destination-faker" + str(ulid.ULID()).lower()[-6:]
    destination_config = DestinationDuckdb(
        destination_path="temp_db",
        motherduck_api_key=motherduck_api_key,
    )

    destination = _api_util.create_destination(
        name=new_resource_name,
        api_root=airbyte_cloud_api_root,
        api_key=airbyte_cloud_api_key,
        workspace_id=workspace_id,
        config=destination_config,
    )
    assert destination.name == new_resource_name
    assert destination.destination_type == "duckdb"
    assert destination.destination_id

    _api_util.delete_destination(
        destination_id=destination.destination_id,
        api_root=airbyte_cloud_api_root,
        api_key=airbyte_cloud_api_key,
        workspace_id=workspace_id,
    )


def test_create_and_delete_connection(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    airbyte_cloud_api_key: str,
    motherduck_api_key: str,
) -> None:
    new_source_name = "deleteme-source-faker" + str(ulid.ULID()).lower()[-6:]
    new_destination_name = "deleteme-destination-dummy" + str(ulid.ULID()).lower()[-6:]
    new_connection_name = "deleteme-connection-dummy" + str(ulid.ULID()).lower()[-6:]
    source = _api_util.create_source(
        name=new_source_name,
        api_root=airbyte_cloud_api_root,
        api_key=airbyte_cloud_api_key,
        workspace_id=workspace_id,
        config=SourceFaker(),
    )
    assert source.name == new_source_name
    assert source.source_type == "faker"
    assert source.source_id

    destination = _api_util.create_destination(
        name=new_destination_name,
        api_root=airbyte_cloud_api_root,
        api_key=airbyte_cloud_api_key,
        workspace_id=workspace_id,
        config=DestinationDuckdb(
            destination_path="temp_db",
            motherduck_api_key=motherduck_api_key,
        ),
    )
    assert destination.name == new_destination_name
    assert destination.destination_type == "duckdb"
    assert destination.destination_id

    connection = _api_util.create_connection(
        name=new_connection_name,
        api_root=airbyte_cloud_api_root,
        api_key=airbyte_cloud_api_key,
        workspace_id=workspace_id,
        source_id=source.source_id,
        destination_id=destination.destination_id,
        prefix="",
        selected_stream_names=["users", "purchases", "products"],
    )
    assert connection.source_id == source.source_id
    assert connection.destination_id == destination.destination_id
    assert connection.connection_id

    _api_util.delete_connection(
        connection_id=connection.connection_id,
        api_root=airbyte_cloud_api_root,
        api_key=airbyte_cloud_api_key,
        workspace_id=workspace_id,
    )
    _api_util.delete_source(
        source_id=source.source_id,
        api_root=airbyte_cloud_api_root,
        api_key=airbyte_cloud_api_key,
        workspace_id=workspace_id,
    )
    _api_util.delete_destination(
        destination_id=destination.destination_id,
        api_root=airbyte_cloud_api_root,
        api_key=airbyte_cloud_api_key,
        workspace_id=workspace_id,
    )
