# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Integration tests which test CRUD operations on the Airbyte API.

These tests are designed to be run against a running instance of the Airbyte API.
"""

from __future__ import annotations

from airbyte_api.models import DestinationResponse, SourceResponse, WorkspaceResponse
from airbyte._util import api_util, text_util
from airbyte_api.models import DestinationDuckdb, SourceFaker

from airbyte.secrets.base import SecretString


def test_get_workspace(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
) -> None:
    workspace = api_util.get_workspace(
        workspace_id=workspace_id,
        api_root=airbyte_cloud_api_root,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
    )
    assert workspace.workspace_id == workspace_id


def test_list_workspaces(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
) -> None:
    result: list[WorkspaceResponse] = api_util.list_workspaces(
        workspace_id=workspace_id,
        api_root=airbyte_cloud_api_root,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
    )
    assert result
    assert len(result) > 0
    assert all(isinstance(workspace, WorkspaceResponse) for workspace in result)


def test_list_sources(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
) -> None:
    result: list[SourceResponse] = api_util.list_sources(
        workspace_id=workspace_id,
        api_root=airbyte_cloud_api_root,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
    )
    assert (
        result
        and len(result) > 0
        and all(isinstance(source, SourceResponse) for source in result)
    )


def test_list_destinations(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
) -> None:
    result: list[DestinationResponse] = api_util.list_destinations(
        workspace_id=workspace_id,
        api_root=airbyte_cloud_api_root,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
    )
    assert (
        result
        and len(result) > 0
        and all(isinstance(source, SourceResponse) for source in result)
    )


def test_create_and_delete_source(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
) -> None:
    new_resource_name = "deleteme-source-faker" + text_util.generate_random_suffix()
    source_config = SourceFaker()
    source = api_util.create_source(
        name=new_resource_name,
        workspace_id=workspace_id,
        config=source_config,
        api_root=airbyte_cloud_api_root,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
    )
    assert source.name == new_resource_name
    assert source.source_type == "faker"
    assert source.source_id

    api_util.delete_source(
        source_id=source.source_id,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
    )


def test_create_and_delete_destination(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    motherduck_api_key: str,
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
) -> None:
    new_resource_name = (
        "deleteme-destination-faker" + text_util.generate_random_suffix()
    )
    destination_config = DestinationDuckdb(
        destination_path="temp_db",
        motherduck_api_key=motherduck_api_key,
    )

    destination = api_util.create_destination(
        name=new_resource_name,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        config=destination_config,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
    )
    assert destination.name == new_resource_name
    assert destination.destination_type == "duckdb"
    assert destination.destination_id

    api_util.delete_destination(
        destination_id=destination.destination_id,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
    )


def test_create_and_delete_connection(
    workspace_id: str,
    airbyte_cloud_api_root: str,
    airbyte_cloud_client_id: SecretString,
    airbyte_cloud_client_secret: SecretString,
    motherduck_api_key: str,
) -> None:
    new_source_name = "deleteme-source-faker" + text_util.generate_random_suffix()
    new_destination_name = (
        "deleteme-destination-dummy" + text_util.generate_random_suffix()
    )
    new_connection_name = (
        "deleteme-connection-dummy" + text_util.generate_random_suffix()
    )
    source = api_util.create_source(
        name=new_source_name,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        config=SourceFaker(),
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
    )
    assert source.name == new_source_name
    assert source.source_type == "faker"
    assert source.source_id

    destination = api_util.create_destination(
        name=new_destination_name,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        config=DestinationDuckdb(
            destination_path="temp_db",
            motherduck_api_key=motherduck_api_key,
        ),
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
    )
    assert destination.name == new_destination_name
    assert destination.destination_type == "duckdb"
    assert destination.destination_id

    connection = api_util.create_connection(
        name=new_connection_name,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        source_id=source.source_id,
        destination_id=destination.destination_id,
        prefix="",
        selected_stream_names=["users", "purchases", "products"],
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
    )
    assert connection.source_id == source.source_id
    assert connection.destination_id == destination.destination_id
    assert connection.connection_id

    api_util.delete_connection(
        connection_id=connection.connection_id,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
    )
    api_util.delete_source(
        source_id=source.source_id,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
    )
    api_util.delete_destination(
        destination_id=destination.destination_id,
        api_root=airbyte_cloud_api_root,
        workspace_id=workspace_id,
        client_id=airbyte_cloud_client_id,
        client_secret=airbyte_cloud_client_secret,
    )
