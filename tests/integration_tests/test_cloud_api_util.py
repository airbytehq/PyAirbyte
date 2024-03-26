# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Integration tests which test CRUD operations on the Airbyte API.

These tests are designed to be run against a running instance of the Airbyte API.
"""
from __future__ import annotations
import os

from airbyte_api.models.shared.sourceresponse import SourceResponse
import pytest
import ulid

import airbyte as ab
from airbyte._util import api_util, api_duck_types
from airbyte_api.models.shared import SourceFaker, DestinationDevNull, DestinationDuckdb
from dotenv import dotenv_values

from airbyte.caches.duckdb import DuckDBCache

CLOUD_API_ROOT = "https://api.airbyte.com/v1"
ENV_AIRBYTE_API_KEY = "AIRBYTE_API_KEY"
ENV_AIRBYTE_API_WORKSPACE_ID = "AIRBYTE_API_WORKSPACE_ID"
ENV_MOTHERDUCK_API_KEY = "MOTHERDUCK_API_KEY"


@pytest.fixture
def workspace_id() -> str:
    return os.environ[ENV_AIRBYTE_API_WORKSPACE_ID]


@pytest.fixture
def api_root() -> str:
    return CLOUD_API_ROOT


@pytest.fixture
def api_key() -> str:
    dotenv_vars: dict[str, str | None] = dotenv_values()
    if ENV_AIRBYTE_API_KEY in dotenv_vars:
        return dotenv_vars[ENV_AIRBYTE_API_KEY]

    if ENV_AIRBYTE_API_KEY not in os.environ:
        raise ValueError("Please set the AIRBYTE_API_KEY environment variable.")

    return os.environ[ENV_AIRBYTE_API_KEY]


@pytest.fixture
def motherduck_api_key() -> str:
    dotenv_vars: dict[str, str | None] = dotenv_values()
    if ENV_MOTHERDUCK_API_KEY in dotenv_vars:
        return dotenv_vars[ENV_MOTHERDUCK_API_KEY]

    if ENV_MOTHERDUCK_API_KEY not in os.environ:
        raise ValueError("Please set the AIRBYTE_API_KEY environment variable.")

    return os.environ[ENV_MOTHERDUCK_API_KEY]


def test_create_and_delete_source(
    workspace_id: str,
    api_root: str,
    api_key: str,
) -> None:
    new_resource_name = "deleteme-source-faker" + str(ulid.ULID()).lower()[-6:]
    source_config = SourceFaker()
    source = api_util.create_source(
        name=new_resource_name,
        api_root=api_root,
        api_key=api_key,
        workspace_id=workspace_id,
        config=source_config,
    )
    assert source.name == new_resource_name
    assert source.source_type == "faker"
    assert source.source_id

    api_util.delete_source(
        source_id=source.source_id,
        api_root=api_root,
        api_key=api_key,
        workspace_id=workspace_id,
    )


def test_create_and_delete_destination(
    workspace_id: str,
    api_root: str,
    api_key: str,
    motherduck_api_key: str,
) -> None:
    new_resource_name = "deleteme-destination-faker" + str(ulid.ULID()).lower()[-6:]
    destination_config = DestinationDuckdb(
        destination_path="temp_db",
        motherduck_api_key=motherduck_api_key,
    )

    destination = api_util.create_destination(
        name=new_resource_name,
        api_root=api_root,
        api_key=api_key,
        workspace_id=workspace_id,
        config=destination_config,
    )
    assert destination.name == new_resource_name
    assert destination.destination_type == "duckdb"
    assert destination.destination_id

    api_util.delete_destination(
        destination_id=destination.destination_id,
        api_root=api_root,
        api_key=api_key,
        workspace_id=workspace_id,
    )



def test_create_and_delete_connection(
    workspace_id: str,
    api_root: str,
    api_key: str,
    motherduck_api_key: str,
) -> None:
    new_source_name = "deleteme-source-faker" + str(ulid.ULID()).lower()[-6:]
    new_destination_name = "deleteme-destination-dummy" + str(ulid.ULID()).lower()[-6:]
    new_connection_name = "deleteme-connection-dummy" + str(ulid.ULID()).lower()[-6:]
    source = api_util.create_source(
        name=new_source_name,
        api_root=api_root,
        api_key=api_key,
        workspace_id=workspace_id,
        config=SourceFaker(),
    )
    assert source.name == new_source_name
    assert source.source_type == "faker"
    assert source.source_id

    destination = api_util.create_destination(
        name=new_destination_name,
        api_root=api_root,
        api_key=api_key,
        workspace_id=workspace_id,
        config=DestinationDuckdb(
            destination_path="temp_db",
            motherduck_api_key=motherduck_api_key,
        ),
    )
    assert destination.name == new_destination_name
    assert destination.destination_type == "duckdb"
    assert destination.destination_id

    connection = api_util.create_connection(
        name=new_connection_name,
        api_root=api_root,
        api_key=api_key,
        workspace_id=workspace_id,
        source_id=source.source_id,
        destination_id=destination.destination_id,
    )
    assert connection.source_id == source.source_id
    assert connection.destination_id == destination.destination_id
    assert connection.connection_id

    api_util.delete_connection(
        connection_id=connection.connection_id,
        api_root=api_root,
        api_key=api_key,
        workspace_id=workspace_id,
    )
    api_util.delete_source(
        source_id=source.source_id,
        api_root=api_root,
        api_key=api_key,
        workspace_id=workspace_id,
    )
    api_util.delete_destination(
        destination_id=destination.destination_id,
        api_root=api_root,
        api_key=api_key,
        workspace_id=workspace_id,
    )
