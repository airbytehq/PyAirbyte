# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Integration tests which test destination capabilities using the JSONL destination (docker-based)."""

from __future__ import annotations

import pytest
from airbyte import get_source
from airbyte._future_cdk.catalog_providers import CatalogProvider
from airbyte._message_generators import MessageGeneratorFromMessages
from airbyte.caches.util import new_local_cache
from airbyte.destinations.base import Destination
from airbyte.executors.base import Executor
from airbyte.executors.util import get_connector_executor
from airbyte.results import ReadResult
from airbyte.sources.base import Source
from airbyte_cdk import AirbyteMessage, AirbyteRecordMessage, Type


@pytest.fixture
def new_duckdb_destination_executor() -> Executor:
    """Return a new JSONL destination executor."""
    return get_connector_executor(
        name="destination-duckdb",
        docker_image="airbyte/destination-duckdb:latest",
        # pip_url="git+https://github.com/airbytehq/airbyte.git#subdirectory=airbyte-integrations/connectors/destination-duckdb",
    )


@pytest.fixture
def new_duckdb_destination(new_duckdb_destination_executor: Destination) -> Destination:
    """Return a new JSONL destination."""
    return Destination(
        name="destination-duckdb",
        config={
            # This path is relative to the container:
            "destination_path": "/local/temp/db.duckdb",
        },
        executor=new_duckdb_destination_executor,
    )


def test_duckdb_destination_spec(new_duckdb_destination: Destination) -> None:
    """Test the JSONL destination."""
    new_duckdb_destination.print_config_spec()


def test_duckdb_destination_check(new_duckdb_destination: Destination) -> None:
    """Test the JSONL destination."""
    new_duckdb_destination.check()


def test_duckdb_destination_write_components(
    new_duckdb_destination: Destination,
) -> None:
    """Test the JSONL destination."""
    # Get a source-faker instance.
    source: Source = get_source(
        "source-faker",
        local_executable="source-faker",
        config={
            "count": 100,
            "seed": 1234,
            "parallelism": 16,
        },
        install_if_missing=False,
        streams=["products"],
    )
    read_result: ReadResult = source.read()
    # Read from the source and write to the destination.
    airbyte_messages = (
        AirbyteMessage(
            type=Type.RECORD,
            record=AirbyteRecordMessage(
                stream="products",
                emitted_at=1704067200,  # Dummy value
                data=record_dict,
            ),
        )
        for record_dict in read_result["products"]
    )
    new_duckdb_destination.write(
        stdin=MessageGeneratorFromMessages(airbyte_messages),
        catalog_provider=CatalogProvider(source.configured_catalog),
    )


def test_duckdb_destination_write(new_duckdb_destination: Destination) -> None:
    """Test the JSONL destination."""
    # Get a source-faker instance.
    source: Source = get_source(
        "source-faker",
        local_executable="source-faker",
        config={
            "count": 100,
            "seed": 1234,
            "parallelism": 16,
        },
        install_if_missing=False,
        streams=["products"],
    )
    read_result: ReadResult = source.read(
        cache=new_local_cache(),
        destination=new_duckdb_destination,
    )
