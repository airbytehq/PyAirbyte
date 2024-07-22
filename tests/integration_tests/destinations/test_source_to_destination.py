# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Integration tests which test destination capabilities using the JSONL destination (docker-based)."""

from __future__ import annotations

import pytest
from airbyte import get_source
from airbyte._future_cdk.catalog_providers import CatalogProvider
from airbyte._message_generators import AirbyteMessageGenerator
from airbyte.caches.util import new_local_cache
from airbyte.destinations.base import Destination
from airbyte.executors.base import Executor
from airbyte.executors.util import get_connector_executor
from airbyte.progress import ProgressTracker
from airbyte.results import ReadResult, WriteResult
from airbyte.sources.base import Source
from airbyte.strategies import WriteStrategy
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


@pytest.fixture
def new_source_faker() -> Source:
    return get_source(
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


def test_duckdb_destination_spec(new_duckdb_destination: Destination) -> None:
    """Test the JSONL destination."""
    new_duckdb_destination.print_config_spec()


def test_duckdb_destination_check(new_duckdb_destination: Destination) -> None:
    """Test the JSONL destination."""
    new_duckdb_destination.check()


def test_duckdb_destination_write_components(
    new_duckdb_destination: Destination,
    new_source_faker: Source,
) -> None:
    """Test the JSONL destination."""
    read_result: ReadResult = new_source_faker.read()
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
    new_duckdb_destination._write_airbyte_message_stream(
        stdin=AirbyteMessageGenerator.from_messages(airbyte_messages),
        catalog_provider=CatalogProvider(
            configured_catalog=new_source_faker.configured_catalog
        ),
        progress_tracker=ProgressTracker(),
    )


def test_destination_write_from_source_with_cache(
    new_duckdb_destination: Destination,
    new_source_faker: Source,
) -> None:
    """Test the JSONL destination."""
    write_result: WriteResult = new_duckdb_destination.write(
        data=new_source_faker,
        streams="*",
        cache=new_local_cache(),
        write_strategy=WriteStrategy.AUTO,
    )
    assert write_result


def test_destination_write_from_source_without_cache(
    new_duckdb_destination: Destination,
    new_source_faker: Source,
) -> None:
    """Test the JSONL destination."""
    write_result: WriteResult = new_duckdb_destination.write(
        data=new_source_faker,
        streams="*",
        cache=False,
        write_strategy=WriteStrategy.AUTO,
    )
    assert write_result


def test_destination_write_from_read_result(
    new_duckdb_destination: Destination,
    new_source_faker: Source,
) -> None:
    """Test the JSONL destination."""
    cache = new_local_cache()
    read_result = new_source_faker.read(cache=cache)
    write_result: WriteResult = new_duckdb_destination.write(
        data=read_result,
        streams="*",
        write_strategy=WriteStrategy.AUTO,
        force_full_refresh=False,
    )
    assert write_result
