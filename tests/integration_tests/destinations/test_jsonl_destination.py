# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Integration tests which test destination capabilities using the JSONL destination (docker-based)."""

from __future__ import annotations

import pytest
from airbyte import get_source
from airbyte._future_cdk.catalog_providers import CatalogProvider
from airbyte._message_generators import MessageGeneratorFromMessages
from airbyte.destinations.base import Destination
from airbyte.executors.base import Executor
from airbyte.executors.util import get_connector_executor
from airbyte.results import ReadResult
from airbyte.sources.base import Source
from airbyte_cdk import AirbyteMessage, AirbyteRecordMessage, Type


@pytest.fixture
def new_jsonl_destination_executor() -> Executor:
    """Return a new JSONL destination executor."""
    return get_connector_executor(
        name="destination-local-json",
        docker_image="airbyte/destination-local-json",
        use_host_network=False,
    )


@pytest.fixture
def new_jsonl_destination(new_jsonl_destination_executor: Destination) -> Destination:
    """Return a new JSONL destination."""
    return Destination(
        name="destination-local-json",
        config={
            "destination_path": "/tmp/airbyte/destination-local-json/",
        },
        executor=new_jsonl_destination_executor,
    )


def test_jsonl_destination_spec(new_jsonl_destination: Destination) -> None:
    """Test the JSONL destination."""
    new_jsonl_destination.print_config_spec()


def test_jsonl_destination_check(new_jsonl_destination: Destination) -> None:
    """Test the JSONL destination."""
    new_jsonl_destination.check()


def test_jsonl_destination_write(new_jsonl_destination: Destination) -> None:
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
    new_jsonl_destination.write(
        stdin=MessageGeneratorFromMessages(airbyte_messages),
        catalog_provider=CatalogProvider(source.configured_catalog),
    )

    # Container paths are not accessible from the host.

    # # Check the output.
    # assert os.path.exists("/tmp/airbyte/destination-jsonl/users.jsonl")
    # assert os.path.getsize("/tmp/airbyte/destination-jsonl/users.jsonl") > 0
    # assert os.path.isfile("/tmp/airbyte/destination-jsonl/users.jsonl")

    # # Clean up.
    # os.remove("/tmp/airbyte/destination-jsonl/users.jsonl")
    # os.rmdir("/tmp/airbyte/destination-jsonl")
