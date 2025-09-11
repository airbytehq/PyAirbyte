# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Integration tests which ensure state handling is correct."""

from __future__ import annotations

from pathlib import Path

import airbyte as ab
import pytest
from airbyte._util import text_util
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.util import new_local_cache
from airbyte.shared.state_providers import StateProviderBase
from airbyte.shared.state_writers import StateWriterBase

from airbyte_protocol import models


# Product count is always the same, regardless of faker scale.
NUM_PRODUCTS = 100

SEED_A = 1234
SEED_B = 5678

# Number of records in each of the 'users' and 'purchases' streams.
FAKER_SCALE_A = 200
# We want this to be different from FAKER_SCALE_A.
FAKER_SCALE_B = 300


@pytest.fixture(scope="function")  # Each test gets a fresh source-faker instance.
def source_faker_seed_a(*, use_docker: bool) -> ab.Source:
    """Fixture to return a source-faker connector instance."""
    source = ab.get_source(
        "source-faker",
        config={
            "count": FAKER_SCALE_A,
            "seed": SEED_A,
            "parallelism": 16,  # Otherwise defaults to 4.
        },
        streams=["users", "products", "purchases"],
        docker_image=use_docker,
    )
    return source


@pytest.fixture(scope="function")  # Each test gets a fresh source-faker instance.
def source_faker_seed_b(*, use_docker: bool) -> ab.Source:
    """Fixture to return a source-faker connector instance."""
    source = ab.get_source(
        "source-faker",
        config={
            "count": FAKER_SCALE_B,
            "seed": SEED_B,
            "parallelism": 16,  # Otherwise defaults to 4.
        },
        streams=["users", "products", "purchases"],
        docker_image=use_docker,
    )
    return source


def test_incremental_state_cache_persistence(
    source_faker_seed_a: ab.Source,
    source_faker_seed_b: ab.Source,
) -> None:
    config_a = source_faker_seed_a.get_config()
    config_b = source_faker_seed_b.get_config()
    config_a["always_updated"] = False  # disable ensuring new `updated_at` timestamps
    config_b["always_updated"] = False  # disable ensuring new `updated_at` timestamps
    source_faker_seed_a.set_config(config_a)
    source_faker_seed_b.set_config(config_b)

    cache_name = text_util.generate_random_suffix()
    cache = new_local_cache(cache_name)
    result = source_faker_seed_a.read(cache)
    assert result.processed_records == NUM_PRODUCTS + FAKER_SCALE_A * 2
    second_cache = new_local_cache(cache_name)
    # The state should be persisted across cache instances.
    result2 = source_faker_seed_b.read(second_cache)
    assert result2.processed_records == 0

    state_provider = second_cache.get_state_provider("source-faker")
    assert len(list(state_provider.state_message_artifacts)) > 0

    assert len(list(result2.cache.streams["products"])) == NUM_PRODUCTS
    assert len(list(result2.cache.streams["purchases"])) == FAKER_SCALE_A

    assert state_provider.get_stream_state("users")
    assert state_provider.get_stream_state("products")
    assert state_provider.get_stream_state("purchases")


def test_incremental_state_prefix_isolation(
    source_faker_seed_a: ab.Source,
    source_faker_seed_b: ab.Source,
) -> None:
    """
    Test that state in the cache correctly isolates streams when different table prefixes are used
    """
    config_a = source_faker_seed_a.get_config()
    config_a["always_updated"] = False  # disable ensuring new `updated_at` timestamps
    source_faker_seed_a.set_config(config_a)
    cache_name = text_util.generate_random_suffix()
    db_path = Path(f"./.cache/{cache_name}.duckdb")
    cache = DuckDBCache(db_path=db_path, table_prefix="prefix_")
    different_prefix_cache = DuckDBCache(
        db_path=db_path, table_prefix="different_prefix_"
    )

    result = source_faker_seed_a.read(cache)
    assert result.processed_records == NUM_PRODUCTS + FAKER_SCALE_A * 2

    result2 = source_faker_seed_b.read(different_prefix_cache)
    assert result2.processed_records == NUM_PRODUCTS + FAKER_SCALE_B * 2

    assert len(list(result2.cache.streams["products"])) == NUM_PRODUCTS
    assert len(list(result2.cache.streams["purchases"])) == FAKER_SCALE_B


def test_destination_state_writer() -> None:
    """Test destination state writer."""
    cache = ab.new_local_cache("aj_test06")

    state_writer: StateWriterBase = cache.get_state_writer(
        source_name="source-foo", destination_name="destination-bar"
    )
    for i in range(1, 4):
        state_writer.write_state(
            models.AirbyteStateMessage(
                type="STREAM",
                stream=models.AirbyteStreamState(
                    stream_descriptor=models.StreamDescriptor(name=f"stream{i}"),
                ),
                data=None,
            )  # type: ignore  # missing 'global' (class def error)
        )

    assert state_writer.known_stream_names == {
        "stream1",
        "stream2",
        "stream3",
    }
    state_provider: StateProviderBase = cache.get_state_provider(
        source_name="source-foo",
        destination_name="destination-bar",
    )
    assert state_provider.known_stream_names == {
        "stream1",
        "stream2",
        "stream3",
    }


@pytest.fixture(scope="function")
def e2e_test_destination() -> ab.Destination:
    return ab.get_destination(
        name="destination-e2e-test",
        config={
            "test_destination": {
                "test_destination_type": "LOGGING",
                "logging_config": {
                    "logging_type": "FirstN",
                    "max_entry_count": 100,
                },
            }
        },
        docker_image=True,
    )


def test_destination_state(
    source_faker_seed_a: ab.Source,
    e2e_test_destination: ab.Destination,
) -> None:
    """Test destination state handling."""
    # config_a = source_faker_seed_a.get_config()
    # config_a["always_updated"] = False  # disable ensuring new `updated_at` timestamps
    # source_faker_seed_a.set_config(config_a)

    cache = ab.new_local_cache("aj_test05")

    source_faker_seed_a.select_streams(["products", "users"])
    read_result = source_faker_seed_a.read(cache)
    # assert read_result.processed_records == NUM_PRODUCTS + FAKER_SCALE_A * 2

    cache_state_provider = cache.get_state_provider("source-faker")
    assert cache_state_provider.known_stream_names == {
        "users",
        "products",
        # "purchases",
    }
    cache_users_states = cache_state_provider.get_stream_state("products")
    assert cache_users_states

    write_result = e2e_test_destination.write(
        read_result,
        state_cache=cache,
    )
    # assert write_result.processed_records == NUM_PRODUCTS + FAKER_SCALE_A * 2
    write_result_state_provider = write_result.get_state_provider()
    assert write_result_state_provider.known_stream_names == {
        "users",
        "products",
        # "purchases",
    }
    destination_state_provider = cache.get_state_provider(
        source_name="source-faker",
        destination_name=e2e_test_destination.name,
        refresh=True,
    )
    assert destination_state_provider.known_stream_names == {
        "users",
        "products",
        # "purchases",
    }
    destination_users_states = destination_state_provider.get_stream_state(
        "products", None
    )
    assert destination_users_states
    assert cache_users_states == destination_users_states
