# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Integration tests which leverage the source-faker connector to test the framework end-to-end with the Docker Executor."""

from __future__ import annotations

import tempfile
from collections.abc import Generator
from pathlib import Path

import airbyte as ab
import pytest
import ulid
from airbyte.caches.base import CacheBase
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.postgres import PostgresCache
from airbyte.caches.util import new_local_cache

# Product count is always the same, regardless of faker scale.
NUM_PRODUCTS = 100

SEED_A = 1234
SEED_B = 5678

# Number of records in each of the 'users' and 'purchases' streams.
FAKER_SCALE_A = 200
# We want this to be different from FAKER_SCALE_A.
FAKER_SCALE_B = 300


# Patch PATH to include the source-faker executable.


@pytest.fixture(scope="function")  # Each test gets a fresh source-faker instance.
def source_docker_faker_seed_a() -> ab.Source:
    """Fixture to return a source-faker connector instance."""
    source = ab.get_source(
        "source-faker",
        docker_executable=True,
        config={
            "count": FAKER_SCALE_A,
            "seed": SEED_A,
            "parallelism": 16,  # Otherwise defaults to 4.
        },
    )
    source.check()
    source.select_streams([
        "users",
        "products",
        "purchases",
    ])
    return source


@pytest.fixture(scope="function")  # Each test gets a fresh source-faker instance.
def source_docker_faker_seed_b() -> ab.Source:
    """Fixture to return a source-faker connector instance."""
    source = ab.get_source(
        "source-faker",
        docker_executable=True,
        config={
            "count": FAKER_SCALE_B,
            "seed": SEED_B,
            "parallelism": 16,  # Otherwise defaults to 4.
        },
    )
    source.check()
    source.select_streams([
        "products",
        "purchases",
        "users",
    ])
    return source


@pytest.fixture(scope="function")
def duckdb_cache() -> Generator[DuckDBCache, None, None]:
    """Fixture to return a fresh cache."""
    cache: DuckDBCache = new_local_cache()
    yield cache
    # TODO: Delete cache DB file after test is complete.
    return


@pytest.fixture(scope="function")
def postgres_cache(new_postgres_cache) -> Generator[PostgresCache, None, None]:
    """Fixture to return a fresh cache."""
    yield new_postgres_cache
    # TODO: Delete cache DB file after test is complete.
    return


@pytest.fixture
def all_cache_types(
    duckdb_cache: DuckDBCache,
    postgres_cache: PostgresCache,
):
    _ = postgres_cache
    return [
        duckdb_cache,
        postgres_cache,
    ]


def test_faker_pks(
    source_docker_faker_seed_a: ab.Source,
    duckdb_cache: DuckDBCache,
) -> None:
    """Test that the append strategy works as expected."""

    catalog = source_docker_faker_seed_a.configured_catalog

    assert catalog.streams[0].primary_key
    assert catalog.streams[1].primary_key

    read_result = source_docker_faker_seed_a.read(duckdb_cache, write_strategy="append")
    assert read_result.cache.processor._get_primary_keys("products") == ["id"]
    assert read_result.cache.processor._get_primary_keys("purchases") == ["id"]


@pytest.mark.slow
def test_replace_strategy(
    source_docker_faker_seed_a: ab.Source,
    all_cache_types: CacheBase,
) -> None:
    """Test that the append strategy works as expected."""
    for (
        cache
    ) in all_cache_types:  # Function-scoped fixtures can't be used in parametrized().
        for _ in range(2):
            result = source_docker_faker_seed_a.read(
                cache, write_strategy="replace", force_full_refresh=True
            )
            assert len(list(result.cache.streams["products"])) == NUM_PRODUCTS
            assert len(list(result.cache.streams["purchases"])) == FAKER_SCALE_A


@pytest.mark.slow
def test_append_strategy(
    source_docker_faker_seed_a: ab.Source,
    all_cache_types: CacheBase,
) -> None:
    """Test that the append strategy works as expected."""
    for (
        cache
    ) in all_cache_types:  # Function-scoped fixtures can't be used in parametrized().
        for iteration in range(1, 3):
            result = source_docker_faker_seed_a.read(cache, write_strategy="append")
            assert (
                len(list(result.cache.streams["products"])) == NUM_PRODUCTS * iteration
            )
            assert (
                len(list(result.cache.streams["purchases"]))
                == FAKER_SCALE_A * iteration
            )


@pytest.mark.slow
@pytest.mark.parametrize("strategy", ["merge", "auto"])
def test_merge_strategy(
    strategy: str,
    source_docker_faker_seed_a: ab.Source,
    source_docker_faker_seed_b: ab.Source,
    all_cache_types: CacheBase,
) -> None:
    """Test that the merge strategy works as expected.

    Since all streams have primary keys, we should expect the auto strategy to be identical to the
    merge strategy.
    """
    for (
        cache
    ) in all_cache_types:  # Function-scoped fixtures can't be used in parametrized().
        # First run, seed A (counts should match the scale or the product count)
        result = source_docker_faker_seed_a.read(cache, write_strategy=strategy)
        assert len(list(result.cache.streams["products"])) == NUM_PRODUCTS
        assert len(list(result.cache.streams["purchases"])) == FAKER_SCALE_A

        # Second run, also seed A (should have same exact data, no change in counts)
        result = source_docker_faker_seed_a.read(cache, write_strategy=strategy)
        assert len(list(result.cache.streams["products"])) == NUM_PRODUCTS
        assert len(list(result.cache.streams["purchases"])) == FAKER_SCALE_A

        # Third run, seed B - should increase record count to the scale of B, which is greater than A.
        # TODO: See if we can reliably predict the exact number of records, since we use fixed seeds.
        result = source_docker_faker_seed_b.read(cache, write_strategy=strategy)
        assert len(list(result.cache.streams["products"])) == NUM_PRODUCTS
        assert len(list(result.cache.streams["purchases"])) == FAKER_SCALE_B

        # Third run, seed A again - count should stay at scale B, since A is smaller.
        # TODO: See if we can reliably predict the exact number of records, since we use fixed seeds.
        result = source_docker_faker_seed_a.read(cache, write_strategy=strategy)
        assert len(list(result.cache.streams["products"])) == NUM_PRODUCTS
        assert len(list(result.cache.streams["purchases"])) == FAKER_SCALE_B


def test_incremental_sync(
    source_docker_faker_seed_a: ab.Source,
    source_docker_faker_seed_b: ab.Source,
    duckdb_cache: CacheBase,
) -> None:
    config_a = source_docker_faker_seed_a.get_config()
    config_b = source_docker_faker_seed_b.get_config()
    config_a["always_updated"] = False
    config_b["always_updated"] = False
    source_docker_faker_seed_a.set_config(config_a)
    source_docker_faker_seed_b.set_config(config_b)

    result1 = source_docker_faker_seed_a.read(duckdb_cache)
    assert len(list(result1.cache.streams["products"])) == NUM_PRODUCTS
    assert len(list(result1.cache.streams["purchases"])) == FAKER_SCALE_A
    assert result1.processed_records == NUM_PRODUCTS + FAKER_SCALE_A * 2

    assert not duckdb_cache.processor._catalog_manager.get_state("source-faker") == []

    # Second run should not return records as it picks up the state and knows it's up to date.
    result2 = source_docker_faker_seed_b.read(duckdb_cache)

    assert result2.processed_records == 0
    assert len(list(result2.cache.streams["products"])) == NUM_PRODUCTS
    assert len(list(result2.cache.streams["purchases"])) == FAKER_SCALE_A


def test_incremental_state_cache_persistence(
    source_docker_faker_seed_a: ab.Source,
    source_docker_faker_seed_b: ab.Source,
) -> None:
    config_a = source_docker_faker_seed_a.get_config()
    config_b = source_docker_faker_seed_b.get_config()
    config_a["always_updated"] = False
    config_b["always_updated"] = False
    source_docker_faker_seed_a.set_config(config_a)
    source_docker_faker_seed_b.set_config(config_b)
    cache_name = str(ulid.ULID())
    cache = new_local_cache(cache_name)
    result = source_docker_faker_seed_a.read(cache)
    assert result.processed_records == NUM_PRODUCTS + FAKER_SCALE_A * 2
    second_cache = new_local_cache(cache_name)
    # The state should be persisted across cache instances.
    result2 = source_docker_faker_seed_b.read(second_cache)
    assert result2.processed_records == 0

    assert (
        second_cache.processor._catalog_manager
        and second_cache.processor._catalog_manager.get_state("source-faker")
    )
    assert len(list(result2.cache.streams["products"])) == NUM_PRODUCTS
    assert len(list(result2.cache.streams["purchases"])) == FAKER_SCALE_A


def test_incremental_state_prefix_isolation(
    source_docker_faker_seed_a: ab.Source,
    source_docker_faker_seed_b: ab.Source,
) -> None:
    """
    Test that state in the cache correctly isolates streams when different table prefixes are used
    """
    config_a = source_docker_faker_seed_a.get_config()
    config_a["always_updated"] = False
    source_docker_faker_seed_a.set_config(config_a)
    cache_name = str(ulid.ULID())
    db_path = Path(f"./.cache/{cache_name}.duckdb")
    cache = DuckDBCache(db_path=db_path, table_prefix="prefix_")
    different_prefix_cache = DuckDBCache(
        db_path=db_path, table_prefix="different_prefix_"
    )

    result = source_docker_faker_seed_a.read(cache)
    assert result.processed_records == NUM_PRODUCTS + FAKER_SCALE_A * 2

    result2 = source_docker_faker_seed_b.read(different_prefix_cache)
    assert result2.processed_records == NUM_PRODUCTS + FAKER_SCALE_B * 2

    assert len(list(result2.cache.streams["products"])) == NUM_PRODUCTS
    assert len(list(result2.cache.streams["purchases"])) == FAKER_SCALE_B


def test_config_spec(source_docker_faker_seed_a: ab.Source) -> None:
    assert source_docker_faker_seed_a.config_spec


def test_example_config_file(source_docker_faker_seed_a: ab.Source) -> None:
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp:
        source_docker_faker_seed_a.print_config_spec(
            format="json",
            output_file=temp.name,
        )
        assert Path(temp.name).exists()
