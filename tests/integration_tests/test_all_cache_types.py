# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Integration tests which leverage the source-faker connector to test the framework end-to-end.

Since source-faker is included in dev dependencies, we can assume `source-faker` is installed
and available on PATH for the poetry-managed venv.
"""
from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest

import airbyte as ab
from airbyte._executor import _get_bin_dir


# Product count is always the same, regardless of faker scale.
NUM_PRODUCTS = 100

SEED_A = 1234
SEED_B = 5678

# Number of records in each of the 'users' and 'purchases' streams.
FAKER_SCALE_A = 200
# We want this to be different from FAKER_SCALE_A.
FAKER_SCALE_B = 300


# Patch PATH to include the source-faker executable.

@pytest.fixture(autouse=True)
def add_venv_bin_to_path(monkeypatch):
    # Get the path to the bin directory of the virtual environment
    venv_bin_path = str(_get_bin_dir(Path(sys.prefix)))

    # Add the bin directory to the PATH
    new_path = f"{venv_bin_path}{os.pathsep}{os.environ['PATH']}"
    monkeypatch.setenv('PATH', new_path)


@pytest.fixture(scope="function")  # Each test gets a fresh source-faker instance.
def source_faker_seed_a() -> ab.Source:
    """Fixture to return a source-faker connector instance."""
    source = ab.get_source(
        "source-faker",
        local_executable="source-faker",
        config={
            "count": FAKER_SCALE_A,
            "seed": SEED_A,
            "parallelism": 16,  # Otherwise defaults to 4.
        },
        install_if_missing=False,  # Should already be on PATH
    )
    source.check()
    source.select_streams([
        "users",
    ])
    return source


@pytest.fixture(scope="function")  # Each test gets a fresh source-faker instance.
def source_faker_seed_b() -> ab.Source:
    """Fixture to return a source-faker connector instance."""
    source = ab.get_source(
        "source-faker",
        local_executable="source-faker",
        config={
            "count": FAKER_SCALE_B,
            "seed": SEED_B,
            "parallelism": 16,  # Otherwise defaults to 4.
        },
        install_if_missing=False,  # Should already be on PATH
    )
    source.check()
    source.select_streams([
        "users",
    ])
    return source


# Uncomment this line if you want to see performance trace logs.
# You can render perf traces using the viztracer CLI or the VS Code VizTracer Extension.
#@viztracer.trace_and_save(output_dir=".pytest_cache/snowflake_trace/")
@pytest.mark.requires_creds
@pytest.mark.slow
def test_faker_read(
    source_faker_seed_a: ab.Source,
    new_generic_cache: ab.caches.CacheBase,
) -> None:
    """Test that the append strategy works as expected."""
    result = source_faker_seed_a.read(
        new_generic_cache, write_strategy="replace", force_full_refresh=True
    )
    assert len(list(result.cache.streams["users"])) == FAKER_SCALE_A


@pytest.mark.requires_creds
@pytest.mark.slow
def test_replace_strategy(
    source_faker_seed_a: ab.Source,
    source_faker_seed_b: ab.Source,
    new_generic_cache: ab.caches.CacheBase,
    mocker: pytest.MockerFixture,
) -> None:
    """Test that the replace strategy works as expected.

    We expect old data to be fully replaced with newer data. For this test,
    we run the 'seed b' test (expected 300 records) before the 'seed a' test
    (expected 200 records). We assert the correct count of records at each step,
    first 300 and then 200 records.
    """
    mocker.spy(new_generic_cache.processor, '_swap_temp_table_with_final_table')
    mocker.spy(new_generic_cache.processor, '_merge_temp_table_to_final_table')

    assert FAKER_SCALE_B > FAKER_SCALE_A, \
        "The test requires that 'b' has a greater count of records than 'a'."

    result = source_faker_seed_b.read(
        new_generic_cache,
        write_strategy="replace",
        force_full_refresh=False,
    )
    assert len(list(result.cache.streams["users"])) == FAKER_SCALE_B

    result = source_faker_seed_a.read(
        new_generic_cache,
        write_strategy="replace",
        force_full_refresh=False,
    )
    assert len(list(result.cache.streams["users"])) == FAKER_SCALE_A

    # Check the call count
    assert new_generic_cache.processor._swap_temp_table_with_final_table.call_count == 2
    assert new_generic_cache.processor._merge_temp_table_to_final_table.call_count == 0


@pytest.mark.requires_creds
@pytest.mark.slow
def test_merge_strategy(
    source_faker_seed_a: ab.Source,
    source_faker_seed_b: ab.Source,
    new_generic_cache: ab.caches.CacheBase,
) -> None:
    """Test that the merge strategy works as expected.

    Since all streams have primary keys, we should expect the auto strategy to be identical to the
    merge strategy.
    """

    assert new_generic_cache, "Cache should not be None."

    # First run, seed A (counts should match the scale or the product count)
    result = source_faker_seed_a.read(new_generic_cache, write_strategy="merge")
    assert len(list(result.cache.streams["users"])) == FAKER_SCALE_A, \
        f"Incorrect number of records in the cache. {new_generic_cache}"

    # Second run, also seed A (should have same exact data, no change in counts)
    result = source_faker_seed_a.read(new_generic_cache, write_strategy="merge")
    assert len(list(result.cache.streams["users"])) == FAKER_SCALE_A

    # Third run, seed B - should increase record count to the scale of B, which is greater than A.
    # TODO: See if we can reliably predict the exact number of records, since we use fixed seeds.
    result = source_faker_seed_b.read(new_generic_cache, write_strategy="merge")
    assert len(list(result.cache.streams["users"])) == FAKER_SCALE_B

    # Third run, seed A again - count should stay at scale B, since A is smaller.
    # TODO: See if we can reliably predict the exact number of records, since we use fixed seeds.
    result = source_faker_seed_a.read(new_generic_cache, write_strategy="merge")
    assert len(list(result.cache.streams["users"])) == FAKER_SCALE_B
