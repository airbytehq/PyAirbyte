# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Integration tests which leverage the source-faker connector to test the framework end-to-end.

Since source-faker is included in dev dependencies, we can assume `source-faker` is installed
and available on PATH for the poetry-managed venv.
"""

from __future__ import annotations

import datetime
import os
import sys
from pathlib import Path

import airbyte as ab
import pytest
from airbyte import get_source
from airbyte._util.venv_util import get_bin_dir
from sqlalchemy import text
from viztracer import VizTracer

from airbyte.results import ReadResult

# Product count is always the same, regardless of faker scale.
NUM_PRODUCTS = 100

SEED_A = 1234
SEED_B = 5678

# Number of records in each of the 'users' and 'purchases' streams.
FAKER_SCALE_A = 200
# We want this to be different from FAKER_SCALE_A.
FAKER_SCALE_B = 300


# Patch PATH to include the source-faker executable.


# Fixture to dynamically generate output_dir based on the test name
@pytest.fixture
def tracer(request):
    # Get date in yyyy-mm-dd format
    timestamp: str = datetime.datetime.now().strftime("%Y-%m-%d-%H%M")

    # Format the directory path to include the parameterized test name
    output_dir = (
        f"./logs/viztracer/{request.node.name.replace('[', '/').replace(']', '')}"
        f"/viztracer-{timestamp}-{request.node.name.replace('[', '-').replace(']', '')}.json"
    )
    tracer = VizTracer(output_file=output_dir)
    yield tracer
    tracer.stop()


@pytest.fixture(autouse=True)
def add_venv_bin_to_path(monkeypatch):
    # Get the path to the bin directory of the virtual environment
    venv_bin_path = str(get_bin_dir(Path(sys.prefix)))

    # Add the bin directory to the PATH
    new_path = f"{venv_bin_path}{os.pathsep}{os.environ['PATH']}"
    monkeypatch.setenv("PATH", new_path)


@pytest.fixture(scope="function")  # Each test gets a fresh source-faker instance.
@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="source-faker is not yet compatible with Python 3.12",
)
def source_faker_seed_a() -> ab.Source:
    """Fixture to return a source-faker connector instance."""
    source = get_source(
        "source-faker",
        config={
            "count": FAKER_SCALE_A,
            "seed": SEED_A,
            "parallelism": 16,  # Otherwise defaults to 4.
        },
        streams=["users"],
    )
    return source


@pytest.fixture(scope="function")  # Each test gets a fresh source-faker instance.
@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="source-faker is not yet compatible with Python 3.12",
)
def source_faker_seed_b() -> ab.Source:
    """Fixture to return a source-faker connector instance."""
    source = get_source(
        "source-faker",
        config={
            "count": FAKER_SCALE_B,
            "seed": SEED_B,
            "parallelism": 16,  # Otherwise defaults to 4.
        },
        streams=["users"],
    )
    return source


@pytest.fixture(scope="function")  # Each test gets a fresh source instance.
def source_pokeapi() -> ab.Source:
    """Fixture to return a source-faker connector instance."""
    source = get_source(
        "source-pokeapi",
        config={
            "pokemon_name": "pikachu",
        },
        source_manifest=True,
        streams="*",
    )
    return source


@pytest.mark.slow
@pytest.mark.skipif(
    "CI" in os.environ,
    reason="Fails inexplicably when run in CI. https://github.com/airbytehq/PyAirbyte/issues/146",
)
def test_pokeapi_read(
    source_pokeapi: ab.Source,
    new_generic_cache: ab.caches.CacheBase,
) -> None:
    """Test that PokeAPI source can load to all cache types.

    This is a meaningful test because the PokeAPI source uses JSON data types.
    """
    result = source_pokeapi.read(
        new_generic_cache, write_strategy="replace", force_full_refresh=True
    )
    assert len(list(result.cache.streams["pokemon"])) == 1


# Uncomment this line if you want to see performance trace logs.
# You can render perf traces using the viztracer CLI or the VS Code VizTracer Extension.
@pytest.mark.requires_creds
@pytest.mark.slow
def test_faker_read(
    source_faker_seed_a: ab.Source,
    new_generic_cache: ab.caches.CacheBase,
    tracer: VizTracer,
) -> None:
    """Test that the append strategy works as expected."""
    with tracer:
        read_result = source_faker_seed_a.read(
            new_generic_cache, write_strategy="replace", force_full_refresh=True
        )
    configured_count = source_faker_seed_a._config["count"]

    # Check row counts match:
    assert len(list(read_result.cache.streams["users"])) == FAKER_SCALE_A

    progress = read_result._progress_tracker

    # These numbers expect only 'users' stream selected:
    assert progress.total_records_read == configured_count
    assert progress.total_records_written == configured_count
    assert progress.total_batches_written == 1
    assert progress.total_batches_finalized == 1
    assert progress.finalized_stream_names == ["users"]

    status_msg: str = progress._get_status_message()
    assert "Read **0** records" not in status_msg
    assert f"Read **{configured_count}** records" in status_msg

    if "bigquery" not in new_generic_cache.get_sql_alchemy_url():
        # BigQuery doesn't support to_arrow
        # https://github.com/airbytehq/PyAirbyte/issues/165
        arrow_dataset = read_result["users"].to_arrow(max_chunk_size=10)
        assert arrow_dataset.count_rows() == FAKER_SCALE_A
        assert sum(1 for _ in arrow_dataset.to_batches()) == FAKER_SCALE_A / 10

    # TODO: Uncomment this line after resolving https://github.com/airbytehq/PyAirbyte/issues/165
    # assert len(result["users"].to_pandas()) == FAKER_SCALE_A


@pytest.mark.requires_creds
@pytest.mark.slow
def test_append_strategy(
    source_faker_seed_a: ab.Source,
    new_duckdb_cache: ab.caches.CacheBase,
) -> None:
    """Test that the append strategy works as expected."""
    result: ReadResult
    for _ in range(2):
        assert isinstance(new_duckdb_cache, ab.caches.CacheBase)
        result = source_faker_seed_a.read(
            new_duckdb_cache, write_strategy="append", force_full_refresh=True
        )
    assert len(list(result.cache.streams["users"])) == FAKER_SCALE_A * 2


@pytest.mark.requires_creds
@pytest.mark.slow
def test_replace_strategy(
    source_faker_seed_a: ab.Source,
    new_generic_cache: ab.caches.CacheBase,
) -> None:
    """Test that the append strategy works as expected."""
    result: ReadResult
    for _ in range(2):
        result = source_faker_seed_a.read(
            new_generic_cache, write_strategy="replace", force_full_refresh=True
        )
    assert len(list(result.cache.streams["users"])) == FAKER_SCALE_A


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
    assert len(list(result.cache.streams["users"])) == FAKER_SCALE_A, (
        f"Incorrect number of records in the cache. {new_generic_cache}"
    )

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


@pytest.mark.requires_creds
@pytest.mark.slow
def test_auto_add_columns(
    source_faker_seed_a: ab.Source,
    new_generic_cache: ab.caches.CacheBase,
) -> None:
    """Test that the auto-add columns works as expected."""
    # Start with a normal read.
    result = source_faker_seed_a.read(
        new_generic_cache,
        write_strategy="auto",
    )
    table_name: str = result["users"].to_sql_table().name

    # Ensure that the raw ID column is present. Then delete it and confirm it's gone.
    assert "_airbyte_raw_id" in result["users"].to_sql_table().columns
    with new_generic_cache.processor.get_sql_connection() as conn:
        conn.execute(
            text(
                f"ALTER TABLE {new_generic_cache.schema_name}.{table_name} "
                "DROP COLUMN _airbyte_raw_id"
            ),
        )
    new_generic_cache.processor._invalidate_table_cache(table_name)

    assert "_airbyte_raw_id" not in result["users"].to_sql_table().columns

    new_generic_cache.processor._invalidate_table_cache(table_name)

    # Now re-read the stream with the auto strategy and ensure the column is back.
    result = source_faker_seed_a.read(cache=new_generic_cache, write_strategy="auto")

    assert "_airbyte_raw_id" in result["users"].to_sql_table().columns
