# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Integration tests which leverage the source-faker connector to test the framework end-to-end.

Since source-faker is included in dev dependencies, we can assume `source-faker` is installed
and available on PATH for the poetry-managed venv.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Generator
from pathlib import Path

import airbyte as ab
import pytest
from airbyte._util.venv_util import get_bin_dir
from airbyte.caches.duckdb import DuckDBCache
from airbyte.caches.util import new_local_cache


# Product count is always the same, regardless of faker scale.
NUM_PRODUCTS = 100

SEED_A = 1234
SEED_B = 5678

# Number of records in each of the 'users' and 'purchases' streams.
FAKER_SCALE_A = 200
# We want this to be different from FAKER_SCALE_A.
FAKER_SCALE_B = 300


@pytest.fixture(autouse=True)
def add_venv_bin_to_path(monkeypatch):
    """Patch the PATH to include the virtual environment's bin directory."""
    # Get the path to the bin directory of the virtual environment
    venv_bin_path = str(get_bin_dir(Path(sys.prefix)))

    # Add the bin directory to the PATH
    new_path = f"{venv_bin_path}{os.pathsep}{os.environ['PATH']}"
    monkeypatch.setenv("PATH", new_path)


def setup_source_faker(*, use_docker: bool) -> ab.Source:
    """Test the source-faker setup."""
    source = ab.get_source(
        "source-faker",
        config={
            "count": FAKER_SCALE_A,
            "seed": SEED_A,
            "parallelism": 16,  # Otherwise defaults to 4.
        },
        docker_image=use_docker,
    )
    source.check()
    source.select_streams([
        "users",
        "products",
        "purchases",
    ])
    return source


@pytest.fixture(scope="function")  # Each test gets a fresh source-faker instance.
def source_faker(*, use_docker: bool) -> ab.Source:
    """Fixture to return a source-faker connector instance."""
    return setup_source_faker(use_docker=use_docker)


def test_setup_source_faker(*, use_docker: bool) -> None:
    """Test that fixture logic works as expected."""
    source = setup_source_faker(use_docker=use_docker)


@pytest.fixture(scope="function")
def duckdb_cache() -> Generator[DuckDBCache, None, None]:
    """Fixture to return a fresh cache."""
    cache: DuckDBCache = new_local_cache()
    yield cache
    # TODO: Delete cache DB file after test is complete.
    return


def test_duckdb_connection_cleanup(tmp_path):
    """Test that DuckDB connections are properly released after cache operations.

    This test verifies the fix for issue #807: DuckDB Cache Does Not Release Database Connections.

    Reproduction steps:
    1. Create a DuckDB cache and write data to it
    2. Delete the cache object or call close()
    3. Attempt to open a new connection to the same database file
    4. Verify the new connection succeeds without errors
    """
    import duckdb

    db_path = tmp_path / "test_connection_cleanup.duckdb"

    cache = DuckDBCache(db_path=db_path)

    cache.processor._ensure_schema_exists()

    cache.processor._execute_sql(
        "CREATE TABLE IF NOT EXISTS test_table (id INTEGER, name VARCHAR)"
    )
    cache.processor._execute_sql("INSERT INTO test_table VALUES (1, 'test')")

    cache.close()

    try:
        conn = duckdb.connect(str(db_path))
        result = conn.execute("SELECT * FROM main.test_table").fetchall()
        assert len(result) == 1
        assert result[0] == (1, "test")
        conn.close()
    except Exception as e:
        pytest.fail(f"Failed to open new connection after cache cleanup: {e}")


def test_duckdb_context_manager_cleanup(tmp_path):
    """Test that DuckDB connections are cleaned up when using cache as context manager."""
    import duckdb

    db_path = tmp_path / "test_context_manager.duckdb"

    with DuckDBCache(db_path=db_path) as cache:
        cache.processor._execute_sql(
            "CREATE TABLE IF NOT EXISTS test_table (id INTEGER)"
        )
        cache.processor._execute_sql("INSERT INTO test_table VALUES (1)")

    conn = duckdb.connect(str(db_path))
    result = conn.execute("SELECT * FROM main.test_table").fetchall()
    assert len(result) == 1
    conn.close()


def test_duckdb_del_cleanup(tmp_path):
    """Test that DuckDB connections are cleaned up via __del__ when cache is garbage collected."""
    import duckdb
    import gc

    db_path = tmp_path / "test_del_cleanup.duckdb"

    def create_and_use_cache():
        cache = DuckDBCache(db_path=db_path)
        cache.processor._execute_sql(
            "CREATE TABLE IF NOT EXISTS test_table (id INTEGER)"
        )
        cache.processor._execute_sql("INSERT INTO test_table VALUES (1)")

    create_and_use_cache()

    gc.collect()

    conn = duckdb.connect(str(db_path))
    result = conn.execute("SELECT * FROM main.test_table").fetchall()
    assert len(result) == 1
    conn.close()
