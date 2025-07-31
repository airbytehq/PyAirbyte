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


def setup_source_faker() -> ab.Source:
    """Test the source-faker setup."""
    source = ab.get_source(
        "source-faker",
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
def source_faker() -> ab.Source:
    """Fixture to return a source-faker connector instance."""
    return setup_source_faker()


def test_setup_source_faker() -> None:
    """Test that fixture logic works as expected."""
    source = setup_source_faker()


@pytest.fixture(scope="function")
def duckdb_cache() -> Generator[DuckDBCache, None, None]:
    """Fixture to return a fresh cache."""
    cache: DuckDBCache = new_local_cache()
    yield cache
    # TODO: Delete cache DB file after test is complete.
    return
