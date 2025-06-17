# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A sample execution script which loads data from `source-faker` to a DuckLake-backed cache.

Usage (from repo root):
    poetry install
    poetry run python examples/run_faker_to_ducklake.py
"""

from __future__ import annotations

import airbyte as ab
from airbyte.caches import DuckLakeCache


source = ab.get_source(
    "source-faker",
    config={"count": 10000, "seed": 0, "parallelism": 1, "always_updated": False},
    install_if_missing=True,
)
source.check()
source.select_all_streams()

cache = DuckLakeCache(
    catalog_name="airbyte_ducklake_test",
)

result = source.read(cache=cache, force_full_refresh=True)

for name, records in result.streams.items():
    print(f"Stream {name}: {len(records)} records")
