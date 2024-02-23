# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A sample execution script which loads data from `source-faker` to a MotherDuck-backed cache.

Usage (from repo root):
    poetry install
    poetry run python examples/run_faker_to_motherduck.py
"""

from __future__ import annotations

import airbyte as ab
from airbyte.caches import MotherDuckCache


MOTHERDUCK_API_KEY = ab.get_secret("MOTHERDUCK_API_KEY")
"""This is the API key for the MotherDuck service.

It can be auto-detected in env vars and/or a .env file in the root of the project.

If will be prompted (and masked during input) if not found in either location.
"""


source = ab.get_source(
    "source-faker",
    config={"count": 10000, "seed": 0, "parallelism": 1, "always_updated": False},
    install_if_missing=True,
)
source.check()
source.select_all_streams()

cache = MotherDuckCache(
    database="airbyte_test",
    schema_name="faker_data",
    api_key=MOTHERDUCK_API_KEY,
)

result = source.read(cache=cache)

for name, records in result.streams.items():
    print(f"Stream {name}: {len(records)} records")
