# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A simple test of PyAirbyte, using the Faker source connector.

Usage (from PyAirbyte root directory):
> poetry run python ./examples/run_faker.py

No setup is needed, but you may need to delete the .venv-source-faker folder
if your installation gets interrupted or corrupted.
"""

from __future__ import annotations

import airbyte as ab


SCALE = 200_000  # Number of records to generate between users and purchases.
FORCE_FULL_REFRESH = True  # Whether to force a full refresh on the source.


print("Initializing cache...")
cache = ab.get_default_cache()


print("Installing Faker source...")
source = ab.get_source(
    "source-faker",
    config={"count": SCALE / 2},
    install_if_missing=True,
)
print("Faker source installed.")
source.check()
source.select_streams(["products", "users", "purchases"])

print("Reading from source...")
result = source.read(
    cache=cache,
    force_full_refresh=FORCE_FULL_REFRESH,
)

print("Read complete. Validating results...")
for name, records in result.streams.items():
    print(f"Stream {name}: {len(records)} records")
