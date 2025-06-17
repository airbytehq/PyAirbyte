# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A sample execution script which loads data from `source-faker` to a DuckLake-backed cache.

Usage (from repo root):
    poetry install
    poetry run python examples/run_faker_to_ducklake.py
"""

from __future__ import annotations

import sqlite3

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
    data_path="ducklake-data",
)

result = source.read(cache=cache, force_full_refresh=True)

for name, records in result.streams.items():
    print(f"Stream {name}: {len(records)} records")

metadata_db_path = cache.cache_dir / "metadata.db"
if metadata_db_path.exists():
    conn = sqlite3.connect(metadata_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM main.ducklake_data_file")
    row_count = cursor.fetchone()[0]
    print("\nDuckLake verification:")
    print(f"  Metadata DB: {metadata_db_path}")
    print(f"  Data files registered: {row_count}")

    if row_count > 0:
        cursor.execute("SELECT path FROM main.ducklake_data_file LIMIT 5")
        paths = cursor.fetchall()
        print("  Sample file paths:")
        for path in paths:
            print(f"    {path[0]}")
    conn.close()
else:
    print(f"\nDuckLake verification: Metadata DB not found at {metadata_db_path}")
