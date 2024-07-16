# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""
Simple script to get performance profile of read throughput.

For performance profiling:
```
poetry run viztracer --open -- ./examples/run_perf_test_reads.py
```
"""

from __future__ import annotations

import airbyte as ab


cache: ab.DuckDBCache = ab.new_local_cache()

source = ab.get_source(
    "source-faker",
    config={"count": 10_000},
    install_if_missing=False,
    streams=["users"],
)
source.check()

result = source.read(cache)
