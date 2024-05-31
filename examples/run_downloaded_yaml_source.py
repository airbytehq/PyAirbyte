# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A test of PyAirbyte calling a declarative manifest.

Usage (from PyAirbyte root directory):
> poetry run python examples/run_downloadable_yaml_source.py

"""

from __future__ import annotations

from typing import cast

import yaml

import airbyte as ab
from airbyte.experimental import get_source


nl = "\n"
print(
    f"Downloadable yaml sources: \n- {(nl + '- ').join(ab.get_available_connectors(install_type='yaml'))}"
)

print("Running declarative source...")
source = get_source(
    "source-xkcd",
    config={},
    source_manifest=True,
)
source.check()
source.select_all_streams()

result = source.read()

for name, records in result.streams.items():
    print(f"Stream {name}: {len(records)} records")
