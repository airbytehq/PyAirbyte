# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A test of PyAirbyte calling a declarative manifest.

Usage (from PyAirbyte root directory):
> poetry run python examples/run_downloadable_yaml_source.py

"""

from __future__ import annotations

import airbyte as ab
from airbyte import get_source

yaml_connectors: list[str] = ab.get_available_connectors(install_type="yaml")

print(
    f"Downloadable yaml sources ({len(yaml_connectors)}): \n- "
    + "\n- ".join(yaml_connectors)
)

failed_installs: dict[str, list[str]] = {}

for yaml_connector in yaml_connectors:
    try:
        _ = get_source(yaml_connector, source_manifest=True)
    except Exception as ex:
        exception_type = type(ex).__name__
        if exception_type in failed_installs:
            failed_installs[exception_type].append(yaml_connector)
        else:
            failed_installs[exception_type] = [yaml_connector]

# Print any connector failures, grouped by the error message
for error, connectors_failed in failed_installs.items():
    print(
        f"\nInstallation Errors ({len(failed_installs)}): {error}\n- "
        + "\n- ".join(connectors_failed)
        + "\n"
    )

print("Running declarative source...")
source = get_source(
    "source-pokeapi",
    config={
        "pokemon_name": "ditto",
    },
    source_manifest=True,
)
source.check()
source.select_all_streams()

result = source.read()

for name, records in result.streams.items():
    print(f"Stream {name}: {len(records)} records")
