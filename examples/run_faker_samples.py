# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A simple test of PyAirbyte, using the Faker source connector.

Usage (from PyAirbyte root directory):
> poetry run python ./examples/run_faker_samples.py
"""

import airbyte as ab

source = ab.get_source(
    "source-faker",
    config={"count": 200_000},
    streams="*",
)

# Print samples of the streams.
source.print_samples()
