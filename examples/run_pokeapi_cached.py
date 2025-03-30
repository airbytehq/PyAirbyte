# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A simple test of PyAirbyte, using the PokeAPI source connector.

Usage (from PyAirbyte root directory):
> poetry run python ./examples/run_pokeapi_cached.py

No setup is needed, but you may need to delete the .venv-source-pokeapi folder
if your installation gets interrupted or corrupted.
"""

from __future__ import annotations

import airbyte as ab
from airbyte import get_source, AirbyteConnectorCache

# Create an HTTP cache
cache = AirbyteConnectorCache(
    mode="read_write",
    serialization_format="native"  # Use mitmproxy's native format
)
cache.start()

source = get_source(
    "source-pokeapi",
    config={"pokemon_name": "bulbasaur"},
    source_manifest=True,
    http_cache=cache,
    streams=["pokemon"],
)
source.check()

# # print(list(source.get_records("pokemon")))
source.read(cache=ab.new_local_cache("poke"))

cache.stop()
