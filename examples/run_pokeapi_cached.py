# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A simple test of PyAirbyte, using the PokeAPI source connector with HTTP caching.

Usage (from PyAirbyte root directory):
> poetry run python ./examples/run_pokeapi_cached.py

No setup is needed, but you may need to delete the .venv-source-pokeapi folder
if your installation gets interrupted or corrupted.

This example demonstrates HTTP caching using mitmproxy's native format.
It will make HTTP requests on the first run and use cached responses on subsequent runs.
"""

from __future__ import annotations

import asyncio
import os
import airbyte as ab
from pathlib import Path
from airbyte import get_source, AirbyteConnectorCache

cache_dir = Path(".airbyte-http-cache")
os.makedirs(cache_dir, exist_ok=True)
Path(cache_dir / ".gitignore").write_text("# Ignore all files in this directory\n*")

# Create an HTTP cache
cache = AirbyteConnectorCache(
    cache_dir=cache_dir,
    mode="read_write",
    serialization_format="native",  # Use mitmproxy's native format
)

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

port = cache.start()
print(f"HTTP cache started on port {port}")

source = get_source(
    "source-pokeapi",
    config={"pokemon_name": "bulbasaur"},
    source_manifest=True,
    streams=["pokemon"],
)

print("Checking source connection...")
source.check()
print("Source connection successful")

local_cache = ab.new_local_cache("poke")

print("First run - making HTTP requests...")
source_with_cache = get_source(
    "source-pokeapi",
    config={"pokemon_name": "bulbasaur"},
    source_manifest=True,
    http_cache=cache,
    streams=["pokemon"],
)
source_with_cache.read(cache=local_cache)
print("First run completed")

print("Second run - should use cached responses...")
source.read(cache=local_cache)
print("Second run completed")

print("Stopping HTTP cache...")
cache.stop()
print("HTTP cache stopped")
