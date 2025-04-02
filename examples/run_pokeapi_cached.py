# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""A simple test of PyAirbyte, using the PokeAPI source connector with HTTP caching.

Usage (from PyAirbyte root directory):
> poetry run python ./examples/run_pokeapi_cached.py

No setup is needed, but you may need to delete the .venv-source-pokeapi folder
if your installation gets interrupted or corrupted.

This example demonstrates HTTP caching using mitmproxy's CLI-based approach.
It will make HTTP requests on the first run and use cached responses on subsequent runs.

Requirements:
- mitmproxy must be installed and available in the PATH
"""

from __future__ import annotations

import os
import airbyte as ab
from pathlib import Path
from airbyte import get_source, AirbyteConnectorCache

cache_dir = Path(".airbyte-http-cache")
os.makedirs(cache_dir, exist_ok=True)
Path(cache_dir / ".gitignore").write_text("# Ignore all files in this directory\n*")

# Create an HTTP cache
http_cache = AirbyteConnectorCache(
    cache_dir=cache_dir,
    mode="read_write",
    serialization_format="native",  # Use mitmproxy's native format
)

# Start the proxy - this will launch mitmdump in a separate process

port = http_cache.start()
print(f"HTTP cache started on port {port}")

local_cache: ab.DuckDBCache = ab.new_local_cache("poke")

print("First run - making HTTP requests...")
source: ab.Source = get_source(
    "source-pokeapi",
    config={"pokemon_name": "bulbasaur"},
    docker_image=True,
    use_host_network=True,
    http_cache=http_cache,
    streams=["pokemon"],
)
source.check()
print("Source check successful")
source.read(cache=local_cache)
print("First run completed")

print("Second run - should use cached responses...")
source.read(cache=local_cache)
print("Second run completed")

print("Stopping HTTP cache...")
http_cache.stop()
print("HTTP cache stopped")

print(
    "Note: If you want to inspect the cached responses, check the .airbyte-http-cache directory"
)
