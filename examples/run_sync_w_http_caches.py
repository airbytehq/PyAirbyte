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
from typing import Any

import airbyte as ab
from pathlib import Path
from airbyte import AirbyteConnectorCache
from airbyte.secrets.google_gsm import GoogleGSMSecretManager


CONNECTOR_NAME = "source-github"

AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"

secret_mgr = GoogleGSMSecretManager(
    project=AIRBYTE_INTERNAL_GCP_PROJECT,
    credentials_json=ab.get_secret("GCP_GSM_CREDENTIALS"),
)

CONNECTOR_CONFIGS = {
    "source-pokeapi": {"pokemon_name": "pikachu"},
}
CONNECTOR_STREAMS = {
    "source-pokeapi": ["pokemon"],
    "source-github": ["repositories", "issues"],
}

cache_dir = Path(".airbyte-http-cache")
os.makedirs(cache_dir, exist_ok=True)
Path(cache_dir / ".gitignore").write_text("# Ignore all files in this directory\n*")

local_cache: ab.DuckDBCache = ab.new_local_cache("cache_testing")

# Create an HTTP cache
http_cache = AirbyteConnectorCache(
    cache_dir=cache_dir,
    mode="read_write",
    serialization_format="native",  # Use mitmproxy's native format
)


def get_connector_config(connector_name: str) -> dict[str, Any]:
    """Merge connector secret and connector config the specified connector name."""
    config = {}
    try:
        config.update(secret_mgr.fetch_connector_secret(connector_name).parse_json())
    except Exception as e:
        print(f"Error fetching connector config: {e}")

    if connector_name in CONNECTOR_CONFIGS:
        config.update(CONNECTOR_CONFIGS[connector_name])

    return config


connector_config = get_connector_config(CONNECTOR_NAME)

# Start the proxy - this will launch mitmdump in a separate process
port = http_cache.start()
print(f"HTTP cache started on port {port}")

source: ab.Source = ab.get_source(
    CONNECTOR_NAME,
    config=connector_config,
    streams=CONNECTOR_STREAMS.get(CONNECTOR_NAME, "*"),
    docker_image=True,
    use_host_network=True,
    http_cache=http_cache,
)

source.check()
print("Source check successful")

print("First run - making HTTP requests...")
source.read(
    cache=local_cache,
    force_full_refresh=True,
)
print("First run completed")

print("Second run - should use cached responses...")
source.read(
    cache=local_cache,
    force_full_refresh=True,
)
print("Second run completed")

print("Stopping HTTP cache...")
http_cache.stop()
print("HTTP cache stopped")

# print("Consolidating HTTP cache...")
# http_cache.consolidate()
# print("HTTP cache consolidated")

print(
    "Note: If you want to inspect the cached responses, check the .airbyte-http-cache directory"
)
