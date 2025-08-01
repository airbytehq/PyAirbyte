# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Test a sync to an Airbyte destination.

Usage:
```
poetry run python examples/run_sync_to_destination_wo_cache.py
```
"""

from __future__ import annotations

import datetime

import airbyte as ab

SCALE = 200_000


def get_my_source() -> ab.Source:
    # Create a token here: https://github.com/settings/tokens
    # Then export as env var `GITHUB_PERSONAL_ACCESS_TOKEN`
    github_pat = ab.get_secret("GITHUB_PERSONAL_ACCESS_TOKEN")
    assert str(github_pat), "Could not locate Github PAT"
    source = ab.get_source(
        "source-github",
        config={
            "repositories": ["airbytehq/PyAirbyte"],
            "credentials": {
                "personal_access_token": github_pat,
            },
        },
    )
    source.check()
    source.select_streams(["issues"])
    return source


def get_cache() -> ab.DuckDBCache:
    return ab.new_local_cache(
        cache_name="state_cache",
    )


def get_my_destination() -> ab.Destination:
    return ab.get_destination(
        name="destination-duckdb",
        config={
            # This path is relative to the container:
            "destination_path": "/local/temp/db.duckdb",
        },
        docker_image="airbyte/destination-duckdb:latest",
        # OR:
        # pip_url="git+https://github.com/airbytehq/airbyte.git#subdirectory=airbyte-integrations/connectors/destination-duckdb",
    )


def main() -> None:
    """Test writing from the source to the destination."""
    source = get_my_source()
    source.check()
    destination = get_my_destination()
    destination.check()
    state_cache = get_cache()
    write_result: ab.WriteResult = destination.write(
        source,
        cache=False,
        state_cache=state_cache,
    )
    print(
        f"Completed writing {write_result.processed_records:,} records "
        f"to destination at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}."
    )


if __name__ == "__main__":
    main()
