# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Test a sync to an Airbyte destination.

Usage:
```
poetry run python examples/run_sync_to_destination_from_read_result.py
```
"""

from __future__ import annotations

import datetime

import airbyte as ab

SCALE = 200_000


def get_my_source() -> ab.Source:
    return ab.get_source(
        "source-faker",
        local_executable="source-faker",
        config={
            "count": SCALE,
            "seed": 1234,
            "parallelism": 16,
        },
        install_if_missing=False,
        streams=["purchases"],
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

    read_result: ab.ReadResult = source.read(
        cache=ab.new_local_cache(),
    )
    print(
        "Completed reading from source at "
        f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}. "
        "Writing to destination..."
    )
    write_result: ab.WriteResult = destination.write(
        source_data=read_result,
    )
    print(
        f"Completed writing {write_result.processed_records:,} records "
        f"to destination at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}."
    )


if __name__ == "__main__":
    main()
