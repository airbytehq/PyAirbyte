# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Test a sync to an Airbyte destination.

Usage:
```
poetry run python examples/run_sync_to_destination_wo_cache.py
```
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte import get_source
from airbyte.destinations.base import Destination
from airbyte.executors.util import get_connector_executor

if TYPE_CHECKING:
    from airbyte.sources.base import Source


SCALE = 500_000


def main() -> None:
    """Test the destination."""
    # Get a source-faker instance.
    source: Source = get_source(
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
    destination = Destination(
        name="destination-duckdb",
        config={
            # This path is relative to the container:
            "destination_path": "/local/temp/db.duckdb",
        },
        executor=get_connector_executor(
            name="destination-duckdb",
            docker_image="airbyte/destination-duckdb:latest",
            # pip_url="git+https://github.com/airbytehq/airbyte.git#subdirectory=airbyte-integrations/connectors/destination-duckdb",
        ),
    )
    destination.check()
    destination.write(source)


if __name__ == "__main__":
    main()
