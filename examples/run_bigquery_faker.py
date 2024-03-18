# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""
Usage:
    poetry install
    poetry run python examples/run_bigquery_faker.py
"""

from __future__ import annotations

import tempfile
import warnings

import airbyte as ab
from airbyte._util.google_secrets import get_gcp_secret_json
from airbyte.caches.bigquery import BigQueryCache


warnings.filterwarnings("ignore", message="Cannot create BigQuery Storage client")


bigquery_destination_secret = get_gcp_secret_json(
    project_name="dataline-integration-testing",
    secret_name="SECRET_DESTINATION-BIGQUERY_CREDENTIALS__CREDS",
)


def main() -> None:
    source = ab.get_source(
        "source-faker",
        config={"count": 1000, "seed": 0, "parallelism": 1, "always_updated": False},
        install_if_missing=True,
    )
    source.check()
    source.select_all_streams()

    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as temp:
        # Write credentials to the temp file
        temp.write(bigquery_destination_secret["credentials_json"])
        temp.flush()
        temp.close()

        cache = BigQueryCache(
            project_name=bigquery_destination_secret["project_id"],
            credentials_path=temp.name,
        )

        result = source.read(cache)

        # Read a second time to make sure table swaps and incremental are working.
        result = source.read(cache)

        for name, records in result.streams.items():
            print(f"Stream {name}: {len(records)} records")


if __name__ == "__main__":
    main()
