# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""
Usage:
    poetry install
    poetry run python examples/run_bigquery_faker.py
"""

from __future__ import annotations

import json
import os
import tempfile
import warnings

from google.cloud import secretmanager

import airbyte as ab
from airbyte.caches.bigquery import BigQueryCache


warnings.filterwarnings("ignore", message="Cannot create BigQuery Storage client")


# load secrets from GSM using the GCP_GSM_CREDENTIALS env variable
secret_client = secretmanager.SecretManagerServiceClient.from_service_account_info(
    json.loads(os.environ["GCP_GSM_CREDENTIALS"])
)

# to-do: make sure database & account id are read from PY_AIRBYTE_BIGQUERY_CREDS
bigquery_destination_secret = json.loads(
    secret_client.access_secret_version(
        name="projects/dataline-integration-testing/secrets/SECRET_DESTINATION-BIGQUERY_CREDENTIALS__CREDS/versions/latest"
    ).payload.data.decode("UTF-8")
)


def main() -> None:
    source = ab.get_source(
        "source-faker",
        config={"count": 10, "seed": 0, "parallelism": 1, "always_updated": False},
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

        for name, records in result.streams.items():
            print(f"Stream {name}: {len(records)} records")


if __name__ == "__main__":
    main()
