# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""
Usage:
    poetry install
    poetry run python examples/run_bigquery_faker.py
"""

from __future__ import annotations

import tempfile
import warnings
from typing import cast

import airbyte as ab
from airbyte.caches.bigquery import BigQueryCache
from airbyte.secrets.base import SecretString
from airbyte.secrets.google_gsm import GoogleGSMSecretManager

warnings.filterwarnings("ignore", message="Cannot create BigQuery Storage client")


AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"
SECRET_NAME = "SECRET_DESTINATION-BIGQUERY_CREDENTIALS__CREDS"

bigquery_destination_secret: dict = cast(
    SecretString,
    GoogleGSMSecretManager(
        project=AIRBYTE_INTERNAL_GCP_PROJECT,
        credentials_json=ab.get_secret("GCP_GSM_CREDENTIALS"),
    ).get_secret(SECRET_NAME),
).parse_json()


def main() -> None:
    source = ab.get_source(
        "source-faker",
        config={"count": 1000, "seed": 0, "parallelism": 1, "always_updated": False},
        install_if_missing=True,
    )
    source.check()
    source.select_all_streams()

    with tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8") as temp:
        # Write credentials to the temp file
        temp.write(bigquery_destination_secret["credentials_json"])
        temp.flush()
        temp.close()

        cache = BigQueryCache(
            project_name=bigquery_destination_secret["project_id"],
            dataset_name=bigquery_destination_secret.get(
                "dataset_id", "pyairbyte_integtest"
            ),
            credentials_path=temp.name,
        )

        result = source.read(cache)

        # Read a second time to make sure table swaps and incremental are working.
        result = source.read(cache)

        for name, records in result.streams.items():
            print(f"Stream {name}: {len(records)} records")


if __name__ == "__main__":
    main()
