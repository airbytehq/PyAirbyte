# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""
Usage:
    poetry run python examples/run_salesforce_to_bigquery_cache.py
"""

from __future__ import annotations

import tempfile
import warnings
from typing import cast

import airbyte as ab
from airbyte.secrets.base import SecretString
from airbyte.secrets.google_gsm import GoogleGSMSecretManager

warnings.filterwarnings("ignore", message="Cannot create BigQuery Storage client")


AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"
BIGQUERY_SECRET_NAME = "SECRET_DESTINATION-BIGQUERY_CREDENTIALS__CREDS"

secret_mgr = GoogleGSMSecretManager(
    project=AIRBYTE_INTERNAL_GCP_PROJECT,
    credentials_json=ab.get_secret("GCP_GSM_CREDENTIALS"),
)

source_salesforce_config: dict = cast(
    SecretString,
    secret_mgr.fetch_connector_secret("source-salesforce"),
).parse_json()
bigquery_destination_config: dict = cast(
    SecretString,
    secret_mgr.get_secret(BIGQUERY_SECRET_NAME),
).parse_json()


def main() -> None:
    source = ab.get_source(
        "source-salesforce",
        config=source_salesforce_config,
        install_if_missing=True,
    )
    source.check()
    source.select_all_streams()

    with tempfile.NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8") as temp:
        # Write credentials to the temp file
        temp.write(bigquery_destination_config["credentials_json"])
        temp.flush()
        temp.close()

        destination = ab.get_destination(
            "destination-bigquery",
            config={**bigquery_destination_config, "dataset_id": "pyairbyte_tests"},
        )

        destination.write(
            source,
            cache=ab.get_default_cache(),
            # cache=False,  # Toggle comment to test with/without caching
        )


if __name__ == "__main__":
    main()
