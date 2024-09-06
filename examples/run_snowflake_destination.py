# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""
Usage:
    poetry install
    poetry run python examples/run_snowflake_destination.py
"""

from __future__ import annotations

import airbyte as ab
from airbyte.secrets.google_gsm import GoogleGSMSecretManager

SCALE = 100

def get_secret_config() -> dict:
    AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"
    secret_mgr = GoogleGSMSecretManager(
        project=AIRBYTE_INTERNAL_GCP_PROJECT,
        credentials_json=ab.get_secret("GCP_GSM_CREDENTIALS"),
    )

    secret = secret_mgr.get_secret(
        secret_name="AIRBYTE_LIB_SNOWFLAKE_CREDS",
    )
    assert secret is not None, "Secret not found."
    secret_config = secret.parse_json()

    cortex_destination_secret = secret_mgr.fetch_connector_secret(
        connector_name="destination-snowflake-cortex",
    ).parse_json()
    return cortex_destination_secret


def sync_to_cortex() -> None:
    source = ab.get_source(
        "source-faker",
        config={
            "count": SCALE,
        },
        install_if_missing=True,
        streams=["products", "users"],
    )
    source.check()
    cortex_config = get_secret_config()
    cortex_config["processing"]["text_fields"] = [
        "make",
        "model",
        "name",
        "gender",
    ]
    cortex_config["indexing"]["default_schema"] = "pyairbyte_demo"
    cortex_destination = ab.get_destination(
        "destination-snowflake-cortex",
        config=cortex_config,
    )
    cortex_destination.write(source)


if __name__ == "__main__":
    sync_to_cortex()
