"""This script will run any source that is registered in the Airbyte integration tests.


Usage:
    poetry run python examples/run_integ_test_source.py source-coin-api
    poetry run python examples/run_integ_test_source.py source-github
    poetry run python examples/run_integ_test_source.py source-google-analytics-v4
    poetry run python examples/run_integ_test_source.py source-klaviyo
    poetry run python examples/run_integ_test_source.py source-shopify

"""
from __future__ import annotations

import json
import os
import sys
from typing import Any

from google.cloud import secretmanager

import airbyte as ab


def get_secret_name(connector_name: str) -> str:
    """Get the secret name for the given connector.

    Some names are hard-coded, if the naming convention is not followed.
    """
    if connector_name.lower() == "source-google-analytics-v4":
        return "SECRET_SOURCE_GOOGLE_ANALYTICS_V4_CLOUD__CREDS"

    if connector_name.lower() == "source-shopify":
        return "SECRET_SOURCE-SHOPIFY__CREDS"

    return f"SECRET_{connector_name.upper()}_CREDS"


def get_integ_test_config(secret_name: str) -> dict[str, Any]:
    if "GCP_GSM_CREDENTIALS" not in os.environ:
        raise Exception(  # noqa: TRY002, TRY003
            f"GCP_GSM_CREDENTIALS env var not set, can't fetch secrets for '{connector_name}'. "
            "Make sure they are set up as described: "
            "https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/ci_credentials/"
            "README.md#get-gsm-access"
        )

    secret_client = secretmanager.SecretManagerServiceClient.from_service_account_info(
        json.loads(os.environ["GCP_GSM_CREDENTIALS"])
    )
    return json.loads(
        secret_client.access_secret_version(
            name=f"projects/dataline-integration-testing/secrets/{secret_name}/versions/latest"
        ).payload.data.decode("UTF-8")
    )


def main(
    connector_name: str,
    secret_name: str | None,
    streams: list[str] | None,
) -> None:
    config = get_integ_test_config(secret_name)
    source = ab.get_source(
        connector_name,
        config=config,
        install_if_missing=True,
    )
    if streams:
        source.select_streams(streams)
    else:
        source.select_all_streams()
    cache = ab.new_local_cache()
    try:
        read_result = source.read(cache=cache)
        print(
            f"Read from `{connector_name}` was successful. ",
            f"Cache results were saved to: {cache.cache_dir}",
            f"Streams list: {', '.join(read_result.streams.keys())}",
        )
    except Exception:
        print(
            f"Read from `{connector_name}` failed. ",
            f"Cache files are located at: {cache.cache_dir}",
        )
        raise


if __name__ == "__main__":
    # Get first arg from CLI
    connector_name = sys.argv[1]
    streams_csv = sys.argv[2] if len(sys.argv) > 2 else None  # noqa: PLR2004
    streams = None
    if streams_csv:
        streams = streams_csv.split(",")
    # TODO: We can optionally take a second arg to override the default secret name.
    secret_name = get_secret_name(connector_name)
    main(connector_name, streams=streams, secret_name=secret_name)
