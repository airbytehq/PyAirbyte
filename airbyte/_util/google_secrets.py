"""Helpers for accessing Google secrets."""

from __future__ import annotations

import json
import os

from google.cloud import secretmanager


def get_gcp_secret(
    project_name: str,
    secret_name: str,
) -> str:
    """Try to get a GCP secret from the environment, or raise an error.

    We assume that the Google service account credentials file contents are stored in the
    environment variable GCP_GSM_CREDENTIALS. If this environment variable is not set, we raise an
    error. Otherwise, we use the Google Secret Manager API to fetch the secret with the given name.
    """
    if "GCP_GSM_CREDENTIALS" not in os.environ:
        raise EnvironmentError(  # noqa: TRY003, UP024
            "GCP_GSM_CREDENTIALS env variable not set, can't fetch secrets. Make sure they are set "
            "up as described: "
            "https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/ci_credentials/"
            "README.md#get-gsm-access"
        )

    # load secrets from GSM using the GCP_GSM_CREDENTIALS env variable
    secret_client = secretmanager.SecretManagerServiceClient.from_service_account_info(
        json.loads(os.environ["GCP_GSM_CREDENTIALS"])
    )
    return secret_client.access_secret_version(
        name=f"projects/{project_name}/secrets/{secret_name}/versions/latest"
    ).payload.data.decode("UTF-8")


def get_gcp_secret_json(
    project_name: str,
    secret_name: str,
) -> dict:
    """Get a JSON GCP secret and return as a dict.

    We assume that the Google service account credentials file contents are stored in the
    environment variable GCP_GSM_CREDENTIALS. If this environment variable is not set, we raise an
    error. Otherwise, we use the Google Secret Manager API to fetch the secret with the given name.
    """
    return json.loads(get_gcp_secret(secret_name, project_name))
