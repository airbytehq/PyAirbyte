"""Simple script to download secrets from GCS.

Secrets will be located based on the `connector` label in the GSM secret metadata, and they
will be written to the connector's secrets directory based upon the `filename` label.

Filename is appended with `.json` and the secret is written to that file.

As a safety measure, we will only write to the connector's secrets directory if it exists.
If it doesn't exist, the script will fail. Users should ensure the directory
exists and is excluded from git before running the script.

Usage:
    poetry run python examples/run_gsm_connector_secret_fetch.py
"""

from __future__ import annotations

from pathlib import Path

import airbyte as ab
from airbyte.secrets import GoogleGSMSecretManager, SecretHandle

AIRBYTE_INTERNAL_GCP_PROJECT = "dataline-integration-testing"
CONNECTOR_NAME = "source-klaviyo"

AIRBYTE_REPO_ROOT = Path(__file__).parent.parent.parent / "airbyte"


CONNECTOR_SECRETS_DIR = (
    AIRBYTE_REPO_ROOT
    / "airbyte-integrations"
    / "connectors"
    / CONNECTOR_NAME
    / "secrets"
)
if not AIRBYTE_REPO_ROOT.exists():
    raise FileNotFoundError(f"Airbyte repo root does not exist: {AIRBYTE_REPO_ROOT}")
if not CONNECTOR_SECRETS_DIR.exists():
    CONNECTOR_SECRETS_DIR.mkdir(parents=True, exist_ok=True)

def main() -> None:
    secret_mgr = GoogleGSMSecretManager(
        project=AIRBYTE_INTERNAL_GCP_PROJECT,
        credentials_json=ab.get_secret("GCP_GSM_CREDENTIALS"),
    )

    secret: SecretHandle
    for secret in secret_mgr.fetch_connector_secrets(
        connector_name=CONNECTOR_NAME,
    ):
        filename_base = "config"  # Default filename if not overridden
        if "filename" in secret.labels:
            filename_base = secret.labels["filename"]
        secret_file_path = CONNECTOR_SECRETS_DIR / f"{filename_base}.json"
        secret.write_to_file(secret_file_path)


if __name__ == "__main__":
    main()
