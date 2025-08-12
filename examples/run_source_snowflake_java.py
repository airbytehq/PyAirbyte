#!/usr/bin/env python3
"""Example script demonstrating Java connector support with source-snowflake.

Usage:
    poetry run python examples/run_source_snowflake_java.py

Requirements:
    - DEVIN_GCP_SERVICE_ACCOUNT_JSON environment variable set
    - GSM secrets configured for source-snowflake
"""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path

import airbyte as ab
import requests
from airbyte.secrets.google_gsm import GoogleGSMSecretManager, GSMSecretHandle


def download_snowflake_tar() -> Path:
    """Download source-snowflake tar file from Google Drive."""
    file_id = "1S0yMrdhs2TLu5u1yvj-52kaeagRAG9ZR"

    # Create session and get initial response
    session = requests.Session()
    response = session.get(f"https://drive.google.com/uc?export=download&id={file_id}")

    if "virus scan warning" in response.text.lower():
        uuid_match = re.search(r'name="uuid" value="([^"]+)"', response.text)
        uuid_value = uuid_match.group(1) if uuid_match else ""

        response = session.get(
            f"https://drive.usercontent.google.com/download?id={file_id}&export=download&confirm=t&uuid={uuid_value}"
        )

    temp_file = Path(tempfile.mktemp(suffix=".tar"))
    temp_file.write_bytes(response.content)
    return temp_file


def get_connector_config(connector_name: str) -> dict[str, str]:
    """Retrieve the connector configuration."""
    secret_mgr = GoogleGSMSecretManager(
        project="dataline-integration-testing",
        credentials_json=os.environ.get("DEVIN_GCP_SERVICE_ACCOUNT_JSON"),
    )
    secret: GSMSecretHandle = secret_mgr.fetch_connector_secret(connector_name)
    return secret.parse_json()


def main() -> None:
    """Main function demonstrating Java connector usage."""
    print("🚀 PyAirbyte Java Connector Demo - source-snowflake")
    print("=" * 60)

    print("📥 Downloading source-snowflake tar file...")
    tar_path = download_snowflake_tar()
    print(f"✅ Downloaded tar to: {tar_path}")

    config = get_connector_config("source-snowflake")
    print(f"✅ Retrieved config for account: {config.get('account', 'N/A')}")

    # Create source with Java execution using downloaded tar
    source = ab.get_source(
        "source-snowflake",
        config=config,
        use_java_tar=tar_path,
        # use_java=True,  # Implied by use_java_tar
    )
    print("✅ Source created successfully!")

    _ = source.config_spec
    print("✅ Config spec retrieved successfully!")

    return  # This is as far as we can go for now.

    # TODO: Fix this part. Connector doesn't seem to get the config properly.
    source.check()
    print("✅ Connection check passed")
    stream_names = source.get_available_streams()
    print(f"📊 Found {len(stream_names)} streams")

    selected_stream = stream_names[0]
    source.select_streams([selected_stream])
    read_result = source.read()
    records = list(read_result[selected_stream])[:10]
    print(f"✅ Read {len(records)} records using Java connector!")



if __name__ == "__main__":
    main()
