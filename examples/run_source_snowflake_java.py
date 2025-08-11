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
import tempfile
from pathlib import Path

import requests

import airbyte as ab
from airbyte.secrets.google_gsm import GoogleGSMSecretManager


def download_snowflake_tar() -> Path:
    """Download source-snowflake tar file from Google Drive."""
    file_id = "1S0yMrdhs2TLu5u1yvj-52kaeagRAG9ZR"

    # Create session and get initial response
    session = requests.Session()
    response = session.get(f"https://drive.google.com/uc?export=download&id={file_id}")

    if "virus scan warning" in response.text.lower():
        import re

        confirm_match = re.search(r'confirm=([^&"]+)', response.text)
        if confirm_match:
            confirm_token = confirm_match.group(1)
            response = session.get(
                f"https://drive.google.com/uc?export=download&id={file_id}&confirm={confirm_token}"
            )

    temp_file = Path(tempfile.mktemp(suffix=".tar"))
    temp_file.write_bytes(response.content)
    return temp_file


def main() -> None:
    """Main function demonstrating Java connector usage."""
    print("🚀 PyAirbyte Java Connector Demo - source-snowflake")
    print("=" * 60)

    try:
        print("📥 Downloading source-snowflake tar file...")
        tar_path = download_snowflake_tar()
        print(f"✅ Downloaded tar to: {tar_path}")

        secret_mgr = GoogleGSMSecretManager(
            project="dataline-integration-testing",
            credentials_json=os.environ.get("DEVIN_GCP_SERVICE_ACCOUNT_JSON"),
        )
        secret = secret_mgr.fetch_connector_secret("source-snowflake")
        config = secret.parse_json()
        print(f"✅ Retrieved config for account: {config.get('account', 'N/A')}")

        # Create source with Java execution using downloaded tar
        source = ab.get_source(
            "source-snowflake",
            config=config,
            use_java=True,
            use_java_tar=tar_path,
        )
        print("✅ Source created successfully!")

        source.check()
        print("✅ Connection check passed")
        stream_names = source.get_available_streams()
        print(f"📊 Found {len(stream_names)} streams")

        if stream_names:
            selected_stream = stream_names[0]
            source.select_streams([selected_stream])
            read_result = source.read()
            records = list(read_result[selected_stream])[:10]
            print(f"✅ Read {len(records)} records using Java connector!")
        else:
            print("❌ No streams found")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
