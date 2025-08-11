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

import airbyte as ab
from airbyte.secrets.google_gsm import GoogleGSMSecretManager


def main() -> None:
    """Main function demonstrating Java connector usage."""
    print("🚀 PyAirbyte Java Connector Demo - source-snowflake")
    print("=" * 60)

    try:
        secret_mgr = GoogleGSMSecretManager(
            project="dataline-integration-testing",
            credentials_json=os.environ.get("DEVIN_GCP_SERVICE_ACCOUNT_JSON"),
        )
        secret = secret_mgr.fetch_connector_secret("source-snowflake")
        config = secret.parse_json()
        print(f"✅ Retrieved config for account: {config.get('account', 'N/A')}")

        # Create source with Java execution
        source = ab.get_source(
            "source-snowflake",
            config=config,
            use_java=True,
            use_java_tar="TODO",
        )
        print("✅ Source created successfully!")

        source.check()
        print("✅ Connection check passed")
        stream_names = source.get_available_streams()
        print(f"📊 Found {len(stream_names)} streams")

        if stream_names:
            selected_stream = stream_names[0]
            records = list(source.get_records(selected_stream, 10))
            print(f"✅ Read {len(records)} records using Java connector!")
        else:
            print("❌ No streams found")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
