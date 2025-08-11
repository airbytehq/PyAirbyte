#!/usr/bin/env python3
"""Example script demonstrating Java connector support with source-snowflake.

This script demonstrates how to use PyAirbyte's Java connector support to run
source-snowflake with automatic JRE management and Java execution.

Usage:
    python examples/run_source_snowflake_java.py

Requirements:
    - Snowflake test credentials configured in environment or secrets
    - Docker available (as fallback) or Java connector tar file
"""

from __future__ import annotations

import os
from typing import Any

import airbyte as ab


def get_snowflake_test_config() -> dict[str, Any]:
    """Get Snowflake test configuration from environment variables.

    This mimics the integration test setup for source-snowflake.
    In a real scenario, you would provide your own Snowflake credentials.

    Returns:
        Dictionary containing Snowflake connection configuration.
    """
    config = {
        "account": os.getenv("SECRET_SOURCE_SNOWFLAKE__CREDS__ACCOUNT", "test_account"),
        "host": os.getenv(
            "SECRET_SOURCE_SNOWFLAKE__CREDS__HOST",
            "test_account.snowflakecomputing.com",
        ),
        "username": os.getenv("SECRET_SOURCE_SNOWFLAKE__CREDS__USERNAME", "test_user"),
        "warehouse": os.getenv(
            "SECRET_SOURCE_SNOWFLAKE__CREDS__WAREHOUSE", "COMPUTE_WH"
        ),
        "database": os.getenv(
            "SECRET_SOURCE_SNOWFLAKE__CREDS__DATABASE", "AIRBYTE_DATABASE"
        ),
        "role": os.getenv("SECRET_SOURCE_SNOWFLAKE__CREDS__ROLE", "AIRBYTE_ROLE"),
        "schema": os.getenv("SECRET_SOURCE_SNOWFLAKE__CREDS__SCHEMA", "AIRBYTE_SCHEMA"),
    }

    password = os.getenv("SECRET_SOURCE_SNOWFLAKE__CREDS__PASSWORD")
    private_key = os.getenv("SECRET_SOURCE_SNOWFLAKE__CREDS__PRIVATE_KEY")

    if password:
        config["password"] = password
    elif private_key:
        config["private_key"] = private_key
    else:
        print(
            "⚠️  No real credentials found. Using demo config (connection will likely fail)."
        )
        config["password"] = "demo_password"

    return config


def main() -> None:
    """Main function demonstrating Java connector usage."""
    print("🚀 PyAirbyte Java Connector Demo - source-snowflake")
    print("=" * 60)

    config = get_snowflake_test_config()
    print(f"📋 Using Snowflake account: {config['account']}")
    print(f"👤 Using username: {config['username']}")
    print(f"🏢 Using warehouse: {config['warehouse']}")
    print(f"🗄️  Using database: {config['database']}")

    try:
        print("\n📥 Downloading source-snowflake connector tar...")
        import tempfile
        import requests
        import re

        file_id = "1S0yMrdhs2TLu5u1yvj-52kaeagRAG9ZR"

        session = requests.Session()
        response = session.get(
            f"https://drive.google.com/uc?export=download&id={file_id}"
        )

        if "virus scan warning" in response.text.lower():
            uuid_match = re.search(r'name="uuid" value="([^"]+)"', response.text)
            if uuid_match:
                uuid_value = uuid_match.group(1)
                download_url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download&confirm=t&uuid={uuid_value}"
                response = session.get(download_url, stream=True)
            else:
                download_url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download&confirm=t"
                response = session.get(download_url, stream=True)

        response.raise_for_status()

        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp_file:
            for chunk in response.iter_content(chunk_size=8192):
                tmp_file.write(chunk)

            tar_path = tmp_file.name

        print(f"✅ Downloaded connector tar to: {tar_path}")

        print("\n🔧 Creating source-snowflake with Java execution...")
        source = ab.get_source(
            "source-snowflake",
            config=config,
            use_java=True,  # Enable Java connector execution
            use_java_tar=tar_path,  # Use the downloaded tar file
        )

        print("✅ Source created successfully!")

        print("\n📦 Installing Java connector...")
        source.install()
        print("✅ Java connector installed successfully!")

        print("\n🔍 Checking connection...")
        try:
            source.check()
            print("Connection check: ✅ PASSED")
        except Exception as e:
            print(f"Connection check: ❌ FAILED - {e}")
            return

        print("\n📊 Discovering streams...")
        stream_names = source.get_available_streams()
        print(
            f"Found {len(stream_names)} streams: {stream_names[:5]}{'...' if len(stream_names) > 5 else ''}"
        )

        if not stream_names:
            print("❌ No streams found. Check your Snowflake configuration.")
            return

        selected_stream = stream_names[0]
        print(f"\n🎯 Selecting stream: {selected_stream}")
        source.select_streams([selected_stream])

        print(f"\n📖 Reading 10 records from {selected_stream}...")
        read_result = source.read()

        records_count = 0
        for record in read_result[selected_stream]:
            print(f"Record {records_count + 1}: {record}")
            records_count += 1
            if records_count >= 10:
                break

        print(f"\n✅ Successfully read {records_count} records using Java connector!")
        print("🎉 Java connector demo completed successfully!")

    except Exception as e:
        print(f"\n❌ Error during execution: {e}")
        print("💡 This might be due to:")
        print("   - Missing or invalid Snowflake credentials")
        print("   - Network connectivity issues")
        print("   - Java connector installation issues")
        print("   - Missing connector tar file")
        raise


if __name__ == "__main__":
    main()
