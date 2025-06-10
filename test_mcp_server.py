
"""Test script to verify MCP server functionality."""

from __future__ import annotations

import json

from airbyte.mcp.server import (
    get_config_spec,
    list_connectors,
    validate_config,
)


def test_mcp_server() -> None:
    """Test the MCP server functionality."""
    print("Testing PyAirbyte MCP Server...")

    print("\n1. Testing list_connectors...")
    try:
        result = list_connectors()
        connector_count = len(result.split("Found ")[1].split(" connectors")[0])
        print(f"✓ Found connectors: {connector_count} connectors")
    except Exception as e:
        print(f"✗ Error listing connectors: {e}")

    print("\n2. Testing get_config_spec for source-faker...")
    try:
        result = get_config_spec("source-faker", output_format="json")
        spec = json.loads(result)
        print(f"✓ Got config spec with {len(spec.get('properties', {}))} properties")
    except Exception as e:
        print(f"✗ Error getting config spec: {e}")

    print("\n3. Testing validate_config with valid config...")
    try:
        valid_config = {
            "count": 100,
            "seed": 12345,
            "records_per_sync": 100,
            "records_per_slice": 100,
            "always_updated": False,
            "parallelism": 1,
        }
        result = validate_config("source-faker", valid_config)
        print(f"✓ Config validation result: {result}")
    except Exception as e:
        print(f"✗ Error validating config: {e}")

    print("\n4. Testing validate_config with hardcoded secret...")
    try:
        invalid_config = {"count": 100, "api_key": "hardcoded_secret_value"}
        result = validate_config("source-faker", invalid_config)
        if "hardcoded secrets" in result.lower():
            print("✓ Correctly rejected hardcoded secrets")
        else:
            print(f"? Unexpected validation result: {result}")
    except Exception as e:
        print(f"✗ Error in secret validation test: {e}")

    print("\n5. Testing list_connectors with JSON output format...")
    try:
        result = list_connectors(output_format="json")
        data = json.loads(result)
        print(f"✓ JSON format: Found {data['count']} connectors")
        print(f"First 3 connectors: {data['connectors'][:3]}")
    except Exception as e:
        print(f"✗ Error with JSON format: {e}")

    print("\n✓ MCP server functionality tests completed!")


if __name__ == "__main__":
    test_mcp_server()
