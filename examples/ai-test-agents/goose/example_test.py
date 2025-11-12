"""
Example test script for PyAirbyte that can be executed by Goose.

This script demonstrates basic PyAirbyte functionality that Goose can run
to validate the library is working correctly.
"""

import airbyte as ab


def test_basic_source_connector():
    """Test creating and reading from a source connector."""
    print("Testing basic source connector functionality...")

    source = ab.get_source(
        "source-faker", config={"count": 10}, install_if_missing=True
    )

    print("Checking connection...")
    source.check()
    print("✓ Connection check passed")

    print("Reading data into cache...")
    cache = ab.new_local_cache()
    result = source.read(cache)

    df = cache["users"].to_pandas()

    assert len(df) > 0, "No data was read from source"
    assert "id" in df.columns, "Expected 'id' column not found"

    print(f"✓ Successfully read {len(df)} records from source-faker")
    print(f"✓ Columns: {', '.join(df.columns)}")

    return True


def test_connector_discovery():
    """Test discovering available connectors."""
    print("\nTesting connector discovery...")

    from airbyte.registry import get_available_connectors

    sources = get_available_connectors(connector_type="source")

    assert len(sources) > 0, "No source connectors found"

    faker_found = any(c.name == "source-faker" for c in sources)
    assert faker_found, "source-faker not found in available connectors"

    print(f"✓ Found {len(sources)} source connectors")
    print("✓ source-faker is available")

    return True


def main():
    """Run all tests."""
    print("=" * 60)
    print("PyAirbyte Test Suite")
    print("=" * 60)

    try:
        test_basic_source_connector()
        test_connector_discovery()

        print("\n" + "=" * 60)
        print("All tests passed! ✓")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        raise


if __name__ == "__main__":
    main()
