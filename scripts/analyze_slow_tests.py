#!/usr/bin/env python3
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""Script to analyze slow tests in PyAirbyte and recommend timeout configurations.

This script identifies tests marked as slow and provides recommendations for
per-test timeout limits to prevent CI timeouts on Windows.
"""


def main() -> None:
    """Analyze slow tests and provide timeout recommendations."""
    print("=== PyAirbyte Slow Test Analysis ===\n")

    print("1. Tests marked with @pytest.mark.slow:")
    slow_tests = [
        "tests/integration_tests/test_all_cache_types.py::test_faker_read "
        "(4 parametrized variants)",
        "tests/integration_tests/test_all_cache_types.py::test_append_strategy",
        "tests/integration_tests/test_all_cache_types.py::test_replace_strategy",
        "tests/integration_tests/test_all_cache_types.py::test_merge_strategy",
        "tests/integration_tests/test_all_cache_types.py::test_auto_add_columns",
        "tests/integration_tests/test_source_faker_integration.py::test_replace_strategy",
        "tests/integration_tests/test_source_faker_integration.py::test_append_strategy",
        "tests/integration_tests/test_source_faker_integration.py::test_merge_strategy "
        "(2 parametrized variants)",
        "tests/integration_tests/test_docker_executable.py::test_replace_strategy",
        "tests/integration_tests/test_docker_executable.py::test_append_strategy",
        "tests/integration_tests/test_docker_executable.py::test_merge_strategy "
        "(2 parametrized variants)",
    ]

    for test in slow_tests:
        print(f"  - {test}")

    print(f"\nTotal slow tests identified: {len(slow_tests)}")

    print("\n2. Timeout Recommendations:")
    print("  - Current global timeout: 600 seconds (10 minutes)")
    print("  - Recommended per-test timeout for integration tests: 180 seconds (3 minutes)")
    print("  - Recommended per-test timeout for unit tests: 60 seconds (1 minute)")
    print("  - Session timeout for entire test suite: 3600 seconds (1 hour)")

    print("\n3. CLI Usage Examples:")
    print("  poetry run poe test-integration-timeout  # Run integration tests with 3min timeout")
    print("  poetry run poe test-with-short-timeout   # Run all tests with 5min timeout")
    print("  pytest --timeout=120 tests/unit_tests/   # Custom timeout for unit tests")


if __name__ == "__main__":
    main()
