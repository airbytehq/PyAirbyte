# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Integration tests for registry spec helper functions."""

from __future__ import annotations

import pytest

from airbyte._util.registry_spec import (
    get_connector_spec_from_registry,
    validate_connector_config_from_registry,
)


@pytest.mark.parametrize(
    "connector_name,platform",
    [
        ("source-faker", "oss"),
        ("source-faker", "cloud"),
        ("destination-duckdb", "oss"),
    ],
)
def test_get_connector_spec_from_registry(connector_name: str, platform: str) -> None:
    """Test fetching connector specs from the registry."""
    spec = get_connector_spec_from_registry(
        connector_name,
        platform=platform,
    )

    assert spec is not None
    assert isinstance(spec, dict)
    assert "type" in spec
    assert spec["type"] == "object"


@pytest.mark.parametrize(
    "connector_name,config,expected_valid",
    [
        (
            "source-faker",
            {
                "count": 100,
                "seed": 12345,
                "parallelism": 1,
            },
            True,
        ),
        (
            "source-faker",
            {
                "count": "not_a_number",
            },
            False,
        ),
        (
            "source-faker",
            {
                "count": 0,
            },
            False,
        ),
    ],
)
def test_validate_connector_config_from_registry(
    connector_name: str,
    config: dict,
    expected_valid: bool,
) -> None:
    """Test validating connector configs against registry specs."""
    is_valid, error_message = validate_connector_config_from_registry(
        connector_name,
        config,
        platform="oss",
    )

    assert is_valid == expected_valid

    if expected_valid:
        assert error_message is None
    else:
        assert error_message is not None
        assert isinstance(error_message, str)
