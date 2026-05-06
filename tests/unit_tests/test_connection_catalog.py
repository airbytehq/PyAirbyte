# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for connection catalog normalization helpers."""

from __future__ import annotations

from typing import Any

import pytest

from airbyte.cloud._connection_catalog import (
    _denormalize_catalog_to_api,
    _is_protocol_catalog_format,
    _normalize_catalog_to_protocol,
)

# Shared fixtures: a typical syncCatalog from the Config API.
_API_CATALOG: dict[str, Any] = {
    "streams": [
        {
            "stream": {
                "name": "users",
                "jsonSchema": {"type": "object"},
                "supportedSyncModes": ["full_refresh", "incremental"],
                "sourceDefinedCursor": True,
                "defaultCursorField": ["updated_at"],
                "sourceDefinedPrimaryKey": [["id"]],
            },
            "config": {
                "syncMode": "incremental",
                "destinationSyncMode": "append_dedup",
                "cursorField": ["updated_at"],
                "primaryKey": [["id"]],
            },
        },
    ],
}

# The expected protocol-format equivalent.
_PROTOCOL_CATALOG: dict[str, Any] = {
    "streams": [
        {
            "stream": {
                "name": "users",
                "json_schema": {"type": "object"},
                "supported_sync_modes": ["full_refresh", "incremental"],
                "source_defined_cursor": True,
                "default_cursor_field": ["updated_at"],
                "source_defined_primary_key": [["id"]],
            },
            "sync_mode": "incremental",
            "destination_sync_mode": "append_dedup",
            "cursor_field": ["updated_at"],
            "primary_key": [["id"]],
        },
    ],
}


def test_normalize_catalog_to_protocol() -> None:
    result = _normalize_catalog_to_protocol(_API_CATALOG)
    assert result == _PROTOCOL_CATALOG


def test_denormalize_catalog_to_api() -> None:
    result = _denormalize_catalog_to_api(_PROTOCOL_CATALOG)
    assert result == _API_CATALOG


def test_catalog_round_trip_api_to_protocol_to_api() -> None:
    protocol = _normalize_catalog_to_protocol(_API_CATALOG)
    restored = _denormalize_catalog_to_api(protocol)
    assert restored == _API_CATALOG


def test_catalog_round_trip_protocol_to_api_to_protocol() -> None:
    api = _denormalize_catalog_to_api(_PROTOCOL_CATALOG)
    restored = _normalize_catalog_to_protocol(api)
    assert restored == _PROTOCOL_CATALOG


def test_normalize_empty_catalog() -> None:
    assert _normalize_catalog_to_protocol({"streams": []}) == {"streams": []}


def test_denormalize_empty_catalog() -> None:
    assert _denormalize_catalog_to_api({"streams": []}) == {"streams": []}


@pytest.mark.parametrize(
    "catalog,expected",
    [
        pytest.param(
            _PROTOCOL_CATALOG,
            True,
            id="protocol_format",
        ),
        pytest.param(
            _API_CATALOG,
            False,
            id="api_format",
        ),
        pytest.param(
            {"streams": []},
            False,
            id="empty_streams",
        ),
    ],
)
def test_is_protocol_catalog_format(
    catalog: dict[str, Any],
    expected: bool,
) -> None:
    assert _is_protocol_catalog_format(catalog) is expected
