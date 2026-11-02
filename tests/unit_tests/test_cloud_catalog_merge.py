# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for merging configured settings into newly discovered catalogs."""

from __future__ import annotations

from typing import Any

from airbyte.cloud._catalog_merge import _merge_catalog_with_configuration


def _stream_entry(
    name: str,
    *,
    schema_fields: tuple[str, ...] = ("id",),
    sync_modes: tuple[str, ...] = ("full_refresh", "incremental"),
    config: dict[str, Any] | None = None,
    **stream_fields: Any,
) -> dict[str, Any]:
    return {
        "stream": {
            "name": name,
            "namespace": None,
            "jsonSchema": {
                "type": "object",
                "properties": {field: {"type": "string"} for field in schema_fields},
            },
            "supportedSyncModes": list(sync_modes),
            **stream_fields,
        },
        "config": config
        or {
            "syncMode": "full_refresh",
            "destinationSyncMode": "append",
            "selected": True,
            "selectedFields": [],
            "fieldSelectionEnabled": False,
        },
    }


def _catalog(*entries: dict[str, Any]) -> dict[str, Any]:
    return {"streams": list(entries)}


def test_merge_keeps_supported_configuration_and_prunes_hashed_fields() -> None:
    configured = _stream_entry(
        "users",
        schema_fields=("id", "old"),
        config={
            "syncMode": "incremental",
            "destinationSyncMode": "append_dedup",
            "cursorField": ["updated_at"],
            "primaryKey": [["configured_id"]],
            "selected": True,
            "suggested": True,
            "includeFiles": True,
            "fieldSelectionEnabled": False,
            "selectedFields": [{"fieldPath": ["id"]}],
            "aliasName": "configured-users",
            "mappers": [{"name": "mapper"}],
            "destinationObjectName": "warehouse_users",
            "hashedFields": [
                {"fieldPath": ["id"]},
                {"fieldPath": ["removed"]},
            ],
        },
        apiOnlyStreamField={"source": "configured"},
    )
    discovered = _stream_entry(
        "users",
        schema_fields=("id", "email"),
        sourceDefinedPrimaryKey=[["source_id"]],
        config={
            "syncMode": "full_refresh",
            "destinationSyncMode": "append",
            "cursorField": ["discovered_cursor"],
            "primaryKey": [["discovered_id"]],
        },
        apiOnlyStreamField={"source": "discovered"},
    )

    result = _merge_catalog_with_configuration(
        _catalog(configured),
        _catalog(discovered),
    )

    merged = result["streams"][0]
    assert merged["config"] == {
        "syncMode": "incremental",
        "destinationSyncMode": "append_dedup",
        "cursorField": ["updated_at"],
        "primaryKey": [["source_id"]],
        "selected": True,
        "suggested": True,
        "includeFiles": True,
        "fieldSelectionEnabled": False,
        "selectedFields": [],
        "aliasName": "configured-users",
        "mappers": [{"name": "mapper"}],
        "destinationObjectName": "warehouse_users",
        "hashedFields": [{"fieldPath": ["id"]}],
    }
    assert merged["stream"]["apiOnlyStreamField"] == {"source": "discovered"}


def test_merge_uses_discovered_sync_mode_and_cursor_when_old_values_are_unavailable() -> (
    None
):
    configured = _stream_entry(
        "users",
        config={
            "syncMode": "full_refresh",
            "destinationSyncMode": "append",
            "cursorField": [],
            "primaryKey": [["configured_id"]],
            "selected": True,
            "fieldSelectionEnabled": False,
        },
    )
    discovered = _stream_entry(
        "users",
        sync_modes=("incremental",),
        config={
            "syncMode": "incremental",
            "destinationSyncMode": "append_dedup",
            "cursorField": ["new_cursor"],
            "primaryKey": [["discovered_id"]],
        },
    )

    merged = _merge_catalog_with_configuration(
        _catalog(configured),
        _catalog(discovered),
    )["streams"][0]["config"]

    assert merged["syncMode"] == "incremental"
    assert merged["cursorField"] == ["new_cursor"]
    assert merged["primaryKey"] == [["configured_id"]]
    assert merged["destinationSyncMode"] == "append"


def test_merge_uses_discovered_primary_key_when_neither_catalog_defines_one() -> None:
    configured = _stream_entry(
        "users",
        config={
            "syncMode": "full_refresh",
            "destinationSyncMode": "append",
            "primaryKey": [],
            "selected": True,
            "fieldSelectionEnabled": False,
        },
    )
    discovered = _stream_entry(
        "users",
        config={
            "syncMode": "full_refresh",
            "destinationSyncMode": "append",
            "primaryKey": [["discovered_id"]],
        },
    )

    merged = _merge_catalog_with_configuration(
        _catalog(configured),
        _catalog(discovered),
    )["streams"][0]["config"]

    assert merged["primaryKey"] == [["discovered_id"]]


def test_merge_prunes_field_selection_and_selects_new_fields() -> None:
    configured = _stream_entry(
        "users",
        schema_fields=("id", "old", "retained"),
        config={
            "syncMode": "full_refresh",
            "destinationSyncMode": "append",
            "selected": True,
            "fieldSelectionEnabled": True,
            "selectedFields": [
                {"fieldPath": ["id", "nested"]},
                {"fieldPath": ["old"]},
            ],
        },
    )
    discovered = _stream_entry(
        "users",
        schema_fields=("id", "retained", "new"),
        config={"syncMode": "full_refresh", "destinationSyncMode": "append"},
    )

    merged = _merge_catalog_with_configuration(
        _catalog(configured),
        _catalog(discovered),
    )["streams"][0]["config"]

    assert merged["selectedFields"] == [{"fieldPath": ["id"]}, {"fieldPath": ["new"]}]


def test_merge_drops_removed_streams_and_unselects_new_streams() -> None:
    configured = _catalog(
        _stream_entry("users"),
        _stream_entry("removed"),
    )
    discovered = _catalog(
        _stream_entry("users"),
        _stream_entry("new"),
    )

    merged = _merge_catalog_with_configuration(configured, discovered)

    assert [entry["stream"]["name"] for entry in merged["streams"]] == ["users", "new"]
    assert merged["streams"][1]["config"]["selected"] is False


def test_merge_round_trips_untouched_api_only_fields() -> None:
    configured = _stream_entry(
        "users",
        config={
            "syncMode": "full_refresh",
            "destinationSyncMode": "append",
            "selected": True,
            "fieldSelectionEnabled": False,
            "apiOnlyConfiguredField": {"keep": True},
        },
    )
    discovered = _catalog(
        _stream_entry(
            "users",
            config={
                "syncMode": "full_refresh",
                "destinationSyncMode": "append",
                "apiOnlyDiscoveredField": {"keep": True},
            },
            apiOnlyStreamField={"keep": True},
        )
    )
    discovered["apiOnlyCatalogField"] = {"keep": True}

    merged = _merge_catalog_with_configuration(_catalog(configured), discovered)

    assert merged["apiOnlyCatalogField"] == {"keep": True}
    assert merged["streams"][0]["stream"]["apiOnlyStreamField"] == {"keep": True}
    assert merged["streams"][0]["config"]["apiOnlyConfiguredField"] == {"keep": True}
    assert merged["streams"][0]["config"]["apiOnlyDiscoveredField"] == {"keep": True}
