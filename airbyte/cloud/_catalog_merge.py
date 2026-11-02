# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Merge configured settings into a newly discovered Config API catalog.

This ports `CatalogMergeHelper.mergeCatalogWithConfiguration` from
`airbyte-platform-internal/oss/airbyte-commons-server/src/main/kotlin/io/airbyte/commons/server/handlers/helpers/CatalogMergeHelper.kt`.
The previous discovered catalog is unavailable here, so the configured catalog is used
for both original catalog arguments, matching the server's configured-catalog fallback.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any


def _stream_descriptor(
    name: str | None,
    namespace: str | None,
) -> tuple[str | None, str | None]:
    return name, namespace or None


def _top_level_fields(stream: dict[str, Any]) -> list[str]:
    schema = stream.get("jsonSchema")
    properties = schema.get("properties") if isinstance(schema, dict) else None
    return list(properties) if isinstance(properties, dict) else []


def _merge_sync_mode(
    output_config: dict[str, Any],
    configured_config: dict[str, Any],
    discovered_config: dict[str, Any],
    discovered_stream: dict[str, Any],
) -> None:
    configured_sync_mode = configured_config.get("syncMode")
    supported_sync_modes = discovered_stream.get("supportedSyncModes", [])
    if configured_sync_mode in supported_sync_modes:
        output_config["syncMode"] = deepcopy(configured_sync_mode)
    elif "syncMode" in discovered_config:
        output_config["syncMode"] = deepcopy(discovered_config["syncMode"])
    else:
        output_config.pop("syncMode", None)


def _merge_cursor_field(
    output_config: dict[str, Any],
    configured_config: dict[str, Any],
    discovered_config: dict[str, Any],
) -> None:
    configured_cursor_field = configured_config.get("cursorField")
    if configured_cursor_field:
        output_config["cursorField"] = deepcopy(configured_cursor_field)
    elif "cursorField" in discovered_config:
        output_config["cursorField"] = deepcopy(discovered_config["cursorField"])
    else:
        output_config.pop("cursorField", None)


def _merge_primary_key(
    output_config: dict[str, Any],
    configured_config: dict[str, Any],
    discovered_config: dict[str, Any],
    discovered_stream: dict[str, Any],
) -> None:
    source_defined_primary_key = discovered_stream.get("sourceDefinedPrimaryKey")
    configured_primary_key = configured_config.get("primaryKey")
    if source_defined_primary_key:
        output_config["primaryKey"] = deepcopy(source_defined_primary_key)
    elif configured_primary_key:
        output_config["primaryKey"] = deepcopy(configured_primary_key)
    elif "primaryKey" in discovered_config:
        output_config["primaryKey"] = deepcopy(discovered_config["primaryKey"])
    else:
        output_config.pop("primaryKey", None)


def _merge_hashed_fields(
    output_config: dict[str, Any],
    configured_config: dict[str, Any],
    discovered_stream: dict[str, Any],
) -> None:
    discovered_fields = set(_top_level_fields(discovered_stream))
    hashed_fields = [
        deepcopy(hashed_field)
        for hashed_field in configured_config.get("hashedFields", [])
        if (field_path := hashed_field.get("fieldPath")) and field_path[0] in discovered_fields
    ]
    if hashed_fields:
        output_config["hashedFields"] = hashed_fields
    else:
        output_config.pop("hashedFields", None)


def _merge_selected_fields(
    output_config: dict[str, Any],
    configured_entry: dict[str, Any],
    configured_config: dict[str, Any],
    discovered_stream: dict[str, Any],
) -> None:
    if not output_config.get("fieldSelectionEnabled"):
        output_config["selectedFields"] = []
        return

    original_discovered_stream = configured_entry.get("stream", {})
    originally_discovered_fields = set(_top_level_fields(original_discovered_stream))
    originally_selected_fields = {
        field_path[0]
        for selected_field in configured_config.get("selectedFields", [])
        if (field_path := selected_field.get("fieldPath"))
    }
    discovered_field_order = _top_level_fields(discovered_stream)
    output_config["selectedFields"] = [
        {"fieldPath": [field]}
        for field in discovered_field_order
        if field in originally_selected_fields or field not in originally_discovered_fields
    ]


def _merge_existing_config(
    discovered_stream: dict[str, Any],
    discovered_config: dict[str, Any],
    configured_entry: dict[str, Any],
) -> dict[str, Any]:
    configured_config = configured_entry.get("config", {})
    output_config = deepcopy(configured_config)
    output_config.update(deepcopy(discovered_config))

    _merge_sync_mode(
        output_config,
        configured_config,
        discovered_config,
        discovered_stream,
    )
    _merge_cursor_field(output_config, configured_config, discovered_config)
    if "destinationSyncMode" in configured_config:
        output_config["destinationSyncMode"] = deepcopy(configured_config["destinationSyncMode"])
    else:
        output_config.pop("destinationSyncMode", None)
    _merge_primary_key(
        output_config,
        configured_config,
        discovered_config,
        discovered_stream,
    )

    for field in (
        "aliasName",
        "selected",
        "suggested",
        "includeFiles",
        "fieldSelectionEnabled",
        "mappers",
        "destinationObjectName",
    ):
        if field in configured_config:
            output_config[field] = deepcopy(configured_config[field])
        else:
            output_config.pop(field, None)

    _merge_hashed_fields(output_config, configured_config, discovered_stream)
    _merge_selected_fields(
        output_config,
        configured_entry,
        configured_config,
        discovered_stream,
    )
    return output_config


def _merge_discovered_entry(
    discovered_entry: dict[str, Any],
    configured_entry: dict[str, Any] | None,
) -> dict[str, Any]:
    discovered_stream = discovered_entry.get("stream", {})
    discovered_config = discovered_entry.get("config", {})
    output_entry = deepcopy(discovered_entry)
    output_entry["stream"] = deepcopy(discovered_stream)
    if configured_entry is None:
        output_config = deepcopy(discovered_config)
        output_config["selected"] = False
    else:
        output_config = _merge_existing_config(
            discovered_stream,
            discovered_config,
            configured_entry,
        )
    output_entry["config"] = output_config
    return output_entry


def _merge_catalog_with_configuration(
    configured_catalog: dict[str, Any],
    discovered_catalog: dict[str, Any],
) -> dict[str, Any]:
    """Return the new discovery with applicable settings from the configured catalog."""
    configured_by_descriptor = {
        _stream_descriptor(
            entry.get("stream", {}).get("name"),
            entry.get("stream", {}).get("namespace"),
        ): entry
        for entry in configured_catalog.get("streams", [])
    }
    merged_catalog = deepcopy(discovered_catalog)
    merged_catalog["streams"] = [
        _merge_discovered_entry(
            discovered_entry,
            configured_by_descriptor.get(
                _stream_descriptor(
                    discovered_entry.get("stream", {}).get("name"),
                    discovered_entry.get("stream", {}).get("namespace"),
                )
            ),
        )
        for discovered_entry in discovered_catalog.get("streams", [])
    ]
    return merged_catalog


def _preserve_api_only_catalog_fields(
    raw_catalog: dict[str, Any],
    typed_catalog: dict[str, Any],
) -> dict[str, Any]:
    """Copy API-only data through the typed catalog update path."""
    result = deepcopy(typed_catalog)
    for key, value in raw_catalog.items():
        result.setdefault(key, deepcopy(value))

    raw_streams_by_descriptor = {
        _stream_descriptor(
            entry.get("stream", {}).get("name"),
            entry.get("stream", {}).get("namespace"),
        ): entry
        for entry in raw_catalog.get("streams", [])
    }
    for entry in result.get("streams", []):
        stream = entry.get("stream", {})
        raw_entry = raw_streams_by_descriptor.get(
            _stream_descriptor(stream.get("name"), stream.get("namespace"))
        )
        if raw_entry is None:
            continue

        for key, value in raw_entry.items():
            entry.setdefault(key, deepcopy(value))
        for key, value in raw_entry.get("stream", {}).items():
            stream.setdefault(key, deepcopy(value))
        config = entry.get("config", {})
        for key, value in raw_entry.get("config", {}).items():
            config.setdefault(key, deepcopy(value))

    return result
