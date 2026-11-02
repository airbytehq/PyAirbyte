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

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from airbyte_protocol.models import AirbyteStream, SyncMode


_API_MODEL_CONFIG = ConfigDict(
    extra="allow",
    alias_generator=to_camel,
    populate_by_name=True,
)


class _ApiStream(AirbyteStream):
    model_config = _API_MODEL_CONFIG


class _SelectedField(BaseModel):
    model_config = _API_MODEL_CONFIG

    field_path: list[str]


class _ApiStreamConfig(BaseModel):
    model_config = _API_MODEL_CONFIG

    sync_mode: SyncMode | None = None
    destination_sync_mode: str | None = None
    cursor_field: list[str] | None = None
    primary_key: list[list[str]] | None = None
    selected: bool | None = None
    suggested: bool | None = None
    field_selection_enabled: bool | None = None
    selected_fields: list[_SelectedField] | None = None
    hashed_fields: list[_SelectedField] | None = None
    mappers: list[dict[str, Any]] | None = None
    alias_name: str | None = None
    include_files: bool | None = None
    destination_object_name: str | None = None


class _ApiCatalogEntry(BaseModel):
    model_config = _API_MODEL_CONFIG

    stream: _ApiStream
    config: _ApiStreamConfig


class _ApiCatalog(BaseModel):
    model_config = _API_MODEL_CONFIG

    streams: list[_ApiCatalogEntry] = Field(default_factory=list)


def _copy_explicit_fields(
    source: BaseModel,
    target: BaseModel,
    *,
    only_missing: bool = False,
) -> None:
    for field_name in source.model_fields_set:
        if only_missing and field_name in target.model_fields_set:
            continue
        setattr(target, field_name, deepcopy(getattr(source, field_name)))


def _unset_field(model: BaseModel, field_name: str) -> None:
    setattr(model, field_name, None)
    model.model_fields_set.discard(field_name)


def _stream_descriptor(
    name: str | None,
    namespace: str | None,
) -> tuple[str | None, str | None]:
    return name, namespace or None


def _top_level_fields(stream: _ApiStream) -> list[str]:
    try:
        properties = stream.json_schema["properties"]
        return list(properties.keys())
    except (AttributeError, KeyError):
        return []


def _merge_sync_mode(
    output_config: _ApiStreamConfig,
    configured_config: _ApiStreamConfig,
    discovered_config: _ApiStreamConfig,
    discovered_stream: _ApiStream,
) -> None:
    configured_sync_mode = configured_config.sync_mode
    if configured_sync_mode in discovered_stream.supported_sync_modes:
        output_config.sync_mode = deepcopy(configured_sync_mode)
    elif "sync_mode" in discovered_config.model_fields_set:
        output_config.sync_mode = deepcopy(discovered_config.sync_mode)
    else:
        _unset_field(output_config, "sync_mode")


def _merge_cursor_field(
    output_config: _ApiStreamConfig,
    configured_config: _ApiStreamConfig,
    discovered_config: _ApiStreamConfig,
) -> None:
    configured_cursor_field = configured_config.cursor_field
    if configured_cursor_field:
        output_config.cursor_field = deepcopy(configured_cursor_field)
    elif "cursor_field" in discovered_config.model_fields_set:
        output_config.cursor_field = deepcopy(discovered_config.cursor_field)
    else:
        _unset_field(output_config, "cursor_field")


def _merge_primary_key(
    output_config: _ApiStreamConfig,
    configured_config: _ApiStreamConfig,
    discovered_config: _ApiStreamConfig,
    discovered_stream: _ApiStream,
) -> None:
    source_defined_primary_key = discovered_stream.source_defined_primary_key
    configured_primary_key = configured_config.primary_key
    if source_defined_primary_key:
        output_config.primary_key = deepcopy(source_defined_primary_key)
    elif configured_primary_key:
        output_config.primary_key = deepcopy(configured_primary_key)
    elif "primary_key" in discovered_config.model_fields_set:
        output_config.primary_key = deepcopy(discovered_config.primary_key)
    else:
        _unset_field(output_config, "primary_key")


def _merge_hashed_fields(
    output_config: _ApiStreamConfig,
    configured_config: _ApiStreamConfig,
    discovered_stream: _ApiStream,
) -> None:
    discovered_fields = set(_top_level_fields(discovered_stream))
    hashed_fields = [
        hashed_field.model_copy(deep=True)
        for hashed_field in configured_config.hashed_fields or []
        if hashed_field.field_path and hashed_field.field_path[0] in discovered_fields
    ]
    if hashed_fields:
        output_config.hashed_fields = hashed_fields
    else:
        _unset_field(output_config, "hashed_fields")


def _merge_selected_fields(
    output_config: _ApiStreamConfig,
    configured_entry: _ApiCatalogEntry,
    configured_config: _ApiStreamConfig,
    discovered_stream: _ApiStream,
) -> None:
    if not output_config.field_selection_enabled:
        output_config.selected_fields = []
        return

    originally_discovered_fields = set(_top_level_fields(configured_entry.stream))
    originally_selected_fields = {
        selected_field.field_path[0]
        for selected_field in configured_config.selected_fields or []
        if selected_field.field_path
    }
    discovered_field_order = _top_level_fields(discovered_stream)
    output_config.selected_fields = [
        _SelectedField(field_path=[field])
        for field in discovered_field_order
        if field in originally_selected_fields or field not in originally_discovered_fields
    ]


def _merge_existing_config(
    discovered_stream: _ApiStream,
    discovered_config: _ApiStreamConfig,
    configured_entry: _ApiCatalogEntry,
) -> _ApiStreamConfig:
    configured_config = configured_entry.config
    output_config = configured_config.model_copy(deep=True)
    _copy_explicit_fields(discovered_config, output_config)

    _merge_sync_mode(
        output_config,
        configured_config,
        discovered_config,
        discovered_stream,
    )
    _merge_cursor_field(output_config, configured_config, discovered_config)
    if "destination_sync_mode" in configured_config.model_fields_set:
        output_config.destination_sync_mode = deepcopy(configured_config.destination_sync_mode)
    else:
        _unset_field(output_config, "destination_sync_mode")
    _merge_primary_key(
        output_config,
        configured_config,
        discovered_config,
        discovered_stream,
    )

    for field in (
        "alias_name",
        "selected",
        "suggested",
        "include_files",
        "field_selection_enabled",
        "mappers",
        "destination_object_name",
    ):
        if field in configured_config.model_fields_set:
            setattr(output_config, field, deepcopy(getattr(configured_config, field)))
        else:
            _unset_field(output_config, field)

    _merge_hashed_fields(output_config, configured_config, discovered_stream)
    _merge_selected_fields(
        output_config,
        configured_entry,
        configured_config,
        discovered_stream,
    )
    return output_config


def _merge_discovered_entry(
    discovered_entry: _ApiCatalogEntry,
    configured_entry: _ApiCatalogEntry | None,
) -> _ApiCatalogEntry:
    output_entry = discovered_entry.model_copy(deep=True)
    if configured_entry is None:
        output_config = discovered_entry.config.model_copy(deep=True)
        output_config.selected = False
    else:
        output_config = _merge_existing_config(
            discovered_entry.stream,
            discovered_entry.config,
            configured_entry,
        )
    output_entry.config = output_config
    return output_entry


def _merge_catalog_with_configuration(
    configured_catalog: dict[str, Any],
    discovered_catalog: dict[str, Any],
) -> dict[str, Any]:
    """Return the new discovery with applicable settings from the configured catalog."""
    configured = _ApiCatalog.model_validate(configured_catalog)
    discovered = _ApiCatalog.model_validate(discovered_catalog)
    configured_by_descriptor = {
        _stream_descriptor(
            entry.stream.name,
            entry.stream.namespace,
        ): entry
        for entry in configured.streams
    }
    merged_catalog = discovered.model_copy(deep=True)
    merged_catalog.streams = [
        _merge_discovered_entry(
            discovered_entry,
            configured_by_descriptor.get(
                _stream_descriptor(
                    discovered_entry.stream.name,
                    discovered_entry.stream.namespace,
                )
            ),
        )
        for discovered_entry in discovered.streams
    ]
    return merged_catalog.model_dump(mode="json", by_alias=True, exclude_unset=True)


def _preserve_api_only_catalog_fields(
    raw_catalog: dict[str, Any],
    typed_catalog: dict[str, Any],
) -> dict[str, Any]:
    """Copy API-only data through the typed catalog update path."""
    raw = _ApiCatalog.model_validate(raw_catalog)
    result = _ApiCatalog.model_validate(typed_catalog)
    _copy_explicit_fields(raw, result, only_missing=True)

    raw_streams_by_descriptor = {
        _stream_descriptor(
            entry.stream.name,
            entry.stream.namespace,
        ): entry
        for entry in raw.streams
    }
    for entry in result.streams:
        raw_entry = raw_streams_by_descriptor.get(
            _stream_descriptor(entry.stream.name, entry.stream.namespace)
        )
        if raw_entry is None:
            continue

        _copy_explicit_fields(raw_entry, entry, only_missing=True)
        _copy_explicit_fields(raw_entry.stream, entry.stream, only_missing=True)
        _copy_explicit_fields(raw_entry.config, entry.config, only_missing=True)

    return result.model_dump(mode="json", by_alias=True, exclude_unset=True)
