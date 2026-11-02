# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Private helpers for evaluating Cloud stream configuration overrides."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airbyte_protocol.models import DestinationSyncMode, SyncMode

from airbyte.cloud.models import StreamConfigChange
from airbyte.exceptions import AirbyteLibInputError, AirbyteUnsafeStreamConfigChangeError


if TYPE_CHECKING:
    from airbyte_protocol.models import (
        AirbyteStream,
        ConfiguredAirbyteCatalog,
        ConfiguredAirbyteStream,
    )

    from airbyte.cloud.models import StreamConfigOverride


def _index_catalog_streams(
    catalog: ConfiguredAirbyteCatalog,
    *,
    connection_id: str,
) -> list[str]:
    if not catalog.streams:
        raise AirbyteLibInputError(
            message="Connection catalog is empty or missing.",
            context={"connection_id": connection_id},
        )

    available_streams: list[str] = []
    catalog_keys: set[tuple[str, str | None]] = set()
    duplicate_catalog_streams: list[str] = []
    for entry in catalog.streams:
        stream = entry.stream
        name = stream.name
        namespace = stream.namespace
        available_streams.append(f"{namespace}.{name}" if namespace else name)
        key = (name, namespace or None)
        if key in catalog_keys:
            duplicate_catalog_streams.append(f"{namespace}.{name}" if namespace else name)
        catalog_keys.add(key)

    if duplicate_catalog_streams:
        raise AirbyteLibInputError(
            message=(
                "Connection catalog contains duplicate stream entries: "
                f"{', '.join(duplicate_catalog_streams)}."
            ),
            context={
                "connection_id": connection_id,
                "duplicate_streams": duplicate_catalog_streams,
            },
        )

    return available_streams


def _resolve_stream_override_target(
    catalog: ConfiguredAirbyteCatalog,
    override: StreamConfigOverride,
    *,
    override_index: int,
    override_keys: set[tuple[str, str | None]],
) -> ConfiguredAirbyteStream | str:
    label = _stream_override_label(override_index, override)
    matching_entries = [
        entry
        for entry in catalog.streams
        if entry.stream.name == override.stream_name
        and (
            override.stream_namespace is None
            or (entry.stream.namespace or None) == (override.stream_namespace or None)
        )
    ]
    if not matching_entries:
        return f"{label}: stream not found."
    if len(matching_entries) > 1:
        matching_namespaces = [entry.stream.namespace for entry in matching_entries]
        return (
            f"{label}: stream name is ambiguous; specify a namespace. "
            f"Matching namespaces: {matching_namespaces}."
        )

    target_entry = matching_entries[0]
    target_key = (override.stream_name, target_entry.stream.namespace or None)
    if target_key in override_keys:
        return f"{label}: this stream is overridden more than once."
    override_keys.add(target_key)
    return target_entry


def _apply_stream_override(
    catalog: ConfiguredAirbyteCatalog,
    override: StreamConfigOverride,
    *,
    override_index: int,
    override_keys: set[tuple[str, str | None]],
) -> StreamConfigChange | str:
    target_entry = _resolve_stream_override_target(
        catalog,
        override,
        override_index=override_index,
        override_keys=override_keys,
    )
    if isinstance(target_entry, str):
        return target_entry

    label = _stream_override_label(override_index, override)
    candidate = target_entry.model_copy(deep=True)
    stream = candidate.stream
    if override.sync_mode is not None:
        supported_sync_modes = [mode.value for mode in stream.supported_sync_modes]
        if override.sync_mode.value not in supported_sync_modes:
            return (
                f"{label}: sync mode '{override.sync_mode.value}' is unsupported. "
                f"Supported modes: {supported_sync_modes}."
            )
        candidate.sync_mode = override.sync_mode

    if override.destination_sync_mode is not None:
        candidate.destination_sync_mode = override.destination_sync_mode
    if override.cursor_field is not None:
        candidate.cursor_field = [override.cursor_field]

    cursor_error = _resolve_incremental_cursor(stream, candidate)
    if cursor_error is not None:
        return f"{label}: {cursor_error}"

    if (
        candidate.destination_sync_mode == DestinationSyncMode.append_dedup
        and not candidate.primary_key
        and not stream.source_defined_primary_key
    ):
        return (
            f"{label}: destination sync mode "
            f"'{candidate.destination_sync_mode.value}' requires a primary key."
        )

    changed_fields = _changed_stream_config_fields(target_entry, candidate)
    if override.sync_mode is not None:
        target_entry.sync_mode = candidate.sync_mode
    if override.destination_sync_mode is not None:
        target_entry.destination_sync_mode = candidate.destination_sync_mode
    if override.cursor_field is not None or candidate.cursor_field != target_entry.cursor_field:
        target_entry.cursor_field = candidate.cursor_field

    return StreamConfigChange(
        name=stream.name,
        namespace=stream.namespace,
        changed_fields=changed_fields,
    )


def _resolve_incremental_cursor(
    stream_def: AirbyteStream,
    config: ConfiguredAirbyteStream,
) -> str | None:
    if config.sync_mode != SyncMode.incremental or config.cursor_field:
        return None
    if stream_def.source_defined_cursor:
        return None
    default_cursor_field = stream_def.default_cursor_field
    if default_cursor_field:
        config.cursor_field = list(default_cursor_field)
        return None
    return "incremental sync requires a cursor field."


def _raise_for_override_errors(
    validation_errors: list[str],
    available_streams: list[str],
    *,
    connection_id: str,
) -> None:
    if not validation_errors:
        return

    message = f"Invalid stream configuration overrides: {' '.join(validation_errors)}"
    context: dict[str, Any] = {
        "connection_id": connection_id,
        "validation_errors": validation_errors,
    }
    if any(": stream not found." in error for error in validation_errors):
        message += f" Available streams: {available_streams}."
        context["available_streams"] = available_streams

    raise AirbyteLibInputError(
        message=message,
        guidance="Correct every listed override and try again.",
        context=context,
    )


def _stream_override_label(
    override_index: int,
    override: StreamConfigOverride,
) -> str:
    return f"override {override_index} for '{override.stream_name}'" + (
        f" in namespace '{override.stream_namespace}'"
        if override.stream_namespace is not None
        else ""
    )


def _changed_stream_config_fields(
    current: ConfiguredAirbyteStream,
    updated: ConfiguredAirbyteStream,
) -> list[str]:
    fields = (
        ("syncMode", "sync_mode"),
        ("destinationSyncMode", "destination_sync_mode"),
        ("cursorField", "cursor_field"),
        ("primaryKey", "primary_key"),
    )
    return [
        wire_name
        for wire_name, field_name in fields
        if getattr(current, field_name) != getattr(updated, field_name)
    ]


def _stream_has_saved_state(
    state_messages: list[dict[str, Any]],
    stream_name: str,
    stream_namespace: str | None,
) -> bool:
    for message in state_messages:
        if not isinstance(message, dict):
            return True
        message_type = message.get("type")
        if message_type in {"GLOBAL", "LEGACY"}:
            return True
        if message_type != "STREAM":
            return True

        stream = message.get("stream")
        descriptor = stream.get("stream_descriptor") if isinstance(stream, dict) else None
        if not isinstance(descriptor, dict) or not isinstance(descriptor.get("name"), str):
            return True
        if descriptor.get("name") != stream_name:
            continue
        if (descriptor.get("namespace") or None) == (stream_namespace or None):
            return True
    return False


def _find_unsafe_stream_changes(
    old_catalog: ConfiguredAirbyteCatalog,
    new_catalog: ConfiguredAirbyteCatalog,
    state_messages: list[dict[str, Any]],
) -> list[StreamConfigChange]:
    old_streams = {
        (entry.stream.name, entry.stream.namespace): entry for entry in old_catalog.streams
    }
    unsafe_streams: list[StreamConfigChange] = []
    for entry in new_catalog.streams:
        stream = entry.stream
        current_entry = old_streams.get((stream.name, stream.namespace))
        if current_entry is None:
            continue
        changed_fields = _changed_stream_config_fields(current_entry, entry)
        if changed_fields and _stream_has_saved_state(
            state_messages,
            stream.name,
            stream.namespace,
        ):
            unsafe_streams.append(
                StreamConfigChange(
                    name=stream.name,
                    namespace=stream.namespace,
                    changed_fields=changed_fields,
                )
            )
    return unsafe_streams


def _raise_for_unsafe_stream_config_changes(
    unsafe_streams: list[StreamConfigChange],
    *,
    allow_unsafe: bool,
    connection_id: str,
) -> None:
    if unsafe_streams and not allow_unsafe:
        stream_details = ", ".join(
            f"{stream.label} ({', '.join(stream.changed_fields)})" for stream in unsafe_streams
        )
        raise AirbyteUnsafeStreamConfigChangeError(
            message=(
                "Stream configuration changes affect streams with saved sync state: "
                f"{stream_details}."
            ),
            context={
                "connection_id": connection_id,
                "unsafe_streams": [stream.model_dump() for stream in unsafe_streams],
            },
        )
