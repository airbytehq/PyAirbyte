# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Normalization helpers for connection catalog format conversion.

The Config API returns catalogs using camelCase keys (`syncCatalog` format).
The Airbyte protocol uses snake_case keys (`ConfiguredAirbyteCatalog` format).
These helpers convert between the two representations using generic
camelCase ↔ snake_case key converters rather than hard-coded field maps.

Structural differences (flattening/nesting the `config` block) are handled
by thin wrappers around the generic converters.
"""

from __future__ import annotations

from typing import Any

from airbyte.cloud._case_conversion import camel_to_snake_keys, snake_to_camel_keys


def _normalize_catalog_to_protocol(
    sync_catalog: dict[str, Any],
) -> dict[str, Any]:
    """Convert a `syncCatalog` dict to `ConfiguredAirbyteCatalog` protocol format.

    Transforms camelCase keys to snake_case and flattens the `config` block into
    each stream entry, matching the structure expected by connector `--catalog` flags.

    Args:
        sync_catalog: Catalog dict from the Config API (camelCase keys, nested `config`).

    Returns:
        Catalog dict in Airbyte protocol format (snake_case keys, flat stream entries).
    """
    configured_streams: list[dict[str, Any]] = []

    for stream_config in sync_catalog.get("streams", []):
        stream_info = stream_config.get("stream", {})
        config_info = stream_config.get("config", {})

        # Convert stream-level keys from camelCase → snake_case (shallow).
        # Opaque values like jsonSchema content are preserved as-is.
        normalized_stream = camel_to_snake_keys(stream_info)

        # Build the configured stream entry with flattened config keys.
        configured_entry: dict[str, Any] = {"stream": normalized_stream}
        configured_entry.update(camel_to_snake_keys(config_info))

        configured_streams.append(configured_entry)

    return {"streams": configured_streams}


def _denormalize_catalog_to_api(
    configured_catalog: dict[str, Any],
) -> dict[str, Any]:
    """Convert a `ConfiguredAirbyteCatalog` dict back to `syncCatalog` API format.

    Reverses `_normalize_catalog_to_protocol`: converts snake_case keys to camelCase
    and nests config fields back under a `config` block.

    Args:
        configured_catalog: Catalog dict in Airbyte protocol format.

    Returns:
        Catalog dict in Config API format (camelCase keys, nested `config`).
    """
    api_streams: list[dict[str, Any]] = []

    for entry in configured_catalog.get("streams", []):
        stream_info = entry.get("stream", {})

        # Convert stream-level keys from snake_case → camelCase (shallow).
        api_stream = snake_to_camel_keys(stream_info)

        # Everything except "stream" is a config field — nest back under "config".
        config = snake_to_camel_keys({k: v for k, v in entry.items() if k != "stream"})

        api_streams.append({"stream": api_stream, "config": config})

    return {"streams": api_streams}


def _is_protocol_catalog_format(catalog: dict[str, Any]) -> bool:
    """Detect whether a catalog dict is in Airbyte protocol format.

    Checks the first stream entry for snake_case config keys (`sync_mode`)
    at the top level, which indicates protocol format. API format nests
    these under a `config` key with camelCase (`syncMode`).

    Returns `True` for protocol format, `False` for Config API format.
    """
    streams = catalog.get("streams", [])
    if not streams:
        return False

    first = streams[0]
    # Protocol format has sync_mode at top level; API format has it inside config
    return "sync_mode" in first or "destination_sync_mode" in first
