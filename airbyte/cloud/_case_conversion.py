# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Generic camelCase ↔ snake_case key conversion helpers.

These helpers convert dict keys between camelCase (Config API format)
and snake_case (Airbyte protocol format). Values are left unchanged;
conversion is shallow (one level of dict keys) so opaque payloads
like `streamState` blobs and `jsonSchema` dicts are never modified.
"""

from __future__ import annotations

import re
from typing import Any


_CAMEL_BOUNDARY = re.compile(r"([A-Z])")


def _camel_to_snake(key: str) -> str:
    """Convert a single camelCase key to snake_case.

    Examples: `streamDescriptor` → `stream_descriptor`,
    `jsonSchema` → `json_schema`, `name` → `name`.
    """
    return _CAMEL_BOUNDARY.sub(r"_\1", key).lower().lstrip("_")


def _snake_to_camel(key: str) -> str:
    """Convert a single snake_case key to camelCase.

    Examples: `stream_descriptor` → `streamDescriptor`,
    `json_schema` → `jsonSchema`, `name` → `name`.
    """
    parts = key.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


def camel_to_snake_keys(data: dict[str, Any]) -> dict[str, Any]:
    """Shallow-convert all dict keys from camelCase to snake_case.

    Only the immediate keys of `data` are converted; nested dicts and
    lists inside values are returned as-is. This prevents accidental
    mutation of opaque payloads (connector state blobs, JSON Schema, etc.).
    """
    return {_camel_to_snake(k): v for k, v in data.items()}


def snake_to_camel_keys(data: dict[str, Any]) -> dict[str, Any]:
    """Shallow-convert all dict keys from snake_case to camelCase.

    Only the immediate keys of `data` are converted; nested dicts and
    lists inside values are returned as-is. This prevents accidental
    mutation of opaque payloads (connector state blobs, JSON Schema, etc.).
    """
    return {_snake_to_camel(k): v for k, v in data.items()}
