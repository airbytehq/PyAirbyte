# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Backwards compatibility shim for airbyte.sources.registry.

This module re-exports symbols from airbyte.registry for backwards compatibility.
New code should import from airbyte.registry directly.
"""

from __future__ import annotations

from airbyte.registry import (
    ConnectorMetadata,
    InstallType,
    Language,
    get_available_connectors,
    get_connector_metadata,
)

__all__ = [
    "ConnectorMetadata",
    "InstallType",
    "Language",
    "get_available_connectors",
    "get_connector_metadata",
]
