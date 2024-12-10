# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Sources connectors module for PyAirbyte."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte.sources.base import Source
from airbyte.sources.registry import (
    ConnectorMetadata,
    get_available_connectors,
    get_connector_metadata,
)
from airbyte.sources.util import (
    get_benchmark_source,
    get_source,
)


# Submodules imported here for documentation reasons: https://github.com/mitmproxy/pdoc/issues/757
if TYPE_CHECKING:
    # ruff: noqa: TC004  # imports used for more than type checking
    from airbyte.sources import (
        base,
        registry,
        util,
    )

__all__ = [
    # Submodules
    "base",
    "registry",
    "util",
    # Factories
    "get_source",
    "get_benchmark_source",
    # Helper Functions
    "get_available_connectors",
    "get_connector_metadata",
    # Classes
    "Source",
    "ConnectorMetadata",
]
