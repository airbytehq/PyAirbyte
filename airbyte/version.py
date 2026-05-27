# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Support for PyAirbyte version checks."""

from __future__ import annotations

import importlib.metadata


def _get_installed_version() -> str:
    for distribution_name in ("airbyte", "airbyte-slim"):
        try:
            return importlib.metadata.version(distribution_name)
        except importlib.metadata.PackageNotFoundError:
            continue

    raise importlib.metadata.PackageNotFoundError("airbyte")


airbyte_version = _get_installed_version()


def get_version() -> str:
    """Return the version of PyAirbyte."""
    return airbyte_version
