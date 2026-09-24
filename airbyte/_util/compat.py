# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Compatibility shims for older supported Python versions.

Currently no shims are required; `StrEnum` is re-exported from the standard library
so existing imports keep working.
"""

from __future__ import annotations

from enum import StrEnum


__all__ = ["StrEnum"]
