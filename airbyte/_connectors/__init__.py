# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Shared connector primitives used across local, Cloud, and MCP implementations."""

from __future__ import annotations

from airbyte._util.compat import StrEnum


class ConnectorType(StrEnum):
    """Connector type: `source` or `destination`."""

    SOURCE = "source"
    DESTINATION = "destination"

    @classmethod
    def parse(cls, value: str) -> ConnectorType:
        """Parse a connector type value."""
        try:
            return cls(value)
        except ValueError:
            valid = ", ".join(f"`{member.value}`" for member in cls)
            raise ValueError(
                f"Unrecognized connector type: {value!r}. Expected one of: {valid}."
            ) from None


__all__ = ["ConnectorType"]
