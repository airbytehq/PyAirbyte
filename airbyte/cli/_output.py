# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Output helpers for CLI commands."""

from __future__ import annotations

from enum import Enum
from typing import Protocol, runtime_checkable

import orjson


@runtime_checkable
class _SupportsToDict(Protocol):
    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable mapping."""
        raise NotImplementedError


def _json_default(value: object) -> object:
    if isinstance(value, _SupportsToDict):
        return value.to_dict()
    if isinstance(value, Enum):
        return value.value
    raise TypeError


def json_output(value: object) -> None:
    """Print a JSON response to stdout."""
    print(orjson.dumps(value, default=_json_default, option=orjson.OPT_INDENT_2).decode())


__all__ = ["json_output"]
