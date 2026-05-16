# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Output helpers for CLI commands."""

from __future__ import annotations

import orjson


def json_output(value: object) -> None:
    """Print a JSON response to stdout."""
    print(orjson.dumps(value, option=orjson.OPT_INDENT_2).decode())


__all__ = ["json_output"]
