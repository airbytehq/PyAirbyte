# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Utility functions for working with text."""

from __future__ import annotations

from typing import overload

import ulid


_TRUE_STR_VALUES: frozenset[str] = frozenset({"1", "true", "t", "yes", "y", "on"})
"""String values that mean `True` in environment variables and config values."""

_FALSE_STR_VALUES: frozenset[str] = frozenset({"0", "false", "f", "no", "n", "off"})
"""String values that mean `False` in environment variables and config values."""


@overload
def _str_to_bool(value: str | None, *, default: bool) -> bool: ...


@overload
def _str_to_bool(value: str | None, *, default: None = None) -> bool | None: ...


def _str_to_bool(value: str | None, *, default: bool | None = None) -> bool | None:
    """Convert an environment variable or config value to a boolean.

    Matching is case-insensitive and ignores surrounding whitespace. A value that is
    unset, blank, or unrecognized yields `default`, which is `None` unless the caller
    says otherwise, so "no value" stays distinguishable from `False`.
    """
    normalized = (value or "").strip().lower()
    if normalized in _TRUE_STR_VALUES:
        return True
    if normalized in _FALSE_STR_VALUES:
        return False
    return default


def generate_ulid() -> str:
    """Generate a new ULID."""
    return str(ulid.ULID())


def generate_random_suffix() -> str:
    """Generate a random suffix for use in temporary names.

    By default, this function generates a ULID and returns a 9-character string
    which will be monotonically sortable. It is not guaranteed to be unique but
    is sufficient for small-scale and medium-scale use cases.
    """
    ulid_str = generate_ulid().lower()
    return ulid_str[:6] + ulid_str[-3:]
