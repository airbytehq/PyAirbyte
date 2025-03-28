# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Utility functions for working with text."""

from __future__ import annotations

import ulid


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
