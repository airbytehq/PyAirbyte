# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Destinations module."""

from __future__ import annotations

from airbyte.destinations import util
from airbyte.destinations.base import Destination
from airbyte.destinations.util import (
    get_destination,
)


__all__ = [
    # Modules
    "util",
    # Methods
    "get_destination",
    # Classes
    "Destination",
]
