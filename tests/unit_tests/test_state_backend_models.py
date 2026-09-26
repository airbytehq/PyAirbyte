# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Tests for the SQL state backend ORM models."""

from __future__ import annotations

from datetime import datetime

import pytest

from airbyte.caches._state_backend import (
    CacheStreamStateModel,
    DestinationStreamStateModel,
)


@pytest.mark.parametrize(
    "model",
    [
        pytest.param(CacheStreamStateModel, id="cache_state"),
        pytest.param(DestinationStreamStateModel, id="destination_state"),
    ],
)
def test_last_updated_defaults_are_evaluated_per_write(model: type) -> None:
    """`default`/`onupdate` must be callables, not a timestamp frozen at import time."""
    column = model.__table__.c.last_updated
    for arg in (column.default, column.onupdate):
        assert arg is not None
        assert arg.is_callable, (
            "last_updated default must be a callable, not a fixed value"
        )
        first = arg.arg(None)
        assert isinstance(first, datetime)
        assert first.tzinfo is not None
        assert arg.arg(None) >= first
