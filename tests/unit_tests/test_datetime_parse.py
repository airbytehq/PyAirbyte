# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for datetime parsing helpers."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from airbyte._util.datetime_parse import parse_datetime


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(
            1_767_225_600,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            id="integer-unix-timestamp",
        ),
        pytest.param(
            "1767225600",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            id="string-unix-timestamp",
        ),
        pytest.param(
            "2026-01-01T00:00:00Z",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            id="rfc3339-zulu",
        ),
        pytest.param(
            "2026-01-01T01:00:00+01:00",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            id="timezone-offset",
        ),
        pytest.param(
            "2026-01-01",
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            id="date-only",
        ),
    ],
)
def test_parse_datetime(value: str | int, expected: datetime) -> None:
    assert parse_datetime(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(object(), id="object"),
        pytest.param("not-a-date", id="invalid-string"),
    ],
)
def test_parse_datetime_rejects_invalid_values(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        parse_datetime(value)  # type: ignore[arg-type]
