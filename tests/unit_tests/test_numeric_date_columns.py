# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Date parsing on load follows the declared type, not the column name."""

from __future__ import annotations

import io

import pandas as pd
import sqlalchemy

from airbyte.types import SQLTypeConverter


def _read(row: str, datetime_columns: list[str]) -> pd.DataFrame:
    """Read one JSONL record the way the SQL processor now does."""
    return pd.read_json(
        io.StringIO(row),
        lines=True,
        convert_dates=datetime_columns,
        keep_default_dates=False,
    )


def _declared_datetime_columns(schema: dict[str, dict]) -> list[str]:
    """The columns a stream's JSON schema declares as timestamps."""
    converter = SQLTypeConverter()
    return [
        name
        for name, prop in schema.items()
        if isinstance(converter.to_sql_type(prop), sqlalchemy.types.DateTime)
    ]


class TestSchemaDrivenDateParsing:
    def test_numeric_column_named_date_stays_numeric(self) -> None:
        """Epoch milliseconds in a column called `date`, declared a number."""
        schema = {"date": {"type": "number"}, "id": {"type": "string"}}
        row = '{"date": 1788528600000, "id": "abc"}'

        frame = _read(row, _declared_datetime_columns(schema))

        assert frame["date"].dtype == "int64"
        assert frame["date"].iloc[0] == 1788528600000

    def test_numeric_column_ending_in_at_stays_numeric(self) -> None:
        """`_at` is one of pandas' default date-like suffixes."""
        schema = {"score_at": {"type": "number"}}
        frame = _read('{"score_at": 1788528600000}', _declared_datetime_columns(schema))
        assert frame["score_at"].dtype == "int64"

    def test_declared_timestamp_is_still_parsed(self) -> None:
        """Declared date-time still becomes a timestamp."""
        schema = {"updated_at": {"type": "string", "format": "date-time"}}
        frame = _read(
            '{"updated_at": "2026-09-04T13:30:00.000Z"}',
            _declared_datetime_columns(schema),
        )
        assert "datetime64" in str(frame["updated_at"].dtype)

    def test_declared_timestamp_sent_as_epoch_is_still_parsed(self) -> None:
        """Declared date-time sent as an epoch still parses, so nothing regresses."""
        schema = {"occurred_at": {"type": "string", "format": "date-time"}}
        frame = _read(
            '{"occurred_at": 1788528600000}', _declared_datetime_columns(schema)
        )
        assert "datetime64" in str(frame["occurred_at"].dtype)

    def test_name_alone_does_not_decide(self) -> None:
        """Same value, different declared types, different results."""
        schema = {
            "date": {"type": "number"},
            "captured_at": {"type": "string", "format": "date-time"},
        }
        row = '{"date": 1788528600000, "captured_at": 1788528600000}'

        frame = _read(row, _declared_datetime_columns(schema))

        assert frame["date"].dtype == "int64", "declared a number"
        assert "datetime64" in str(frame["captured_at"].dtype), "declared date-time"

    def test_conversion_no_longer_depends_on_the_values(self) -> None:
        """The same declared type gives the same column type every batch."""
        schema = {"end_time": {"type": "number"}}
        columns = _declared_datetime_columns(schema)

        small = _read('{"end_time": 900}', columns)
        epoch_sized = _read('{"end_time": 1788528600000}', columns)

        assert small["end_time"].dtype == epoch_sized["end_time"].dtype == "int64"


class TestPandasDefaultIsTheBug:
    """Pin the pandas behaviour being corrected."""

    def test_pandas_default_coerces_by_name(self) -> None:
        """Same value, three names, three outcomes."""
        row = (
            '{"date": 1788528600000, "score_at": 1788528600000, '
            '"plain_num": 1788528600000}'
        )
        frame = pd.read_json(io.StringIO(row), lines=True)

        assert "datetime64" in str(frame["date"].dtype)
        assert "datetime64" in str(frame["score_at"].dtype)
        assert frame["plain_num"].dtype == "int64"
