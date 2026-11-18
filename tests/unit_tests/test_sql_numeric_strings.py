# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Regression coverage for schema-declared numeric strings at the SQL load boundary."""

from pathlib import Path
from unittest.mock import patch

import json

import pandas as pd
import pytest
import sqlalchemy

from airbyte._processors.sql.postgres import PostgresConfig, PostgresSqlProcessor


@pytest.mark.parametrize(
    "values",
    [
        ["120248952188490650", "120248952188490651"],
        ["120248952188490650", None],
        ["0012", "0099"],
        ["0012", "not-a-number"],
    ],
)
def test_sql_load_preserves_numeric_strings(
    tmp_path: Path,
    values: list[str | None],
) -> None:
    """Capture the real processor's database-bound frame without requiring a database."""
    processor = object.__new__(PostgresSqlProcessor)
    processor._sql_config = PostgresConfig(
        host="localhost",
        port=5432,
        database="offline",
        username="offline",
        password="offline",
    )
    source = tmp_path / "batch.jsonl"
    source.write_text(
        "".join(
            json.dumps({
                "ad_id": value,
                "count": 4,
                "ratio": 1.25,
                "active": True,
                "event_time": "2026-01-01T01:02:03",
            })
            + "\n"
            for value in values
        ),
        encoding="utf-8",
    )
    frames: list[pd.DataFrame] = []

    def capture(frame: pd.DataFrame, *_args: object, **_kwargs: object) -> None:
        frames.append(frame.copy())

    with (
        patch.object(processor, "_create_table_for_loading", return_value="batch"),
        patch.object(
            processor,
            "_get_sql_column_definitions",
            return_value={
                "ad_id": sqlalchemy.String(),
                "count": sqlalchemy.Integer(),
                "ratio": sqlalchemy.Float(),
                "active": sqlalchemy.Boolean(),
                "event_time": sqlalchemy.DateTime(),
            },
        ),
        patch.object(processor, "_table_exists", return_value=True),
        patch.object(pd.DataFrame, "to_sql", capture),
    ):
        assert processor._write_files_to_new_table([source], "ads", "batch") == "batch"

    assert len(frames) == 1
    assert [None if pd.isna(value) else value for value in frames[0]["ad_id"]] == values
    assert frames[0]["count"].tolist() == [4, 4]
    assert frames[0]["ratio"].tolist() == [1.25, 1.25]
    assert frames[0]["active"].tolist() == [True, True]

    assert frames[0]["event_time"].tolist() == [pd.Timestamp("2026-01-01T01:02:03")] * 2
