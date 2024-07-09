# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from pathlib import Path

import pytest
from airbyte._util.meta import is_windows
from airbyte.experimental import get_source

UNIT_TEST_DB_PATH: Path = Path(".cache") / "unit_tests" / "test_db.duckdb"


@pytest.mark.parametrize(
    "connector_name, config",
    [
        ("source-pokeapi", {"pokemon_name": "ditto"}),
    ],
)
@pytest.mark.xfail(condition=is_windows(), reason="Test expected to fail on Windows.")
def test_nocode_execution(connector_name: str, config: dict) -> None:
    source = get_source(
        name=connector_name,
        config=config,
        source_manifest=True,
    )
    source.check()
    source.select_all_streams()
    source.read()
    for name, records in source.read().streams.items():
        assert name
        assert len(records) > 0, f"No records were returned from the '{name}' stream."
