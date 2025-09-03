# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

from pathlib import Path
import sys

import pytest
from airbyte import get_source
from airbyte._util.meta import is_windows

UNIT_TEST_DB_PATH: Path = Path(".cache") / "unit_tests" / "test_db.duckdb"


@pytest.mark.parametrize(
    "connector_name, config",
    [
        ("source-pokeapi", {"pokemon_name": "ditto"}),
    ],
)
@pytest.mark.xfail(condition=is_windows(), reason="Test expected to fail on Windows.")
@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Test fails in Python 3.12 as pokeAPI interface is blocked for bots/CI runners",
)
def test_nocode_execution(connector_name: str, config: dict) -> None:
    source = get_source(
        name=connector_name,
        config=config,
        source_manifest=True,
    )
    source.check()
    source.select_all_streams()
    read_result = source.read()
    for name, records in read_result.streams.items():
        assert name
        assert len(records) > 0, f"No records were returned from the '{name}' stream."

    # Confirm we can read twice:
    read_result_2 = source.read()
    for name, records in read_result_2.streams.items():
        assert name
        assert len(records) > 0, f"No records were returned from the '{name}' stream."
