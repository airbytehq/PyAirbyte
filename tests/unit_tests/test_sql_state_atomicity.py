# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Regression tests for atomic SQL state replacement."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine, event, select

from airbyte_protocol.models import (
    AirbyteStateMessage,
    AirbyteStateType,
    AirbyteStreamState,
    StreamDescriptor,
)

from airbyte.caches._state_backend import (
    CacheStreamStateModel,
    DestinationStreamStateModel,
    SqlStateBackend,
    SqlStateWriter,
)
from airbyte.shared.sql_processor import SqlConfig


if TYPE_CHECKING:
    from pathlib import Path

    from sqlalchemy import Engine


def _message(cursor: int, stream: str = "records") -> AirbyteStateMessage:
    return AirbyteStateMessage(
        type=AirbyteStateType.STREAM,
        stream=AirbyteStreamState(
            stream_descriptor=StreamDescriptor(name=stream),
            stream_state={"cursor": cursor},
        ),
    )


def _writer(engine: Engine, destination: str | None) -> SqlStateWriter:
    config = Mock(spec=SqlConfig)
    config.get_sql_engine.return_value = engine
    return SqlStateWriter(
        "source-test", SqlStateBackend(config), destination_name=destination
    )


def _states(engine: Engine, destination: str | None) -> dict[str, str]:
    model = DestinationStreamStateModel if destination else CacheStreamStateModel
    with engine.connect() as connection:
        return dict(
            connection.execute(select(model.stream_name, model.state_json))
            .tuples()
            .all()
        )


@pytest.mark.parametrize("destination", [None, "destination-test"])
def test_readers_keep_previous_state_until_replacement_commits(
    tmp_path: Path, destination: str | None
) -> None:
    """A separate connection must never observe the gap between DELETE and INSERT."""
    engine = create_engine(f"sqlite:///{tmp_path / 'state.db'}")
    writer = _writer(engine, destination)
    writer.write_state(_message(1))
    observed: list[dict[str, str]] = []

    def observe_insert(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        if statement.lstrip().upper().startswith("INSERT INTO _AIRBYTE_"):
            observed.append(_states(engine, destination))

    event.listen(engine, "before_cursor_execute", observe_insert)
    try:
        writer.write_state(_message(2))
    finally:
        event.remove(engine, "before_cursor_execute", observe_insert)
        engine.dispose()

    assert observed == [{"records": _message(1).model_dump_json()}]
    assert _states(engine, destination) == {"records": _message(2).model_dump_json()}
    engine.dispose()


@pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
@pytest.mark.parametrize("destination", [None, "destination-test"])
def test_failed_replacement_preserves_previous_state(
    tmp_path: Path, dialect: str, destination: str | None
) -> None:
    """An insertion failure rolls back the deletion instead of losing the checkpoint."""
    engine = create_engine(f"{dialect}:///{tmp_path / 'state.db'}")
    writer = _writer(engine, destination)
    writer.write_state(_message(1))
    writer.write_state(_message(10, "other"))

    def fail_insert(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        if statement.lstrip().upper().startswith("INSERT INTO _AIRBYTE_"):
            raise RuntimeError("injected state insertion failure")

    event.listen(engine, "before_cursor_execute", fail_insert)
    try:
        with pytest.raises(RuntimeError, match="injected state insertion failure"):
            writer.write_state(_message(2))
    finally:
        event.remove(engine, "before_cursor_execute", fail_insert)

    assert _states(engine, destination) == {
        "records": _message(1).model_dump_json(),
        "other": _message(10, "other").model_dump_json(),
    }
    engine.dispose()


@pytest.mark.parametrize("destination", [None, "destination-test"])
def test_duckdb_repeated_replacements_keep_primary_key_and_other_streams(
    tmp_path: Path, destination: str | None
) -> None:
    """Deleting and re-inserting the same primary key works in a single transaction."""
    engine = create_engine(f"duckdb:///{tmp_path / 'state.db'}")
    writer = _writer(engine, destination)
    writer.write_state(_message(10, "other"))
    for cursor in range(4):
        writer.write_state(_message(cursor))
    assert _states(engine, destination) == {
        "records": _message(3).model_dump_json(),
        "other": _message(10, "other").model_dump_json(),
    }
    engine.dispose()
