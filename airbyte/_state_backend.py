# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""State backend implementation."""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

from pytz import utc
from sqlalchemy import Column, DateTime, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from airbyte_protocol.models import (
    AirbyteStateMessage,
    AirbyteStreamState,
)

from airbyte._future_cdk.state.state_writer_base import StateWriterBase
from airbyte._future_cdk.state.static_input_state import StaticInputState
from airbyte.caches._state_backend_base import (
    StateBackendBase,
)


if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from airbyte._future_cdk.state.state_provider_base import StateProviderBase


STREAMS_TABLE_NAME = "_airbyte_streams"
STATE_TABLE_NAME = "_airbyte_state"

GLOBAL_STATE_STREAM_NAMES = ["_GLOBAL", "_LEGACY"]


Base = declarative_base()


class StreamState(Base):  # type: ignore[valid-type,misc]
    __tablename__ = STATE_TABLE_NAME

    source_name = Column(String)
    stream_name = Column(String)
    table_name = Column(String, primary_key=True)
    state_json = Column(String)
    last_updated = Column(
        DateTime(timezone=True), onupdate=datetime.now(utc), default=datetime.now(utc)
    )


class SqlStateWriter(StateWriterBase):
    """State writer for SQL backends."""

    def __init__(self, source_name: str, backend: SqlStateBackend) -> None:
        self._state_backend: SqlStateBackend = backend
        self.source_name = source_name

    def write_state(
        self,
        state_message: AirbyteStateMessage,
    ) -> None:
        if state_message.type == "GLOBAL":
            stream_name = "_GLOBAL"

        self._state_backend._ensure_internal_tables()  # noqa: SLF001  # Non-public member access
        table_prefix = self._state_backend._table_prefix  # noqa: SLF001
        engine = self._state_backend._engine  # noqa: SLF001

        with Session(engine) as session:
            session.query(StreamState).filter(
                StreamState.table_name == table_prefix + stream_name
            ).delete()

            # This prevents "duplicate key" errors but (in theory) should not be necessary.
            session.commit()
            session.add(
                StreamState(
                    source_name=self.source_name,
                    stream_name=stream_name,
                    table_name=table_prefix + stream_name,
                    state_json=state_message.json(),
                )
            )
            session.commit()


class SqlStateBackend(StateBackendBase):
    """
    A class to manage the stream catalog of data synced to a cache:

    - What streams exist and to what tables they map
    - The JSON schema for each stream
    """

    def __init__(
        self,
        engine: Engine,
        table_prefix: str = "",
    ) -> None:
        """Initialize the state manager with a static catalog state."""
        self._engine: Engine = engine
        self._table_prefix = table_prefix
        super().__init__()

    def _ensure_internal_tables(self) -> None:
        """Ensure the internal tables exist in the SQL database."""
        engine = self._engine
        Base.metadata.create_all(engine)

    def get_state_provider(
        self,
        source_name: str,
        table_prefix: str = "",
    ) -> StateProviderBase:
        """Return the state provider.

        Subclasses may add additional keyword arguments to this method as needed.
        """
        if self._state_artifacts is None:
            self._initialize_backend(source_name=source_name, table_prefix=table_prefix)

        assert self._state_artifacts is not None, "State artifacts is initialized."
        return StaticInputState(input_state_artifacts=self._state_artifacts)

    def _initialize_backend(
        self,
        *,
        force_refresh: bool = False,
        source_name: str | None = None,
        table_prefix: str | None = None,
    ) -> None:
        """Load all state artifacts from the cache."""
        if self._state_artifacts is not None and not force_refresh:
            return

        self._ensure_internal_tables()
        engine = self._engine
        with Session(engine) as session:
            query = session.query(StreamState).filter(StreamState.source_name == source_name)
            states = query.all()

        if not states:
            # No state artifacts found
            self._state_artifacts = []
            return

        # Only return the states if the table name matches what the current cache
        # would generate. Otherwise consider it part of a different cache.

        state_dicts: list[dict] = [
            json.loads(state)
            for state in states
            if state.table_name == self._table_prefix + state.stream_name
        ]
        self._state_artifacts = [
            AirbyteStreamState.parse_obj(state_dict) for state_dict in state_dicts
        ]

    def get_state_writer(
        self,
        source_name: str,
        table_prefix: str,
    ) -> StateWriterBase:
        return SqlStateWriter(source_name=source_name, backend=self)
