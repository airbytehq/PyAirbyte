# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""State backend implementation."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, cast

from pytz import utc
from sqlalchemy import Column, DateTime, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from airbyte_protocol.models import (
    AirbyteStateMessage,
    AirbyteStateType,
)

from airbyte._future_cdk.state_providers import StaticInputState
from airbyte._future_cdk.state_writers import StateWriterBase
from airbyte.caches._state_backend_base import (
    StateBackendBase,
)
from airbyte.exceptions import PyAirbyteInternalError


if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from airbyte._future_cdk.state_providers import StateProviderBase


STATE_TABLE_NAME = "_airbyte_state"

GLOBAL_STATE_STREAM_NAME = "_GLOBAL"
LEGACY_STATE_STREAM_NAME = "_LEGACY"
GLOBAL_STATE_STREAM_NAMES = [GLOBAL_STATE_STREAM_NAME, LEGACY_STATE_STREAM_NAME]


SqlAlchemyModel = declarative_base()
"""A base class to use for SQLAlchemy ORM models."""


class StreamState(SqlAlchemyModel):  # type: ignore[valid-type,misc]
    """A SQLAlchemy ORM model to store state metadata."""

    __tablename__ = STATE_TABLE_NAME

    source_name = Column(String)
    """The source name."""

    stream_name = Column(String)
    """The stream name."""

    table_name = Column(String, primary_key=True)
    """The table name holding records for the stream."""

    state_json = Column(String)
    """The JSON string representation of the state message."""

    last_updated = Column(
        DateTime(timezone=True), onupdate=datetime.now(utc), default=datetime.now(utc)
    )
    """The last time the state was updated."""


class SqlStateWriter(StateWriterBase):
    """State writer for SQL backends."""

    def __init__(self, source_name: str, backend: SqlStateBackend) -> None:
        self._state_backend: SqlStateBackend = backend
        self.source_name = source_name

    def write_state(
        self,
        state_message: AirbyteStateMessage,
    ) -> None:
        if state_message.type == AirbyteStateType.GLOBAL:
            stream_name = GLOBAL_STATE_STREAM_NAME
        if state_message.type == AirbyteStateType.LEGACY:
            stream_name = LEGACY_STATE_STREAM_NAME
        elif state_message.type == AirbyteStateType.STREAM:
            stream_name = state_message.stream.stream_descriptor.name
        else:
            raise PyAirbyteInternalError(
                message="Invalid state message type.",
                context={"state_message": state_message},
            )

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
                    state_json=state_message.model_dump_json(),
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
        SqlAlchemyModel.metadata.create_all(engine)

    def get_state_provider(
        self,
        source_name: str,
        table_prefix: str = "",
        streams_filter: list[str] | None = None,
        *,
        refresh: bool = True,
    ) -> StateProviderBase:
        """Return the state provider.

        Subclasses may add additional keyword arguments to this method as needed.
        """
        _ = refresh  # Always refresh the state
        self._ensure_internal_tables()

        engine = self._engine
        with Session(engine) as session:
            query = session.query(StreamState).filter(
                StreamState.source_name == source_name
                and (
                    StreamState.table_name.startswith(table_prefix)
                    or StreamState.stream_name.in_(GLOBAL_STATE_STREAM_NAMES)
                )
            )
            if streams_filter:
                query = query.filter(
                    StreamState.stream_name.in_([*streams_filter, *GLOBAL_STATE_STREAM_NAMES])
                )
            states: list[StreamState] = cast(list[StreamState], query.all())
            # Only return the states if the table name matches what the current cache
            # would generate. Otherwise consider it part of a different cache.
            states = [
                state for state in states if state.table_name == table_prefix + state.stream_name
            ]

        return StaticInputState(
            from_state_messages=[
                AirbyteStateMessage.model_validate_json(state.state_json) for state in states
            ]
        )

    def get_state_writer(
        self,
        source_name: str,
    ) -> StateWriterBase:
        return SqlStateWriter(source_name=source_name, backend=self)
