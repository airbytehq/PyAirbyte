# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""State backend implementation."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

from airbyte_protocol.models import (
    AirbyteStreamState,
)

from airbyte.caches._state_backend_base import (
    StateBackendBase,
    StreamState,
)


if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from airbyte._future_cdk.state.state_provider_base import StateProviderBase


Base = declarative_base()

STREAMS_TABLE_NAME = "_airbyte_streams"
STATE_TABLE_NAME = "_airbyte_state"

GLOBAL_STATE_STREAM_NAMES = ["_GLOBAL", "_LEGACY"]


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
        # TODO!
        raise NotImplementedError

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

    # TODO!: Move to StateWriterBase
    # def save_state(
    #     self,
    #     state: AirbyteStreamState,
    #     stream_name: str,
    # ) -> None:
    #     self._ensure_internal_tables()
    #     engine = self._engine
    #     with Session(engine) as session:
    #         session.query(StreamState).filter(
    #             StreamState.table_name == self._table_prefix + stream_name
    #         ).delete()
    #         session.commit()
    #         session.add(
    #             StreamState(
    #                 source_name=self.source_name,
    #                 stream_name=stream_name,
    #                 table_name=self._table_prefix + stream_name,
    #                 state_json=state.json(),
    #             )
    #         )
    #         session.commit()
