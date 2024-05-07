# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""A SQL Cache implementation."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Callable

from sqlalchemy.orm import Session

from airbyte._future_cdk.state_manager import StateManagerBase, StreamState


if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from airbyte_protocol.models import (
        AirbyteStateMessage,
        ConfiguredAirbyteCatalog,
    )

STREAMS_TABLE_NAME = "_airbyte_streams"
STATE_TABLE_NAME = "_airbyte_state"

GLOBAL_STATE_STREAM_NAMES = ["_GLOBAL", "_LEGACY"]


class OLTPStateManager(StateManagerBase):
    """
    A class to manage the stream catalog of data synced to a cache:
    * What streams exist and to what tables they map
    * The JSON schema for each stream
    * The state of each stream if available
    """

    def __init__(
        self,
        engine: Engine,
        table_name_resolver: Callable[[str], str],
    ) -> None:
        self._engine: Engine = engine
        self._table_name_resolver = table_name_resolver
        self._source_catalog: ConfiguredAirbyteCatalog | None = None
        self._load_catalog_info()
        assert self._source_catalog is not None

    @property
    def stream_names(self) -> list[str]:
        """Return the names of all streams in the cache."""
        return [stream.stream.name for stream in self.source_catalog.streams]

    def _ensure_internal_tables(self) -> None:
        engine = self._engine
        Base.metadata.create_all(engine)

    def save_state(
        self,
        source_name: str,
        state: AirbyteStateMessage,
        stream_name: str,
    ) -> None:
        self._ensure_internal_tables()
        engine = self._engine
        with Session(engine) as session:
            session.query(StreamState).filter(
                StreamState.table_name == self._table_name_resolver(stream_name)
            ).delete()
            session.commit()
            session.add(
                StreamState(
                    source_name=source_name,
                    stream_name=stream_name,
                    table_name=self._table_name_resolver(stream_name),
                    state_json=state.json(),
                )
            )
            session.commit()

    def get_state(
        self,
        source_name: str,
        streams: list[str] | None = None,
    ) -> list[dict] | None:
        self._ensure_internal_tables()
        engine = self._engine
        with Session(engine) as session:
            query = session.query(StreamState).filter(StreamState.source_name == source_name)
            if streams:
                query = query.filter(
                    StreamState.stream_name.in_([*streams, *GLOBAL_STATE_STREAM_NAMES])
                )
            states = query.all()
            if not states:
                return None
            # Only return the states if the table name matches what the current cache
            # would generate. Otherwise consider it part of a different cache.
            states = [
                state
                for state in states
                if state.table_name == self._table_name_resolver(state.stream_name)
            ]
            return [json.loads(state.state_json) for state in states]
