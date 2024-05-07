# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""A SQL Cache implementation."""

from __future__ import annotations

import abc
from datetime import datetime
from typing import TYPE_CHECKING

from pytz import utc
from sqlalchemy import Column, DateTime, String
from sqlalchemy.ext.declarative import declarative_base


if TYPE_CHECKING:
    from airbyte_protocol.models import (
        AirbyteStateMessage,
    )

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


class StateManagerBase(abc.ABC):
    """
    A class to manage the stream catalog of data synced to a cache:
    * What streams exist and to what tables they map
    * The JSON schema for each stream
    * The state of each stream if available
    """

    @property
    @abc.abstractmethod
    def stream_names(self) -> list[str]:
        """Return the names of all streams in the cache."""
        return [stream.stream.name for stream in self.source_catalog.streams]

    @abc.abstractmethod
    def save_state(
        self,
        source_name: str,
        state: AirbyteStateMessage,
        stream_name: str,
    ) -> None:
        """Save the state of a stream to the cache."""
        ...

    @abc.abstractmethod
    def get_state(
        self,
        source_name: str,
        streams: list[str] | None = None,
    ) -> list[dict] | None:
        """Get the state of a stream from the cache."""
        ...
