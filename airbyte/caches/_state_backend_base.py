# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""State backend implementation."""

from __future__ import annotations

import abc
from datetime import datetime
from typing import TYPE_CHECKING

from pytz import utc
from sqlalchemy import Column, DateTime, String
from sqlalchemy.ext.declarative import declarative_base

from airbyte_protocol.models.airbyte_protocol import AirbyteStreamState


if TYPE_CHECKING:
    from airbyte_protocol.models import (
        AirbyteStreamState,
    )

    from airbyte._future_cdk.state.state_provider_base import StateProviderBase
    from airbyte._future_cdk.state.state_writer_base import StateWriterBase

STREAMS_TABLE_NAME = "_airbyte_streams"
STATE_TABLE_NAME = "_airbyte_state"

GLOBAL_STATE_STREAM_NAMES = ["_GLOBAL", "_LEGACY"]

Base = declarative_base()



class StateBackendBase(abc.ABC):
    """A class which manages the stream state for data synced.

    The backend is responsible for storing and retrieving the state of streams. It generates
    `StateProvider` objects, which are paired to a specific source and table prefix.
    """

    def __init__(self) -> None:
        """Initialize the state manager with a static catalog state."""
        self._state_artifacts: list[AirbyteStreamState] | None = None

    @abc.abstractmethod
    def get_state_provider(
        self,
        source_name: str,
        table_prefix: str,
    ) -> StateProviderBase:
        """Return the state provider."""
        ...

    @abc.abstractmethod
    def get_state_writer(
        self,
        source_name: str,
        table_prefix: str,
    ) -> StateWriterBase:
        """Return a state writer."""
        ...

    def _initialize_backend(
        self,
        *,
        force_refresh: bool = False,
    ) -> None:
        """Do any needed initialization, for instance to load state artifacts from the cache.

        By default, this method does nothing. Base classes may override this method to load state
        artifacts or perform other initialization tasks.
        """
        _ = force_refresh  # Unused
        pass

    # @final
    # def get_state_artifacts(
    #     self,
    #     *,
    #     source_name: str | None = None,
    #     table_prefix: str | None = None,
    # ) -> list[AirbyteStreamState]:
    #     """Returns all state artifacts.

    #     This is a "final" implementation, which base classes should not override. Instead, they
    #     should implement the `_load_state_artifacts()` method.
    #     """
    #     if self._state_artifacts is None:
    #         self._initialize_backend(force_refresh=False)
    #         if self._state_artifacts is None:
    #             raise exc.PyAirbyteInternalError(message="No state artifacts were declared.")

    #     return self._state_artifacts

    # @abc.abstractmethod
    # def write_processed_state(
    #     self,
    #     state: AirbyteStreamState,
    #     stream_name: str,
    #     *,
    #     source_name: str | None = None,
    #     table_prefix: str | None = None,
    # ) -> None:
    #     """Save the state of a stream to the cache."""
    #     ...

    # @abc.abstractmethod
    # def get_state(
    #     self,
    #     streams: list[str] | None = None,
    #     *,
    #     source_name: str | None = None,
    #     table_prefix: str | None = None,
    # ) -> list[AirbyteStreamState] | None:
    #     """Get the state of a stream from the cache."""
    #     ...
