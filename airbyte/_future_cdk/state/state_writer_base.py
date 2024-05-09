# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""State writer implementation."""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from airbyte_protocol.models.airbyte_protocol import AirbyteStreamState

from airbyte import exceptions as exc


if TYPE_CHECKING:
    from airbyte_protocol.models import (
        AirbyteStreamState,
    )
    from airbyte_protocol.models.airbyte_protocol import AirbyteGlobalState


class StateWriterBase(abc.ABC):
    """A class to write state artifacts."""

    def __init__(self) -> None:
        """Initialize the state manager with a static catalog state.

        This constructor may be overridden by subclasses to initialize the state artifacts.
        """
        self._stream_state_artifacts: list[AirbyteStreamState] | None = None
        self._global_state: AirbyteGlobalState | None = None

    @property
    def stream_state_artifacts(
        self,
    ) -> list[AirbyteStreamState]:
        """Return all state artifacts.

        This is just a type guard around the private variable `_stream_state_artifacts`.
        """
        if self._stream_state_artifacts is None:
            raise exc.PyAirbyteInternalError(message="No state artifacts were declared.")

        return self._stream_state_artifacts

    @property
    def all_state_artifacts(
        self,
    ) -> list[AirbyteStreamState | AirbyteGlobalState]:
        """Return all state artifacts."""
        result = self.stream_state_artifacts or []
        if self._global_state:
            result.append(self._global_state)

        return result

    @abc.abstractmethod
    def get_stream_state(
        self,
        stream_name: str | None = None,
        *,
        namespace: str | None = None,
    ) -> AirbyteStreamState | None:
        """Return a specific stream state or None if none exists."""
        ...

    def get_global_state(self) -> AirbyteGlobalState | None:
        """Return the global state or None if none exists."""
        return self._global_state

    @property
    def known_stream_names(
        self,
    ) -> list[str]:
        """Return the unique set of all stream names with stored state."""
        return list({state.stream_descriptor.name for state in self.stream_state_artifacts})
