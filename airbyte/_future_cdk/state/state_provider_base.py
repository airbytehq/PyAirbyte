# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""State provider implementation."""

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


class StateProviderBase(abc.ABC):  # noqa: B024
    """A class to provide state artifacts."""

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
        return self.get_state_artifacts(include_global_state=True)

    def get_state_artifacts(
        self,
        stream_names_filter: list[str] | None = None,
        *,
        include_global_state: bool = True,
    ) -> list[AirbyteStreamState | AirbyteGlobalState]:
        """Return all stream states.

        If `stream_names_filter` is provided, only return stream states for the specified stream
        names.
        """
        if not stream_names_filter:
            result = self.stream_state_artifacts or []
        else:
            result = [
                state
                for state in self.stream_state_artifacts
                if state.stream_descriptor.name in stream_names_filter
            ]
        if include_global_state and self._global_state:
            result.append(self._global_state)

        return result

    def get_stream_state(
        self,
        stream_name: str | None = None,
    ) -> AirbyteStreamState | None:
        """Return a specific stream state or None if none exists."""
        if stream_name is None:
            raise exc.PyAirbyteInputError(guidance="stream_name is required")

        for state in self.stream_state_artifacts:
            if state.stream_descriptor.name == stream_name:
                return state

        return None

    def get_global_state(self) -> AirbyteGlobalState | None:
        """Return the global state or None if none exists."""
        return self._global_state

    @property
    def known_stream_names(
        self,
    ) -> list[str]:
        """Return the unique set of all stream names with stored state."""
        return list({state.stream_descriptor.name for state in self.stream_state_artifacts})
