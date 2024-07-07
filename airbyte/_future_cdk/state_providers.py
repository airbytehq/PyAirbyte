# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

"""State provider implementation."""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from airbyte_protocol.models import (
    AirbyteStateType,
)
from airbyte_protocol.models.airbyte_protocol import AirbyteStreamState

from airbyte import exceptions as exc


if TYPE_CHECKING:
    from airbyte_protocol.models import (
        AirbyteStateMessage,
        AirbyteStreamState,
    )


class StateProviderBase(abc.ABC):  # noqa: B024
    """A class to provide state artifacts."""

    def __init__(self) -> None:
        """Initialize the state manager with a static catalog state.

        This constructor may be overridden by subclasses to initialize the state artifacts.
        """
        self._state_message_artifacts: list[AirbyteStateMessage] | None = None

    @property
    def stream_state_artifacts(
        self,
    ) -> list[AirbyteStreamState]:
        """Return all state artifacts.

        This is just a type guard around the private variable `_stream_state_artifacts` and the
        cast to `AirbyteStreamState` objects.
        """
        if self._state_message_artifacts is None:
            raise exc.PyAirbyteInternalError(message="No state artifacts were declared.")

        return [
            state_msg.stream
            for state_msg in self._state_message_artifacts
            if state_msg.type == AirbyteStateType.STREAM
        ]

    @property
    def state_message_artifacts(
        self,
    ) -> list[AirbyteStreamState]:
        """Return all state artifacts.

        This is just a type guard around the private variable `_state_message_artifacts`.
        """
        if self._state_message_artifacts is None:
            raise exc.PyAirbyteInternalError(message="No state artifacts were declared.")

        return self._state_message_artifacts

    @property
    def known_stream_names(
        self,
    ) -> set[str]:
        """Return the unique set of all stream names with stored state."""
        return {state.stream_descriptor.name for state in self.stream_state_artifacts}

    def to_state_input_file_text(self) -> str:
        """Return the state artifacts as a JSON string.

        This is used when sending the state artifacts to the destination.
        """
        return (
            "["
            + "\n, ".join(
                [
                    state_artifact.model_dump_json()
                    for state_artifact in (self._state_message_artifacts or [])
                ]
            )
            + "]"
        )


class StaticInputState(StateProviderBase):
    """A state manager that uses a static catalog state as input."""

    def __init__(
        self,
        from_state_messages: list[AirbyteStateMessage] | None = None,
    ) -> None:
        """Initialize the state manager with a static catalog state."""
        self._state_message_artifacts: list[AirbyteStateMessage] | None = from_state_messages
