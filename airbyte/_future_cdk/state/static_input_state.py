# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""State manager implementation that uses a static catalog state collection."""

from __future__ import annotations

from airbyte_protocol.models import AirbyteGlobalState, AirbyteStreamState

from airbyte._future_cdk.state.state_provider_base import StateProviderBase  # noqa: PLC2701
from airbyte.exceptions import PyAirbyteInternalError


class StaticInputState(StateProviderBase):
    """A state manager that uses a static catalog state as input."""

    def __init__(
        self,
        input_state_artifacts: list[AirbyteStreamState | AirbyteGlobalState],
    ) -> None:
        """Initialize the state manager with a static catalog state."""
        self._global_state: AirbyteGlobalState | None = None
        self._stream_state_artifacts: list[AirbyteStreamState] = []

        for state_artifact in input_state_artifacts:
            if isinstance(state_artifact, AirbyteGlobalState):
                self._global_state = state_artifact
            elif isinstance(state_artifact, AirbyteStreamState):
                self._stream_state_artifacts.append(state_artifact)
            else:
                raise PyAirbyteInternalError(
                    message=f"Invalid state artifact type: {type(state_artifact)}"
                )
