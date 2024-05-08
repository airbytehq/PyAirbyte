# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""State manager implementation that uses a static catalog state collection."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte._future_cdk.state_manager import StateManagerBase


if TYPE_CHECKING:
    from airbyte_protocol.models import AirbyteStreamState


class StaticStateManager(StateManagerBase):
    """A state manager that uses a static catalog state."""

    def __init__(self, state_artifacts: list[AirbyteStreamState]) -> None:
        """Initialize the state manager with a static catalog state."""
        self._state_artifacts = state_artifacts

    @property
    def stream_names(self) -> list[str]:
        """Return the names of all streams in the cache."""
        return list({state.stream_descriptor.name for state in self._state_artifacts})

    def get_state_artifacts(
        self,
        source_name: str,
        *,
        ignore_cache: bool = False,
    ) -> list[AirbyteStreamState]:
        """Load all state artifacts."""
        _ = ignore_cache, source_name
        return self._state_artifacts

    def save_state(
        self,
        source_name: str,
        state: AirbyteStreamState,
        stream_name: str,
    ) -> None:
        """Save the state of a stream to the cache."""
        raise NotImplementedError("StaticStateManager does not support saving state.")

    def get_state(
        self,
        source_name: str,
        streams: list[str] | None = None,
    ) -> list[dict] | None:
        """Get the state of a stream from the cache."""
        return [
            state
            for state in self._state_artifacts
            if streams is None or state.stream_descriptor.name in streams
        ]
