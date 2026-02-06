# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Pydantic models for connection state management.

These models represent the state of an Airbyte connection, which tracks sync progress
for incremental syncs. The state can be one of several types:
- stream: Per-stream state
- global: Global state with optional per-stream states
- legacy: Legacy state blob
- not_set: No state has been set yet
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class StreamDescriptor(BaseModel):
    """Descriptor for a single stream within a connection state."""

    name: str = Field(description="The stream name")
    namespace: str | None = Field(
        default=None,
        description="The stream namespace (optional)",
    )


class StreamState(BaseModel):
    """State for a single stream."""

    model_config = ConfigDict(populate_by_name=True)

    stream_descriptor: StreamDescriptor = Field(
        alias="streamDescriptor",
        description="The stream descriptor (name and namespace)",
    )
    stream_state: dict[str, Any] | None = Field(
        default=None,
        alias="streamState",
        description="The state blob for this stream",
    )


class GlobalState(BaseModel):
    """Global state containing shared state and per-stream states."""

    model_config = ConfigDict(populate_by_name=True)

    shared_state: dict[str, Any] | None = Field(
        default=None,
        alias="sharedState",
        description="Shared state across all streams",
    )
    stream_states: list[StreamState] = Field(
        default_factory=list,
        alias="streamStates",
        description="Per-stream states within the global state",
    )


class ConnectionStateResponse(BaseModel):
    """Response model for connection state operations.

    Represents the state of a connection, which can be one of:
    - stream: Per-stream state (streamState field populated)
    - global: Global state with optional per-stream states (globalState field populated)
    - legacy: Legacy state blob (state field populated)
    - not_set: No state has been set yet
    """

    model_config = ConfigDict(populate_by_name=True)

    state_type: Literal["global", "stream", "legacy", "not_set"] = Field(
        alias="stateType",
        description="The type of state: global, stream, legacy, or not_set",
    )
    connection_id: str = Field(
        alias="connectionId",
        description="The connection ID (UUID)",
    )
    state: dict[str, Any] | None = Field(
        default=None,
        description="Legacy state blob (populated when stateType is 'legacy')",
    )
    stream_state: list[StreamState] | None = Field(
        default=None,
        alias="streamState",
        description="Per-stream states (populated when stateType is 'stream')",
    )
    global_state: GlobalState | None = Field(
        default=None,
        alias="globalState",
        description="Global state (populated when stateType is 'global')",
    )

    def __str__(self) -> str:
        """Return a string representation of the connection state."""
        stream_count = 0
        if self.stream_state:
            stream_count = len(self.stream_state)
        elif self.global_state and self.global_state.stream_states:
            stream_count = len(self.global_state.stream_states)
        return (
            f"Connection {self.connection_id}: "
            f"stateType={self.state_type}, streams={stream_count}"
        )


def _match_stream(
    stream: StreamState,
    stream_name: str,
    stream_namespace: str | None = None,
) -> bool:
    """Check if a StreamState matches the given name and optional namespace."""
    if stream.stream_descriptor.name != stream_name:
        return False
    if stream_namespace is not None:
        return stream.stream_descriptor.namespace == stream_namespace
    return True


def _get_stream_list(
    state: ConnectionStateResponse,
) -> list[StreamState]:
    """Extract the stream list from a ConnectionStateResponse regardless of state type."""
    if state.state_type == "stream" and state.stream_state:
        return state.stream_state
    if state.state_type == "global" and state.global_state:
        return state.global_state.stream_states
    return []
