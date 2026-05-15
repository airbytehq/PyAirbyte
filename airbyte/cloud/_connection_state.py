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

from airbyte.cloud._case_conversion import snake_to_camel_keys


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


def _normalize_state_to_protocol(
    raw_state: dict[str, Any],
) -> list[dict[str, Any]]:
    """Convert a raw Config API state blob to Airbyte protocol state messages.

    Transforms the camelCase API format into a list of `AirbyteStateMessage` dicts
    with snake_case keys, suitable for passing to a connector's `--state` flag.

    Args:
        raw_state: Full state dict from the Config API, including `stateType`.

    Returns:
        List of protocol-format state message dicts. Empty list if state is `not_set`.
    """
    parsed = ConnectionStateResponse(**raw_state)

    if parsed.state_type == "not_set":
        return []

    if parsed.state_type == "legacy":
        return [{"type": "LEGACY", "data": parsed.state or {}}]

    if parsed.state_type == "global" and parsed.global_state:
        return [
            {
                "type": "GLOBAL",
                "global": parsed.global_state.model_dump(by_alias=False),
            }
        ]

    # state_type == "stream"
    if not parsed.stream_state:
        return []

    return [
        {
            "type": "STREAM",
            "stream": entry.model_dump(by_alias=False),
        }
        for entry in parsed.stream_state
    ]


def _stream_entry_to_api(stream_entry: dict[str, Any]) -> dict[str, Any]:
    """Convert a single protocol-format stream entry to Config API format.

    Converts `stream_descriptor` keys to camelCase (via generic helper) and
    preserves the opaque `stream_state` blob as-is. Strips `None` values from
    the descriptor (the API omits null fields like `namespace`).
    """
    descriptor = stream_entry.get("stream_descriptor", {})
    api_descriptor = {k: v for k, v in snake_to_camel_keys(descriptor).items() if v is not None}
    return {
        "streamDescriptor": api_descriptor,
        "streamState": stream_entry.get("stream_state"),
    }


def _denormalize_protocol_state_to_api(
    protocol_messages: list[dict[str, Any]],
    connection_id: str,
) -> dict[str, Any]:
    """Convert Airbyte protocol state messages back to Config API format.

    Reverses `_normalize_state_to_protocol`, producing a dict suitable for
    `import_raw_state()` / the Config API `create_or_update_safe` endpoint.
    Uses generic snake_case → camelCase key conversion for stream entries;
    the opaque state blobs are preserved as-is.

    Args:
        protocol_messages: List of protocol-format `AirbyteStateMessage` dicts.
        connection_id: Connection ID to embed in the result.

    Returns:
        A Config API state dict with camelCase keys and `stateType`.
    """
    if not protocol_messages:
        return {
            "stateType": "not_set",
            "connectionId": connection_id,
        }

    first = protocol_messages[0]
    msg_type = first.get("type", "").upper()

    if msg_type == "LEGACY":
        return {
            "stateType": "legacy",
            "connectionId": connection_id,
            "state": first.get("data", {}),
        }

    if msg_type == "GLOBAL":
        global_body = first.get("global", {})
        return {
            "stateType": "global",
            "connectionId": connection_id,
            "globalState": {
                "sharedState": global_body.get("shared_state"),
                "streamStates": [
                    _stream_entry_to_api(s) for s in global_body.get("stream_states", [])
                ],
            },
        }

    # STREAM type
    return {
        "stateType": "stream",
        "connectionId": connection_id,
        "streamState": [_stream_entry_to_api(s.get("stream", {})) for s in protocol_messages],
    }


def _is_protocol_state_format(
    state_input: dict[str, Any] | list[dict[str, Any]],
) -> bool:
    """Detect whether the input is in Airbyte protocol format.

    Returns `True` for protocol format:
    - an empty list (representing no protocol state), or
    - a non-empty list where every item is a dict with a known `type`, or
    - a single dict with top-level snake_case `type` and no `stateType`.

    Returns `False` for Config API format (for example, a dict with `stateType`
    or a raw `streamState` list from the Config API).
    """
    protocol_message_types = {"STREAM", "GLOBAL", "LEGACY"}

    if isinstance(state_input, list):
        if not state_input:
            return True
        return all(
            isinstance(message, dict) and message.get("type") in protocol_message_types
            for message in state_input
        )
    # A dict with snake_case 'type' at top-level is a single protocol message
    return "type" in state_input and "stateType" not in state_input


def _match_stream(
    stream: StreamState,
    stream_name: str,
    stream_namespace: str | None = None,
) -> bool:
    """Check if a StreamState matches the given name and namespace.

    Namespace matching treats None and "" as equivalent (both mean "no namespace").
    There is no wildcard behavior: the caller must match the actual namespace.
    """
    if stream.stream_descriptor.name != stream_name:
        return False
    return (stream.stream_descriptor.namespace or None) == (stream_namespace or None)


def _get_stream_list(
    state: ConnectionStateResponse,
) -> list[StreamState]:
    """Extract the stream list from a ConnectionStateResponse regardless of state type."""
    if state.state_type == "stream" and state.stream_state:
        return state.stream_state
    if state.state_type == "global" and state.global_state:
        return state.global_state.stream_states
    return []
