# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for connection state models and helper functions."""

from __future__ import annotations

import pytest

from airbyte.cloud.connection_state import (
    ConnectionStateResponse,
    GlobalState,
    StreamDescriptor,
    StreamState,
    _get_stream_list,
    _match_stream,
)


@pytest.mark.parametrize(
    "stream_name,stream_namespace,descriptor_name,descriptor_namespace,expected",
    [
        pytest.param("users", None, "users", None, True, id="name_match_no_namespace"),
        pytest.param(
            "users", None, "users", "public", True, id="name_match_any_namespace"
        ),
        pytest.param(
            "users", "public", "users", "public", True, id="name_and_namespace_match"
        ),
        pytest.param(
            "users", "public", "users", "private", False, id="namespace_mismatch"
        ),
        pytest.param("orders", None, "users", None, False, id="name_mismatch"),
        pytest.param("users", "public", "users", None, False, id="namespace_vs_none"),
    ],
)
def test_match_stream(
    stream_name: str,
    stream_namespace: str | None,
    descriptor_name: str,
    descriptor_namespace: str | None,
    expected: bool,
) -> None:
    stream = StreamState(
        stream_descriptor=StreamDescriptor(
            name=descriptor_name,
            namespace=descriptor_namespace,
        ),
        stream_state={"cursor": "2024-01-01"},
    )
    assert _match_stream(stream, stream_name, stream_namespace) == expected


@pytest.mark.parametrize(
    "state_type,has_stream_state,has_global_state,expected_count",
    [
        pytest.param("stream", True, False, 2, id="stream_type_with_streams"),
        pytest.param("global", False, True, 2, id="global_type_with_streams"),
        pytest.param("legacy", False, False, 0, id="legacy_type_empty"),
        pytest.param("not_set", False, False, 0, id="not_set_empty"),
        pytest.param("stream", False, False, 0, id="stream_type_no_streams"),
    ],
)
def test_get_stream_list(
    state_type: str,
    has_stream_state: bool,
    has_global_state: bool,
    expected_count: int,
) -> None:
    streams = [
        StreamState(
            stream_descriptor=StreamDescriptor(name="users"),
            stream_state={"cursor": "2024-01-01"},
        ),
        StreamState(
            stream_descriptor=StreamDescriptor(name="orders"),
            stream_state={"cursor": "2024-06-01"},
        ),
    ]
    state = ConnectionStateResponse(
        state_type=state_type,
        connection_id="test-conn-id",
        stream_state=streams if has_stream_state else None,
        global_state=GlobalState(stream_states=streams) if has_global_state else None,
    )
    result = _get_stream_list(state)
    assert len(result) == expected_count


def test_connection_state_response_str_stream_type() -> None:
    state = ConnectionStateResponse(
        state_type="stream",
        connection_id="abc-123",
        stream_state=[
            StreamState(
                stream_descriptor=StreamDescriptor(name="users"),
                stream_state={"cursor": "2024-01-01"},
            ),
        ],
    )
    assert "abc-123" in str(state)
    assert "stateType=stream" in str(state)
    assert "streams=1" in str(state)


def test_connection_state_response_str_global_type() -> None:
    state = ConnectionStateResponse(
        state_type="global",
        connection_id="abc-456",
        global_state=GlobalState(
            shared_state={"cdc": True},
            stream_states=[
                StreamState(
                    stream_descriptor=StreamDescriptor(name="users"),
                    stream_state={"cursor": "2024-01-01"},
                ),
                StreamState(
                    stream_descriptor=StreamDescriptor(name="orders"),
                    stream_state={"cursor": "2024-06-01"},
                ),
            ],
        ),
    )
    assert "abc-456" in str(state)
    assert "stateType=global" in str(state)
    assert "streams=2" in str(state)


def test_connection_state_response_from_camel_case() -> None:
    raw_api_response = {
        "stateType": "stream",
        "connectionId": "test-conn-id",
        "streamState": [
            {
                "streamDescriptor": {"name": "users", "namespace": "public"},
                "streamState": {"cursor": "2024-01-01"},
            },
        ],
    }
    state = ConnectionStateResponse(**raw_api_response)
    assert state.state_type == "stream"
    assert state.connection_id == "test-conn-id"
    assert state.stream_state is not None
    assert len(state.stream_state) == 1
    assert state.stream_state[0].stream_descriptor.name == "users"
    assert state.stream_state[0].stream_descriptor.namespace == "public"
    assert state.stream_state[0].stream_state == {"cursor": "2024-01-01"}


def test_connection_state_response_global_from_camel_case() -> None:
    raw_api_response = {
        "stateType": "global",
        "connectionId": "test-conn-id",
        "globalState": {
            "sharedState": {"cdc_offset": "12345"},
            "streamStates": [
                {
                    "streamDescriptor": {"name": "users"},
                    "streamState": {"cursor": "2024-01-01"},
                },
            ],
        },
    }
    state = ConnectionStateResponse(**raw_api_response)
    assert state.state_type == "global"
    assert state.global_state is not None
    assert state.global_state.shared_state == {"cdc_offset": "12345"}
    assert len(state.global_state.stream_states) == 1
    assert state.global_state.stream_states[0].stream_descriptor.name == "users"
