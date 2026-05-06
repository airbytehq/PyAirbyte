# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for connection state models and helper functions."""

from __future__ import annotations

from typing import Any

import pytest

from airbyte.cloud._connection_state import (
    ConnectionStateResponse,
    GlobalState,
    StreamDescriptor,
    StreamState,
    _denormalize_protocol_state_to_api,
    _get_stream_list,
    _is_protocol_state_format,
    _match_stream,
    _normalize_state_to_protocol,
)


@pytest.mark.parametrize(
    "stream_name,stream_namespace,descriptor_name,descriptor_namespace,expected",
    [
        pytest.param("users", None, "users", None, True, id="name_match_no_namespace"),
        pytest.param(
            "users", None, "users", "public", False, id="no_wildcard_namespace"
        ),
        pytest.param("users", "", "users", None, True, id="empty_str_matches_none"),
        pytest.param("users", None, "users", "", True, id="none_matches_empty_str"),
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


# --- Tests for _normalize_state_to_protocol ---


@pytest.mark.parametrize(
    "raw_state,expected",
    [
        pytest.param(
            {
                "stateType": "stream",
                "connectionId": "conn-1",
                "streamState": [
                    {
                        "streamDescriptor": {"name": "users", "namespace": "public"},
                        "streamState": {"cursor": "2024-01-01"},
                    },
                    {
                        "streamDescriptor": {"name": "orders"},
                        "streamState": {"cursor": "2024-06-01"},
                    },
                ],
            },
            [
                {
                    "type": "STREAM",
                    "stream": {
                        "stream_descriptor": {"name": "users", "namespace": "public"},
                        "stream_state": {"cursor": "2024-01-01"},
                    },
                },
                {
                    "type": "STREAM",
                    "stream": {
                        "stream_descriptor": {"name": "orders", "namespace": None},
                        "stream_state": {"cursor": "2024-06-01"},
                    },
                },
            ],
            id="stream_type",
        ),
        pytest.param(
            {
                "stateType": "global",
                "connectionId": "conn-2",
                "globalState": {
                    "sharedState": {"cdc_offset": "12345"},
                    "streamStates": [
                        {
                            "streamDescriptor": {"name": "users"},
                            "streamState": {"cursor": "2024-01-01"},
                        },
                    ],
                },
            },
            [
                {
                    "type": "GLOBAL",
                    "global": {
                        "shared_state": {"cdc_offset": "12345"},
                        "stream_states": [
                            {
                                "stream_descriptor": {
                                    "name": "users",
                                    "namespace": None,
                                },
                                "stream_state": {"cursor": "2024-01-01"},
                            },
                        ],
                    },
                },
            ],
            id="global_type",
        ),
        pytest.param(
            {
                "stateType": "legacy",
                "connectionId": "conn-3",
                "state": {"some_key": "some_value"},
            },
            [{"type": "LEGACY", "data": {"some_key": "some_value"}}],
            id="legacy_type",
        ),
        pytest.param(
            {"stateType": "not_set", "connectionId": "conn-4"},
            [],
            id="not_set_type",
        ),
    ],
)
def test_normalize_state_to_protocol(
    raw_state: dict[str, Any],
    expected: list[dict[str, Any]],
) -> None:
    assert _normalize_state_to_protocol(raw_state) == expected


# --- Tests for _denormalize_protocol_state_to_api ---


@pytest.mark.parametrize(
    "protocol_messages,connection_id,expected",
    [
        pytest.param(
            [
                {
                    "type": "STREAM",
                    "stream": {
                        "stream_descriptor": {"name": "users", "namespace": "public"},
                        "stream_state": {"cursor": "2024-01-01"},
                    },
                },
            ],
            "conn-1",
            {
                "stateType": "stream",
                "connectionId": "conn-1",
                "streamState": [
                    {
                        "streamDescriptor": {"name": "users", "namespace": "public"},
                        "streamState": {"cursor": "2024-01-01"},
                    },
                ],
            },
            id="stream_type",
        ),
        pytest.param(
            [
                {
                    "type": "GLOBAL",
                    "global": {
                        "shared_state": {"cdc_offset": "12345"},
                        "stream_states": [
                            {
                                "stream_descriptor": {"name": "users"},
                                "stream_state": {"cursor": "2024-01-01"},
                            },
                        ],
                    },
                },
            ],
            "conn-2",
            {
                "stateType": "global",
                "connectionId": "conn-2",
                "globalState": {
                    "sharedState": {"cdc_offset": "12345"},
                    "streamStates": [
                        {
                            "streamDescriptor": {"name": "users"},
                            "streamState": {"cursor": "2024-01-01"},
                        },
                    ],
                },
            },
            id="global_type",
        ),
        pytest.param(
            [{"type": "LEGACY", "data": {"some_key": "some_value"}}],
            "conn-3",
            {
                "stateType": "legacy",
                "connectionId": "conn-3",
                "state": {"some_key": "some_value"},
            },
            id="legacy_type",
        ),
        pytest.param(
            [],
            "conn-4",
            {"stateType": "not_set", "connectionId": "conn-4"},
            id="empty_list",
        ),
    ],
)
def test_denormalize_protocol_state_to_api(
    protocol_messages: list[dict[str, Any]],
    connection_id: str,
    expected: dict[str, Any],
) -> None:
    assert (
        _denormalize_protocol_state_to_api(protocol_messages, connection_id) == expected
    )


# --- Tests for round-trip (normalize → denormalize) ---


@pytest.mark.parametrize(
    "raw_state",
    [
        pytest.param(
            {
                "stateType": "stream",
                "connectionId": "conn-rt",
                "streamState": [
                    {
                        "streamDescriptor": {"name": "users", "namespace": "public"},
                        "streamState": {"cursor": "2024-01-01"},
                    },
                    {
                        "streamDescriptor": {"name": "orders"},
                        "streamState": {"cursor": "2024-06-01"},
                    },
                ],
            },
            id="stream_round_trip",
        ),
        pytest.param(
            {
                "stateType": "global",
                "connectionId": "conn-rt-g",
                "globalState": {
                    "sharedState": {"cdc": True},
                    "streamStates": [
                        {
                            "streamDescriptor": {
                                "name": "users",
                                "namespace": "public",
                            },
                            "streamState": {"cursor": "2024-01-01"},
                        },
                    ],
                },
            },
            id="global_round_trip",
        ),
        pytest.param(
            {
                "stateType": "legacy",
                "connectionId": "conn-rt-l",
                "state": {"opaque": "blob"},
            },
            id="legacy_round_trip",
        ),
    ],
)
def test_state_round_trip(raw_state: dict[str, Any]) -> None:
    """Normalize to protocol, then denormalize back — stateType and data must survive."""
    protocol = _normalize_state_to_protocol(raw_state)
    restored = _denormalize_protocol_state_to_api(
        protocol, connection_id=raw_state["connectionId"]
    )
    assert restored["stateType"] == raw_state["stateType"]
    assert restored["connectionId"] == raw_state["connectionId"]
    if raw_state["stateType"] == "stream":
        assert restored["streamState"] == raw_state["streamState"]
    elif raw_state["stateType"] == "global":
        assert restored["globalState"] == raw_state["globalState"]
    elif raw_state["stateType"] == "legacy":
        assert restored["state"] == raw_state["state"]


# --- Tests for _is_protocol_state_format ---


@pytest.mark.parametrize(
    "state_input,expected",
    [
        pytest.param([{"type": "STREAM", "stream": {}}], True, id="list_of_messages"),
        pytest.param({"type": "STREAM", "stream": {}}, True, id="single_message_dict"),
        pytest.param(
            {"stateType": "stream", "connectionId": "x"}, False, id="api_format"
        ),
        pytest.param([], True, id="empty_list"),
        pytest.param(
            [{"streamDescriptor": {"name": "x"}, "streamState": {}}],
            False,
            id="raw_stream_state_list",
        ),
        pytest.param(
            [{"type": "STREAM", "stream": {}}, {"no_type": True}],
            False,
            id="mixed_list_invalid",
        ),
    ],
)
def test_is_protocol_state_format(
    state_input: dict[str, Any] | list[dict[str, Any]],
    expected: bool,
) -> None:
    assert _is_protocol_state_format(state_input) is expected
