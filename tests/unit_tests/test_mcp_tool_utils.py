# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP tool utility functions."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from airbyte.mcp._tool_utils import (
    SafeModeError,
    _GUIDS_CREATED_IN_SESSION,
    _resolve_transport_bearer_token,
    check_guid_created_in_session,
    register_guid_created_in_session,
)


@pytest.fixture(autouse=True)
def clear_session_guids() -> None:
    """Clear the session GUIDs before each test."""
    _GUIDS_CREATED_IN_SESSION.clear()


def test_register_guid_created_in_session() -> None:
    """Test that GUIDs can be registered as created in session."""
    assert "test-guid-123" not in _GUIDS_CREATED_IN_SESSION
    register_guid_created_in_session("test-guid-123")
    assert "test-guid-123" in _GUIDS_CREATED_IN_SESSION


def test_check_guid_created_in_session_passes_for_registered_guid() -> None:
    """Test that check passes for GUIDs registered in session."""
    register_guid_created_in_session("test-guid-456")
    # Should not raise
    check_guid_created_in_session("test-guid-456")


def test_check_guid_created_in_session_raises_for_unregistered_guid() -> None:
    """Test that check raises SafeModeError for unregistered GUIDs when safe mode is enabled."""
    with patch("airbyte.mcp._tool_utils.AIRBYTE_CLOUD_MCP_SAFE_MODE", True):
        with pytest.raises(SafeModeError) as exc_info:
            check_guid_created_in_session("unregistered-guid")
        assert "unregistered-guid" in str(exc_info.value)
        assert "not created in this session" in str(exc_info.value)


def test_check_guid_created_in_session_passes_when_safe_mode_disabled() -> None:
    """Test that check passes for any GUID when safe mode is disabled."""
    with patch("airbyte.mcp._tool_utils.AIRBYTE_CLOUD_MCP_SAFE_MODE", False):
        # Should not raise even for unregistered GUID
        check_guid_created_in_session("any-guid-at-all")


def test_multiple_guids_can_be_registered() -> None:
    """Test that multiple GUIDs can be registered in the same session."""
    guids = ["guid-1", "guid-2", "guid-3"]
    for guid in guids:
        register_guid_created_in_session(guid)

    for guid in guids:
        assert guid in _GUIDS_CREATED_IN_SESSION


def test_duplicate_guid_registration_is_idempotent() -> None:
    """Test that registering the same GUID multiple times is safe."""
    register_guid_created_in_session("duplicate-guid")
    register_guid_created_in_session("duplicate-guid")
    assert "duplicate-guid" in _GUIDS_CREATED_IN_SESSION


class _FakeAccessToken:
    """Minimal stand-in for a FastMCP `AccessToken` carrying a token string."""

    def __init__(self, token: str) -> None:
        """Store the token value exposed via the `token` attribute."""
        self.token = token


@pytest.mark.parametrize(
    "access_token, expected",
    [
        pytest.param(None, "", id="stdio_mode_returns_empty_string"),
        pytest.param(_FakeAccessToken(""), "", id="empty_token_returns_empty_string"),
        pytest.param(
            _FakeAccessToken("verified-jwt"),
            "verified-jwt",
            id="verified_token_is_returned",
        ),
    ],
)
def test_resolve_transport_bearer_token(
    access_token: _FakeAccessToken | None,
    expected: str,
) -> None:
    """Verify the transport-token resolver returns the verified token or `""`.

    `get_access_token` returns `None` outside a request (stdio mode), so the
    resolver yields `""` and downstream config resolution falls back to client
    credentials.
    """
    with patch(
        "airbyte.mcp._tool_utils.get_access_token",
        return_value=access_token,
    ):
        assert _resolve_transport_bearer_token() == expected
