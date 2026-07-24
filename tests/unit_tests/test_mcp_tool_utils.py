# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP tool utility functions."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import patch

import pytest

from airbyte.mcp._tool_utils import (
    SafeModeError,
    _GUIDS_CREATED_IN_SESSION,
    _resolve_transport_bearer_token,
    check_guid_created_in_session,
    register_guid_created_in_session,
)


@dataclass
class _FakeAccessToken:
    """Minimal stand-in for a FastMCP `AccessToken` (only `token` is read)."""

    token: str


@pytest.mark.parametrize(
    ("verified_token", "headers", "expected"),
    [
        pytest.param(
            "upstream-airbyte-token",
            {"authorization": "Bearer minted-mcp-reference-jwt"},
            "upstream-airbyte-token",
            id="verified_token_wins_over_authorization_header",
        ),
        pytest.param(
            None,
            {"authorization": "Bearer real-airbyte-token"},
            "real-airbyte-token",
            id="falls_back_to_bearer_prefixed_header",
        ),
        pytest.param(
            None,
            {"Authorization": "bare-token-no-prefix"},
            "bare-token-no-prefix",
            id="header_lookup_is_case_insensitive_and_accepts_bare_token",
        ),
        pytest.param(
            "",
            {"authorization": "Bearer header-token"},
            "header-token",
            id="empty_verified_token_falls_back_to_header",
        ),
        pytest.param(
            None,
            {},
            "",
            id="no_verified_token_and_no_header_returns_empty",
        ),
    ],
)
def test_resolve_transport_bearer_token(
    verified_token: str | None,
    headers: dict[str, str],
    expected: str,
) -> None:
    """The downstream bearer prefers the verified token over the raw header.

    Regression guard for the `OAuthProxy` `401`: the raw `Authorization` header
    carries the proxy's minted reference JWT, so it must never win over the
    upstream token exposed by `get_access_token`.
    """
    access_token = (
        _FakeAccessToken(verified_token) if verified_token is not None else None
    )
    with (
        patch("airbyte.mcp._tool_utils.get_access_token", return_value=access_token),
        patch(
            "airbyte.mcp._tool_utils.get_http_headers", return_value=headers
        ) as mock_get_http_headers,
    ):
        assert _resolve_transport_bearer_token() == expected

    if verified_token:
        mock_get_http_headers.assert_not_called()
    else:
        mock_get_http_headers.assert_called_once_with(include={"authorization"})


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
