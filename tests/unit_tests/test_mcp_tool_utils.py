# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP tool utility functions."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import patch

import pytest
from fastmcp_extensions import MCPServerConfigArg

from airbyte.exceptions import AirbyteSafeModeError
from airbyte.mcp._tool_utils import (
    API_URL_CONFIG_ARG,
    ALLOW_EXTERNAL_ACCESS_CONFIG_ARG,
    ALLOW_PIPELINE_CHANGES_CONFIG_ARG,
    CLIENT_ID_CONFIG_ARG,
    CLIENT_SECRET_CONFIG_ARG,
    CONFIG_API_URL_CONFIG_ARG,
    TRUSTED_EXECUTION_CONFIG_ARG,
    _GUIDS_CREATED_IN_SESSION,
    _TOOL_POLICIES,
    ToolPolicy,
    _resolve_transport_bearer_token,
    check_guid_created_in_session,
    mcp_tool,
    register_guid_created_in_session,
)
from airbyte.mcp import server as server_module


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
    """Test that check raises AirbyteSafeModeError for unregistered GUIDs when safe mode is enabled."""
    with patch("airbyte.mcp._tool_utils.AIRBYTE_CLOUD_MCP_SAFE_MODE", True):
        with pytest.raises(AirbyteSafeModeError) as exc_info:
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


@pytest.mark.parametrize(
    "config_arg",
    [
        pytest.param(API_URL_CONFIG_ARG, id="api_url"),
        pytest.param(CONFIG_API_URL_CONFIG_ARG, id="config_api_url"),
        pytest.param(CLIENT_ID_CONFIG_ARG, id="client_id"),
        pytest.param(CLIENT_SECRET_CONFIG_ARG, id="client_secret"),
        pytest.param(TRUSTED_EXECUTION_CONFIG_ARG, id="trusted_execution"),
    ],
)
def test_config_args_are_not_caller_controllable(
    config_arg: MCPServerConfigArg,
) -> None:
    """API roots, downstream credentials, and the trust gate are server-side only.

    A request header source for these would let a caller redirect the server's
    credentialed outbound requests, act as another Cloud identity, or widen the
    tool surface.
    """
    assert config_arg.http_header_key is None


def test_mcp_tool_registers_policy_defaults_and_overrides() -> None:
    @mcp_tool(read_only=True)
    def policy_read_only_default() -> None:
        pass

    @mcp_tool()
    def policy_write_default() -> None:
        pass

    @mcp_tool(read_only=True, pipeline_change=True, external_access=True)
    def policy_explicit_override() -> None:
        pass

    assert _TOOL_POLICIES[policy_read_only_default.__name__] == ToolPolicy(
        pipeline_change=False,
        external_access=False,
    )
    assert _TOOL_POLICIES[policy_write_default.__name__] == ToolPolicy(
        pipeline_change=True,
        external_access=False,
    )
    assert _TOOL_POLICIES[policy_explicit_override.__name__] == ToolPolicy(
        pipeline_change=True,
        external_access=True,
    )


def test_server_tools_register_expected_policies() -> None:
    assert server_module.app
    external_tools = (
        "execute_external_api_query",
        "execute_external_sql_query",
        "get_cloud_search_status",
        "get_agent_skill_docs",
    )
    for tool_name in external_tools:
        assert _TOOL_POLICIES[tool_name].external_access

    for tool_name in ("run_cloud_sync", "cancel_cloud_sync"):
        assert _TOOL_POLICIES[tool_name].pipeline_change is False


@pytest.mark.parametrize(
    ("config_arg", "expected_env", "expected_header", "expected_name"),
    [
        pytest.param(
            ALLOW_PIPELINE_CHANGES_CONFIG_ARG,
            "AIRBYTE_CLOUD_MCP_ALLOW_PIPELINE_CHANGES",
            "X-MCP-Allow-Pipeline-Changes",
            "allow_pipeline_changes",
            id="pipeline-changes",
        ),
        pytest.param(
            ALLOW_EXTERNAL_ACCESS_CONFIG_ARG,
            "AIRBYTE_CLOUD_MCP_ALLOW_EXTERNAL_ACCESS",
            "X-MCP-Allow-External-Access",
            "allow_external_access",
            id="external-access",
        ),
    ],
)
def test_policy_config_args(
    config_arg: MCPServerConfigArg,
    expected_env: str,
    expected_header: str,
    expected_name: str,
) -> None:
    assert config_arg.name == expected_name
    assert config_arg.env_var == expected_env
    assert config_arg.http_header_key == expected_header
    assert config_arg.default == ""
    assert not config_arg.required


_DOCSTRING_SECTION_HEADERS = ("Args:", "Returns:", "Raises:")


def _assert_docstring_is_dedented(doc: str) -> None:
    in_section = False
    for line in doc.splitlines():
        if line in _DOCSTRING_SECTION_HEADERS:
            in_section = True
            continue
        if not line:
            in_section = False
            continue
        assert line.strip(), f"Whitespace-only line in docstring: {doc!r}"
        if not in_section:
            assert not line.startswith(" "), f"Indented body line {line!r} in: {doc!r}"


def test_mcp_tool_dedents_docstring_before_extra_help_text() -> None:
    @mcp_tool(read_only=True, extra_help_text="Extra help\nline two")
    def dedent_docstring_tool(value: str) -> None:
        """Summary line.

        Indented body line one.
        Indented body line two.

        Args:
            value: Some value.
        """

    doc = dedent_docstring_tool.__doc__
    assert doc is not None
    _assert_docstring_is_dedented(doc)
    assert "\nIndented body line one.\nIndented body line two.\n" in doc
    assert "\nArgs:\n    value: Some value.\n" in doc
    assert doc.endswith("\n\nExtra help\nline two")


@pytest.mark.parametrize(
    "tool_name",
    [
        "validate_connector_config",
        "list_dotenv_secrets",
        "list_source_streams",
        "get_stream_previews",
        "run_sql_query",
    ],
)
def test_local_tool_descriptions_are_dedented(tool_name: str) -> None:
    from airbyte.mcp import local as local_module

    doc = getattr(local_module, tool_name).__doc__
    assert doc is not None
    _assert_docstring_is_dedented(doc)
