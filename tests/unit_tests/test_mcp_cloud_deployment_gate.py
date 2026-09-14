"""Unit tests for the MCP Cloud-deployment gate."""

from __future__ import annotations

from typing import cast

import pytest
from fastmcp import Context

from airbyte.constants import (
    CLOUD_API_ROOT,
    CLOUD_CONFIG_API_ROOT,
    MCP_CONFIG_API_URL,
    MCP_CONFIG_CONFIG_API_URL,
)
from airbyte.mcp import _tool_utils


CTX = cast(Context, object())


@pytest.fixture
def mcp_config(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Patch `get_mcp_config` so tests can set MCP config values directly."""
    config: dict[str, str] = {}
    monkeypatch.setattr(
        _tool_utils,
        "get_mcp_config",
        lambda ctx, key, **kwargs: config.get(key),  # noqa: ARG005
    )
    return config


def test_is_agents_api_available(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    """Recognize public Cloud roots and explicit Agents API roots."""
    monkeypatch.delenv("AIRBYTE_AGENTS_API_URL", raising=False)
    assert _tool_utils.is_agents_api_available(CTX)

    mcp_config.update({
        MCP_CONFIG_API_URL: f"{CLOUD_API_ROOT}/",
        MCP_CONFIG_CONFIG_API_URL: f"{CLOUD_CONFIG_API_ROOT}/",
    })
    assert _tool_utils.is_agents_api_available(CTX)

    mcp_config[MCP_CONFIG_API_URL] = "https://airbyte.example.com/api/public/v1"
    assert not _tool_utils.is_agents_api_available(CTX)

    mcp_config.pop(MCP_CONFIG_API_URL)
    mcp_config[MCP_CONFIG_CONFIG_API_URL] = "https://airbyte.example.com/api/v1"
    assert not _tool_utils.is_agents_api_available(CTX)

    monkeypatch.setenv("AIRBYTE_AGENTS_API_URL", "https://agents.example.com/api/v1")
    assert _tool_utils.is_agents_api_available(CTX)
