"""Unit tests for the MCP Cloud-deployment gate."""

from __future__ import annotations

from typing import cast

import pytest
from fastmcp import Context

from airbyte.agents import _api_util
from airbyte.constants import (
    CLOUD_API_ROOT,
    CLOUD_CONFIG_API_ROOT,
    MCP_CONFIG_API_URL,
    MCP_CONFIG_CONFIG_API_URL,
)
from airbyte.mcp import _guards


CTX = cast(Context, object())


@pytest.fixture
def mcp_config(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Patch `get_mcp_config` so tests can set MCP config values directly."""
    config: dict[str, str] = {}
    monkeypatch.setattr(
        _guards,
        "get_mcp_config",
        lambda ctx, key, **kwargs: config.get(key),  # noqa: ARG005
    )
    return config


def test_is_cloud_deployment(mcp_config: dict[str, str]) -> None:
    """Recognize public Cloud roots and reject either override."""
    assert _guards.is_cloud_deployment(CTX)

    mcp_config.update({
        MCP_CONFIG_API_URL: f"{CLOUD_API_ROOT}/",
        MCP_CONFIG_CONFIG_API_URL: f"{CLOUD_CONFIG_API_ROOT}/",
    })
    assert _guards.is_cloud_deployment(CTX)

    mcp_config[MCP_CONFIG_API_URL] = "https://airbyte.example.com/api/public/v1"
    assert not _guards.is_cloud_deployment(CTX)

    mcp_config.pop(MCP_CONFIG_API_URL)
    mcp_config[MCP_CONFIG_CONFIG_API_URL] = "https://airbyte.example.com/api/v1"
    assert not _guards.is_cloud_deployment(CTX)


def test_get_overridden_cloud_api_roots() -> None:
    """Return only non-public, non-blank Cloud API roots."""
    assert (
        _api_util.get_overridden_cloud_api_roots(
            public_api_root=None,
            config_api_root=None,
        )
        == {}
    )
    assert (
        _api_util.get_overridden_cloud_api_roots(
            public_api_root="",
            config_api_root=f"{CLOUD_CONFIG_API_ROOT}/",
        )
        == {}
    )
    assert _api_util.get_overridden_cloud_api_roots(
        public_api_root="https://airbyte.example.com/api/public/v1",
        config_api_root=CLOUD_CONFIG_API_ROOT,
    ) == {"api_root": "https://airbyte.example.com/api/public/v1"}
