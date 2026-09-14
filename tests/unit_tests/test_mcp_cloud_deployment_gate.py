"""Unit tests for the MCP Cloud-deployment gate."""

from __future__ import annotations

from typing import cast

import pytest
from fastmcp import Context

from airbyte.agents.organizations import AgentOrganization
from airbyte.constants import (
    CLOUD_API_ROOT,
    CLOUD_CONFIG_API_ROOT,
    MCP_CONFIG_API_URL,
    MCP_CONFIG_BEARER_TOKEN,
    MCP_CONFIG_CONFIG_API_URL,
)
from airbyte.exceptions import AirbyteCloudDeploymentRequiredError
from airbyte.mcp import _guards
from airbyte.mcp import agents as agents_mcp


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


def test_raise_if_not_cloud_deployment(mcp_config: dict[str, str]) -> None:
    """Raise with overridden roots and otherwise do nothing."""
    _guards.raise_if_not_cloud_deployment(CTX, feature="Agents tools")

    mcp_config[MCP_CONFIG_API_URL] = "https://airbyte.example.com/api/public/v1"
    with pytest.raises(AirbyteCloudDeploymentRequiredError) as exc_info:
        _guards.raise_if_not_cloud_deployment(CTX, feature="Agents tools")

    assert exc_info.value.context == {
        MCP_CONFIG_API_URL: "https://airbyte.example.com/api/public/v1"
    }


def test_agents_helpers_raise_on_non_cloud_deployment(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    """Agents helper constructors hard-fail on overridden roots."""
    monkeypatch.setattr(
        agents_mcp,
        "get_mcp_config",
        lambda ctx, key, **kwargs: mcp_config.get(key),  # noqa: ARG005
    )
    mcp_config[MCP_CONFIG_API_URL] = "https://airbyte.example.com/api/public/v1"

    with pytest.raises(AirbyteCloudDeploymentRequiredError):
        agents_mcp._get_agent_organization(CTX, None)  # noqa: SLF001
    with pytest.raises(AirbyteCloudDeploymentRequiredError):
        agents_mcp._get_agent_workspace(CTX, "ws-id", "org-id")  # noqa: SLF001


def test_agent_organization_constructs_on_cloud(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    """Agent organization construction does not call the network."""
    monkeypatch.setattr(
        agents_mcp,
        "get_mcp_config",
        lambda ctx, key, **kwargs: mcp_config.get(key),  # noqa: ARG005
    )
    mcp_config[MCP_CONFIG_BEARER_TOKEN] = "tok"

    organization = agents_mcp._get_agent_organization(CTX, "org-id")  # noqa: SLF001

    assert isinstance(organization, AgentOrganization)
