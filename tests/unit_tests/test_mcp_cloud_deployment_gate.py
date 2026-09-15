"""Cloud root overrides do not imply execution availability or change insiders access."""

from typing import cast

import pytest
from fastmcp import FastMCP
from fastmcp_extensions.tool_filters import ANNOTATION_MCP_MODULE
from mcp.types import Tool

from airbyte.constants import (
    MCP_CONFIG_API_URL,
    MCP_CONFIG_CONFIG_API_URL,
    MCP_CONFIG_INSIDERS,
)
from airbyte.mcp import _tool_utils


@pytest.mark.parametrize("root_key", [MCP_CONFIG_API_URL, MCP_CONFIG_CONFIG_API_URL])
@pytest.mark.parametrize("insiders", ["0", "1"])
def test_custom_cloud_root_preserves_insiders_gate(
    monkeypatch: pytest.MonkeyPatch,
    root_key: str,
    insiders: str,
) -> None:
    """Custom deployments advertise opted-in tools without a direct Agents URL."""
    monkeypatch.delenv("AIRBYTE_AGENTS_API_URL", raising=False)
    config = {
        root_key: "https://cloud.example.test/api/v1",
        MCP_CONFIG_INSIDERS: insiders,
    }
    monkeypatch.setattr(
        _tool_utils, "get_mcp_config", lambda app, key, **kwargs: config.get(key)
    )
    tool = Tool(
        name="read_agent_skill_docs",
        inputSchema={"type": "object"},
        annotations={ANNOTATION_MCP_MODULE: "agents"},
    )
    assert _tool_utils.airbyte_module_filter(tool, cast(FastMCP, object())) is (
        insiders == "1"
    )
