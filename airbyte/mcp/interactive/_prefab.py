# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Prefab generative UI MCP provider."""

from fastmcp import FastMCP
from fastmcp.apps.generative import GenerativeUI
from fastmcp.server.providers import Provider

from airbyte.mcp._tool_utils import (
    INTERACTIVE_UI_ANNOTATION,
    mcp_provider,
    register_mcp_tools,
)


@mcp_provider(
    read_only=True,
    idempotent=True,
    open_world=True,
    annotations={
        INTERACTIVE_UI_ANNOTATION: True,
    },
)
def _generative_ui_provider() -> Provider:
    """Create the FastMCP Prefab generative UI provider."""
    return GenerativeUI()


def register_generative_ui_tools(app: FastMCP) -> None:
    """Register Prefab generative UI provider tools."""
    register_mcp_tools(app, mcp_module="interactive", include_tools=False)
