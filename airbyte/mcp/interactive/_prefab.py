# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Prefab generative UI MCP provider.

For more info: https://gofastmcp.com/apps/generative
"""

from fastmcp.apps.generative import GenerativeUI
from fastmcp.server.providers import Provider
from fastmcp_extensions import mcp_provider
from fastmcp_extensions.tool_filters import ANNOTATION_MCP_MODULE

from airbyte.mcp._tool_utils import INTERACTIVE_UI_ANNOTATION


@mcp_provider(
    annotations={
        ANNOTATION_MCP_MODULE: "interactive",
        INTERACTIVE_UI_ANNOTATION: True,
    },
)
def prefab_generative_ui_provider() -> Provider:
    """Create the FastMCP Prefab generative UI provider."""
    return GenerativeUI()
