# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""MCP execution mode state."""

from __future__ import annotations


_HOSTED_MCP_MODE_ENABLED: bool = False
"""Whether the process is serving MCP over hosted HTTP transport."""


def set_hosted_mcp_mode() -> None:
    """Set the flag indicating the process serves MCP over hosted HTTP transport."""
    global _HOSTED_MCP_MODE_ENABLED
    _HOSTED_MCP_MODE_ENABLED = True


def is_hosted_mcp_mode() -> bool:
    """Return True if the process serves MCP over hosted HTTP transport."""
    return _HOSTED_MCP_MODE_ENABLED
