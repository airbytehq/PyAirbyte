# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Secret manager tools for MCP."""

from fastmcp import FastMCP

import airbyte.secrets.util as ab_secrets_util


def list_registered_secret_managers() -> list[str]:
    """List all registered secret managers."""
    return [manager.name for manager in ab_secrets_util.get_secret_sources()]


def list_available_secrets() -> list[tuple[str, str]]:
    """List all secrets from the configured secret sources."""
    return ab_secrets_util.list_available_secrets()


def register_secret_manager_tools(app: FastMCP) -> None:
    """Register a custom secret manager."""
    app.tool(list_registered_secret_managers)
    app.tool(list_available_secrets)
