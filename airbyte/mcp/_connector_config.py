# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Local connector config MCP operations."""

from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field


def register_connector_config_tools(app: FastMCP) -> None:
    """Register development tools with the FastMCP app."""
    pass
