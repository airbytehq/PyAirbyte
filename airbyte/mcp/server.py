# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Experimental MCP (Model Context Protocol) server for PyAirbyte connector management."""

import asyncio
import sys

from fastmcp import FastMCP

from airbyte.mcp._cloud_ops import register_cloud_ops_tools
from airbyte.mcp._connector_registry import register_connector_registry_tools
from airbyte.mcp._local_ops import register_local_ops_tools
from airbyte.mcp._util import initialize_secrets, log_mcp_message


initialize_secrets()

app: FastMCP = FastMCP("airbyte-mcp")
register_connector_registry_tools(app)
register_local_ops_tools(app)
register_cloud_ops_tools(app)


def main() -> None:
    """Main entry point for the MCP server."""
    log_mcp_message("Starting Airbyte MCP server", component="mcp_server", event="startup")
    try:
        asyncio.run(app.run_stdio_async())
    except KeyboardInterrupt:
        log_mcp_message(
            "Airbyte MCP server interrupted by user", component="mcp_server", event="interrupt"
        )
    except Exception as ex:
        log_mcp_message(
            f"Error running Airbyte MCP server: {ex}",
            level="error",
            component="mcp_server",
            event="error",
            error=str(ex),
        )
        sys.exit(1)

    log_mcp_message("Airbyte MCP server stopped", component="mcp_server", event="shutdown")


if __name__ == "__main__":
    main()
