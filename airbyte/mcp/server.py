# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Experimental MCP (Model Context Protocol) server for PyAirbyte connector management."""

import asyncio
import sys

from fastmcp import FastMCP

from airbyte.mcp._cloud_ops import register_cloud_ops_tools
from airbyte.mcp._connector_registry import register_connector_registry_tools
from airbyte.mcp._local_ops import register_local_ops_tools
from airbyte.mcp._util import initialize_secrets


initialize_secrets()

app: FastMCP = FastMCP("airbyte-mcp")
register_connector_registry_tools(app)
register_local_ops_tools(app)
register_cloud_ops_tools(app)


def main() -> None:
    """Main entry point for the MCP server."""
    print("Starting Airbyte MCP server.", file=sys.stderr)
    try:
        asyncio.run(app.run_stdio_async())
    except KeyboardInterrupt:
        print("Airbyte MCP server interrupted by user.", file=sys.stderr)
    except Exception as ex:
        print(f"Error running Airbyte MCP server: {ex}", file=sys.stderr)
        sys.exit(1)

    print("Airbyte MCP server stopped.", file=sys.stderr)


if __name__ == "__main__":
    main()
