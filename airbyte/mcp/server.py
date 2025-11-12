# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Experimental MCP (Model Context Protocol) server for PyAirbyte connector management."""

import asyncio
import sys

from fastmcp import FastMCP

from airbyte._util.meta import set_mcp_mode
from airbyte.mcp._util import initialize_secrets
from airbyte.mcp.cloud_ops import register_cloud_ops_tools
from airbyte.mcp.connector_registry import register_connector_registry_tools
from airbyte.mcp.local_ops import register_local_ops_tools
from airbyte.mcp.smoke_tests import register_smoke_test_prompt


set_mcp_mode()
initialize_secrets()

app: FastMCP = FastMCP("airbyte-mcp")
"""The Airbyte MCP Server application instance."""

register_connector_registry_tools(app)
register_local_ops_tools(app)
register_cloud_ops_tools(app)
register_smoke_test_prompt(app)


def main() -> None:
    """@private Main entry point for the MCP server.

    This function starts the FastMCP server to handle MCP requests.

    It should not be called directly; instead, consult the MCP client documentation
    for instructions on how to connect to the server.
    """
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
