# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Experimental MCP (Model Context Protocol) server for PyAirbyte connector management."""

import asyncio
import sys

from fastmcp_extensions import mcp_server, register_mcp_prompts, register_mcp_tools

import airbyte.mcp.cloud_ops
import airbyte.mcp.connector_registry
import airbyte.mcp.local_ops
import airbyte.mcp.prompts  # noqa: F401 - Import to register prompts
from airbyte._util.meta import set_mcp_mode
from airbyte.mcp._tool_utils import AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET
from airbyte.mcp._util import initialize_secrets


# =============================================================================
# Server Instructions
# =============================================================================
# This text is provided to AI agents via the MCP protocol's "instructions" field.
# It helps agents understand when to use this server's tools, especially when
# tool search is enabled. For more context, see:
# - FastMCP docs: https://gofastmcp.com/servers/overview
# - Claude tool search: https://www.anthropic.com/news/tool-use-improvements
# =============================================================================

MCP_SERVER_INSTRUCTIONS = """
PyAirbyte connector management and data integration server for discovering,
deploying, and running Airbyte connectors.

Use this server for:
- Discovering connectors from the Airbyte registry (sources and destinations)
- Deploying sources, destinations, and connections to Airbyte Cloud
- Running cloud syncs and monitoring sync status
- Managing custom connector definitions in Airbyte Cloud
- Local connector execution for data extraction without cloud deployment
- Listing and describing environment variables for connector configuration

Operational modes:
- Cloud operations: Deploy and manage connectors on Airbyte Cloud (requires
  AIRBYTE_CLOUD_CLIENT_ID, AIRBYTE_CLOUD_CLIENT_SECRET, AIRBYTE_CLOUD_WORKSPACE_ID)
- Local operations: Run connectors locally for data extraction (requires
  AIRBYTE_PROJECT_DIR for artifact storage)

Safety features:
- Safe mode (default): Restricts destructive operations to objects created in
  the current session
- Read-only mode: Disables all write operations for cloud resources
""".strip()

set_mcp_mode()
initialize_secrets()

app = mcp_server(
    name="airbyte-mcp",
    package_name="airbyte",
    instructions=MCP_SERVER_INSTRUCTIONS,
    include_standard_tool_filters=True,
)
"""The Airbyte MCP Server application instance."""

# Register tools from each module
# For cloud_ops, conditionally hide workspace_id when env var is set
register_mcp_tools(
    app,
    mcp_module="cloud_ops",
    exclude_args=["workspace_id"] if AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET else None,
)
register_mcp_tools(app, mcp_module="local_ops")
register_mcp_tools(app, mcp_module="connector_registry")
register_mcp_prompts(app, mcp_module="prompts")


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
