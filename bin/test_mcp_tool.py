#!/usr/bin/env python3
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""One-liner CLI tool for testing PyAirbyte MCP tools directly with JSON arguments.

Usage:
    poe mcp-tool-test <tool_name> '<json_args>'

Examples:
    poe mcp-tool-test list_connectors '{}'
    poe mcp-tool-test get_config_spec '{"connector_name": "source-pokeapi"}'
    poe mcp-tool-test validate_config \
        '{"connector_name": "source-pokeapi", "config": {"pokemon_name": "pikachu"}}'
    poe mcp-tool-test run_sync \
        '{"connector_name": "source-pokeapi", "config": {"pokemon_name": "pikachu"}}'

    poe mcp-tool-test check_airbyte_cloud_workspace '{}'
    poe mcp-tool-test list_deployed_cloud_connections '{}'
    poe mcp-tool-test get_cloud_sync_status \
        '{"connection_id": "0791e193-811b-4fcf-91c3-f8c5963e74a0", "include_attempts": true}'
    poe mcp-tool-test get_cloud_sync_logs \
        '{"connection_id": "0791e193-811b-4fcf-91c3-f8c5963e74a0"}'
"""

import asyncio
import json
import sys
import traceback
from typing import Any

from fastmcp import Client

from airbyte.mcp.server import app


MIN_ARGS = 3


async def call_mcp_tool(tool_name: str, args: dict[str, Any]) -> object:
    """Call an MCP tool using the FastMCP client."""
    async with Client(app) as client:
        return await client.call_tool(tool_name, args)


def main() -> None:
    """Main entry point for the MCP tool tester."""
    if len(sys.argv) < MIN_ARGS:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    tool_name = sys.argv[1]
    json_args = sys.argv[2]

    try:
        args: dict[str, Any] = json.loads(json_args)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON arguments: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        result = asyncio.run(call_mcp_tool(tool_name, args))

        if hasattr(result, "text"):
            print(result.text)
        else:
            print(str(result))

    except Exception as e:
        print(f"Error executing tool '{tool_name}': {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
