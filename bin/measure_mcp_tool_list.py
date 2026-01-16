#!/usr/bin/env python3
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Measure the size of the MCP tool list output.

This script measures the character count of the tool list returned by the MCP server,
which is useful for tracking context truncation issues when AI agents call list_tools.

Usage:
    poe mcp-measure-tools

Output:
    Prints tool count and character count for the tool list descriptions.
"""

import asyncio
import sys

from fastmcp import Client

from airbyte.mcp.server import app


async def measure_tool_list() -> tuple[int, int]:
    """Measure the tool list size from the MCP server.

    Returns:
        Tuple of (tool_count, total_character_count)
    """
    async with Client(app) as client:
        tools = await client.list_tools()

        tool_count = len(tools)
        total_chars = 0

        for tool in tools:
            # Count characters in tool name
            total_chars += len(tool.name)

            # Count characters in description
            if tool.description:
                total_chars += len(tool.description)

            # Count characters in input schema (parameter descriptions)
            if tool.inputSchema:
                total_chars += len(str(tool.inputSchema))

        return tool_count, total_chars


def main() -> None:
    """Main entry point for the MCP tool list measurement."""
    try:
        tool_count, total_chars = asyncio.run(measure_tool_list())

        print("MCP Server: airbyte-coral-mcp")
        print(f"Tool count: {tool_count}")
        print(f"Total characters: {total_chars:,}")
        print(f"Average chars per tool: {total_chars // tool_count:,}")

    except Exception as e:
        print(f"Error measuring tool list: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
