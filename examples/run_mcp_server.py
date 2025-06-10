#!/usr/bin/env python3

"""Example script demonstrating PyAirbyte MCP (Model Context Protocol) server usage.

This script shows how to:
1. Start the MCP server with stdio transport
2. Use the 5 available MCP tools for connector management
3. Handle configuration validation with secret detection

The MCP server provides these tools:
- list_connectors: List available connectors with filtering
- get_config_spec: Get connector configuration schema
- validate_config: Validate config and detect hardcoded secrets
- run_sync: Execute sync operations to DuckDB cache

For production use, the MCP server would typically be started as a separate process
and accessed by MCP clients (like AI assistants) via the stdio transport.
"""

from __future__ import annotations

import asyncio
import json

from airbyte.mcp.server import (
    PyAirbyteServer,
    get_config_spec,
    list_connectors,
    validate_config,
)


async def demonstrate_mcp_tools() -> None:
    """Demonstrate the MCP server tools functionality."""
    print("=== PyAirbyte MCP Server Tools Demo ===\n")

    print("1. Listing available connectors...")
    connectors_markdown = list_connectors()
    print(f"Found connectors (markdown format, {len(connectors_markdown)} chars)")
    print(connectors_markdown[:200] + "...\n")

    print("2. Listing connectors in JSON format...")
    connectors_json = list_connectors(output_format="json")
    connectors_data = json.loads(connectors_json)
    print(f"Found {connectors_data['count']} connectors")
    print(f"First 3: {connectors_data['connectors'][:3]}\n")

    print("3. Getting config spec for source-faker...")
    config_spec_yaml = get_config_spec("source-faker")
    print(f"Config spec (YAML format, {len(config_spec_yaml)} chars):")
    print(config_spec_yaml[:300] + "...\n")

    print("4. Getting config spec in JSON format...")
    config_spec_json = get_config_spec("source-faker", output_format="json")
    spec_data = json.loads(config_spec_json)
    print(f"Config spec has {len(spec_data.get('properties', {}))} properties\n")

    print("5. Validating a valid configuration...")
    valid_config = {
        "count": 100,
        "seed": 12345,
        "records_per_sync": 100,
        "records_per_slice": 100,
        "always_updated": False,
        "parallelism": 1,
    }
    validation_result = validate_config("source-faker", valid_config)
    print(f"Validation result: {validation_result}\n")

    print("6. Testing secret detection...")
    config_with_secret = {
        "count": 100,
        "api_key": "hardcoded_secret_value",  # This would be detected as a secret
    }
    secret_validation = validate_config("source-faker", config_with_secret)
    print(f"Secret validation result: {secret_validation}\n")

    print("=== MCP Tools Demo Complete ===")


async def run_mcp_server() -> None:
    """Start the MCP server with stdio transport.

    In production, this would be run as a separate process and accessed
    by MCP clients via stdin/stdout communication.
    """
    print("Starting PyAirbyte MCP Server...")
    print("Note: This would normally run indefinitely, waiting for MCP client requests")
    print("Press Ctrl+C to stop the server\n")

    server = PyAirbyteServer()
    try:
        await server.run_stdio()
    except KeyboardInterrupt:
        print("\nMCP Server stopped by user")


async def main() -> None:
    """Main entry point - demonstrate tools then optionally start server."""
    await demonstrate_mcp_tools()

    print("\nTo start the MCP server interactively, uncomment the line below:")
    print("# await run_mcp_server()")

    print("\nTo use the MCP server with an MCP client:")
    print("python examples/run_mcp_server.py")


if __name__ == "__main__":
    asyncio.run(main())
