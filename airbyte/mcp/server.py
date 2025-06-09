# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

"""PyAirbyte MCP Server implementation."""

from __future__ import annotations

import json
import os
from typing import Any

import mcp.server.stdio
from mcp import types
from mcp.server.lowlevel import Server

from airbyte import get_source
from airbyte.caches.util import get_default_cache
from airbyte.sources import get_available_connectors


app = Server("pyairbyte-mcp")


def _detect_hardcoded_secrets(config: dict[str, Any], spec: dict[str, Any]) -> list[str]:
    """Detect hardcoded secrets in config that should be environment variables."""
    hardcoded_secrets = []

    properties = spec.get("properties", {})

    for field_name, field_value in config.items():
        if field_name not in properties:
            continue

        field_spec = properties[field_name]

        is_secret = (
            field_spec.get("format") == "password"
            or field_spec.get("writeOnly") is True
            or "password" in field_name.lower()
            or "secret" in field_name.lower()
            or "token" in field_name.lower()
            or "key" in field_name.lower()
        )

        if (
            is_secret
            and isinstance(field_value, str)
            and not (field_value.startswith("${") and field_value.endswith("}"))
            and field_value not in os.environ
        ):
            hardcoded_secrets.append(field_name)

    return hardcoded_secrets


def _generate_config_markdown(connector_name: str, spec: dict[str, Any]) -> str:
    """Generate markdown documentation for connector configuration."""
    properties = spec.get("properties", {})
    required = spec.get("required", [])

    markdown = f"# {connector_name} Configuration\n\n"
    markdown += "## Configuration Properties\n\n"

    for prop_name, prop_spec in properties.items():
        markdown += f"### {prop_name}\n\n"

        if prop_spec.get("description"):
            markdown += f"{prop_spec['description']}\n\n"

        markdown += f"- **Type**: {prop_spec.get('type', 'unknown')}\n"
        markdown += f"- **Required**: {'Yes' if prop_name in required else 'No'}\n"

        if prop_spec.get("format") == "password" or prop_spec.get("writeOnly"):
            markdown += (
                f"- **Secret**: Yes - Use environment variable like `${{{prop_name.upper()}}}`\n"
            )

        if prop_spec.get("default") is not None:
            markdown += f"- **Default**: {prop_spec['default']}\n"

        markdown += "\n"

    markdown += "\n## Environment Variables for Secrets\n\n"
    markdown += "For security, all secret fields must be provided as environment variables:\n\n"

    for prop_name, prop_spec in properties.items():
        if prop_spec.get("format") == "password" or prop_spec.get("writeOnly"):
            markdown += f"- `{prop_name.upper()}`: {prop_spec.get('description', prop_name)}\n"

    return markdown


@app.list_tools()
def list_tools() -> list[types.Tool]:
    """List available MCP tools."""
    return [
        types.Tool(
            name="list_connectors",
            description="List available Airbyte connectors with optional filtering",
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword_filter": {
                        "type": "string",
                        "description": "Filter connectors by keyword in name or description",
                    },
                    "connector_type_filter": {
                        "type": "string",
                        "description": "Filter by connector type (source, destination)",
                    },
                    "language_filter": {
                        "type": "string",
                        "description": "Filter by implementation language (python, docker)",
                    },
                },
            },
        ),
        types.Tool(
            name="get_config_spec",
            description="Get the JSON schema configuration specification for a connector",
            inputSchema={
                "type": "object",
                "required": ["connector_name"],
                "properties": {
                    "connector_name": {
                        "type": "string",
                        "description": "Name of the connector (e.g., 'source-faker')",
                    }
                },
            },
        ),
        types.Tool(
            name="create_config_markdown",
            description="Generate markdown documentation for a connector's configuration",
            inputSchema={
                "type": "object",
                "required": ["connector_name"],
                "properties": {
                    "connector_name": {
                        "type": "string",
                        "description": "Name of the connector (e.g., 'source-faker')",
                    }
                },
            },
        ),
        types.Tool(
            name="validate_config",
            description="Validate a connector configuration, ensuring no hardcoded secrets",
            inputSchema={
                "type": "object",
                "required": ["connector_name", "config"],
                "properties": {
                    "connector_name": {
                        "type": "string",
                        "description": "Name of the connector (e.g., 'source-faker')",
                    },
                    "config": {"type": "object", "description": "Configuration object to validate"},
                },
            },
        ),
        types.Tool(
            name="run_sync",
            description="Run a sync from a source connector to the default DuckDB cache",
            inputSchema={
                "type": "object",
                "required": ["connector_name", "config"],
                "properties": {
                    "connector_name": {
                        "type": "string",
                        "description": "Name of the source connector (e.g., 'source-faker')",
                    },
                    "config": {
                        "type": "object",
                        "description": "Configuration object for the connector",
                    },
                },
            },
        ),
    ]


@app.call_tool()
def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    """Handle tool calls."""
    try:
        tool_map = {
            "list_connectors": _list_connectors,
            "get_config_spec": _get_config_spec,
            "create_config_markdown": _create_config_markdown,
            "validate_config": _validate_config,
            "run_sync": _run_sync,
        }

        if name in tool_map:
            return tool_map[name](**arguments)

        return [types.TextContent(type="text", text=f"Unknown tool: {name}")]
    except Exception as e:
        return [types.TextContent(type="text", text=f"Error: {e!s}")]


def _list_connectors(
    keyword_filter: str | None = None,
    connector_type_filter: str | None = None,
    language_filter: str | None = None,
) -> list[types.TextContent]:
    """List available connectors with optional filtering."""
    connectors = get_available_connectors()

    filtered_connectors = []
    for connector in connectors:
        if keyword_filter and keyword_filter.lower() not in connector.name.lower():
            continue

        if connector_type_filter and connector_type_filter.lower() not in connector.name.lower():
            continue

        if language_filter:
            pass

        filtered_connectors.append(connector)

    result = f"Found {len(filtered_connectors)} connectors:\n\n"
    for connector in filtered_connectors:
        result += f"- **{connector.name}**\n"
        result += "\n"

    return [types.TextContent(type="text", text=result)]


def _get_config_spec(connector_name: str) -> list[types.TextContent]:
    """Get the configuration specification for a connector."""
    try:
        source = get_source(connector_name)
        spec = source.config_spec

        return [types.TextContent(type="text", text=json.dumps(spec, indent=2))]
    except Exception as e:
        return [
            types.TextContent(
                type="text", text=f"Error getting config spec for {connector_name}: {e!s}"
            )
        ]


def _create_config_markdown(connector_name: str) -> list[types.TextContent]:
    """Generate markdown documentation for a connector's configuration."""
    try:
        source = get_source(connector_name)
        spec = source.config_spec

        markdown = _generate_config_markdown(connector_name, spec)

        return [types.TextContent(type="text", text=markdown)]
    except Exception as e:
        return [
            types.TextContent(
                type="text", text=f"Error generating config markdown for {connector_name}: {e!s}"
            )
        ]


def _validate_config(connector_name: str, config: dict[str, Any]) -> list[types.TextContent]:
    """Validate a connector configuration."""
    try:
        source = get_source(connector_name)
        spec = source.config_spec

        hardcoded_secrets = _detect_hardcoded_secrets(config, spec)
        if hardcoded_secrets:
            error_msg = (
                f"Configuration contains hardcoded secrets in fields: "
                f"{', '.join(hardcoded_secrets)}\n"
            )
            error_msg += "Please use environment variables instead. For example:\n"
            for field in hardcoded_secrets:
                error_msg += (
                    f"- Set {field.upper()} environment variable and use "
                    f"${{{field.upper()}}} in config\n"
                )
            return [types.TextContent(type="text", text=error_msg)]

        source.validate_config(config)

        return [
            types.TextContent(type="text", text=f"Configuration for {connector_name} is valid!")
        ]
    except Exception as e:
        return [
            types.TextContent(
                type="text", text=f"Configuration validation failed for {connector_name}: {e!s}"
            )
        ]


def _run_sync(connector_name: str, config: dict[str, Any]) -> list[types.TextContent]:
    """Run a sync from a source connector to the default DuckDB cache."""
    try:
        validation_result = _validate_config(connector_name, config)
        if "valid" not in validation_result[0].text.lower():
            return validation_result

        source = get_source(connector_name, config=config)
        cache = get_default_cache()

        source.read(cache=cache)

        summary = f"Sync completed for {connector_name}!\n\n"
        summary += "Data written to default DuckDB cache\n"

        return [types.TextContent(type="text", text=summary)]
    except Exception as e:
        return [types.TextContent(type="text", text=f"Sync failed for {connector_name}: {e!s}")]


class PyAirbyteServer:
    """PyAirbyte MCP Server wrapper."""

    def __init__(self) -> None:
        """Initialize the PyAirbyte MCP Server."""
        self.app = app

    async def run_stdio(self) -> None:
        """Run the server with stdio transport."""
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await self.app.run(
                read_stream,
                write_stream,
                types.InitializationOptions(server_name="pyairbyte-mcp", server_version="1.0.0"),
            )


async def main() -> None:
    """Main entry point for the MCP server."""
    server = PyAirbyteServer()
    await server.run_stdio()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
