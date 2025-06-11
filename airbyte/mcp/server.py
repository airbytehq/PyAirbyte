# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
# ruff: noqa: ANN001, ANN201
# mypy: disable-error-code=no-untyped-def

"""Experimental MCP (Model Context Protocol) server for PyAirbyte connector management.

> **NOTE:**
> This MCP server implementation is experimental and may change without notice between minor
> versions of PyAirbyte. The API may be modified or entirely refactored in future versions.


This module provides a Model Context Protocol server that enables AI assistants and other MCP
clients to interact with PyAirbyte connectors. The server exposes tools for listing connectors,
retrieving configuration specifications, validating configurations, and running sync operations.


Start the MCP server with stdio transport:

```python
from airbyte.mcp.server import PyAirbyteServer
import asyncio


async def main():
    server = PyAirbyteServer()
    await server.run_stdio()


asyncio.run(main())
```


- [Model Context Protocol Documentation](https://modelcontextprotocol.io/)
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)

You can help improve this experimental feature by reporting issues and providing feedback in our
[GitHub issue tracker](https://github.com/airbytehq/pyairbyte/issues).
"""

from __future__ import annotations

import json
import os
from typing import Any

import yaml
from mcp.server.fastmcp import FastMCP

from airbyte import get_source
from airbyte.caches.util import get_default_cache
from airbyte.sources import get_available_connectors


app = FastMCP("airbyte-mcp")


def _detect_hardcoded_secrets(config: dict[str, Any], spec: dict[str, Any]) -> list[str]:
    """Detect hardcoded secrets in config that should be environment variables."""
    hardcoded_secrets = []
    properties = spec.get("properties", {})

    for field_name, field_value in config.items():
        if field_name not in properties:
            continue

        field_spec = properties[field_name]

        is_secret = (
            field_spec.get("writeOnly") is True
            or field_spec.get("format") == "password"
            or "password" in field_name.lower()
            or "secret" in field_name.lower()
            or "token" in field_name.lower()
            or "key" in field_name.lower()
        )

        if is_secret and isinstance(field_value, str):
            is_env_var = (
                (field_value.startswith("${") and field_value.endswith("}"))
                or os.environ.get(field_value) is not None
                or field_value.startswith("$")
            )

            if not is_env_var:
                hardcoded_secrets.append(field_name)

    return hardcoded_secrets


@app.tool()
def list_connectors(
    keyword_filter=None,
    connector_type_filter=None,
    language_filter=None,
    output_format="markdown",
):
    """List available Airbyte connectors with optional filtering.

    Args:
        keyword_filter: Filter connectors by keyword (case-insensitive substring match).
        connector_type_filter: Filter connectors by type (case-insensitive substring match).
        language_filter: Filter connectors by implementation language (currently unused).
        output_format: Output format, either "markdown" or "json".

    Returns:
        Formatted string containing the list of connectors in the specified format.
    """
    connectors: list[str] = get_available_connectors()

    filtered_connectors: list[str] = []
    for connector in connectors:
        if keyword_filter and keyword_filter.lower() not in connector.lower():
            continue

        if connector_type_filter and connector_type_filter.lower() not in connector.lower():
            continue

        if language_filter:
            pass

        filtered_connectors.append(connector)

    if output_format.lower() == "json":
        return json.dumps(
            {"count": len(filtered_connectors), "connectors": filtered_connectors}, indent=2
        )

    result = f"Found {len(filtered_connectors)} connectors:\n\n"
    for connector in filtered_connectors:
        result += f"- **{connector}**\n"
        result += "\n"

    return result


@app.tool()
def get_config_spec(connector_name, output_format="yaml"):
    """Get the configuration specification for a connector in YAML or JSON format."""
    source = get_source(connector_name)
    spec = source.config_spec

    if output_format.lower() == "json":
        return json.dumps(spec, indent=2)

    return yaml.dump(spec, default_flow_style=False, indent=2)


@app.tool()
def validate_config(connector_name, config):
    """Validate a connector configuration, ensuring no hardcoded secrets."""
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
        return error_msg

    source.validate_config(config)
    return f"Configuration for {connector_name} is valid!"


@app.tool()
def run_sync(connector_name, config):
    """Run a sync from a source connector to the default DuckDB cache."""
    validation_result = validate_config(connector_name, config)
    if "valid" not in validation_result.lower():
        return validation_result

    source = get_source(connector_name, config=config)
    cache = get_default_cache()

    source.read(cache=cache)

    summary = f"Sync completed for {connector_name}!\n\n"
    summary += "Data written to default DuckDB cache\n"
    return summary


class PyAirbyteServer:
    """PyAirbyte MCP Server wrapper."""

    def __init__(self) -> None:
        """Initialize the PyAirbyte MCP Server."""
        self.app = app

    async def run_stdio(self) -> None:
        """Run the server with stdio transport."""
        await self.app.run_stdio_async()


async def main() -> None:
    """Main entry point for the MCP server."""
    server = PyAirbyteServer()
    await server.run_stdio()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
