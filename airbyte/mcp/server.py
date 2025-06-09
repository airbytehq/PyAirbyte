# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

"""PyAirbyte MCP Server implementation."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

import mcp.server.stdio
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
                or field_value in os.environ
                or field_value.startswith("$")
            )

            if not is_env_var:
                hardcoded_secrets.append(field_name)

    return hardcoded_secrets


@app.tool()
def list_connectors(
    keyword_filter: str | None = None,
    connector_type_filter: str | None = None,
    language_filter: str | None = None,
) -> str:
    """List available Airbyte connectors with optional filtering."""
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

    result = f"Found {len(filtered_connectors)} connectors:\n\n"
    for connector in filtered_connectors:
        result += f"- **{connector}**\n"
        result += "\n"

    return result


@app.tool()
def get_config_spec(connector_name: str, output_format: str = "yaml") -> str:
    """Get the configuration specification for a connector in YAML or JSON format."""
    source = get_source(connector_name)
    spec = source.config_spec

    if output_format.lower() == "json":
        return json.dumps(spec, indent=2)

    return yaml.dump(spec, default_flow_style=False, indent=2)


@app.tool()
def create_config_markdown(connector_name: str) -> str:
    """Generate markdown documentation for a connector's configuration."""
    source = get_source(connector_name)

    with tempfile.NamedTemporaryFile(
        mode="w+", suffix=".yaml", delete=False, encoding="utf-8"
    ) as temp_file:
        source.print_config_spec(format="yaml", output_file=temp_file.name)

    content = Path(temp_file.name).read_text(encoding="utf-8")
    Path(temp_file.name).unlink()

    return f"# {connector_name} Configuration\n\n```yaml\n{content}\n```"


@app.tool()
def validate_config(connector_name: str, config: dict[str, Any]) -> str:
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
def run_sync(connector_name: str, config: dict[str, Any]) -> str:
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
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await self.app.run(read_stream, write_stream)  # type: ignore[func-returns-value]


async def main() -> None:
    """Main entry point for the MCP server."""
    server = PyAirbyteServer()
    await server.run_stdio()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
