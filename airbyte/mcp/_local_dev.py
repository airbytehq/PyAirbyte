# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Local development MCP operations."""

from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field

from airbyte.mcp._local_code_templates import DOCS_TEMPLATE, SCRIPT_TEMPLATE
from airbyte.sources import get_available_connectors


def generate_pyairbyte_pipeline(
    source_connector_name: Annotated[
        str,
        Field(description="The name of the source connector (e.g., 'source-faker')."),
    ],
    destination_connector_name: Annotated[
        str,
        Field(description="The name of the destination connector (e.g., 'destination-duckdb')."),
    ],
    pipeline_name: Annotated[
        str | None,
        Field(
            description="A descriptive name for the pipeline. "
            "If not provided, a default name will be generated.",
        ),
    ] = None,
) -> dict[str, str]:
    """Generate a PyAirbyte pipeline script with setup instructions.

    This tool creates a complete PyAirbyte pipeline script that extracts data from
    a source connector and loads it to a destination connector, along with setup
    instructions for running the pipeline.

    Returns a dictionary with 'code' and 'instructions' keys containing the
    generated pipeline script and setup instructions respectively.
    """
    source_short_name = source_connector_name.replace("source-", "")
    destination_short_name = destination_connector_name.replace("destination-", "")
    if not pipeline_name:
        pipeline_name = f"{source_short_name}_to_{destination_short_name}_pipeline"

    pipeline_id = pipeline_name.lower().replace(" ", "_").replace("'", "")
    available_connectors: list[str] = get_available_connectors()
    if source_connector_name not in available_connectors:
        return {
            "error": (
                f"Source connector '{source_connector_name}' not found. "
                f"Available connectors: {', '.join(sorted(available_connectors))}"
            )
        }

    if destination_connector_name not in available_connectors:
        return {
            "error": (
                f"Destination connector '{destination_connector_name}' not found. "
                f"Available connectors: {', '.join(sorted(available_connectors))}"
            )
        }

    pipeline_code: str = SCRIPT_TEMPLATE.format(
        source_connector_name=source_connector_name,
        source_config_dict={},  # Placeholder for source config
        destination_connector_name=destination_connector_name,
        destination_config_dict={},  # Placeholder for destination config
    )

    setup_instructions: str = DOCS_TEMPLATE.format(
        source_connector_name=source_short_name,
        destination_connector_name=destination_short_name,
        pipeline_id=pipeline_id,
        source_short_name=source_short_name,
        dest_short_name=destination_short_name,
    )

    return {
        "code": pipeline_code,
        "instructions": setup_instructions,
        "filename": f"{pipeline_id}.py",
    }


def register_local_dev_tools(app: FastMCP) -> None:
    """Register development tools with the FastMCP app."""
    app.tool(generate_pyairbyte_pipeline)
