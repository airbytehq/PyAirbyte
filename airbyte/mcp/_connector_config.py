# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Local connector config MCP operations."""

from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field

from airbyte._util.openai import try_get_openai_client


def is_llm_api_available() -> tuple[bool, str | None]:
    """Check if LLM-assisted generation is enabled.

    Returns a tuple (is_available: bool, base_url: str | None).
    """
    client = try_get_openai_client()
    if client is None:
        return False, None

    return True, str(client.base_url) or None


def generate_connector_config_template(
    connector_name: Annotated[
        str,
        Field(description="The name of the connector (e.g., 'source-faker')."),
    ],
    llm_guidance: Annotated[
        str | None,
        Field(
            description="Optional guidance for LLM-assisted generation. "
            "Ignored if `llm_assisted=False`. "
            "When provided, it will be used to guide the template generation. "
            "Helpful guidance includes expected authentication methods, "
            "data formats, and any specific requirements that would be needed "
            "for the connector. Secrets should never be included in this guidance.",
            default=None,
        ),
    ] = None,
    llm_assisted: Annotated[
        bool | None,
        Field(
            description="Whether to use LLM-assisted generation for the template. "
            "If True, the template will be guided by the LLM, failing if no LLM is available. "
            "If False, a basic template will be generated without LLM assistance. "
            "If omitted or None, LLM assistance will be used only if available.",
            default=None,
        ),
    ] = None,
) -> dict[str, str]:
    """Generate a connector configuration template.

    This tool creates a basic configuration template for a given connector.
    The template includes common fields and can be customized further.

    Returns a dictionary with 'template' key containing the generated config template.
    """


def register_connector_config_tools(app: FastMCP) -> None:
    """Register development tools with the FastMCP app."""
    app.tool(is_llm_api_available)
    app.tool(generate_connector_config_template)
