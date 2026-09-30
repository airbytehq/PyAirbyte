# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Search Airbyte knowledge sources through Kapa's Retrieval API."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Annotated

import requests
from fastmcp_extensions import mcp_tool, register_mcp_tools
from pydantic import Field

from airbyte.constants import is_hosted_mcp_mode
from airbyte.secrets.util import try_get_secret


if TYPE_CHECKING:
    from fastmcp import FastMCP


__all__: list[str] = []

logger = logging.getLogger(__name__)

KAPA_API_KEY_ENV_VAR = "KAPA_API_KEY"
KAPA_RETRIEVAL_API_URL_ENV_VAR = "KAPA_RETRIEVAL_API_URL"
_KAPA_TIMEOUT_SECONDS = 30.0


def _get_configured_value(name: str) -> str:
    secret = try_get_secret(name)
    return str(secret).strip() if secret is not None else ""


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
)
def search_airbyte_knowledge_sources(
    query: Annotated[
        str,
        Field(
            description=(
                "A single, well-formed natural-language query. " "Must be a complete sentence."
            )
        ),
    ],
) -> list[dict[str, str]]:
    """Search Airbyte knowledge sources.

    Search Airbyte's documentation and other knowledge sources and return the most
    relevant chunks, each with its source URL and markdown content.

    Sources include documentation, the website, OpenAPI specifications, YouTube, and GitHub.
    """
    api_key = _get_configured_value(KAPA_API_KEY_ENV_VAR)
    url = _get_configured_value(KAPA_RETRIEVAL_API_URL_ENV_VAR)
    response = requests.post(
        url,
        headers={"X-API-KEY": api_key},
        json={"query": query},
        timeout=_KAPA_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return [
        {"source_url": item["source_url"], "content": item["content"]} for item in response.json()
    ]


def register_kapa_tools(app: FastMCP) -> bool:
    """Register the Kapa knowledge search tool for configured hosted MCP servers."""
    if not is_hosted_mcp_mode():
        logger.info("Kapa knowledge search is disabled outside hosted MCP mode.")
        return False

    api_key = _get_configured_value(KAPA_API_KEY_ENV_VAR)
    retrieval_api_url = _get_configured_value(KAPA_RETRIEVAL_API_URL_ENV_VAR)
    if not api_key or not retrieval_api_url:
        logger.info(
            "Kapa knowledge search is disabled because the API key or Retrieval API URL "
            "is not configured."
        )
        return False

    register_mcp_tools(app, mcp_module=__name__)
    return True
