# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Kapa knowledge source MCP operations."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Annotated

import requests
from fastmcp_extensions import mcp_tool, register_mcp_tools
from pydantic import Field


if TYPE_CHECKING:
    from fastmcp import FastMCP


_KAPA_RETRIEVAL_URL_TEMPLATE = "https://api.kapa.ai/query/v1/projects/{project_id}/retrieval/"
_KAPA_TIMEOUT_SECONDS = 30.0


def _kapa_credentials_configured() -> bool:
    return bool(
        os.getenv("KAPA_API_KEY")
        or os.getenv("KAPA_DOCS_MCP_BEARER_TOKEN")
        or os.getenv("KAPA_BEARER_TOKEN")
    )


def _kapa_auth_headers() -> dict[str, str]:
    api_key = (os.getenv("KAPA_API_KEY") or "").strip()
    if api_key:
        return {"X-API-KEY": api_key}

    bearer_token = (
        (os.getenv("KAPA_DOCS_MCP_BEARER_TOKEN") or os.getenv("KAPA_BEARER_TOKEN")) or ""
    ).strip()
    if bearer_token:
        return {"Authorization": f"Bearer {bearer_token}"}

    raise ValueError(
        "Kapa docs search is not configured. Set KAPA_API_KEY, "
        "KAPA_DOCS_MCP_BEARER_TOKEN, or KAPA_BEARER_TOKEN."
    )


def _kapa_retrieval_url() -> str:
    project_id = (os.getenv("KAPA_PROJECT_ID") or "").strip()
    if not project_id:
        raise ValueError("KAPA_PROJECT_ID must be set to use Kapa docs search.")

    return _KAPA_RETRIEVAL_URL_TEMPLATE.format(project_id=project_id)


def _kapa_request_payload(query: str) -> dict[str, str]:
    payload = {"query": query}
    integration_id = (os.getenv("KAPA_INTEGRATION_ID") or "").strip()
    if integration_id:
        payload["integration_id"] = integration_id
    return payload


def _normalize_kapa_results(data: object) -> list[dict[str, str]]:
    if not isinstance(data, list):
        raise TypeError("Kapa retrieval API returned an unexpected response shape.")

    results: list[dict[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            raise TypeError("Kapa retrieval API returned an unexpected result item.")

        source_url = item.get("source_url")
        content = item.get("content")
        if not isinstance(source_url, str) or not isinstance(content, str):
            raise TypeError(
                "Kapa retrieval API returned a result without source_url and content strings."
            )

        results.append({"source_url": source_url, "content": content})

    return results


@mcp_tool(
    read_only=True,
    idempotent=True,
)
def search_airbyte_knowledge_sources(
    query: Annotated[
        str,
        Field(
            description=(
                "A single, well-formed natural-language query. Must be a complete sentence."
            ),
        ),
    ],
) -> list[dict[str, str]]:
    """Search Airbyte knowledge sources."""
    response = requests.post(
        _kapa_retrieval_url(),
        headers=_kapa_auth_headers(),
        json=_kapa_request_payload(query),
        timeout=_KAPA_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return _normalize_kapa_results(response.json())


def register_kapa_tools(app: FastMCP) -> None:
    """Register Kapa tools with the FastMCP app."""
    if _kapa_credentials_configured():
        register_mcp_tools(app, mcp_module=__name__)
