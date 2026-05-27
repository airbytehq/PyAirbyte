# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Kapa knowledge source MCP operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

import requests
from fastmcp_extensions import mcp_tool, register_mcp_tools
from pydantic import Field

from airbyte.secrets.util import try_get_secret


if TYPE_CHECKING:
    from fastmcp import FastMCP


_KAPA_RETRIEVAL_URL_TEMPLATE = "https://api.kapa.ai/query/v1/projects/{project_id}/retrieval/"
_KAPA_TIMEOUT_SECONDS = 30.0
_KAPA_API_KEY_ENV_VAR = "KAPA_API_KEY"
_KAPA_DOCS_MCP_BEARER_TOKEN_ENV_VAR = "KAPA_DOCS_MCP_BEARER_TOKEN"
_KAPA_BEARER_TOKEN_ENV_VAR = "KAPA_BEARER_TOKEN"
_KAPA_CREDENTIAL_ENV_VARS = (
    _KAPA_API_KEY_ENV_VAR,
    _KAPA_DOCS_MCP_BEARER_TOKEN_ENV_VAR,
    _KAPA_BEARER_TOKEN_ENV_VAR,
)
_KAPA_PROJECT_ID_ENV_VAR = "KAPA_PROJECT_ID"
_KAPA_INTEGRATION_ID_ENV_VAR = "KAPA_INTEGRATION_ID"


def _kapa_config_value(name: str) -> str:
    value = try_get_secret(name)
    if value is None:
        return ""

    return str(value).strip()


def _kapa_credentials_configured() -> bool:
    return any(_kapa_config_value(name) for name in _KAPA_CREDENTIAL_ENV_VARS)


def _kapa_project_configured() -> bool:
    return bool(_kapa_config_value(_KAPA_PROJECT_ID_ENV_VAR))


def _kapa_auth_headers() -> dict[str, str]:
    api_key = _kapa_config_value(_KAPA_API_KEY_ENV_VAR)
    if api_key:
        return {"X-API-KEY": api_key}

    bearer_token = _kapa_config_value(_KAPA_DOCS_MCP_BEARER_TOKEN_ENV_VAR) or _kapa_config_value(
        _KAPA_BEARER_TOKEN_ENV_VAR
    )
    if bearer_token:
        return {"Authorization": f"Bearer {bearer_token}"}

    raise ValueError(
        "Kapa docs search is not configured. Set KAPA_API_KEY, "
        "KAPA_DOCS_MCP_BEARER_TOKEN, or KAPA_BEARER_TOKEN."
    )


def _kapa_retrieval_url() -> str:
    project_id = _kapa_config_value(_KAPA_PROJECT_ID_ENV_VAR)
    if not project_id:
        raise ValueError("KAPA_PROJECT_ID must be set to use Kapa docs search.")

    return _KAPA_RETRIEVAL_URL_TEMPLATE.format(project_id=project_id)


def _kapa_request_payload(query: str) -> dict[str, str]:
    payload = {"query": query}
    integration_id = _kapa_config_value(_KAPA_INTEGRATION_ID_ENV_VAR)
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
    if _kapa_credentials_configured() and _kapa_project_configured():
        register_mcp_tools(app, mcp_module=__name__)
