# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Proxy the Airbyte knowledge MCP server in hosted HTTP mode."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import httpx
from fastmcp import Client, FastMCP
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.server import create_proxy

from airbyte.constants import is_hosted_mcp_mode
from airbyte.secrets.util import try_get_secret


if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

KAPA_DOCS_MCP_BEARER_TOKEN_ENV_VAR = "KAPA_DOCS_MCP_BEARER_TOKEN"
KAPA_MCP_SERVER_URL_ENV_VAR = "KAPA_MCP_SERVER_URL"
DEFAULT_KAPA_MCP_SERVER_URL = "https://airbyte.mcp.kapa.ai"


def _get_configured_value(secret_name: str) -> str:
    secret = try_get_secret(secret_name)
    return str(secret).strip() if secret is not None else ""


def _kapa_http_client_factory(token: str) -> Callable[..., httpx.AsyncClient]:
    def create_http_client(
        headers: dict[str, str] | None = None,
        timeout: httpx.Timeout | None = None,
        auth: object | None = None,
        **kwargs: object,
    ) -> httpx.AsyncClient:
        del headers, auth, kwargs
        return httpx.AsyncClient(
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout or httpx.Timeout(30.0, read=300.0),
            follow_redirects=True,
        )

    return create_http_client


def mount_kapa_knowledge_proxy(app: FastMCP) -> bool:
    """Mount the Kapa knowledge proxy only in hosted mode with a bearer token."""
    if not is_hosted_mcp_mode():
        logger.info("Kapa knowledge proxy is disabled outside hosted MCP mode.")
        return False

    token = _get_configured_value(KAPA_DOCS_MCP_BEARER_TOKEN_ENV_VAR)
    if not token:
        logger.info("Kapa knowledge proxy is disabled because no bearer token is configured.")
        return False

    server_url = _get_configured_value(KAPA_MCP_SERVER_URL_ENV_VAR)
    transport = StreamableHttpTransport(
        server_url or DEFAULT_KAPA_MCP_SERVER_URL,
        httpx_client_factory=_kapa_http_client_factory(token),
    )
    proxy = create_proxy(Client(transport), name="airbyte-knowledge")
    app.mount(proxy)
    return True
