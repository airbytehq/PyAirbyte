# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the hosted Airbyte knowledge MCP proxy."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import httpx
import pytest
from fastmcp import Client, FastMCP
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.server.providers.proxy import FastMCPProxy

from airbyte.mcp import kapa


@pytest.fixture(autouse=True)
def clear_kapa_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(kapa.KAPA_DOCS_MCP_BEARER_TOKEN_ENV_VAR, raising=False)
    monkeypatch.delenv(kapa.KAPA_MCP_SERVER_URL_ENV_VAR, raising=False)


def test_mount_is_disabled_outside_hosted_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(kapa.KAPA_DOCS_MCP_BEARER_TOKEN_ENV_VAR, "dummy-token")
    monkeypatch.setattr(kapa, "is_hosted_mcp_mode", lambda: False)
    app = MagicMock(spec=FastMCP)

    assert kapa.mount_kapa_knowledge_proxy(app) is False
    app.mount.assert_not_called()


@pytest.mark.parametrize("token", [None, "  \t "])
def test_mount_is_disabled_without_a_nonempty_token(
    monkeypatch: pytest.MonkeyPatch,
    token: str | None,
) -> None:
    monkeypatch.setattr(kapa, "is_hosted_mcp_mode", lambda: True)
    if token is not None:
        monkeypatch.setenv(kapa.KAPA_DOCS_MCP_BEARER_TOKEN_ENV_VAR, token)
    app = MagicMock(spec=FastMCP)

    assert kapa.mount_kapa_knowledge_proxy(app) is False
    app.mount.assert_not_called()


def test_mount_creates_proxy_with_custom_url_and_no_namespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server_url = "https://kapa.example.test/mcp"
    monkeypatch.setattr(kapa, "is_hosted_mcp_mode", lambda: True)
    monkeypatch.setenv(kapa.KAPA_DOCS_MCP_BEARER_TOKEN_ENV_VAR, "dummy-token")
    monkeypatch.setenv(kapa.KAPA_MCP_SERVER_URL_ENV_VAR, server_url)
    app = MagicMock(spec=FastMCP)

    with patch(
        "airbyte.mcp.kapa.StreamableHttpTransport",
        wraps=StreamableHttpTransport,
    ) as transport_factory:
        assert kapa.mount_kapa_knowledge_proxy(app) is True

    transport_factory.assert_called_once()
    assert transport_factory.call_args.args == (server_url,)
    proxy = app.mount.call_args.args[0]
    assert isinstance(proxy, FastMCPProxy)
    assert proxy.name == "airbyte-knowledge"
    app.mount.assert_called_once_with(proxy)


def test_kapa_http_client_factory_does_not_forward_request_credentials() -> None:
    factory = kapa._kapa_http_client_factory("kapa-token")
    caller_auth = httpx.BasicAuth("caller", "caller-password")
    caller_headers = {
        "authorization": "Bearer caller-token",
        "x-airbyte-workspace-id": "workspace-id",
    }

    async def inspect_client() -> None:
        async with factory(headers=caller_headers, auth=caller_auth) as client:
            assert client.headers["Authorization"] == "Bearer kapa-token"
            assert "caller-token" not in str(client.headers)
            assert "x-airbyte-workspace-id" not in client.headers
            assert client.auth is not caller_auth
            assert client.auth is None

    asyncio.run(inspect_client())


def test_mounted_proxy_lists_and_calls_upstream_tool_unprefixed() -> None:
    upstream = FastMCP("kapa-upstream")

    @upstream.tool()
    def search_airbyte_knowledge_sources(query: str) -> str:
        return f"results for {query}"

    parent = FastMCP("parent")
    parent.mount(kapa.create_proxy(Client(upstream), name="airbyte-knowledge"))

    async def round_trip() -> None:
        async with Client(parent) as client:
            tools = await client.list_tools()
            tool_names = [tool.name for tool in tools]
            assert "search_airbyte_knowledge_sources" in tool_names
            result = await client.call_tool(
                "search_airbyte_knowledge_sources",
                {"query": "replication"},
            )
            assert result.content[0].text == "results for replication"

    asyncio.run(round_trip())
