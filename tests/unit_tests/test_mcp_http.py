# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for stateless HTTP extension declarations and UI tool visibility."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
from mcp import types
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamable_http_client
from starlette.middleware import Middleware

from airbyte.constants import MCP_EXTENSIONS_HEADER
from airbyte.mcp.server import app
from fastmcp_extensions import CapabilityTokenMiddleware
from fastmcp_extensions import DEFAULT_EXTENSIONS_HEADER


UI_EXTENSION = {"io.modelcontextprotocol/ui": {}}
UI_TOOL_NAMES = {
    "show_connectors_list",
    "show_workspace_sync_status",
    "show_connection_sync_history",
}
REPO_ROOT = Path(__file__).parents[2]


def test_mcp_extensions_header_matches_fastmcp_extensions() -> None:
    """Keep the public Airbyte header constant aligned with the shared default."""
    assert MCP_EXTENSIONS_HEADER == DEFAULT_EXTENSIONS_HEADER


def test_importing_airbyte_does_not_load_mcp_dependencies() -> None:
    """Keep importing `airbyte` free of optional MCP dependencies."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import airbyte, sys; print(*sys.modules, sep='\\n')",
        ],
        capture_output=True,
        check=True,
        text=True,
    )
    loaded_modules = set(result.stdout.splitlines())
    forbidden_modules = {
        "fastmcp",
        "fastmcp_extensions",
        "uvicorn",
        "sentry_sdk",
        "segment",
    }
    offending_modules = sorted(
        module
        for module in loaded_modules
        if module in forbidden_modules
        or any(module.startswith(f"{prefix}.") for prefix in forbidden_modules)
    )
    assert not offending_modules, (
        "Importing `airbyte` loaded optional MCP dependencies: "
        + ", ".join(offending_modules)
    )


@pytest.fixture
def configure_client_capabilities(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Configure initialize capabilities for a test client."""
    original = types.ClientCapabilities

    def client_capabilities(**kwargs: Any) -> types.ClientCapabilities:
        kwargs["extensions"] = UI_EXTENSION
        return original(**kwargs)

    def configure(ui: bool) -> None:
        # ClientSession has no hook for declaring initialize extensions.
        monkeypatch.setattr(
            types,
            "ClientCapabilities",
            client_capabilities if ui else original,
        )

    return configure


def _http_app() -> Any:
    """Build the in-process HTTP app, mirroring `http_main`'s stateless config."""
    return app.http_app(
        path="/mcp",
        middleware=[Middleware(CapabilityTokenMiddleware)],
        transport="streamable-http",
        stateless_http=True,
    )


@asynccontextmanager
async def _stdio_session(
    *,
    ui: bool,
    configure_client_capabilities: Any,
) -> AsyncIterator[ClientSession]:
    configure_client_capabilities(ui)
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("AIRBYTE_MCP_")
    }
    parameters = StdioServerParameters(
        command=sys.executable,
        args=["-m", "airbyte.mcp.server"],
        cwd=REPO_ROOT,
        env=environment,
    )
    async with stdio_client(parameters) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            yield session


@asynccontextmanager
async def _http_session(
    *,
    headers: dict[str, str] | None,
    configure_client_capabilities: Any,
    ui: bool = False,
    wire_headers: list[list[tuple[bytes, bytes]]] | None = None,
    response_headers: list[list[tuple[bytes, bytes]]] | None = None,
) -> AsyncIterator[ClientSession]:
    configure_client_capabilities(ui)
    http_app = _http_app()

    async def capture_request(request: httpx.Request) -> None:
        if wire_headers is not None:
            wire_headers.append(request.headers.raw)

    async def capture_response(response: httpx.Response) -> None:
        if response_headers is not None:
            response_headers.append(response.headers.raw)

    async with http_app.router.lifespan_context(http_app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=http_app),
            base_url="http://testserver",
            headers=headers,
            event_hooks={
                "request": [capture_request],
                "response": [capture_response],
            },
        ) as http_client:
            async with streamable_http_client(
                "http://testserver/mcp",
                http_client=http_client,
            ) as (read_stream, write_stream, _):
                async with ClientSession(read_stream, write_stream) as session:
                    yield session


async def _tool_names(session_factory: Any) -> set[str]:
    async with session_factory as session:
        await session.initialize()
        result = await session.list_tools()
        return {tool.name for tool in result.tools}


@pytest.mark.parametrize("ui", [True, False], ids=["ui-client", "non-ui-client"])
def test_stdio_ui_tool_visibility(
    ui: bool,
    configure_client_capabilities: Any,
) -> None:
    """Stdio uses initialize capabilities for UI tool visibility."""
    names = asyncio.run(
        _tool_names(
            _stdio_session(
                ui=ui,
                configure_client_capabilities=configure_client_capabilities,
            )
        )
    )
    if ui:
        assert UI_TOOL_NAMES <= names
    else:
        assert UI_TOOL_NAMES.isdisjoint(names)


def test_stateless_http_initialize_capabilities_survive_via_session_token(
    configure_client_capabilities: Any,
) -> None:
    """Stateless HTTP carries initialize extensions through the session token."""
    names = asyncio.run(
        _tool_names(
            _http_session(
                headers={},
                ui=True,
                configure_client_capabilities=configure_client_capabilities,
            )
        )
    )
    assert UI_TOOL_NAMES <= names


def test_stateless_http_without_extensions_mints_no_session_token(
    configure_client_capabilities: Any,
) -> None:
    """Clients without extensions receive no session token or UI tools."""
    response_headers: list[list[tuple[bytes, bytes]]] = []
    names = asyncio.run(
        _tool_names(
            _http_session(
                headers={},
                configure_client_capabilities=configure_client_capabilities,
                response_headers=response_headers,
            )
        )
    )
    assert UI_TOOL_NAMES.isdisjoint(names)
    assert all(
        header_name.lower() != b"mcp-session-id"
        for headers in response_headers
        for header_name, _ in headers
    )


@pytest.mark.parametrize(
    "headers, expected",
    [
        pytest.param(
            {"x-McP-ExTeNsIoNs": "io.modelcontextprotocol/ui"},
            True,
            id="ui-header",
        ),
        pytest.param({}, False, id="no-header"),
        pytest.param({"X-MCP-Extensions": " "}, False, id="blank-header"),
    ],
)
def test_stateless_http_ui_tool_visibility(
    headers: dict[str, str],
    expected: bool,
    configure_client_capabilities: Any,
) -> None:
    """Stateless HTTP uses the extension header without failing open."""
    wire_headers: list[list[tuple[bytes, bytes]]] = []
    names = asyncio.run(
        _tool_names(
            _http_session(
                headers=headers,
                configure_client_capabilities=configure_client_capabilities,
                wire_headers=wire_headers,
            )
        )
    )
    if expected:
        assert UI_TOOL_NAMES <= names
        assert any(
            header_name.lower() == b"x-mcp-extensions"
            for request_headers in wire_headers
            for header_name, _ in request_headers
        )
    else:
        assert UI_TOOL_NAMES.isdisjoint(names)
