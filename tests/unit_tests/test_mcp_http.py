# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for stateless HTTP extension declarations and UI tool visibility."""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
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

from airbyte.mcp import _tool_utils
from airbyte.mcp._capability_tokens import (
    CapabilityTokenMiddleware,
    _MAX_INITIALIZE_BODY_BYTES,
    decode_capability_token,
    encode_capability_token,
)
from airbyte.mcp.server import app


UI_EXTENSION = {"io.modelcontextprotocol/ui": {}}
UI_TOOL_NAMES = {
    "show_connectors_list",
    "show_workspace_sync_status",
    "show_connection_sync_history",
}
REPO_ROOT = Path(__file__).parents[2]


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


@pytest.mark.parametrize(
    "extension_ids, expected",
    [
        pytest.param(
            {"io.modelcontextprotocol/ui"},
            {"io.modelcontextprotocol/ui"},
            id="single-extension",
        ),
        pytest.param(
            {"io.modelcontextprotocol/ui", "io.modelcontextprotocol/roots"},
            {"io.modelcontextprotocol/ui", "io.modelcontextprotocol/roots"},
            id="multiple-extensions",
        ),
        pytest.param(set(), set(), id="empty"),
        pytest.param({"foo bar"}, set(), id="whitespace-containing-id"),
        pytest.param({" \t\n"}, set(), id="all-whitespace-ids"),
    ],
)
def test_capability_token_round_trip(
    extension_ids: set[str],
    expected: set[str],
) -> None:
    """Capability tokens round-trip extension IDs."""
    token = encode_capability_token(extension_ids)
    assert decode_capability_token(token) == expected


@pytest.mark.parametrize(
    "token",
    [
        pytest.param("garbage", id="garbage"),
        pytest.param(
            f"{uuid.uuid4().hex}.not-base64!",
            id="non-base64",
        ),
        pytest.param(
            f"00000000000000000000000000000000.{'aW8ubW9kZWxjb250ZXh0cHJvdG9jb2wvdWk'}",
            id="non-uuid4-with-valid-payload",
        ),
        pytest.param("00000000-0000-0000-0000-000000000000", id="uuid-only"),
        pytest.param("", id="empty"),
    ],
)
def test_capability_token_decode_fails_closed(token: str) -> None:
    """Malformed capability tokens do not expose extensions."""
    assert decode_capability_token(token) == set()


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
            {"x-mcp-extensions": "io.modelcontextprotocol/ui"},
            True,
            id="single-value",
        ),
        pytest.param(
            {"x-mcp-extensions": "other, io.modelcontextprotocol/ui, another"},
            True,
            id="comma-separated",
        ),
        pytest.param(
            {"x-mcp-extensions": "other io.modelcontextprotocol/ui another"},
            True,
            id="whitespace-separated",
        ),
        pytest.param(
            {"x-mcp-extensions": "other"},
            False,
            id="unknown-extension",
        ),
        pytest.param({"x-mcp-extensions": " , "}, False, id="blank"),
        pytest.param({}, False, id="missing-header"),
    ],
)
def test_client_declared_extensions_from_headers(
    headers: dict[str, str],
    expected: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Header extension parsing tolerates common HTTP formatting.

    `get_http_headers()` lowercases header names, so the parser looks up the
    lowercased key; comma- and whitespace-separated values are accepted.
    End-to-end casing is covered by the HTTP visibility test.
    """
    monkeypatch.setattr(_tool_utils, "get_http_headers", lambda **_: headers)
    extensions = _tool_utils._client_declared_extensions_from_headers()
    assert ("io.modelcontextprotocol/ui" in extensions) is expected


def test_client_declared_extensions_union_session_token_and_fallback_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Session-token and fallback-header extensions are combined."""
    token = encode_capability_token({"io.modelcontextprotocol/roots"})
    monkeypatch.setattr(
        _tool_utils,
        "get_http_headers",
        lambda **_: {
            "mcp-session-id": token,
            "x-mcp-extensions": "io.modelcontextprotocol/ui",
        },
    )

    assert _tool_utils._client_declared_extensions_from_headers() == {
        "io.modelcontextprotocol/roots",
        "io.modelcontextprotocol/ui",
    }


async def _capture_middleware_request(
    messages: list[dict[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    received: list[dict[str, object]] = []
    responses: list[dict[str, object]] = []
    message_index = 0

    async def receive() -> Any:
        nonlocal message_index
        message = messages[message_index]
        message_index += 1
        return message

    async def send(message: dict[str, object]) -> None:
        responses.append(message)

    async def app(scope: Any, receive: Any, send: Any) -> None:
        del scope
        while True:
            message = await receive()
            received.append(message)
            if message["type"] == "http.disconnect" or not message.get("more_body"):
                break
        await send({"type": "http.response.start", "status": 200, "headers": []})

    middleware = CapabilityTokenMiddleware(app)
    await middleware(
        {"type": "http", "method": "POST"},
        receive,
        send,
    )
    return received, responses


def test_capability_token_middleware_forwards_large_body_without_token() -> None:
    """Bodies larger than the sniffing cap reach the app intact."""
    body = b"x" * (_MAX_INITIALIZE_BODY_BYTES + 1)
    received, responses = asyncio.run(
        _capture_middleware_request([
            {"type": "http.request", "body": body[:1024], "more_body": True},
            {"type": "http.request", "body": body[1024:], "more_body": False},
        ])
    )

    assert b"".join(message.get("body", b"") for message in received) == body
    assert all(
        header_name != b"mcp-session-id"
        for response in responses
        for header_name, _ in response.get("headers", [])
    )


def test_capability_token_middleware_forwards_disconnect_without_token() -> None:
    """A disconnected request is forwarded without attempting token parsing."""
    received, responses = asyncio.run(
        _capture_middleware_request([
            {"type": "http.request", "body": b'{"jsonrpc":', "more_body": True},
            {"type": "http.disconnect"},
        ])
    )

    assert received[-1]["type"] == "http.disconnect"
    assert all(
        header_name != b"mcp-session-id"
        for response in responses
        for header_name, _ in response.get("headers", [])
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
