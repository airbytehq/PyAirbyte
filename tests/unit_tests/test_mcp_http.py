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

import httpx2
import mcp_types as types
import pytest
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamable_http_client
from starlette.middleware import Middleware

from airbyte.constants import (
    MCP_ALLOW_DESTRUCTIVE_TOOLS_ENV_VAR,
    MCP_ALLOW_DESTRUCTIVE_TOOLS_HEADER,
    MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR,
    MCP_ALLOW_EXTERNAL_ACCESS_HEADER,
    MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR,
    MCP_ALLOW_PIPELINE_CHANGES_HEADER,
    MCP_DOMAINS_DISABLED_ENV_VAR,
    MCP_DOMAINS_ENV_VAR,
    MCP_EXTENSIONS_HEADER,
    MCP_INSIDERS_ENV_VAR,
    MCP_INSIDERS_HEADER,
    MCP_READONLY_MODE_ENV_VAR,
)
from airbyte.mcp.server import app
from fastmcp_extensions import CapabilityTokenMiddleware
from fastmcp_extensions import DEFAULT_EXTENSIONS_HEADER
from fastmcp_extensions.capability_tokens import decode_capability_token


UI_EXTENSION = {"io.modelcontextprotocol/ui": {}}
UI_TOOL_NAMES = {
    "show_connector_definitions",
    "show_workspace_sync_status",
    "show_connection_sync_history",
}
REPO_ROOT = Path(__file__).parents[2]


def test_mcp_types_aliases_mcp_types_package() -> None:
    """`mcp.types` and `mcp_types` expose the same capability class in mcp 2."""
    import mcp.types as shim_types

    assert shim_types.ClientCapabilities is types.ClientCapabilities


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
        # The dev-only logfire extra auto-loads an OTel SDK Pydantic plugin.
        # Check PyAirbyte imports without this unrelated third-party hook.
        env={**os.environ, "PYDANTIC_DISABLE_PLUGINS": "logfire-plugin"},
    )
    loaded_modules = set(result.stdout.splitlines())
    forbidden_modules = {
        "opentelemetry.sdk",
        "opentelemetry.exporter",
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
    url: str = "http://testserver/mcp",
    ui: bool = False,
    wire_headers: list[list[tuple[bytes, bytes]]] | None = None,
    response_headers: list[list[tuple[bytes, bytes]]] | None = None,
) -> AsyncIterator[ClientSession]:
    configure_client_capabilities(ui)
    http_app = _http_app()

    async def capture_request(request: httpx2.Request) -> None:
        if wire_headers is not None:
            wire_headers.append(request.headers.raw)

    async def capture_response(response: httpx2.Response) -> None:
        if response_headers is not None:
            response_headers.append(response.headers.raw)

    async with http_app.router.lifespan_context(http_app):
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=http_app),
            base_url="http://testserver",
            headers=headers,
            event_hooks={
                "request": [capture_request],
                "response": [capture_response],
            },
        ) as http_client:
            async with streamable_http_client(
                url,
                http_client=http_client,
            ) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    yield session


async def _tool_names(session_factory: Any) -> set[str]:
    async with session_factory as session:
        await session.initialize()
        result = await session.list_tools()
        return {tool.name for tool in result.tools}


def _http_tool_names(
    *,
    headers: dict[str, str] | None,
    configure_client_capabilities: Any,
    url: str = "http://testserver/mcp",
) -> set[str]:
    return asyncio.run(
        _tool_names(
            _http_session(
                headers=headers,
                configure_client_capabilities=configure_client_capabilities,
                url=url,
            )
        )
    )


@pytest.mark.parametrize(
    ("query_param", "header", "header_value", "tool_name", "visible"),
    [
        pytest.param(
            "allow-pipeline-changes=0",
            MCP_ALLOW_PIPELINE_CHANGES_HEADER,
            "0",
            "create_connection",
            False,
            id="pipeline-changes",
        ),
        pytest.param(
            "allow-destructive-tools=false",
            MCP_ALLOW_DESTRUCTIVE_TOOLS_HEADER,
            "false",
            "permanently_delete_connection",
            False,
            id="destructive-tools",
        ),
        pytest.param(
            "allow-external-access=0",
            MCP_ALLOW_EXTERNAL_ACCESS_HEADER,
            "0",
            "execute_external_api_query",
            False,
            id="external-access",
        ),
        pytest.param(
            "insiders=1",
            MCP_INSIDERS_HEADER,
            "1",
            "create_connector_builder_project",
            True,
            id="insiders",
        ),
    ],
)
def test_http_url_policy_params_match_headers(
    monkeypatch: pytest.MonkeyPatch,
    configure_client_capabilities: Any,
    query_param: str,
    header: str,
    header_value: str,
    tool_name: str,
    visible: bool,
) -> None:
    """URL policy parameters have the same tool-list effects as request headers."""
    for env_var in (
        MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR,
        MCP_ALLOW_DESTRUCTIVE_TOOLS_ENV_VAR,
        MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR,
        MCP_DOMAINS_DISABLED_ENV_VAR,
        MCP_DOMAINS_ENV_VAR,
        MCP_INSIDERS_ENV_VAR,
        MCP_READONLY_MODE_ENV_VAR,
    ):
        monkeypatch.delenv(env_var, raising=False)

    query_names = _http_tool_names(
        headers={},
        configure_client_capabilities=configure_client_capabilities,
        url=f"http://testserver/mcp?{query_param}",
    )
    header_names = _http_tool_names(
        headers={header: header_value},
        configure_client_capabilities=configure_client_capabilities,
    )

    assert (tool_name in query_names) is visible
    assert (tool_name in header_names) is visible


@pytest.mark.parametrize(
    ("env_var", "query_param", "tool_name"),
    [
        pytest.param(
            MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR,
            "allow-pipeline-changes=1",
            "create_connection",
            id="pipeline-changes",
        ),
        pytest.param(
            MCP_ALLOW_DESTRUCTIVE_TOOLS_ENV_VAR,
            "allow-destructive-tools=1",
            "permanently_delete_connection",
            id="destructive-tools",
        ),
        pytest.param(
            MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR,
            "allow-external-access=1",
            "execute_external_api_query",
            id="external-access",
        ),
        pytest.param(
            MCP_INSIDERS_ENV_VAR,
            "insiders=1",
            "create_connector_builder_project",
            id="insiders",
        ),
    ],
)
def test_http_falsy_environment_overrides_truthy_url_param(
    monkeypatch: pytest.MonkeyPatch,
    configure_client_capabilities: Any,
    env_var: str,
    query_param: str,
    tool_name: str,
) -> None:
    """A falsy deployment setting overrides a truthy per-request URL parameter."""
    for policy_env_var in (
        MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR,
        MCP_ALLOW_DESTRUCTIVE_TOOLS_ENV_VAR,
        MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR,
        MCP_DOMAINS_DISABLED_ENV_VAR,
        MCP_DOMAINS_ENV_VAR,
        MCP_INSIDERS_ENV_VAR,
        MCP_READONLY_MODE_ENV_VAR,
    ):
        monkeypatch.delenv(policy_env_var, raising=False)
    monkeypatch.setenv(env_var, "0")

    names = _http_tool_names(
        headers={},
        configure_client_capabilities=configure_client_capabilities,
        url=f"http://testserver/mcp?{query_param}",
    )

    assert tool_name not in names


def test_http_read_only_url_param_hides_non_read_only_tools(
    monkeypatch: pytest.MonkeyPatch,
    configure_client_capabilities: Any,
) -> None:
    """The shared read-only filter applies to HTTP `tools/list` requests."""
    for env_var in (
        MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR,
        MCP_ALLOW_DESTRUCTIVE_TOOLS_ENV_VAR,
        MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR,
        MCP_DOMAINS_DISABLED_ENV_VAR,
        MCP_DOMAINS_ENV_VAR,
        MCP_INSIDERS_ENV_VAR,
        MCP_READONLY_MODE_ENV_VAR,
    ):
        monkeypatch.delenv(env_var, raising=False)

    unrestricted_names = _http_tool_names(
        headers={},
        configure_client_capabilities=configure_client_capabilities,
    )
    read_only_names = _http_tool_names(
        headers={},
        configure_client_capabilities=configure_client_capabilities,
        url="http://testserver/mcp?read-only=true",
    )

    assert "run_connection_sync" in unrestricted_names
    assert "run_connection_sync" not in read_only_names
    assert "list_workspaces" in read_only_names


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


def test_stateless_http_without_extensions_mints_extensionless_session_token(
    configure_client_capabilities: Any,
) -> None:
    """Clients without extensions get a session token that grants no UI tools."""
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
    tokens = [
        value.decode("latin-1")
        for headers in response_headers
        for header_name, value in headers
        if header_name.lower() == b"mcp-session-id"
    ]
    assert tokens
    assert all(decode_capability_token(token) == set() for token in tokens)


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
