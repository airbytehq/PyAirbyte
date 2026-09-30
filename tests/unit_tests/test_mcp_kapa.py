"""Unit tests for the hosted Airbyte knowledge search tool."""

from __future__ import annotations

import asyncio
import json

import pytest
import requests
import responses
from fastmcp import Client, FastMCP
from fastmcp_extensions import mcp_server

from airbyte.constants import (
    MCP_DOMAINS_DISABLED_ENV_VAR,
    MCP_DOMAINS_ENV_VAR,
    MCP_READONLY_MODE_ENV_VAR,
)
from airbyte.mcp import _tool_utils, kapa


@pytest.fixture(autouse=True)
def clear_kapa_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear Kapa and tool-filter environment variables for each test."""
    for variable in (
        kapa.KAPA_API_KEY_ENV_VAR,
        kapa.KAPA_RETRIEVAL_API_URL_ENV_VAR,
        MCP_DOMAINS_ENV_VAR,
        MCP_DOMAINS_DISABLED_ENV_VAR,
        MCP_READONLY_MODE_ENV_VAR,
    ):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr(kapa, "is_hosted_mcp_mode", lambda: False)


def _list_tool_names(app: FastMCP) -> list[str]:
    async def list_tools() -> list[str]:
        async with Client(app) as client:
            return [tool.name for tool in await client.list_tools()]

    return asyncio.run(list_tools())


@pytest.mark.parametrize(
    ("hosted", "api_key", "retrieval_api_url", "expected"),
    [
        pytest.param(
            False,
            "dummy-api-key",
            "https://api.kapa.ai/query/v1/projects/test-project/retrieval/",
            False,
            id="not-hosted",
        ),
        pytest.param(
            True,
            None,
            "https://api.kapa.ai/query/v1/projects/test-project/retrieval/",
            False,
            id="missing-api-key",
        ),
        pytest.param(
            True,
            " \t ",
            "https://api.kapa.ai/query/v1/projects/test-project/retrieval/",
            False,
            id="whitespace-api-key",
        ),
        pytest.param(
            True,
            "dummy-api-key",
            None,
            False,
            id="missing-retrieval-api-url",
        ),
        pytest.param(
            True,
            "dummy-api-key",
            "https://api.kapa.ai/query/v1/projects/test-project/retrieval/",
            True,
            id="configured",
        ),
    ],
)
def test_register_kapa_tools(
    monkeypatch: pytest.MonkeyPatch,
    hosted: bool,
    api_key: str | None,
    retrieval_api_url: str | None,
    expected: bool,
) -> None:
    monkeypatch.setattr(kapa, "is_hosted_mcp_mode", lambda: hosted)
    if api_key is not None:
        monkeypatch.setenv(kapa.KAPA_API_KEY_ENV_VAR, api_key)
    if retrieval_api_url is not None:
        monkeypatch.setenv(kapa.KAPA_RETRIEVAL_API_URL_ENV_VAR, retrieval_api_url)

    app = FastMCP("kapa-registration-test")

    assert kapa.register_kapa_tools(app) is expected
    assert ("search_airbyte_knowledge_sources" in _list_tool_names(app)) is expected


@pytest.mark.parametrize(
    ("status", "response_body", "expected_result"),
    [
        pytest.param(
            200,
            [
                {
                    "source_url": "https://docs.airbyte.com/example",
                    "content": "Example knowledge chunk",
                }
            ],
            [
                {
                    "source_url": "https://docs.airbyte.com/example",
                    "content": "Example knowledge chunk",
                }
            ],
            id="success",
        ),
        pytest.param(
            401,
            {"detail": "Unauthorized"},
            None,
            id="unauthorized",
        ),
    ],
)
@responses.activate
def test_search_airbyte_knowledge_sources(
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    response_body: object,
    expected_result: list[dict[str, str]] | None,
) -> None:
    url = "https://api.kapa.ai/query/v1/projects/test-project/retrieval/"
    query = "How do I configure an Airbyte source?"
    monkeypatch.setenv(kapa.KAPA_API_KEY_ENV_VAR, "dummy-api-key")
    monkeypatch.setenv(kapa.KAPA_RETRIEVAL_API_URL_ENV_VAR, url)
    responses.add(responses.POST, url, json=response_body, status=status)

    if status == 401:
        with pytest.raises(requests.HTTPError):
            kapa.search_airbyte_knowledge_sources(query)
    else:
        assert kapa.search_airbyte_knowledge_sources(query) == expected_result

    assert len(responses.calls) == 1
    request = responses.calls[0].request
    assert request.url == url
    assert request.headers["X-API-KEY"] == "dummy-api-key"
    assert json.loads(request.body) == {"query": query}


@pytest.mark.parametrize(
    ("environment_variable", "environment_value", "expected_visible"),
    [
        pytest.param(None, None, True, id="no-domain-config"),
        pytest.param(
            MCP_DOMAINS_ENV_VAR,
            "cloud,kapa",
            True,
            id="include-cloud-and-kapa",
        ),
        pytest.param(
            MCP_DOMAINS_ENV_VAR,
            "cloud",
            False,
            id="include-cloud-only",
        ),
        pytest.param(
            MCP_DOMAINS_DISABLED_ENV_VAR,
            "kapa",
            False,
            id="disable-kapa",
        ),
        pytest.param(
            MCP_READONLY_MODE_ENV_VAR,
            "1",
            True,
            id="readonly-mode",
        ),
    ],
)
def test_search_airbyte_knowledge_sources_respects_tool_filters(
    monkeypatch: pytest.MonkeyPatch,
    environment_variable: str | None,
    environment_value: str | None,
    expected_visible: bool,
) -> None:
    monkeypatch.setattr(kapa, "is_hosted_mcp_mode", lambda: True)
    monkeypatch.setenv(kapa.KAPA_API_KEY_ENV_VAR, "dummy-api-key")
    monkeypatch.setenv(
        kapa.KAPA_RETRIEVAL_API_URL_ENV_VAR,
        "https://api.kapa.ai/query/v1/projects/test-project/retrieval/",
    )
    if environment_variable is not None and environment_value is not None:
        monkeypatch.setenv(environment_variable, environment_value)

    app = mcp_server(
        name="kapa-filter-test",
        include_standard_tool_filters=True,
        server_config_args=[
            _tool_utils.AIRBYTE_READONLY_MODE_CONFIG_ARG,
            _tool_utils.AIRBYTE_EXCLUDE_MODULES_CONFIG_ARG,
            _tool_utils.AIRBYTE_INCLUDE_MODULES_CONFIG_ARG,
        ],
        tool_filters=[
            _tool_utils.airbyte_readonly_mode_filter,
            _tool_utils.airbyte_module_filter,
        ],
        telemetry=False,
    )
    assert kapa.register_kapa_tools(app)

    tool_names = _list_tool_names(app)
    assert ("search_airbyte_knowledge_sources" in tool_names) is expected_visible
