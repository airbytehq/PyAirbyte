"""Unit tests for Airbyte MCP guidance tools and prompts."""

from __future__ import annotations

import asyncio
import json
from typing import cast
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
import responses
from fastmcp import Client, Context, FastMCP

from airbyte.constants import (
    MCP_DOMAINS_DISABLED_ENV_VAR,
    MCP_DOMAINS_ENV_VAR,
    MCP_READONLY_MODE_ENV_VAR,
)
from airbyte.exceptions import AirbyteLibInputError
from airbyte.mcp import guidance


@pytest.fixture(autouse=True)
def clear_guidance_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear Kapa and tool-filter environment variables for each test."""
    for variable in (
        guidance.KAPA_API_KEY_ENV_VAR,
        guidance.KAPA_RETRIEVAL_API_URL_ENV_VAR,
        MCP_DOMAINS_ENV_VAR,
        MCP_DOMAINS_DISABLED_ENV_VAR,
        MCP_READONLY_MODE_ENV_VAR,
    ):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr(guidance, "is_hosted_mcp_mode", lambda: False)


def _list_tool_names(app: FastMCP) -> list[str]:
    async def list_tools() -> list[str]:
        async with Client(app) as client:
            return [tool.name for tool in await client.list_tools()]

    return asyncio.run(list_tools())


def _airbyte_mcp_app() -> FastMCP:
    from airbyte.mcp.server import app

    return app


@pytest.mark.parametrize(
    ("category", "slug"),
    [
        pytest.param(
            guidance.GitHubIssueCategory.CLOUD_MCP_BUG_REPORT,
            "cloud-mcp-bug-report",
            id="bug-report",
        ),
        pytest.param(
            guidance.GitHubIssueCategory.CLOUD_MCP_FEATURE_REQUEST,
            "cloud-mcp-feature-request",
            id="feature-request",
        ),
    ],
)
def test_get_github_issue_creation_link(
    category: guidance.GitHubIssueCategory, slug: str
) -> None:
    title = "Bug & regression #42 + edge case? café"
    description = (
        "What happened: the `tool+name` call failed & returned #1? Unexpectedly.\n"
        "Expected: success.\nTool calls: `get_cloud_sync_status`."
    )

    parsed_url = urlsplit(
        guidance.get_github_issue_creation_link(category, title, description)
    )
    query = parse_qs(parsed_url.query)

    assert parsed_url.scheme == "https"
    assert parsed_url.netloc == "github.com"
    assert parsed_url.path == "/airbytehq/PyAirbyte/issues/new"
    assert set(query) == {"template", "title", "description"}
    assert query["template"] == [f"{slug}.yml"]
    assert query["title"] == [title]
    assert query["description"] == [description]


def test_get_github_issue_creation_link_is_listed_by_app() -> None:
    assert "get_github_issue_creation_link" in _list_tool_names(_airbyte_mcp_app())


def test_get_github_issue_creation_link_rejects_oversized_encoded_description() -> None:
    with pytest.raises(AirbyteLibInputError) as exc_info:
        guidance.get_github_issue_creation_link(
            guidance.GitHubIssueCategory.CLOUD_MCP_BUG_REPORT,
            "Issue",
            "😀" * 4000,
        )

    assert exc_info.value.context["url_length"] > guidance.MAX_GITHUB_ISSUE_URL_LENGTH
    assert (
        exc_info.value.context["max_url_length"] == guidance.MAX_GITHUB_ISSUE_URL_LENGTH
    )


def test_get_github_issue_creation_link_accepts_ascii_description_at_field_limit() -> (
    None
):
    url = guidance.get_github_issue_creation_link(
        guidance.GitHubIssueCategory.CLOUD_MCP_BUG_REPORT,
        "Issue",
        "a" * 4000,
    )

    assert len(url) <= guidance.MAX_GITHUB_ISSUE_URL_LENGTH


@pytest.mark.parametrize(
    ("hosted", "api_key", "retrieval_api_url", "expected_visible"),
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
def test_search_airbyte_knowledge_sources_visibility(
    monkeypatch: pytest.MonkeyPatch,
    hosted: bool,
    api_key: str | None,
    retrieval_api_url: str | None,
    expected_visible: bool,
) -> None:
    monkeypatch.setattr(guidance, "is_hosted_mcp_mode", lambda: hosted)
    if api_key is not None:
        monkeypatch.setenv(guidance.KAPA_API_KEY_ENV_VAR, api_key)
    if retrieval_api_url is not None:
        monkeypatch.setenv(guidance.KAPA_RETRIEVAL_API_URL_ENV_VAR, retrieval_api_url)

    app = _airbyte_mcp_app()

    assert (
        "search_airbyte_knowledge_sources" in _list_tool_names(app)
    ) is expected_visible
    if not expected_visible:

        async def call_hidden_tool() -> None:
            await app.call_tool(
                "search_airbyte_knowledge_sources",
                {"query": "How do I configure an Airbyte source?"},
            )

        with pytest.raises(ValueError, match="not available"):
            asyncio.run(call_hidden_tool())


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
    config_values = {
        "kapa_api_key": "dummy-api-key",
        "kapa_retrieval_api_url": url,
    }
    monkeypatch.setattr(
        guidance,
        "get_mcp_config",
        lambda _context, name: config_values[name],
    )
    responses.add(responses.POST, url, json=response_body, status=status)

    if status == 401:
        with pytest.raises(requests.HTTPError):
            guidance.search_airbyte_knowledge_sources(cast(Context, object()), query)
    else:
        assert (
            guidance.search_airbyte_knowledge_sources(cast(Context, object()), query)
            == expected_result
        )

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
            "guidance",
            True,
            id="include-guidance",
        ),
        pytest.param(
            MCP_DOMAINS_ENV_VAR,
            "cloud",
            False,
            id="include-cloud-only",
        ),
        pytest.param(
            MCP_DOMAINS_DISABLED_ENV_VAR,
            "guidance",
            False,
            id="disable-guidance",
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
    monkeypatch.setattr(guidance, "is_hosted_mcp_mode", lambda: True)
    monkeypatch.setenv(guidance.KAPA_API_KEY_ENV_VAR, "dummy-api-key")
    monkeypatch.setenv(
        guidance.KAPA_RETRIEVAL_API_URL_ENV_VAR,
        "https://api.kapa.ai/query/v1/projects/test-project/retrieval/",
    )
    if environment_variable is not None and environment_value is not None:
        monkeypatch.setenv(environment_variable, environment_value)

    app = _airbyte_mcp_app()
    tool_names = _list_tool_names(app)
    assert ("search_airbyte_knowledge_sources" in tool_names) is expected_visible
