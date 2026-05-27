# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for Kapa MCP tools."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import responses

from airbyte.mcp import kapa


@pytest.fixture(autouse=True)
def clear_kapa_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear Kapa environment variables before each test."""
    monkeypatch.delenv("KAPA_API_KEY", raising=False)
    monkeypatch.delenv("KAPA_DOCS_MCP_BEARER_TOKEN", raising=False)
    monkeypatch.delenv("KAPA_BEARER_TOKEN", raising=False)
    monkeypatch.delenv("KAPA_PROJECT_ID", raising=False)
    monkeypatch.delenv("KAPA_INTEGRATION_ID", raising=False)


@pytest.mark.parametrize(
    ("env_name", "expected_headers"),
    [
        pytest.param("KAPA_API_KEY", {"X-API-KEY": "secret"}, id="api-key"),
        pytest.param(
            "KAPA_DOCS_MCP_BEARER_TOKEN",
            {"Authorization": "Bearer secret"},
            id="docs-mcp-bearer-token",
        ),
        pytest.param(
            "KAPA_BEARER_TOKEN", {"Authorization": "Bearer secret"}, id="bearer-token"
        ),
    ],
)
def test_kapa_auth_headers(
    env_name: str,
    expected_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test Kapa auth header selection from supported env vars."""
    monkeypatch.setenv(env_name, "secret")

    assert kapa._kapa_auth_headers() == expected_headers  # noqa: SLF001


@responses.activate
def test_search_airbyte_knowledge_sources_calls_kapa_retrieval_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test Kapa search request construction and response normalization."""
    monkeypatch.setenv("KAPA_API_KEY", "secret")
    monkeypatch.setenv("KAPA_PROJECT_ID", "project-id")
    monkeypatch.setenv("KAPA_INTEGRATION_ID", "integration-id")
    responses.add(
        responses.POST,
        "https://api.kapa.ai/query/v1/projects/project-id/retrieval/",
        json=[{"source_url": "https://docs.airbyte.com", "content": "Airbyte docs"}],
        status=200,
    )

    result = kapa.search_airbyte_knowledge_sources("How do I set up a source?")

    assert result == [
        {"source_url": "https://docs.airbyte.com", "content": "Airbyte docs"}
    ]
    assert len(responses.calls) == 1
    request = responses.calls[0].request
    assert request.headers["X-API-KEY"] == "secret"
    assert (
        request.body
        == b'{"query": "How do I set up a source?", "integration_id": "integration-id"}'
    )


def test_register_kapa_tools_skips_registration_without_credentials() -> None:
    """Test that Kapa tools are hidden when credentials are absent."""
    app = MagicMock()

    with patch("airbyte.mcp.kapa.register_mcp_tools") as register:
        kapa.register_kapa_tools(app)

    register.assert_not_called()


def test_register_kapa_tools_registers_when_credentials_are_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that Kapa tools are visible when credentials are configured."""
    app = MagicMock()
    monkeypatch.setenv("KAPA_API_KEY", "secret")

    with patch("airbyte.mcp.kapa.register_mcp_tools") as register:
        kapa.register_kapa_tools(app)

    register.assert_called_once_with(app, mcp_module="airbyte.mcp.kapa")
