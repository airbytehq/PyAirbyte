# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP connector registry tools."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


from airbyte.mcp.connector_registry import (
    ApiDocsUrl,
    _fetch_manifest_docs_urls,
    get_api_docs_urls,
)


class TestFetchManifestDocsUrls:
    """Tests for _fetch_manifest_docs_urls function."""

    def test_manifest_not_found(self) -> None:
        """Test handling when manifest.yaml doesn't exist (404)."""
        with patch("airbyte.mcp.connector_registry.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_get.return_value = mock_response

            urls = _fetch_manifest_docs_urls("source-nonexistent")
            assert len(urls) == 0

    def test_manifest_with_external_docs_urls(self) -> None:
        """Test extracting URLs from data.externalDocumentationUrls field."""
        manifest_yaml = """
version: 1.0.0
type: DeclarativeSource
data:
  externalDocumentationUrls:
    - title: Versioning docs
      url: https://api.example.com/versioning
      type: api_reference
    - title: Changelog
      url: https://api.example.com/changelog
      type: api_release_history
    - title: Deprecated API calls
      url: https://api.example.com/deprecations
      type: api_deprecations
      requiresLogin: true
"""
        with patch("airbyte.mcp.connector_registry.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = manifest_yaml
            mock_get.return_value = mock_response

            urls = _fetch_manifest_docs_urls("source-example")
            assert len(urls) == 3
            assert urls[0].title == "Versioning docs"
            assert urls[0].url == "https://api.example.com/versioning"
            assert urls[0].doc_type == "api_reference"
            assert urls[0].requires_login is False
            assert urls[1].title == "Changelog"
            assert urls[1].doc_type == "api_release_history"
            assert urls[2].title == "Deprecated API calls"
            assert urls[2].doc_type == "api_deprecations"
            assert urls[2].requires_login is True

    def test_manifest_with_external_docs_no_type(self) -> None:
        """Test extracting URLs from data.externalDocumentationUrls without type field."""
        manifest_yaml = """
version: 1.0.0
type: DeclarativeSource
data:
  externalDocumentationUrls:
    - title: General docs
      url: https://api.example.com/docs
"""
        with patch("airbyte.mcp.connector_registry.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = manifest_yaml
            mock_get.return_value = mock_response

            urls = _fetch_manifest_docs_urls("source-example")
            assert len(urls) == 1
            assert urls[0].title == "General docs"
            assert urls[0].doc_type == "other"
            assert urls[0].requires_login is False


class TestGetApiDocsUrls:
    """Tests for get_api_docs_urls function."""

    def test_connector_not_found(self) -> None:
        """Test handling when connector is not found."""
        with patch(
            "airbyte.mcp.connector_registry.get_available_connectors"
        ) as mock_get:
            mock_get.return_value = ["source-faker", "source-facebook-marketing"]

            result = get_api_docs_urls("nonexistent-connector")
            assert result == "Connector not found."

    def test_deduplication_of_urls(self) -> None:
        """Test that duplicate URLs are deduplicated."""
        with (
            patch(
                "airbyte.mcp.connector_registry.get_available_connectors"
            ) as mock_get,
            patch("airbyte.mcp.connector_registry.get_source") as mock_source,
            patch(
                "airbyte.mcp.connector_registry._fetch_manifest_docs_urls"
            ) as mock_fetch,
            patch(
                "airbyte.mcp.connector_registry._extract_docs_from_registry"
            ) as mock_registry,
        ):
            mock_get.return_value = ["source-example", "source-faker"]

            mock_connector = MagicMock()
            mock_connector.docs_url = (
                "https://docs.airbyte.com/integrations/sources/example"
            )
            mock_source.return_value = mock_connector

            mock_registry.return_value = []

            mock_fetch.return_value = [
                ApiDocsUrl(
                    title="Airbyte Documentation",
                    url="https://docs.airbyte.com/integrations/sources/example",
                    source="data_external_docs",
                )
            ]

            result = get_api_docs_urls("source-example")

            assert isinstance(result, list)
            assert len(result) == 1
