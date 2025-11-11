# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP connector registry tools."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


from airbyte.mcp.connector_registry import (
    ApiDocsUrl,
    ApiDocsUrlsResult,
    _extract_urls_from_manifest_description,
    _fetch_manifest_docs_urls,
    _resolve_connector_name,
    get_api_docs_urls,
)


class TestResolveConnectorName:
    """Tests for _resolve_connector_name function."""

    def test_canonical_name_exact_match(self) -> None:
        """Test that canonical connector names are returned as-is."""
        with patch(
            "airbyte.mcp.connector_registry.get_available_connectors"
        ) as mock_get:
            mock_get.return_value = ["source-faker", "source-facebook-marketing"]
            result = _resolve_connector_name("source-faker")
            assert result == "source-faker"

    def test_api_name_fuzzy_match(self) -> None:
        """Test that API names are resolved to canonical names."""
        with patch(
            "airbyte.mcp.connector_registry.get_available_connectors"
        ) as mock_get:
            mock_get.return_value = ["source-faker", "source-facebook-marketing"]
            result = _resolve_connector_name("Facebook Marketing")
            assert result == "source-facebook-marketing"

    def test_api_name_with_api_suffix(self) -> None:
        """Test that API names with 'API' suffix are resolved correctly."""
        with patch(
            "airbyte.mcp.connector_registry.get_available_connectors"
        ) as mock_get:
            mock_get.return_value = ["source-faker", "source-facebook-marketing"]
            result = _resolve_connector_name("Facebook Marketing API")
            assert result == "source-facebook-marketing"

    def test_nonexistent_connector(self) -> None:
        """Test that nonexistent connectors return None."""
        with patch(
            "airbyte.mcp.connector_registry.get_available_connectors"
        ) as mock_get:
            mock_get.return_value = ["source-faker"]
            result = _resolve_connector_name("nonexistent-connector")
            assert result is None


class TestExtractUrlsFromManifestDescription:
    """Tests for _extract_urls_from_manifest_description function."""

    def test_extract_api_reference_url(self) -> None:
        """Test extracting API Reference URLs from description."""
        description = "API Reference: https://api.example.com/docs"
        urls = _extract_urls_from_manifest_description(description)
        assert len(urls) == 1
        assert urls[0].title == "API Reference (from manifest description)"
        assert urls[0].url == "https://api.example.com/docs"
        assert urls[0].source == "manifest_description"

    def test_extract_multiple_urls(self) -> None:
        """Test extracting multiple URLs from description."""
        description = """
        Website: https://dashboard.example.com/
        API Reference: https://api.example.com/docs
        """
        urls = _extract_urls_from_manifest_description(description)
        assert len(urls) >= 2
        url_strings = set(u.url for u in urls)
        expected_urls = {
            "https://api.example.com/docs",
            "https://dashboard.example.com/",
        }
        assert expected_urls.issubset(url_strings)

    def test_no_urls_in_description(self) -> None:
        """Test handling description with no URLs."""
        description = "This is a connector for some API"
        urls = _extract_urls_from_manifest_description(description)
        assert len(urls) == 0

    def test_deduplication(self) -> None:
        """Test that duplicate URLs are not returned."""
        description = """
        API Reference: https://api.example.com/docs
        Also see: https://api.example.com/docs
        """
        urls = _extract_urls_from_manifest_description(description)
        url_strings = [u.url for u in urls]
        assert url_strings.count("https://api.example.com/docs") == 1


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

    def test_manifest_with_description(self) -> None:
        """Test extracting URLs from manifest description field."""
        manifest_yaml = """
version: 1.0.0
type: DeclarativeSource
description: >-
  API Reference: https://api.example.com/docs
"""
        with patch("airbyte.mcp.connector_registry.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = manifest_yaml
            mock_get.return_value = mock_response

            urls = _fetch_manifest_docs_urls("source-example")
            assert len(urls) >= 1
            assert any("https://api.example.com/docs" in u.url for u in urls)

    def test_manifest_with_assist_docs_url(self) -> None:
        """Test extracting URLs from metadata.assist.docsUrl field."""
        manifest_yaml = """
version: 1.0.0
type: DeclarativeSource
metadata:
  assist:
    docsUrl: https://api.example.com/reference
"""
        with patch("airbyte.mcp.connector_registry.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = manifest_yaml
            mock_get.return_value = mock_response

            urls = _fetch_manifest_docs_urls("source-example")
            assert len(urls) == 1
            assert urls[0].url == "https://api.example.com/reference"
            assert urls[0].source == "manifest_assist"

    def test_manifest_with_api_docs(self) -> None:
        """Test extracting URLs from metadata.apiDocs field."""
        manifest_yaml = """
version: 1.0.0
type: DeclarativeSource
metadata:
  apiDocs:
    - title: API Reference
      url: https://api.example.com/reference
    - title: API Deprecations
      url: https://api.example.com/deprecations
"""
        with patch("airbyte.mcp.connector_registry.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = manifest_yaml
            mock_get.return_value = mock_response

            urls = _fetch_manifest_docs_urls("source-example")
            assert len(urls) == 2
            assert urls[0].title == "API Reference"
            assert urls[0].url == "https://api.example.com/reference"
            assert urls[1].title == "API Deprecations"
            assert urls[1].url == "https://api.example.com/deprecations"

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

    def test_manifest_with_mixed_formats(self) -> None:
        """Test backward compatibility with multiple doc formats."""
        manifest_yaml = """
version: 1.0.0
type: DeclarativeSource
data:
  externalDocumentationUrls:
    - title: New format docs
      url: https://api.example.com/new
      type: api_reference
metadata:
  assist:
    docsUrl: https://api.example.com/assist
  apiDocs:
    - title: Old format docs
      url: https://api.example.com/old
"""
        with patch("airbyte.mcp.connector_registry.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = manifest_yaml
            mock_get.return_value = mock_response

            urls = _fetch_manifest_docs_urls("source-example")
            assert len(urls) == 3
            sources = [u.source for u in urls]
            assert "data_external_docs" in sources
            assert "manifest_assist" in sources
            assert "manifest_api_docs" in sources

    def test_manifest_request_error(self) -> None:
        """Test handling request errors gracefully."""
        with patch("airbyte.mcp.connector_registry.requests.get") as mock_get:
            mock_get.side_effect = Exception("Network error")

            urls = _fetch_manifest_docs_urls("source-example")
            assert len(urls) == 0


class TestGetApiDocsUrls:
    """Tests for get_api_docs_urls function."""

    def test_connector_not_found(self) -> None:
        """Test handling when connector is not found."""
        with patch(
            "airbyte.mcp.connector_registry._resolve_connector_name"
        ) as mock_resolve:
            mock_resolve.return_value = None

            result = get_api_docs_urls("nonexistent-connector")
            assert result == "Connector not found."

    def test_successful_retrieval(self) -> None:
        """Test successful retrieval of API docs URLs."""
        with (
            patch(
                "airbyte.mcp.connector_registry._resolve_connector_name"
            ) as mock_resolve,
            patch(
                "airbyte.mcp.connector_registry.get_connector_metadata"
            ) as mock_metadata,
            patch("airbyte.mcp.connector_registry.get_source") as mock_source,
            patch(
                "airbyte.mcp.connector_registry._fetch_manifest_docs_urls"
            ) as mock_fetch,
        ):
            mock_resolve.return_value = "source-example"
            mock_metadata.return_value = None

            mock_connector = MagicMock()
            mock_connector.docs_url = (
                "https://docs.airbyte.com/integrations/sources/example"
            )
            mock_source.return_value = mock_connector

            mock_fetch.return_value = [
                ApiDocsUrl(
                    title="API Reference",
                    url="https://api.example.com/docs",
                    source="manifest_description",
                )
            ]

            result = get_api_docs_urls("source-example")

            assert isinstance(result, ApiDocsUrlsResult)
            assert result.connector_name == "source-example"
            assert len(result.docs_urls) == 2
            assert result.docs_urls[0].title == "Airbyte Documentation"
            assert result.docs_urls[1].title == "API Reference"

    def test_deduplication_of_urls(self) -> None:
        """Test that duplicate URLs are deduplicated."""
        with (
            patch(
                "airbyte.mcp.connector_registry._resolve_connector_name"
            ) as mock_resolve,
            patch(
                "airbyte.mcp.connector_registry.get_connector_metadata"
            ) as mock_metadata,
            patch("airbyte.mcp.connector_registry.get_source") as mock_source,
            patch(
                "airbyte.mcp.connector_registry._fetch_manifest_docs_urls"
            ) as mock_fetch,
        ):
            mock_resolve.return_value = "source-example"
            mock_metadata.return_value = None

            mock_connector = MagicMock()
            mock_connector.docs_url = (
                "https://docs.airbyte.com/integrations/sources/example"
            )
            mock_source.return_value = mock_connector

            mock_fetch.return_value = [
                ApiDocsUrl(
                    title="Airbyte Documentation",
                    url="https://docs.airbyte.com/integrations/sources/example",
                    source="manifest_description",
                )
            ]

            result = get_api_docs_urls("source-example")

            assert isinstance(result, ApiDocsUrlsResult)
            assert len(result.docs_urls) == 1
