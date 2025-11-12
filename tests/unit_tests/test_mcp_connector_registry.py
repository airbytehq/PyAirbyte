# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP connector registry tools."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


from airbyte.mcp.connector_registry import (
    ApiDocsUrl,
    _fetch_manifest_dict,
    _manifest_url_for,
    get_api_docs_urls,
)


class TestManifestUrlFor:
    """Tests for _manifest_url_for function."""

    def test_manifest_url_for(self) -> None:
        """Test generating manifest URL for a connector."""
        url = _manifest_url_for("source-example")
        assert "source-example" in url
        assert "manifest.yaml" in url
        assert "latest" in url


class TestFetchManifestDict:
    """Tests for _fetch_manifest_dict function."""

    def test_manifest_not_found(self) -> None:
        """Test handling when manifest.yaml doesn't exist (404)."""
        with patch("airbyte.mcp.connector_registry.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_get.return_value = mock_response

            manifest_dict = _fetch_manifest_dict("https://example.com/manifest.yaml")
            assert manifest_dict == {}

    def test_fetch_manifest_dict(self) -> None:
        """Test fetching and parsing manifest.yaml."""
        manifest_yaml = """
version: 1.0.0
type: DeclarativeSource
data:
  name: Example
"""
        with patch("airbyte.mcp.connector_registry.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = manifest_yaml
            mock_get.return_value = mock_response

            manifest_dict = _fetch_manifest_dict("https://example.com/manifest.yaml")
            assert manifest_dict["version"] == "1.0.0"
            assert manifest_dict["type"] == "DeclarativeSource"
            assert manifest_dict["data"]["name"] == "Example"


class TestApiDocsUrlFromManifestDict:
    """Tests for ApiDocsUrl.from_manifest_dict classmethod."""

    def test_manifest_with_external_docs_urls(self) -> None:
        """Test extracting URLs from data.externalDocumentationUrls field."""
        manifest_dict = {
            "version": "1.0.0",
            "type": "DeclarativeSource",
            "data": {
                "externalDocumentationUrls": [
                    {
                        "title": "Versioning docs",
                        "url": "https://api.example.com/versioning",
                        "type": "api_reference",
                    },
                    {
                        "title": "Changelog",
                        "url": "https://api.example.com/changelog",
                        "type": "api_release_history",
                    },
                    {
                        "title": "Deprecated API calls",
                        "url": "https://api.example.com/deprecations",
                        "type": "api_deprecations",
                        "requiresLogin": True,
                    },
                ]
            },
        }

        urls = ApiDocsUrl.from_manifest_dict(manifest_dict)
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
        manifest_dict = {
            "version": "1.0.0",
            "type": "DeclarativeSource",
            "data": {
                "externalDocumentationUrls": [
                    {
                        "title": "General docs",
                        "url": "https://api.example.com/docs",
                    }
                ]
            },
        }

        urls = ApiDocsUrl.from_manifest_dict(manifest_dict)
        assert len(urls) == 1
        assert urls[0].title == "General docs"
        assert urls[0].doc_type == "other"
        assert urls[0].requires_login is False

    def test_empty_manifest(self) -> None:
        """Test handling empty manifest dict."""
        urls = ApiDocsUrl.from_manifest_dict({})
        assert len(urls) == 0


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
                "airbyte.mcp.connector_registry._fetch_manifest_dict"
            ) as mock_fetch_dict,
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

            mock_fetch_dict.return_value = {
                "data": {
                    "externalDocumentationUrls": [
                        {
                            "title": "Airbyte Documentation",
                            "url": "https://docs.airbyte.com/integrations/sources/example",
                        }
                    ]
                }
            }

            result = get_api_docs_urls("source-example")

            assert isinstance(result, list)
            assert len(result) == 1
