# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP connector registry tools."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airbyte import exceptions as exc
from airbyte.mcp.connector_registry import get_connector_docs_urls
from airbyte.registry import (
    ApiDocsUrl,
    _fetch_metadata_dict,
    _metadata_url_for,
)


class TestMetadataUrlFor:
    """Tests for _metadata_url_for function."""

    def test_metadata_url_for(self) -> None:
        """Test generating metadata URL for a connector."""
        url = _metadata_url_for("source-example")
        assert "source-example" in url
        assert "metadata.yaml" in url
        assert "latest" in url


class TestFetchMetadataDict:
    """Tests for _fetch_metadata_dict function."""

    def test_metadata_not_found(self) -> None:
        """Test handling when metadata.yaml doesn't exist (404)."""
        with patch("airbyte.registry.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_get.return_value = mock_response

            metadata_dict = _fetch_metadata_dict("https://example.com/metadata.yaml")
            assert metadata_dict == {}

    def test_fetch_metadata_dict(self) -> None:
        """Test fetching and parsing metadata.yaml."""
        metadata_yaml = """
version: 1.0.0
type: DeclarativeSource
data:
  name: Example
"""
        with patch("airbyte.registry.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.text = metadata_yaml
            mock_get.return_value = mock_response

            metadata_dict = _fetch_metadata_dict("https://example.com/metadata.yaml")
            assert metadata_dict["version"] == "1.0.0"
            assert metadata_dict["type"] == "DeclarativeSource"
            assert metadata_dict["data"]["name"] == "Example"


class TestApiDocsUrlFromMetadataDict:
    """Tests for ApiDocsUrl.from_metadata_dict classmethod."""

    def test_metadata_with_external_docs_urls(self) -> None:
        """Test extracting URLs from data.externalDocumentationUrls field."""
        metadata_dict = {
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

        urls = ApiDocsUrl.from_metadata_dict(metadata_dict)
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

    def test_metadata_with_external_docs_no_type(self) -> None:
        """Test extracting URLs from data.externalDocumentationUrls without type field."""
        metadata_dict = {
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

        urls = ApiDocsUrl.from_metadata_dict(metadata_dict)
        assert len(urls) == 1
        assert urls[0].title == "General docs"
        assert urls[0].doc_type == "other"
        assert urls[0].requires_login is False

    def test_empty_metadata(self) -> None:
        """Test handling empty metadata dict."""
        urls = ApiDocsUrl.from_metadata_dict({})
        assert len(urls) == 0

    def test_metadata_missing_title_raises_error(self) -> None:
        """Test that missing 'title' field raises PyAirbyteInternalError."""
        metadata_dict = {
            "version": "1.0.0",
            "type": "DeclarativeSource",
            "data": {
                "externalDocumentationUrls": [
                    {
                        "url": "https://api.example.com/docs",
                    }
                ]
            },
        }

        with pytest.raises(
            exc.PyAirbyteInternalError, match="Metadata parsing error.*'title'"
        ):
            ApiDocsUrl.from_metadata_dict(metadata_dict)

    def test_metadata_missing_url_raises_error(self) -> None:
        """Test that missing 'url' field raises PyAirbyteInternalError."""
        metadata_dict = {
            "version": "1.0.0",
            "type": "DeclarativeSource",
            "data": {
                "externalDocumentationUrls": [
                    {
                        "title": "API Documentation",
                    }
                ]
            },
        }

        with pytest.raises(
            exc.PyAirbyteInternalError, match="Metadata parsing error.*'url'"
        ):
            ApiDocsUrl.from_metadata_dict(metadata_dict)


class TestGetConnectorDocsUrls:
    """Tests for get_connector_docs_urls function."""

    def test_connector_not_found(self) -> None:
        """Test handling when connector is not found."""
        with patch(
            "airbyte.mcp.connector_registry.get_connector_docs_urls"
        ) as mock_get_docs:
            mock_get_docs.side_effect = exc.AirbyteConnectorNotRegisteredError(
                connector_name="nonexistent-connector",
                context={},
            )

            result = get_connector_docs_urls("nonexistent-connector")
            assert result == "Connector not found."
