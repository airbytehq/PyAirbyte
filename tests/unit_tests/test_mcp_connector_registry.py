# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP connector registry tools."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest
from mcp.types import TextContent

from airbyte import exceptions as exc
from airbyte.mcp.interactive import show_connectors_list
from airbyte.mcp.interactive._registry import _list_public_registry_connectors
from airbyte.mcp.interactive._shared_models import ConnectorType, SupportLevel
from airbyte.mcp.registry import get_api_docs_urls
from airbyte.registry import (
    ApiDocsUrl,
    ConnectorMetadata,
    _fetch_manifest_dict,
    _manifest_url_for,
    list_connector_metadata,
)


@pytest.fixture
def connector_metadata_cache() -> dict[str, ConnectorMetadata]:
    """Return representative connector metadata keyed by connector name."""
    connectors = [
        ConnectorMetadata(
            name="destination-snowflake",
            display_name="Snowflake",
            connector_type="destination",
            definition_id="destination-snowflake-definition",
            docker_repository="airbyte/destination-snowflake",
            latest_available_version="3.0.0",
            pypi_package_name=None,
            language=None,
            install_types=set(),
            support_level="certified",
            release_stage="generally_available",
            source_type=None,
            documentation_url="https://docs.airbyte.com/integrations/destinations/snowflake",
        ),
        ConnectorMetadata(
            name="source-faker",
            display_name="Faker",
            connector_type="source",
            definition_id="source-faker-definition",
            docker_repository="airbyte/source-faker",
            latest_available_version="1.0.0",
            pypi_package_name=None,
            language=None,
            install_types=set(),
            support_level="community",
            release_stage="alpha",
            source_type="file",
            documentation_url="https://docs.airbyte.com/integrations/sources/faker",
        ),
        ConnectorMetadata(
            name="source-github",
            display_name="GitHub",
            connector_type="source",
            definition_id="source-github-definition",
            docker_repository="airbyte/source-github",
            latest_available_version="2.0.0",
            pypi_package_name=None,
            language=None,
            install_types=set(),
            support_level="certified",
            release_stage="generally_available",
            source_type="api",
            documentation_url="https://docs.airbyte.com/integrations/sources/github",
        ),
        ConnectorMetadata(
            name="source-legacy",
            display_name="Legacy",
            connector_type="source",
            definition_id="source-legacy-definition",
            docker_repository="airbyte/source-legacy",
            latest_available_version="0.1.0",
            pypi_package_name=None,
            language=None,
            install_types=set(),
            support_level="archived",
            release_stage="alpha",
            source_type="api",
            documentation_url="https://docs.airbyte.com/integrations/sources/legacy",
        ),
    ]
    return {connector.name: connector for connector in connectors}


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
        with patch("airbyte.registry.requests.get") as mock_get:
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
        with patch("airbyte.registry.requests.get") as mock_get:
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
        with patch("airbyte.mcp.registry.get_connector_api_docs_urls") as mock_get_docs:
            mock_get_docs.side_effect = exc.AirbyteConnectorNotRegisteredError(
                connector_name="nonexistent-connector",
                context={},
            )

            result = get_api_docs_urls("nonexistent-connector")
            assert result == "Connector not found."

    def test_deduplication_of_urls(self) -> None:
        """Test that duplicate URLs are deduplicated."""
        with patch("airbyte.mcp.registry.get_connector_api_docs_urls") as mock_get_docs:
            mock_get_docs.return_value = [
                ApiDocsUrl(
                    title="Airbyte Documentation",
                    url="https://docs.airbyte.com/integrations/sources/example",
                    source="registry",
                )
            ]

            result = get_api_docs_urls("source-example")

            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0].title == "Airbyte Documentation"


def test_show_connectors_list_limits_text_and_meta_payload() -> None:
    """Test that the model payload is capped while the UI gets all rows."""
    connector_metadata = [
        ConnectorMetadata(
            name=f"source-test-{i:02d}",
            display_name=f"Test {i:02d}",
            connector_type="source",
            definition_id=f"definition-{i:02d}",
            docker_repository=f"airbyte/source-test-{i:02d}",
            latest_available_version="1.0.0",
            pypi_package_name=None,
            language=None,
            install_types=[],
            support_level="community",
            release_stage="alpha",
            source_type="api",
            documentation_url=f"https://docs.airbyte.com/integrations/sources/test-{i:02d}",
        )
        for i in range(30)
    ]

    with patch(
        "airbyte.mcp.interactive._registry.list_connector_metadata"
    ) as mock_list_metadata:
        mock_list_metadata.return_value = connector_metadata

        result = show_connectors_list()

    text_content = result.content[0]
    assert isinstance(text_content, TextContent)
    text_payload = text_content.text
    assert '"model_preview_count": 25' in text_payload
    assert '"model_preview_truncated": true' in text_payload
    assert '"full_count_rendered_to_user": 30' in text_payload
    assert result.meta is not None
    assert result.meta["airbyte_mcp_raw_result"]["model_preview_count"] == 25
    assert result.meta["airbyte_mcp_raw_result"]["full_count_rendered_to_user"] == 30
    assert result.structured_content is not None
    structured_content = result.structured_content
    assert "$prefab" in structured_content
    assert "source-test-29" in str(structured_content["view"])


def test_list_public_registry_connectors_uses_underlying_registry_filters() -> None:
    """Test that the interactive list wraps the public registry implementation."""
    with patch(
        "airbyte.mcp.interactive._registry.list_connector_metadata"
    ) as mock_list_metadata:
        mock_list_metadata.return_value = []

        _list_public_registry_connectors(
            support_level=SupportLevel.CERTIFIED,
            min_support_level=None,
            connector_type=ConnectorType.SOURCE,
            search="github",
            limit=10,
        )

    mock_list_metadata.assert_called_once_with(
        support_level="certified",
        min_support_level=None,
        connector_type="source",
        search="github",
        limit=10,
    )


@pytest.mark.parametrize(
    (
        "support_level",
        "min_support_level",
        "connector_type",
        "search",
        "limit",
        "expected_names",
    ),
    [
        pytest.param(
            "certified",
            None,
            None,
            "",
            None,
            ["destination-snowflake", "source-github"],
            id="exact_support_level",
        ),
        pytest.param(
            None,
            "community",
            None,
            "",
            None,
            ["destination-snowflake", "source-faker", "source-github"],
            id="minimum_support_level",
        ),
        pytest.param(
            None,
            None,
            "source",
            "",
            None,
            ["source-faker", "source-github", "source-legacy"],
            id="connector_type",
        ),
        pytest.param(
            None,
            None,
            None,
            "snow",
            None,
            ["destination-snowflake"],
            id="search",
        ),
        pytest.param(
            None,
            None,
            None,
            "",
            2,
            ["destination-snowflake", "source-faker"],
            id="limit",
        ),
    ],
)
def test_list_connector_metadata_applies_filters(
    connector_metadata_cache: dict[str, ConnectorMetadata],
    support_level: str | None,
    min_support_level: str | None,
    connector_type: str | None,
    search: str,
    limit: int | None,
    expected_names: list[str],
) -> None:
    """Test that connector metadata filters are applied by the registry."""
    with patch(
        "airbyte.registry._get_registry_cache", return_value=connector_metadata_cache
    ):
        connectors = list_connector_metadata(
            support_level=support_level,
            min_support_level=min_support_level,
            connector_type=connector_type,
            search=search,
            limit=limit,
        )

    assert [connector.name for connector in connectors] == expected_names


def test_list_connector_metadata_rejects_invalid_min_support_level() -> None:
    """Test that invalid support level thresholds fail clearly."""
    with pytest.raises(ValueError, match="Unrecognized min_support_level: 'gold'"):
        list_connector_metadata(min_support_level="gold")


def test_show_connectors_list_rejects_negative_limit() -> None:
    """Test that negative connector limits fail clearly."""
    with pytest.raises(
        exc.PyAirbyteInputError, match="Limit parameter must be non-negative."
    ):
        show_connectors_list(limit=-1)


def test_show_connectors_list_uses_resolved_registry_url(
    connector_metadata_cache: dict[str, ConnectorMetadata],
) -> None:
    """Test that the tool reports the active registry URL."""
    registry_url = "file:///tmp/local_registry.json"
    with (
        patch(
            "airbyte.registry._get_registry_cache",
            return_value=connector_metadata_cache,
        ),
        patch(
            "airbyte.mcp.interactive._registry._get_registry_url",
            return_value=registry_url,
        ),
    ):
        result = show_connectors_list(limit=1)

    assert result.meta is not None
    assert result.meta["airbyte_mcp_raw_result"]["registry_url"] == registry_url


def test_show_connectors_list_rejects_conflicting_support_filters() -> None:
    """Test that support-level shorthands do not silently override each other."""
    with pytest.raises(
        ValueError, match="Cannot specify both `certified` and `support_level`"
    ):
        show_connectors_list(certified=True, support_level="community")


@pytest.mark.parametrize(
    ("supports_ui", "expected_visible"),
    [
        pytest.param(False, False, id="hidden_without_ui"),
        pytest.param(True, True, id="visible_with_ui"),
    ],
)
def test_interactive_tools_are_filtered_by_ui_support(
    supports_ui: bool,
    expected_visible: bool,
) -> None:
    """Test that interactive tools are filtered by MCP Apps UI support."""
    from airbyte.mcp import interactive
    from airbyte.mcp._tool_utils import airbyte_ui_support_filter
    from fastmcp_extensions import mcp_server

    app = mcp_server(
        name="test",
        tool_filters=[airbyte_ui_support_filter],
    )
    interactive.register_interactive_tools(app)

    with patch(
        "airbyte.mcp._tool_utils._fastmcp_context_supports_ui",
        return_value=supports_ui,
    ):
        tools = asyncio.run(app.list_tools())

    assert ("show_connectors_list" in {tool.name for tool in tools}) is expected_visible


def test_interactive_tools_are_rejected_by_tool_filter_without_ui_support() -> None:
    """Test that non-UI clients cannot call interactive tools directly."""
    from fastmcp_extensions import mcp_server

    from airbyte.mcp import interactive
    from airbyte.mcp._tool_utils import airbyte_ui_support_filter

    app = mcp_server(
        name="test",
        tool_filters=[airbyte_ui_support_filter],
    )
    interactive.register_interactive_tools(app)

    with pytest.raises(ValueError, match="not available"):
        asyncio.run(app.call_tool("show_connectors_list"))


def test_interactive_tools_include_prefab_metadata() -> None:
    """Test that Prefab metadata is registered for interactive tools."""
    from fastmcp_extensions import mcp_server

    from airbyte.mcp import interactive

    app = mcp_server(name="test")
    interactive.register_interactive_tools(app)

    provider = getattr(app, "_local_provider")
    tool = provider._components["tool:show_connectors_list@"]  # noqa: SLF001

    assert tool.meta is not None
    assert tool.meta["ui"]["resourceUri"] == "ui://prefab/renderer.html"


def test_prefab_generative_provider_registers_tools_and_renderer() -> None:
    """Test that the FastMCP Prefab generative provider is registered."""
    from fastmcp_extensions import mcp_server

    from airbyte.mcp import prefab

    app = mcp_server(name="test")
    prefab.register_prefab_tools(app)

    tool_names = {tool.name for tool in asyncio.run(app.list_tools())}
    resource_uris = {
        str(resource.uri) for resource in asyncio.run(app.list_resources())
    }

    assert {"generate_prefab_ui", "search_prefab_components"} <= tool_names
    assert "ui://prefab/generative.html" in resource_uris


def test_prefab_generative_tools_are_filtered_by_ui_support() -> None:
    """Test that Prefab generative tools require MCP Apps UI support."""
    from fastmcp_extensions import mcp_server

    from airbyte.mcp import prefab
    from airbyte.mcp._tool_utils import airbyte_ui_support_filter

    app = mcp_server(
        name="test",
        tool_filters=[airbyte_ui_support_filter],
    )
    prefab.register_prefab_tools(app)

    with patch(
        "airbyte.mcp._tool_utils._fastmcp_context_supports_ui",
        return_value=False,
    ):
        tools = asyncio.run(app.list_tools())

    assert "generate_prefab_ui" not in {tool.name for tool in tools}


def test_prefab_generative_tools_include_airbyte_annotations() -> None:
    """Test that Airbyte annotations are applied to FastMCP provider tools."""
    from fastmcp_extensions import mcp_server

    from airbyte.mcp import prefab
    from airbyte.mcp._tool_utils import INTERACTIVE_UI_ANNOTATION

    app = mcp_server(name="test")
    prefab.register_prefab_tools(app)

    fastmcp_tool = asyncio.run(app.get_tool("generate_prefab_ui"))
    assert fastmcp_tool is not None
    tool = fastmcp_tool.to_mcp_tool()

    assert tool.annotations is not None
    assert tool.annotations.readOnlyHint is True
    assert tool.annotations.idempotentHint is True
    assert tool.annotations.openWorldHint is True
    assert getattr(tool.annotations, "mcp_module") == "prefab"
    assert getattr(tool.annotations, INTERACTIVE_UI_ANNOTATION) is True
