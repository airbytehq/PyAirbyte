# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP connector registry tools."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, call, patch

import pytest
from mcp.types import TextContent

from airbyte import exceptions as exc
from airbyte.mcp._tool_utils import _mcp_module_for_tool
from airbyte.mcp.interactive import show_connectors_list, show_workspace_sync_status
from airbyte.mcp.interactive._registry_ui import (
    _connector_metadata_to_public_summary,
    _list_public_registry_connectors,
)
from airbyte.mcp.interactive._shared_models import ConnectorType, SupportLevel
from airbyte.mcp.registry import get_api_docs_urls, get_connector_info
from airbyte.registry import (
    ApiDocsUrl,
    ConnectorMetadata,
    _fetch_manifest_dict,
    _manifest_url_for,
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
        "airbyte.mcp.interactive._registry_ui._list_public_registry_connectors"
    ) as mock_list_connectors:
        mock_list_connectors.return_value = [
            _connector_metadata_to_public_summary(connector)
            for connector in connector_metadata
        ]
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


def test_list_public_registry_connectors_uses_existing_registry_tool() -> None:
    """Test that the interactive list starts from the existing MCP registry tool."""
    with (
        patch(
            "airbyte.mcp.interactive._registry_ui._list_connectors",
            return_value=[],
        ) as mock_list_connectors,
        patch("airbyte.mcp.interactive._registry_ui.get_connector_metadata"),
    ):
        _list_public_registry_connectors(
            support_level=SupportLevel.CERTIFIED,
            min_support_level=None,
            connector_type=ConnectorType.SOURCE,
            search="github",
            limit=10,
        )

    mock_list_connectors.assert_called_once_with(
        keyword_filter=None,
        connector_type_filter="source",
        install_types=None,
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
def test_list_public_registry_connectors_applies_filters(
    connector_metadata_cache: dict[str, ConnectorMetadata],
    support_level: str | None,
    min_support_level: str | None,
    connector_type: str | None,
    search: str,
    limit: int | None,
    expected_names: list[str],
) -> None:
    """Test that connector metadata filters are applied by the interactive list."""
    with (
        patch(
            "airbyte.mcp.interactive._registry_ui._list_connectors",
            return_value=list(connector_metadata_cache),
        ),
        patch(
            "airbyte.mcp.interactive._registry_ui.get_connector_metadata",
            side_effect=connector_metadata_cache.get,
        ),
    ):
        connectors = _list_public_registry_connectors(
            support_level=SupportLevel(support_level) if support_level else None,
            min_support_level=SupportLevel(min_support_level)
            if min_support_level
            else None,
            connector_type=ConnectorType(connector_type) if connector_type else None,
            search=search,
            limit=limit,
        )

    assert [connector.connector_name for connector in connectors] == expected_names


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
            "airbyte.mcp.interactive._registry_ui._list_connectors",
            return_value=list(connector_metadata_cache),
        ),
        patch(
            "airbyte.mcp.interactive._registry_ui.get_connector_metadata",
            side_effect=connector_metadata_cache.get,
        ),
        patch(
            "airbyte.mcp.interactive._registry_ui._get_registry_url",
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

    tool_names = {tool.name for tool in tools}
    assert ("show_connectors_list" in tool_names) is expected_visible
    assert ("show_workspace_sync_status" in tool_names) is expected_visible


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


def test_mcp_module_for_tool_uses_nearest_public_module() -> None:
    """Test that tools in private implementation modules use their public module."""
    assert _mcp_module_for_tool(show_connectors_list) == "interactive"
    assert _mcp_module_for_tool(show_workspace_sync_status) == "interactive"
    assert _mcp_module_for_tool(get_api_docs_urls) == "registry"


def test_prefab_generative_provider_registers_tools_and_renderer() -> None:
    """Test that the FastMCP Prefab generative provider is registered."""
    from fastmcp_extensions import mcp_server

    from airbyte.mcp import interactive

    app = mcp_server(name="test")
    interactive.register_interactive_tools(app)

    tool_names = {tool.name for tool in asyncio.run(app.list_tools())}
    resource_uris = {
        str(resource.uri) for resource in asyncio.run(app.list_resources())
    }

    assert {"generate_prefab_ui", "search_prefab_components"} <= tool_names
    assert "ui://prefab/generative.html" in resource_uris


def test_prefab_generative_tools_are_filtered_by_ui_support() -> None:
    """Test that Prefab generative tools require MCP Apps UI support."""
    from fastmcp_extensions import mcp_server

    from airbyte.mcp import interactive
    from airbyte.mcp._tool_utils import airbyte_ui_support_filter

    app = mcp_server(
        name="test",
        tool_filters=[airbyte_ui_support_filter],
    )
    interactive.register_interactive_tools(app)

    with patch(
        "airbyte.mcp._tool_utils._fastmcp_context_supports_ui",
        return_value=False,
    ):
        tools = asyncio.run(app.list_tools())

    assert "generate_prefab_ui" not in {tool.name for tool in tools}


def test_prefab_generative_tools_include_airbyte_annotations() -> None:
    """Test that Airbyte annotations are applied to FastMCP provider tools."""
    from fastmcp_extensions import mcp_server

    from airbyte.mcp import interactive
    from airbyte.mcp._tool_utils import INTERACTIVE_UI_ANNOTATION

    app = mcp_server(name="test")
    interactive.register_interactive_tools(app)

    fastmcp_tool = asyncio.run(app.get_tool("generate_prefab_ui"))
    assert fastmcp_tool is not None
    tool = fastmcp_tool.to_mcp_tool()

    assert tool.annotations is not None
    assert getattr(tool.annotations, "mcp_module") == "interactive"
    assert getattr(tool.annotations, INTERACTIVE_UI_ANNOTATION) is True


def test_get_connector_info_resolves_spec_from_registry_without_docker() -> None:
    """`get_connector_info` must resolve the config spec over HTTP, never via install.

    The spec is fetched from the public connector registry keyed by version, so it
    works in a hosted, no-Docker runtime without the unbounded `connector.install()`
    fallback that previously hung requests for minutes.
    """
    connector = MagicMock()
    connector.name = "source-faker"
    connector.docs_url = "https://docs.airbyte.com/integrations/sources/faker"

    cloud_spec = {"type": "object", "properties": {"count": {"type": "integer"}}}

    with (
        patch(
            "airbyte.mcp.registry.get_available_connectors",
            return_value=["source-faker"],
        ),
        patch("airbyte.mcp.registry.get_source", return_value=connector),
        patch("airbyte.mcp.registry.get_connector_metadata", return_value=None),
        patch(
            "airbyte.mcp.registry.get_connector_spec_from_registry",
            return_value=cloud_spec,
        ) as mock_get_spec,
    ):
        result = get_connector_info("source-faker")

    assert not isinstance(result, str)
    assert result.config_spec_jsonschema == cloud_spec
    assert result.manifest_url is not None
    connector.install.assert_not_called()
    mock_get_spec.assert_called_once_with(
        "source-faker", version=None, platform="cloud"
    )


def test_get_connector_info_falls_back_to_oss_spec() -> None:
    """When the `cloud` spec is unavailable, `get_connector_info` uses the `oss` spec."""
    connector = MagicMock()
    connector.name = "source-faker"
    connector.docs_url = "https://docs.airbyte.com/integrations/sources/faker"

    oss_spec = {"type": "object", "properties": {"seed": {"type": "integer"}}}

    with (
        patch(
            "airbyte.mcp.registry.get_available_connectors",
            return_value=["source-faker"],
        ),
        patch("airbyte.mcp.registry.get_source", return_value=connector),
        patch("airbyte.mcp.registry.get_connector_metadata", return_value=None),
        patch(
            "airbyte.mcp.registry.get_connector_spec_from_registry",
            side_effect=[None, oss_spec],
        ) as mock_get_spec,
    ):
        result = get_connector_info("source-faker")

    assert not isinstance(result, str)
    assert result.config_spec_jsonschema == oss_spec
    connector.install.assert_not_called()
    assert mock_get_spec.call_args_list == [
        call("source-faker", version=None, platform="cloud"),
        call("source-faker", version=None, platform="oss"),
    ]


def test_get_connector_info_spec_none_when_registry_has_no_spec() -> None:
    """`config_spec_jsonschema` is `None` when the registry has no spec for either platform."""
    connector = MagicMock()
    connector.name = "source-faker"
    connector.docs_url = "https://docs.airbyte.com/integrations/sources/faker"

    with (
        patch(
            "airbyte.mcp.registry.get_available_connectors",
            return_value=["source-faker"],
        ),
        patch("airbyte.mcp.registry.get_source", return_value=connector),
        patch("airbyte.mcp.registry.get_connector_metadata", return_value=None),
        patch(
            "airbyte.mcp.registry.get_connector_spec_from_registry",
            return_value=None,
        ),
    ):
        result = get_connector_info("source-faker")

    assert not isinstance(result, str)
    assert result.config_spec_jsonschema is None
    assert result.manifest_url is not None
    connector.install.assert_not_called()
