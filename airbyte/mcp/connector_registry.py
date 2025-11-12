# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Airbyte Cloud MCP operations."""

# Note: Deferred type evaluation must be avoided due to FastMCP/Pydantic needing
# types to be available at import time for tool registration.
import contextlib
import re
from typing import Annotated, Any, Literal

import requests
import yaml
from fastmcp import FastMCP
from pydantic import BaseModel, Field

from airbyte._executors.util import DEFAULT_MANIFEST_URL
from airbyte._util.meta import is_docker_installed
from airbyte.mcp._tool_utils import mcp_tool, register_tools
from airbyte.mcp._util import resolve_list_of_strings
from airbyte.sources import get_available_connectors
from airbyte.sources.registry import ConnectorMetadata, InstallType, get_connector_metadata
from airbyte.sources.util import get_source


@mcp_tool(
    domain="registry",
    read_only=True,
    idempotent=True,
)
def list_connectors(
    keyword_filter: Annotated[
        str | None,
        Field(
            description="Filter connectors by keyword.",
            default=None,
        ),
    ],
    connector_type_filter: Annotated[
        Literal["source", "destination"] | None,
        Field(
            description="Filter connectors by type ('source' or 'destination').",
            default=None,
        ),
    ],
    install_types: Annotated[
        Literal["java", "python", "yaml", "docker"]
        | list[Literal["java", "python", "yaml", "docker"]]
        | None,
        Field(
            description=(
                """
                Filter connectors by install type.
                These are not mutually exclusive:
                - "python": Connectors that can be installed as Python packages.
                - "yaml": Connectors that can be installed simply via YAML download.
                    These connectors are the fastest to install and run, as they do not require any
                    additional dependencies.
                - "java": Connectors that can only be installed via Java. Since PyAirbyte does not
                    currently ship with a JVM, these connectors will be run via Docker instead.
                    In environments where Docker is not available, these connectors may not be
                    runnable.
                - "docker": Connectors that can be installed via Docker. Note that all connectors
                    can be run in Docker, so this filter should generally return the same results as
                    not specifying a filter.
                If no install types are specified, all connectors will be returned.
                """
            ),
            default=None,
        ),
    ],
) -> list[str]:
    """List available Airbyte connectors with optional filtering.

    Returns:
        List of connector names.
    """
    # Start with the full list of known connectors (all support Docker):
    connectors: list[str] = get_available_connectors(install_type=InstallType.DOCKER)

    install_types_list: list[str] | None = resolve_list_of_strings(
        install_types,  # type: ignore[arg-type]  # Type check doesn't understand literal is str
    )

    if install_types_list:
        # If install_types is provided, filter connectors based on the specified install types.
        connectors = [
            connector
            for connector in connectors
            if any(
                connector in get_available_connectors(install_type=install_type)
                for install_type in install_types_list
            )
        ]

    if keyword_filter:
        # Filter connectors by keyword, case-insensitive.
        connectors = [
            connector for connector in connectors if keyword_filter.lower() in connector.lower()
        ]

    if connector_type_filter:
        # Filter connectors by type ('source' or 'destination').
        # This assumes connector names are prefixed with 'source-' or 'destination-'.
        connectors = [
            connector
            for connector in connectors
            if connector.startswith(f"{connector_type_filter}-")
        ]

    return sorted(connectors)


class ConnectorInfo(BaseModel):
    """@private Class to hold connector information."""

    connector_name: str
    connector_metadata: ConnectorMetadata | None = None
    documentation_url: str | None = None
    config_spec_jsonschema: dict | None = None
    manifest_url: str | None = None


@mcp_tool(
    domain="registry",
    read_only=True,
    idempotent=True,
)
def get_connector_info(
    connector_name: Annotated[
        str,
        Field(description="The name of the connector to get information for."),
    ],
) -> ConnectorInfo | Literal["Connector not found."]:
    """Get the documentation URL for a connector."""
    if connector_name not in get_available_connectors():
        return "Connector not found."

    connector = get_source(
        connector_name,
        docker_image=is_docker_installed() or False,
        install_if_missing=False,  # Defer to avoid failing entirely if it can't be installed.
    )

    connector_metadata: ConnectorMetadata | None = None
    with contextlib.suppress(Exception):
        connector_metadata = get_connector_metadata(connector_name)

    config_spec_jsonschema: dict[str, Any] | None = None
    with contextlib.suppress(Exception):
        # This requires running the connector. Install it if it isn't already installed.
        connector.install()
        config_spec_jsonschema = connector.config_spec

    manifest_url = DEFAULT_MANIFEST_URL.format(
        source_name=connector_name,
        version="latest",
    )

    return ConnectorInfo(
        connector_name=connector.name,
        connector_metadata=connector_metadata,
        documentation_url=connector.docs_url,
        config_spec_jsonschema=config_spec_jsonschema,
        manifest_url=manifest_url,
    )


class ApiDocsUrl(BaseModel):
    """@private Class to hold API documentation URL information."""

    title: str
    url: str
    source: str
    doc_type: str = Field(default="other", alias="type")
    requires_login: bool = Field(default=False, alias="requiresLogin")

    model_config = {"populate_by_name": True}


class ApiDocsUrlsResult(BaseModel):
    """@private Class to hold API docs URLs result."""

    connector_name: str
    api_name: str | None = None
    docs_urls: list[ApiDocsUrl]


def _resolve_connector_name(connector_identifier: str) -> str | None:
    """Resolve a connector identifier to a canonical connector name.

    Args:
        connector_identifier: Either a canonical connector name (e.g., "source-facebook-marketing")
                            or an API name (e.g., "Facebook Marketing API" or "Facebook Marketing")

    Returns:
        Canonical connector name if found, None otherwise.
    """
    available_connectors = get_available_connectors()

    if connector_identifier in available_connectors:
        return connector_identifier

    connector_identifier_lower = connector_identifier.lower()

    search_term = re.sub(r"\s+(api|rest api)$", "", connector_identifier_lower, flags=re.IGNORECASE)

    for connector_name in available_connectors:
        metadata = None
        with contextlib.suppress(Exception):
            metadata = get_connector_metadata(connector_name)

        if metadata:
            pass

        connector_name_clean = (
            connector_name.replace("source-", "").replace("destination-", "").replace("-", " ")
        )
        if search_term in connector_name_clean or connector_name_clean in search_term:
            return connector_name

    return None


def _extract_urls_from_manifest_description(description: str) -> list[ApiDocsUrl]:
    """Extract URLs from manifest description field."""
    urls = []

    url_pattern = r"(API Reference|Documentation|Docs|API|Reference):\s*(https?://[^\s\n]+)"
    matches = re.finditer(url_pattern, description, re.IGNORECASE)

    for match in matches:
        title = match.group(1)
        url = match.group(2)
        urls.append(
            ApiDocsUrl(
                title=f"{title} (from manifest description)", url=url, source="manifest_description"
            )
        )

    standalone_url_pattern = r"https?://[^\s\n]+"
    standalone_matches = re.finditer(standalone_url_pattern, description)

    existing_urls = {u.url for u in urls}
    for match in standalone_matches:
        url = match.group(0)
        if url not in existing_urls:
            urls.append(
                ApiDocsUrl(
                    title="API Documentation (from manifest)",
                    url=url,
                    source="manifest_description",
                )
            )
            existing_urls.add(url)

    return urls


def _extract_docs_from_manifest(manifest_data: dict) -> list[ApiDocsUrl]:
    """Extract documentation URLs from parsed manifest data."""
    docs_urls = []

    if manifest_data.get("description"):
        docs_urls.extend(_extract_urls_from_manifest_description(manifest_data["description"]))

    data_section = manifest_data.get("data")
    if isinstance(data_section, dict):
        external_docs = data_section.get("externalDocumentationUrls")
        if isinstance(external_docs, list):
            docs_urls.extend(
                [
                    ApiDocsUrl(
                        title=doc["title"],
                        url=doc["url"],
                        source="data_external_docs",
                        doc_type=doc.get("type", "other"),
                        requires_login=doc.get("requiresLogin", False),
                    )
                    for doc in external_docs
                    if isinstance(doc, dict) and "title" in doc and "url" in doc
                ]
            )

    metadata = manifest_data.get("metadata")
    if isinstance(metadata, dict):
        assist = metadata.get("assist")
        if isinstance(assist, dict) and "docsUrl" in assist:
            docs_urls.append(
                ApiDocsUrl(
                    title="API Documentation (assist)",
                    url=assist["docsUrl"],
                    source="manifest_assist",
                )
            )

        api_docs = metadata.get("apiDocs")
        if isinstance(api_docs, list):
            docs_urls.extend(
                [
                    ApiDocsUrl(title=doc["title"], url=doc["url"], source="manifest_api_docs")
                    for doc in api_docs
                    if isinstance(doc, dict) and "title" in doc and "url" in doc
                ]
            )

    return docs_urls


def _fetch_manifest_docs_urls(connector_name: str) -> list[ApiDocsUrl]:
    """Fetch documentation URLs from connector manifest.yaml file."""
    manifest_url = DEFAULT_MANIFEST_URL.format(
        source_name=connector_name,
        version="latest",
    )

    http_not_found = 404

    try:
        response = requests.get(manifest_url, timeout=10)
        if response.status_code == http_not_found:
            return []

        response.raise_for_status()
        manifest_data = yaml.safe_load(response.text)

        return _extract_docs_from_manifest(manifest_data)

    except Exception:
        return []


def _extract_docs_from_registry(connector_name: str) -> list[ApiDocsUrl]:
    """Extract documentation URLs from connector registry metadata.

    Args:
        connector_name: The canonical connector name (e.g., "source-facebook-marketing")

    Returns:
        List of ApiDocsUrl objects extracted from the registry
    """
    docs_urls = []

    try:
        registry_url = "https://connectors.airbyte.com/files/registries/v0/oss_registry.json"
        response = requests.get(registry_url, timeout=10)
        response.raise_for_status()
        registry_data = response.json()

        connector_list = registry_data.get("sources", []) + registry_data.get("destinations", [])
        connector_entry = None
        for entry in connector_list:
            if entry.get("dockerRepository", "").endswith(f"/{connector_name}"):
                connector_entry = entry
                break

        if connector_entry and "externalDocumentationUrls" in connector_entry:
            external_docs = connector_entry["externalDocumentationUrls"]
            if isinstance(external_docs, list):
                docs_urls.extend(
                    [
                        ApiDocsUrl(
                            title=doc["title"],
                            url=doc["url"],
                            source="registry_external_docs",
                            doc_type=doc.get("type", "other"),
                            requires_login=doc.get("requiresLogin", False),
                        )
                        for doc in external_docs
                        if isinstance(doc, dict) and "title" in doc and "url" in doc
                    ]
                )
    except Exception:
        pass

    return docs_urls


@mcp_tool(
    domain="registry",
    read_only=True,
    idempotent=True,
)
def get_api_docs_urls(
    connector_identifier: Annotated[
        str,
        Field(
            description=(
                "The connector identifier. Can be either:\n"
                "- A canonical connector name (e.g., 'source-facebook-marketing')\n"
                "- An API name (e.g., 'Facebook Marketing API' or 'Facebook Marketing')"
            )
        ),
    ],
) -> ApiDocsUrlsResult | Literal["Connector not found."]:
    """Get API documentation URLs for a connector.

    This tool retrieves documentation URLs for a connector's upstream API from multiple sources:
    - Registry metadata (documentationUrl, externalDocumentationUrls)
    - Connector manifest.yaml file (data.externalDocumentationUrls, metadata.assist.docsUrl,
      metadata.apiDocs)

    The tool accepts either a canonical connector ID (e.g., "source-facebook-marketing") or
    an API name (e.g., "Facebook Marketing API" or "Facebook Marketing").

    Returns:
        ApiDocsUrlsResult with connector name and list of documentation URLs, or error message.
    """
    connector_name = _resolve_connector_name(connector_identifier)

    if not connector_name:
        return "Connector not found."

    docs_urls: list[ApiDocsUrl] = []
    api_name: str | None = None

    connector = None
    with contextlib.suppress(Exception):
        connector = get_source(
            connector_name,
            docker_image=is_docker_installed() or False,
            install_if_missing=False,
        )

    if connector and connector.docs_url:
        docs_urls.append(
            ApiDocsUrl(title="Airbyte Documentation", url=connector.docs_url, source="registry")
        )

    registry_urls = _extract_docs_from_registry(connector_name)
    docs_urls.extend(registry_urls)

    manifest_urls = _fetch_manifest_docs_urls(connector_name)
    docs_urls.extend(manifest_urls)

    seen_urls = set()
    unique_docs_urls = []
    for doc_url in docs_urls:
        if doc_url.url not in seen_urls:
            seen_urls.add(doc_url.url)
            unique_docs_urls.append(doc_url)

    return ApiDocsUrlsResult(
        connector_name=connector_name,
        api_name=api_name,
        docs_urls=unique_docs_urls,
    )


def register_connector_registry_tools(app: FastMCP) -> None:
    """@private Register tools with the FastMCP app.

    This is an internal function and should not be called directly.
    """
    register_tools(app, domain="registry")
