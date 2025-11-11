# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Airbyte Cloud MCP operations."""

# Note: Deferred type evaluation must be avoided due to FastMCP/Pydantic needing
# types to be available at import time for tool registration.
import contextlib
import logging
from typing import Annotated, Any, Literal

import requests
from fastmcp import FastMCP
from pydantic import BaseModel, Field

from airbyte._executors.util import DEFAULT_MANIFEST_URL
from airbyte._registry_utils import fetch_registry_version_date, parse_changelog_html
from airbyte._util.meta import is_docker_installed
from airbyte.mcp._tool_utils import mcp_tool, register_tools
from airbyte.mcp._util import resolve_list_of_strings
from airbyte.sources import get_available_connectors
from airbyte.sources.registry import ConnectorMetadata, InstallType, get_connector_metadata
from airbyte.sources.util import get_source


logger = logging.getLogger("airbyte.mcp")


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


class ConnectorVersionInfo(BaseModel):
    """@private Class to hold information about a specific connector version."""

    version: str
    release_date: str | None = None
    docker_image_url: str | None = None
    changelog_url: str | None = None
    pr_url: str | None = None
    pr_title: str | None = None
    parsing_errors: list[str] = Field(default_factory=list)


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


@mcp_tool(
    domain="registry",
    read_only=True,
    idempotent=True,
)
def get_connector_version_history(
    connector_name: Annotated[
        str,
        Field(
            description="The name of the connector (e.g., 'source-faker', 'destination-postgres')"
        ),
    ],
    limit: Annotated[
        int | None,
        Field(
            description=(
                "Maximum number of versions to return (most recent first). "
                "If not specified, returns all versions."
            ),
            default=None,
        ),
    ] = None,
) -> list[ConnectorVersionInfo] | Literal["Connector not found.", "Failed to fetch changelog."]:
    """Get version history for a connector.

    This tool retrieves the version history for a connector, including:
    - Version number
    - Release date (from changelog, with registry override for recent versions)
    - DockerHub URL for the version
    - Changelog URL
    - PR URL and title (scraped from changelog)

    For the most recent 5 versions, release dates are fetched from the registry
    for accuracy. For older versions, changelog dates are used.

    Returns:
        List of version information, sorted by most recent first.
    """
    if connector_name not in get_available_connectors():
        return "Connector not found."

    connector_type = "sources" if connector_name.startswith("source-") else "destinations"
    connector_short_name = connector_name.replace("source-", "").replace("destination-", "")

    changelog_url = f"https://docs.airbyte.com/integrations/{connector_type}/{connector_short_name}"

    try:
        response = requests.get(changelog_url, timeout=30)
        response.raise_for_status()
        html_content = response.text
    except Exception:
        logger.exception(f"Failed to fetch changelog for {connector_name}")
        return "Failed to fetch changelog."

    version_dicts = parse_changelog_html(html_content, connector_name)

    if not version_dicts:
        logger.warning(f"No versions found in changelog for {connector_name}")
        return []

    versions = [ConnectorVersionInfo(**version_dict) for version_dict in version_dicts]

    for version_info in versions[:5]:
        registry_date = fetch_registry_version_date(connector_name, version_info.version)
        if registry_date:
            version_info.release_date = registry_date
            logger.debug(
                f"Updated release date for {connector_name} v{version_info.version} "
                f"from registry: {registry_date}"
            )

    if limit is not None and limit > 0:
        versions = versions[:limit]

    return versions


def register_connector_registry_tools(app: FastMCP) -> None:
    """@private Register tools with the FastMCP app.

    This is an internal function and should not be called directly.
    """
    register_tools(app, domain="registry")
