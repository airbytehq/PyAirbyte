# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Airbyte Cloud MCP operations."""

# Note: Deferred type evaluation must be avoided due to FastMCP/Pydantic needing
# types to be available at import time for tool registration.
import contextlib
import logging
from typing import Annotated, Any, Literal

import requests
from fastmcp_extensions import mcp_tool
from pydantic import BaseModel, Field

from airbyte import exceptions as exc
from airbyte._util.meta import is_docker_installed
from airbyte.mcp._util import resolve_list_of_strings
from airbyte.registry import (
    _DEFAULT_MANIFEST_URL,
    ApiDocsUrl,
    ConnectorMetadata,
    ConnectorVersionInfo,
    InstallType,
    get_available_connectors,
    get_connector_api_docs_urls,
    get_connector_metadata,
)
from airbyte.registry import get_connector_version_history as _get_connector_version_history
from airbyte.sources.util import get_source


logger = logging.getLogger("airbyte.mcp")


@mcp_tool(
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
    connectors: list[str] = get_available_connectors(install_type=InstallType.ANY)

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

    manifest_url = _DEFAULT_MANIFEST_URL.format(
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
    read_only=True,
    idempotent=True,
)
def get_api_docs_urls(
    connector_name: Annotated[
        str,
        Field(
            description=(
                "The canonical connector name "
                "(e.g., 'source-facebook-marketing', 'destination-snowflake')"
            )
        ),
    ],
) -> list[ApiDocsUrl] | Literal["Connector not found."]:
    """Get API documentation URLs for a connector.

    This tool retrieves documentation URLs for a connector's upstream API from multiple sources:
    - Registry metadata (documentationUrl, externalDocumentationUrls)
    - Connector manifest.yaml file (data.externalDocumentationUrls)
    """
    try:
        return get_connector_api_docs_urls(connector_name)
    except exc.AirbyteConnectorNotRegisteredError:
        return "Connector not found."


@mcp_tool(
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
    num_versions_to_validate: Annotated[
        int,
        Field(
            description=(
                "Number of most recent versions to validate with registry data for accurate "
                "release dates. Defaults to 5."
            ),
            default=5,
        ),
    ] = 5,
    limit: Annotated[
        int | None,
        Field(
            description=(
                "DEPRECATED: Use num_versions_to_validate instead. "
                "Maximum number of versions to return (most recent first). "
                "If specified, only the first N versions will be returned."
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

    For the most recent N versions (default 5), release dates are fetched from the
    registry for accuracy. For older versions, changelog dates are used.

    Returns:
        List of version information, sorted by most recent first.
    """
    try:
        versions = _get_connector_version_history(
            connector_name=connector_name,
            num_versions_to_validate=num_versions_to_validate,
        )
    except exc.AirbyteConnectorNotRegisteredError:
        return "Connector not found."
    except requests.exceptions.RequestException:
        logger.exception(f"Failed to fetch changelog for {connector_name}")
        return "Failed to fetch changelog."
    else:
        if limit is not None and limit > 0:
            return versions[:limit]
        return versions
