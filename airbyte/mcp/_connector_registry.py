# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Airbyte Cloud MCP operations."""

# Note: Deferred type evaluation must be avoided due to FastMCP/Pydantic needing
# types to be available at import time for tool registration.
import contextlib
from typing import Annotated, Any, Literal

from fastmcp import FastMCP
from pydantic import BaseModel, Field

from airbyte._util.meta import is_docker_installed
from airbyte.sources import get_available_connectors
from airbyte.sources.registry import ConnectorMetadata, get_connector_metadata
from airbyte.sources.util import get_source


# @app.tool()  # << deferred
def list_connectors(
    keyword_filter: Annotated[
        str | None,
        Field(description="Filter connectors by keyword."),
    ] = None,
    connector_type_filter: Annotated[
        Literal["source", "destination"] | None,
        Field(description="Filter connectors by type ('source' or 'destination')."),
    ] = None,
    install_types: Annotated[
        set[Literal["java", "python", "yaml", "docker"]] | None,
        Field(
            description="""
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
    ] = None,
) -> list[str]:
    """List available Airbyte connectors with optional filtering.

    Returns:
        List of connector names.
    """
    connectors: list[str] = get_available_connectors()
    if install_types:
        # If install_types is provided, filter connectors based on the specified install types.
        connectors = [
            connector
            for connector in connectors
            if any(
                connector in get_available_connectors(install_type=install_type)
                for install_type in install_types
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
    """Class to hold connector information."""

    connector_name: str
    connector_metadata: ConnectorMetadata | None = None
    documentation_url: str | None = None
    config_spec_jsonschema: dict | None = None


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
        docker_image=is_docker_installed(),
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

    return ConnectorInfo(
        connector_name=connector.name,
        connector_metadata=connector_metadata,
        documentation_url=connector.docs_url,
        config_spec_jsonschema=config_spec_jsonschema,
    )


def register_connector_registry_tools(app: FastMCP) -> None:
    """Register tools with the FastMCP app."""
    app.tool(list_connectors)
    app.tool(get_connector_info)
