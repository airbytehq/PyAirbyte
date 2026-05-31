# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Airbyte connector registry MCP operations.

.. include:: ../../docs/mcp-generated/registry.md
"""

# No public Python API — MCP primitives are registered via decorators and
# documented via the generated Markdown include above. Setting `__all__` to an
# empty list tells pdoc (and other doc tools) not to surface the individual
# tool / helper definitions as a redundant "API Documentation" list.
__all__: list[str] = []

# Note: Deferred type evaluation must be avoided due to FastMCP/Pydantic needing
# types to be available at import time for tool registration.
import contextlib
import logging
from enum import StrEnum
from typing import Annotated, Any, Literal

import requests
from fastmcp import FastMCP
from fastmcp.tools.base import ToolResult
from fastmcp_extensions import mcp_tool, register_mcp_tools
from pydantic import BaseModel, Field

from airbyte import exceptions as exc
from airbyte._util.meta import is_docker_installed
from airbyte.mcp._arg_resolvers import resolve_list_of_strings
from airbyte.mcp.ui_builders import (
    connector_catalog_app,
    register_prefab_tool_metadata,
    tool_result_with_prefab,
)
from airbyte.registry import (
    _DEFAULT_MANIFEST_URL,
    _REGISTRY_URL,
    ApiDocsUrl,
    ConnectorMetadata,
    ConnectorVersionInfo,
    InstallType,
    get_available_connectors,
    get_connector_api_docs_urls,
    get_connector_metadata,
    list_connector_metadata,
)
from airbyte.registry import get_connector_version_history as _get_connector_version_history
from airbyte.sources.util import get_source


logger = logging.getLogger("airbyte.mcp")

CONNECTOR_CATALOG_AGENT_PREVIEW_LIMIT = 25


class SupportLevel(StrEnum):
    """Connector support levels ordered by precedence."""

    ARCHIVED = "archived"
    COMMUNITY = "community"
    CERTIFIED = "certified"

    @property
    def precedence(self) -> int:
        """Return numeric precedence for support-level comparisons."""
        return _SUPPORT_LEVEL_PRECEDENCE[self]

    @classmethod
    def parse(cls, value: str) -> "SupportLevel":
        """Parse a support level keyword or legacy integer precedence value."""
        try:
            return cls(value)
        except ValueError:
            pass
        try:
            return next(
                level
                for level, precedence in _SUPPORT_LEVEL_PRECEDENCE.items()
                if precedence == int(value)
            )
        except (ValueError, StopIteration):
            valid_kw = ", ".join(f"`{member.value}`" for member in cls)
            valid_int = ", ".join(
                f"`{precedence}`" for precedence in _SUPPORT_LEVEL_PRECEDENCE.values()
            )
            raise ValueError(
                f"Unrecognized support level: {value!r}. "
                f"Expected keyword ({valid_kw}) or integer ({valid_int})."
            ) from None


class ConnectorType(StrEnum):
    """Connector type: `source` or `destination`."""

    SOURCE = "source"
    DESTINATION = "destination"

    @classmethod
    def parse(cls, value: str) -> "ConnectorType":
        """Parse a connector type value."""
        try:
            return cls(value)
        except ValueError:
            valid = ", ".join(f"`{member.value}`" for member in cls)
            raise ValueError(
                f"Unrecognized connector type: {value!r}. Expected one of: {valid}."
            ) from None


_SUPPORT_LEVEL_PRECEDENCE: dict[SupportLevel, int] = {
    SupportLevel.ARCHIVED: 100,
    SupportLevel.COMMUNITY: 200,
    SupportLevel.CERTIFIED: 300,
}


class PublicConnectorFilters(BaseModel):
    """Filters applied to the public connector catalog listing."""

    certified: bool = Field(
        default=False,
        description=(
            "When `True`, only certified connectors are returned. "
            "Shorthand for `support_level='certified'`."
        ),
    )
    support_level: str | None = Field(
        default=None,
        description="Exact support level filter.",
    )
    min_support_level: str | None = Field(
        default=None,
        description="Minimum support level threshold.",
    )
    connector_type: str | None = Field(
        default=None,
        description="Connector type filter.",
    )
    search: str = Field(default="", description="Case-insensitive search string.")
    limit: int | None = Field(default=None, description="Maximum returned connectors.")


class PublicConnectorSummary(BaseModel):
    """Connector summary from the public registry."""

    connector_name: str = Field(description="Canonical connector name.")
    display_name: str = Field(description="Human-readable connector name.")
    connector_type: ConnectorType = Field(description="Connector type.")
    definition_id: str | None = Field(default=None, description="Connector definition ID.")
    docker_repository: str = Field(description="Docker repository.")
    docker_image_tag: str | None = Field(default=None, description="Docker image tag.")
    support_level: str | None = Field(default=None, description="Support level.")
    release_stage: str | None = Field(default=None, description="Release stage.")
    source_type: str | None = Field(default=None, description="Connector subtype.")
    documentation_url: str | None = Field(
        default=None,
        description="Connector documentation URL.",
    )
    release_date: str | None = Field(default=None, description="Release date.")
    github_issue_label: str | None = Field(
        default=None,
        description="GitHub issue label for the connector.",
    )


class PublicConnectorListResult(BaseModel):
    """Result for public connector catalog listing."""

    registry_url: str = Field(description="Registry URL used for the listing.")
    connector_count: int = Field(description="Number of matching connectors.")
    filters: PublicConnectorFilters = Field(description="Applied filters.")
    connectors: list[PublicConnectorSummary] = Field(description="Matching connectors.")


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


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
)
def show_connectors_list(
    support_level: Annotated[
        str,
        Field(
            description=(
                "Exact support level to match, such as `certified`, `community`, "
                "or `archived`. Empty string means no filter."
            ),
        ),
    ] = "",
    certified: Annotated[  # noqa: FBT002 - FastMCP tool parameter.
        bool,
        Field(
            description=(
                "When `True`, return only certified connectors. "
                "Shorthand for `support_level='certified'`."
            ),
        ),
    ] = False,
    min_support_level: Annotated[
        str,
        Field(
            description=(
                "Minimum support level threshold. Levels: `archived` < `community` "
                "< `certified`. Empty string means no filter."
            ),
        ),
    ] = "",
    connector_type: Annotated[
        str,
        Field(
            description=(
                "Filter by connector type: `source` or `destination`. "
                "Empty string means no filter."
            ),
        ),
    ] = "",
    search: Annotated[
        str,
        Field(
            description=(
                "Case-insensitive search across connector name, display name, "
                "definition ID, Docker repository, subtype, and docs URL."
            ),
        ),
    ] = "",
    limit: Annotated[
        int,
        Field(description="Maximum number of connectors to return. Use `0` for no limit."),
    ] = 0,
) -> ToolResult:
    """Show an interactive public connector catalog from the OSS registry."""
    eff_support_level = SupportLevel.CERTIFIED if certified else None
    if support_level:
        eff_support_level = SupportLevel.parse(support_level)
    eff_min_support_level = SupportLevel.parse(min_support_level) if min_support_level else None
    if eff_support_level and eff_min_support_level:
        raise ValueError(
            "Cannot specify both `support_level` and `min_support_level`. "
            "Use `support_level` for an exact match or `min_support_level` for a threshold."
        )
    eff_connector_type = ConnectorType.parse(connector_type) if connector_type else None
    filters = PublicConnectorFilters(
        certified=certified,
        support_level=eff_support_level.value if eff_support_level else None,
        min_support_level=(eff_min_support_level.value if eff_min_support_level else None),
        connector_type=eff_connector_type.value if eff_connector_type else None,
        search=search,
        limit=limit or None,
    )
    connectors = _list_public_registry_connectors(
        support_level=eff_support_level,
        min_support_level=eff_min_support_level,
        connector_type=eff_connector_type,
        search=search,
        limit=limit or None,
    )
    raw_value = PublicConnectorListResult(
        registry_url=_REGISTRY_URL,
        connector_count=len(connectors),
        filters=filters,
        connectors=connectors,
    )
    agent_value = PublicConnectorListResult(
        registry_url=raw_value.registry_url,
        connector_count=raw_value.connector_count,
        filters=raw_value.filters,
        connectors=connectors[:CONNECTOR_CATALOG_AGENT_PREVIEW_LIMIT],
    ).model_dump(mode="json")
    model_preview_count = len(agent_value["connectors"])
    full_count_rendered_to_user = len(connectors)
    agent_value["model_preview_count"] = model_preview_count
    agent_value["model_preview_limit"] = CONNECTOR_CATALOG_AGENT_PREVIEW_LIMIT
    agent_value["model_preview_truncated"] = full_count_rendered_to_user > model_preview_count
    agent_value["model_preview_omitted_count"] = full_count_rendered_to_user - model_preview_count
    agent_value["full_count_rendered_to_user"] = full_count_rendered_to_user
    agent_value["render_note"] = (
        f"The `connectors` array is capped to {CONNECTOR_CATALOG_AGENT_PREVIEW_LIMIT} "
        "entries for model context. The interactive widget renders all "
        f"{full_count_rendered_to_user} matching connectors."
    )
    return tool_result_with_prefab(
        raw_value=agent_value,
        meta_value=agent_value,
        app=connector_catalog_app(
            connectors=connectors,
            filters=filters,
            registry_url=_REGISTRY_URL,
        ),
    )


def _list_public_registry_connectors(
    *,
    support_level: SupportLevel | None = None,
    min_support_level: SupportLevel | None = None,
    connector_type: ConnectorType | None = None,
    search: str = "",
    limit: int | None = None,
) -> list[PublicConnectorSummary]:
    entries = list_connector_metadata(
        support_level=support_level.value if support_level else None,
        min_support_level=min_support_level.value if min_support_level else None,
        connector_type=connector_type.value if connector_type else None,
        search=search,
        limit=limit,
    )
    return [_connector_metadata_to_public_summary(entry) for entry in entries]


def _connector_metadata_to_public_summary(
    connector: ConnectorMetadata,
) -> PublicConnectorSummary:
    connector_type = ConnectorType.parse(
        connector.connector_type or _connector_type_from_name(connector.name)
    )
    return PublicConnectorSummary(
        connector_name=connector.name,
        display_name=connector.display_name or connector.name,
        connector_type=connector_type,
        definition_id=connector.definition_id,
        docker_repository=connector.docker_repository or f"airbyte/{connector.name}",
        docker_image_tag=connector.latest_available_version,
        support_level=connector.support_level,
        release_stage=connector.release_stage,
        source_type=connector.source_type,
        documentation_url=connector.documentation_url,
        release_date=connector.release_date,
        github_issue_label=connector.github_issue_label,
    )


def _connector_type_from_name(name: str) -> str:
    if name.startswith("source-"):
        return ConnectorType.SOURCE.value
    if name.startswith("destination-"):
        return ConnectorType.DESTINATION.value
    raise ValueError(
        f"Cannot determine connector type from connector name: {name!r}. "
        "Expected a name prefixed with `source-` or `destination-`."
    )


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


def register_registry_tools(app: FastMCP) -> None:
    """Register registry tools with the FastMCP app.

    Args:
        app: FastMCP application instance
    """
    register_mcp_tools(app, mcp_module=__name__)
    register_prefab_tool_metadata(app, ("show_connectors_list",))
