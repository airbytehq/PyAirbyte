# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Interactive connector registry MCP tools."""

from typing import Annotated

from fastmcp.tools.base import ToolResult
from fastmcp_extensions import mcp_tool
from pydantic import Field

from airbyte.mcp.interactive._shared_models import (
    ConnectorType,
    PublicConnectorFilters,
    PublicConnectorListResult,
    PublicConnectorSummary,
    SupportLevel,
)
from airbyte.mcp.ui_builders import connector_catalog_app, tool_result_with_prefab
from airbyte.registry import _REGISTRY_URL, ConnectorMetadata, list_connector_metadata


CONNECTOR_CATALOG_AGENT_PREVIEW_LIMIT = 25


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
