# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Interactive connector registry MCP tools."""

import json
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Annotated, Literal, TypeAlias
from uuid import UUID

from fastmcp.apps import PrefabAppConfig
from fastmcp.tools.base import ToolResult
from prefab_ui.actions import OpenLink, SendMessage, SetState
from prefab_ui.app import PrefabApp
from prefab_ui.components import (
    Alert,
    AlertDescription,
    AlertTitle,
    Button,
    Card,
    CardContent,
    CardHeader,
    CardTitle,
    Column,
    DataTable,
    DataTableColumn,
    Div,
    Grid,
    Heading,
    If,
    Metric,
    Row,
    Text,
)
from pydantic import BaseModel, Field

from airbyte import exceptions as exc
from airbyte.mcp._tool_utils import INTERACTIVE_UI_ANNOTATION, mcp_tool
from airbyte.mcp.interactive._shared_models import (
    ConnectorType,
    PublicConnectorFilters,
    PublicConnectorListResult,
    PublicConnectorSummary,
    SupportLevel,
)
from airbyte.mcp.registry import list_connectors as _list_connectors
from airbyte.registry import ConnectorMetadata, _get_registry_url, get_connector_metadata


JsonValue: TypeAlias = str | int | float | bool | list["JsonValue"] | dict[str, "JsonValue"] | None
ConnectorTypeValue: TypeAlias = Literal["source", "destination"]
CONNECTOR_CATALOG_AGENT_PREVIEW_LIMIT = 25


def _jsonable(value: object) -> JsonValue:
    if isinstance(value, BaseModel):
        result: JsonValue = _jsonable(value.model_dump(mode="json"))
    elif isinstance(value, datetime | date):
        result = value.isoformat()
    elif isinstance(value, UUID | Decimal):
        result = str(value)
    elif isinstance(value, Enum):
        result = _jsonable(value.value)
    elif isinstance(value, Mapping):
        result = {str(key): _jsonable(item) for key, item in value.items()}
    elif isinstance(value, list | tuple | set):
        result = [_jsonable(item) for item in value]
    elif isinstance(value, str | int | float | bool) or value is None:
        result = value
    else:
        result = str(value)
    return result


def _json_dumps(value: JsonValue) -> str:
    return json.dumps(value, indent=2)


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    annotations={
        INTERACTIVE_UI_ANNOTATION: True,
    },
    app=PrefabAppConfig(),
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
        Field(
            description="Maximum number of connectors to return. Use `0` for no limit.",
            ge=0,
        ),
    ] = 0,
) -> ToolResult:
    """Show an interactive public connector catalog from the OSS registry."""
    if limit < 0:
        raise exc.PyAirbyteInputError(
            message="Limit parameter must be non-negative.",
            context={"limit": limit},
        )

    eff_support_level = SupportLevel.CERTIFIED if certified else None
    if support_level:
        if certified:
            raise ValueError(
                "Cannot specify both `certified` and `support_level`. "
                "Use `certified=True` as shorthand or `support_level` for explicit control."
            )
        eff_support_level = SupportLevel.parse(support_level)
    eff_min_support_level = SupportLevel.parse(min_support_level) if min_support_level else None
    if eff_support_level and eff_min_support_level:
        raise ValueError(
            "Cannot specify both `certified` or `support_level` and `min_support_level`. "
            "Use an exact match or a threshold."
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
    registry_url = _get_registry_url()
    raw_value = PublicConnectorListResult(
        registry_url=registry_url,
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
    return ToolResult(
        content=_json_dumps(_jsonable(agent_value)),
        structured_content=connector_catalog_app(
            connectors=connectors,
            filters=filters,
            registry_url=registry_url,
        ),
        meta={"airbyte_mcp_raw_result": _jsonable(agent_value)},
    )


def connector_catalog_app(
    *,
    connectors: list[PublicConnectorSummary],
    filters: PublicConnectorFilters,
    registry_url: str,
) -> PrefabApp:
    """Build the interactive connector catalog UI."""
    connector_payloads = [
        _public_connector_summary_to_payload(connector)
        for connector in connectors
        if connector is not None
    ]
    rows = [
        {
            "display_name": connector.get("display_name"),
            "connector_name": connector.get("connector_name"),
            "connector_type": _connector_display_value(
                connector.get("connector_type") or "unknown"
            ),
            "support_level": _connector_support_label(connector.get("support_level") or "unknown"),
            "release_stage": _connector_display_value(connector.get("release_stage") or "unknown"),
            "source_type": _connector_display_value(connector.get("source_type")),
            "docker_image_tag": connector.get("docker_image_tag"),
            "latest_version": connector.get("docker_image_tag"),
            "definition_id": connector.get("definition_id"),
            "docker_repository": connector.get("docker_repository"),
            "documentation_url": connector.get("documentation_url"),
        }
        for connector in connector_payloads
    ]
    filters_payload = _public_connector_filters_to_payload(filters)
    applied_filters = _connector_applied_filters(filters_payload)

    with (
        PrefabApp(
            title="Airbyte connectors",
            state={
                "filters": filters_payload,
                "applied_filters": applied_filters,
                "applied_filter_summary": _connector_applied_filter_summary(applied_filters),
                "registry_url": registry_url,
                "selected_connector": None,
            },
        ) as app,
        Column(gap=4, css_class="p-6"),
    ):
        Heading("Airbyte connectors")
        Text(
            "Public Cloud registry connector catalog. Search, sort, scroll, and "
            "click a row to inspect details.",
            css_class="text-sm text-muted-foreground",
        )
        with Row(gap=4):
            Metric(label="Connectors", value=len(connectors))
            Metric(
                label="Sources",
                value=sum(
                    1
                    for connector in connector_payloads
                    if connector.get("connector_type") == "source"
                ),
            )
            Metric(
                label="Destinations",
                value=sum(
                    1
                    for connector in connector_payloads
                    if connector.get("connector_type") == "destination"
                ),
            )
        with Alert(variant="info", icon="mouse-pointer-click"):
            AlertTitle("Interactive table")
            AlertDescription("Click a connector row to update the details panel below the table.")
        with Card():
            with CardHeader():
                CardTitle("Applied filters")
            with CardContent():
                Text(
                    "{{ applied_filter_summary }}",
                    css_class="text-sm text-muted-foreground",
                )
        with Div(
            css_class="rounded-md",
            style={
                "border": "1px solid rgba(148, 163, 184, 0.28)",
                "maxHeight": "560px",
                "overflowY": "auto",
            },
        ):
            DataTable(
                columns=[
                    DataTableColumn(key="display_name", header="Name", sortable=True),
                    DataTableColumn(key="connector_type", header="Type", sortable=True),
                    DataTableColumn(key="source_type", header="Subtype", sortable=True),
                    DataTableColumn(key="support_level", header="Support", sortable=True),
                    DataTableColumn(key="release_stage", header="Stage", sortable=True),
                    DataTableColumn(
                        key="latest_version",
                        header="Latest Version",
                        sortable=True,
                    ),
                ],
                rows=rows,
                search=True,
                paginated=False,
                on_row_click=SetState("selected_connector", "{{ $event }}"),
            )
        with If("selected_connector"), Card():
            with CardHeader():
                CardTitle("{{ selected_connector.display_name }}")
                Text(
                    "{{ selected_connector.connector_type }}",
                    css_class="text-sm text-muted-foreground font-mono",
                )
            with CardContent():
                with Grid(columns=2, gap=4):
                    _connector_detail(
                        "Connector ID",
                        "{{ selected_connector.connector_name }}",
                    )
                    _connector_detail("Type", "{{ selected_connector.connector_type }}")
                    _connector_detail(
                        "Support",
                        "{{ selected_connector.support_level }}",
                    )
                    _connector_detail(
                        "Release stage",
                        "{{ selected_connector.release_stage }}",
                    )
                    _connector_detail(
                        "Docker image",
                        "{{ selected_connector.docker_repository }}:"
                        "{{ selected_connector.docker_image_tag }}",
                    )
                    _connector_detail(
                        "Definition ID",
                        "{{ selected_connector.definition_id }}",
                    )
                with Row(gap=2, css_class="mt-4"):
                    Button(
                        "Open docs",
                        variant="outline",
                        icon="external-link",
                        onClick=OpenLink("{{ selected_connector.documentation_url }}"),
                    )
                    Button(
                        "Ask about connector",
                        variant="secondary",
                        icon="message-square",
                        onClick=SendMessage(
                            "Summarize Airbyte connector "
                            "{{ selected_connector.connector_name }} "
                            "from the selected connector list."
                        ),
                    )
    return app


def _list_public_registry_connectors(
    *,
    support_level: SupportLevel | None = None,
    min_support_level: SupportLevel | None = None,
    connector_type: ConnectorType | None = None,
    search: str = "",
    limit: int | None = None,
) -> list[PublicConnectorSummary]:
    connector_names = _list_connectors(
        keyword_filter=None,
        connector_type_filter=_connector_type_value(connector_type),
        install_types=None,
    )
    entries = [
        metadata
        for connector_name in connector_names
        if (metadata := get_connector_metadata(connector_name)) is not None
    ]

    if connector_type:
        entries = [
            connector for connector in entries if connector.connector_type == connector_type.value
        ]

    if support_level:
        entries = [
            connector for connector in entries if connector.support_level == support_level.value
        ]

    if min_support_level:
        entries = [
            connector
            for connector in entries
            if connector.support_level
            and SupportLevel.parse(connector.support_level).precedence
            >= min_support_level.precedence
        ]

    if search:
        normalized_search = search.casefold()
        entries = [
            connector
            for connector in entries
            if any(
                normalized_search in value.casefold()
                for value in _connector_metadata_search_values(connector)
            )
        ]

    entries = sorted(entries, key=lambda connector: connector.name)
    limited_entries = entries[:limit] if limit is not None else entries
    return [_connector_metadata_to_public_summary(entry) for entry in limited_entries]


def _connector_type_value(
    connector_type: ConnectorType | None,
) -> ConnectorTypeValue | None:
    if connector_type is None:
        return None
    if connector_type is ConnectorType.SOURCE:
        return "source"
    return "destination"


def _connector_metadata_search_values(connector: ConnectorMetadata) -> tuple[str, ...]:
    return tuple(
        str(value or "")
        for value in (
            connector.name,
            connector.display_name,
            connector.definition_id,
            connector.docker_repository,
            connector.source_type,
            connector.documentation_url,
        )
    )


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


def _public_connector_summary_to_payload(
    connector: PublicConnectorSummary,
) -> dict[str, JsonValue]:
    jsonable = _jsonable(connector)
    if not isinstance(jsonable, dict):
        raise TypeError(f"Expected connector summary payload to be a dict: {jsonable!r}")
    return jsonable


def _public_connector_filters_to_payload(
    filters: PublicConnectorFilters,
) -> dict[str, JsonValue]:
    jsonable = _jsonable(filters)
    if not isinstance(jsonable, dict):
        raise TypeError(f"Expected connector filters payload to be a dict: {jsonable!r}")
    return jsonable


def _connector_filter_label(key: str) -> str:
    labels = {
        "certified": "Certified",
        "support_level": "Support level",
        "min_support_level": "Minimum support level",
        "connector_type": "Type",
        "search": "Search",
        "limit": "Limit",
    }
    return labels.get(key, key.replace("_", " ").title())


def _connector_applied_filters(filters: dict[str, JsonValue]) -> list[dict[str, str]]:
    return [
        {"label": _connector_filter_label(key), "value": str(value)}
        for key, value in filters.items()
        if value not in {"", None, 0}
    ]


def _connector_applied_filter_summary(filters: list[dict[str, str]]) -> str:
    if not filters:
        return "No filters applied"
    return ", ".join(
        f"{filter_value['label']}: {filter_value['value']}" for filter_value in filters
    )


def _connector_support_label(value: object) -> str:
    support_labels = {
        "certified": "Airbyte",
        "community": "Marketplace",
        "enterprise": "Enterprise",
    }
    normalized = str(value or "unknown").lower()
    return support_labels.get(normalized, _connector_display_value(normalized))


def _connector_display_value(value: object) -> str:
    if value in {"", None}:
        return ""
    return str(value).replace("_", " ").title()


def _connector_detail(label: str, value: str) -> None:
    with Column(gap=1):
        Text(label, css_class="text-xs uppercase text-muted-foreground")
        Text(value, css_class="text-sm font-medium")


def _connector_type_from_name(name: str) -> str:
    if name.startswith("source-"):
        return ConnectorType.SOURCE.value
    if name.startswith("destination-"):
        return ConnectorType.DESTINATION.value
    raise ValueError(
        f"Cannot determine connector type from connector name: {name!r}. "
        "Expected a name prefixed with `source-` or `destination-`."
    )
