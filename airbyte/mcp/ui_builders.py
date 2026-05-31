# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Prefab UI builders for MCP tool results."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, TypeAlias, cast
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
from pydantic import BaseModel


if TYPE_CHECKING:
    from fastmcp import FastMCP


JsonValue: TypeAlias = str | int | float | bool | list["JsonValue"] | dict[str, "JsonValue"] | None


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


def tool_result_with_prefab(
    *,
    raw_value: object,
    app: PrefabApp,
    meta_value: object | None = None,
) -> ToolResult:
    """Return agent-readable JSON plus a Prefab app payload."""
    raw_result = _jsonable(raw_value)
    meta_result = raw_result if meta_value is None else _jsonable(meta_value)
    return ToolResult(
        content=json.dumps(raw_result, indent=2),
        structured_content=app,
        meta={"airbyte_mcp_raw_result": meta_result},
    )


def register_prefab_tool_metadata(app: FastMCP, tool_names: tuple[str, ...]) -> None:
    """Attach Prefab MCP Apps metadata to registered tools."""
    prefab_config = PrefabAppConfig()
    ui_meta = prefab_config.model_dump(by_alias=True, exclude_none=True)
    provider = getattr(app, "_local_provider", None)
    if provider is None:
        return

    components = getattr(provider, "_components", None)
    remove_tool = getattr(provider, "remove_tool", None)
    if not isinstance(components, dict) or not callable(remove_tool):
        return

    app.tool(
        lambda: None,
        name="__prefab_resource_registration_probe",
        app=prefab_config,
    )
    remove_tool("__prefab_resource_registration_probe")

    for tool_name in tool_names:
        tool = components.get(f"tool:{tool_name}@")
        if tool is not None:
            tool.meta = {**(tool.meta or {}), "ui": ui_meta}


def connector_catalog_app(
    *,
    connectors: Sequence[BaseModel],
    filters: BaseModel,
    registry_url: str,
) -> PrefabApp:
    """Build the interactive connector catalog UI."""
    connector_payloads = [
        cast("dict[str, JsonValue]", _jsonable(connector))
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
    filters_payload = cast("dict[str, JsonValue]", _jsonable(filters))
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
