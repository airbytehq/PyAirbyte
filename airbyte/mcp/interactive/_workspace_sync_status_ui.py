# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Interactive workspace sync status MCP tool with Prefab UI."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Annotated, Literal

from fastmcp import Context  # noqa: TC002 - required at runtime for FastMCP tool registration
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
from prefab_ui.components.charts import PieChart
from pydantic import Field

from airbyte.cloud.constants import FAILED_STATUSES
from airbyte.mcp._tool_utils import INTERACTIVE_UI_ANNOTATION, mcp_tool
from airbyte.mcp.cloud import CLOUD_AUTH_TIP_TEXT, WORKSPACE_ID_TIP_TEXT, _get_cloud_workspace
from airbyte.mcp.interactive._sync_history_ui import (
    _format_bytes,
    _format_records,
)


if TYPE_CHECKING:
    from airbyte.cloud.connections import CloudConnection
    from airbyte.cloud.sync_results import SyncResult
    from airbyte.cloud.workspaces import CloudWorkspace


WORKSPACE_SYNC_STATUS_AGENT_PREVIEW_LIMIT = 25
_ConnectionStatusRow = dict[str, str | int | float | bool | None]
_SUCCESS_HIGH = 90
_SUCCESS_LOW = 50
_RECENT_HOURS_DEFAULT = 24
_STATUS_PIE_CATEGORIES = (
    ("Succeeded", "#22c55e", "success"),
    ("Canceled", "#eab308", "warning"),
    ("No syncs", "#94a3b8", "muted"),
    ("Failed", "#ef4444", "destructive"),
    ("Other", "#64748b", "secondary"),
)
_STATUS_PIE_STYLE_BY_STATUS = {
    "succeeded": ("Succeeded", "#22c55e", "success"),
    "cancelled": ("Canceled", "#eab308", "warning"),
    "canceled": ("Canceled", "#eab308", "warning"),
    "no syncs": ("No syncs", "#94a3b8", "muted"),
    "failed": ("Failed", "#ef4444", "destructive"),
    "error": ("Failed", "#ef4444", "destructive"),
    "running": ("Other", "#64748b", "secondary"),
}


@dataclass
class WorkspaceConnectionSyncStatus:
    """Workspace-level sync status summary for one Airbyte Cloud connection."""

    connection_id: str
    connection_name: str
    connection_url: str
    source_name: str
    source_url: str
    destination_name: str
    destination_url: str
    latest_status: str
    latest_job_id: int | None
    latest_sync_time: str | None
    latest_records_synced: int
    latest_bytes_synced: int
    recent_jobs: int
    recent_successes: int
    recent_failures: int
    recent_records_synced: int
    recent_bytes_synced: int
    running_job_id: int | None
    suggested_tool_call: str

    @property
    def is_problem(self) -> bool:
        """Whether this connection needs attention."""
        return self.latest_status in {"failed", "cancelled", "error", "unknown"}

    @property
    def success_rate(self) -> float:
        """Recent completed-job success rate for this connection."""
        completed_jobs = self.recent_successes + self.recent_failures
        if completed_jobs == 0:
            return 0.0
        return self.recent_successes / completed_jobs * 100


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    annotations={
        INTERACTIVE_UI_ANNOTATION: True,
    },
    app=PrefabAppConfig(),
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def show_workspace_sync_status(
    ctx: Context,
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ] = None,
    max_connections: Annotated[
        int,
        Field(
            description=(
                "Maximum number of workspace connections to inspect. "
                "Defaults to 50. Maximum allowed value is 100."
            ),
            default=50,
            ge=1,
            le=100,
        ),
    ] = 50,
    max_jobs_per_connection: Annotated[
        int,
        Field(
            description=(
                "Maximum number of recent jobs to inspect for each connection. "
                "Defaults to 5. Maximum allowed value is 10."
            ),
            default=5,
            ge=1,
            le=10,
        ),
    ] = 5,
    recent_hours: Annotated[
        int,
        Field(
            description=("Window, in hours, used for the Recently Synced metric. Defaults to 24."),
            default=_RECENT_HOURS_DEFAULT,
            ge=1,
            le=720,
        ),
    ] = _RECENT_HOURS_DEFAULT,
    agent_context: Annotated[
        Literal["verbose", "summary", "min"],
        Field(
            description=(
                "Controls how much context is returned to the agent in the text response. "
                "'verbose': capped connection-level data for follow-up analysis. "
                "'summary': aggregates and key observations only. "
                "'min': one-liner confirmation that the dashboard rendered."
            ),
            default="min",
        ),
    ] = "min",
    suppress_ui: Annotated[
        bool,
        Field(
            description=(
                "If True, skip rendering the visual dashboard and return only the agent "
                "text response. Use this for follow-up data retrieval without re-rendering "
                "the UI that the user has already seen."
            ),
            default=False,
        ),
    ] = False,
) -> ToolResult:
    """Show an interactive sync status dashboard for an Airbyte Cloud workspace."""
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connections = workspace.list_connections(limit=max_connections)
    now = datetime.now(tz=timezone.utc)
    connection_statuses = [
        _summarize_connection(
            connection=connection,
            max_jobs_per_connection=max_jobs_per_connection,
        )
        for connection in connections
    ]
    metric_summary = _build_workspace_metric_summary(
        connection_statuses=connection_statuses,
        recent_hours=recent_hours,
        now=now,
    )
    agent_text = _build_agent_text(
        agent_context=agent_context,
        workspace_id=workspace.workspace_id,
        connection_statuses=connection_statuses,
        metric_summary=metric_summary,
    )
    raw_result = {
        **metric_summary,
        "connections": [
            asdict(connection_status)
            for connection_status in connection_statuses[:WORKSPACE_SYNC_STATUS_AGENT_PREVIEW_LIMIT]
        ],
        "model_preview_count": min(
            len(connection_statuses),
            WORKSPACE_SYNC_STATUS_AGENT_PREVIEW_LIMIT,
        ),
        "model_preview_truncated": (
            len(connection_statuses) > WORKSPACE_SYNC_STATUS_AGENT_PREVIEW_LIMIT
        ),
        "full_count_rendered_to_user": len(connection_statuses),
        "suppress_ui": suppress_ui,
    }

    if suppress_ui:
        return ToolResult(
            content=agent_text,
            meta={"airbyte_mcp_raw_result": raw_result},
        )

    return ToolResult(
        content=agent_text,
        structured_content=_build_workspace_sync_status_app(
            workspace_id=workspace.workspace_id,
            workspace_url=workspace.workspace_url or "",
            connection_statuses=connection_statuses,
            metric_summary=metric_summary,
            recent_hours=recent_hours,
        ),
        meta={"airbyte_mcp_raw_result": raw_result},
    )


def _summarize_connection(
    *,
    connection: CloudConnection,
    max_jobs_per_connection: int,
) -> WorkspaceConnectionSyncStatus:
    """Fetch recent sync status for one connection."""
    sync_results = connection.get_previous_sync_logs(
        limit=max_jobs_per_connection,
        from_tail=True,
    )
    completed_results = [
        sync_result for sync_result in sync_results if sync_result.is_job_complete()
    ]
    latest_result = sync_results[0] if sync_results else None
    latest_completed_result = completed_results[0] if completed_results else None
    latest_display_result = (
        latest_result
        if latest_result is not None and latest_result.is_job_complete()
        else latest_completed_result or latest_result
    )
    latest_status = (
        _sync_status_value(latest_display_result) if latest_display_result else "no syncs"
    )
    latest_records = latest_display_result.records_synced if latest_display_result else 0
    latest_bytes = latest_display_result.bytes_synced if latest_display_result else 0
    latest_start_time = (
        latest_display_result.start_time.isoformat() if latest_display_result else None
    )
    running_job_id = (
        latest_result.job_id
        if latest_result is not None and not latest_result.is_job_complete()
        else None
    )
    recent_successes = sum(
        1 for sync_result in completed_results if _sync_status_value(sync_result) == "succeeded"
    )
    recent_failures = sum(
        1 for sync_result in completed_results if sync_result.get_job_status() in FAILED_STATUSES
    )

    return WorkspaceConnectionSyncStatus(
        connection_id=connection.connection_id,
        connection_name=connection.name or connection.connection_id,
        connection_url=connection.connection_url or "",
        source_name=connection.source.name or connection.source_id,
        source_url=connection.source.connector_url,
        destination_name=connection.destination.name or connection.destination_id,
        destination_url=connection.destination.connector_url,
        latest_status=latest_status,
        latest_job_id=latest_display_result.job_id if latest_display_result else None,
        latest_sync_time=latest_start_time,
        latest_records_synced=latest_records,
        latest_bytes_synced=latest_bytes,
        recent_jobs=len(completed_results),
        recent_successes=recent_successes,
        recent_failures=recent_failures,
        recent_records_synced=sum(sync_result.records_synced for sync_result in completed_results),
        recent_bytes_synced=sum(sync_result.bytes_synced for sync_result in completed_results),
        running_job_id=running_job_id,
        suggested_tool_call=(
            f'show_connection_sync_history(connection_id="{connection.connection_id}")'
        ),
    )


def _sync_status_value(sync_result: SyncResult) -> str:
    """Return a lowercase status string for a sync result."""
    status = sync_result.get_job_status()
    if isinstance(status, Enum):
        return str(status.value).lower()
    return str(status).lower()


def _build_workspace_metric_summary(
    *,
    connection_statuses: list[WorkspaceConnectionSyncStatus],
    recent_hours: int,
    now: datetime,
) -> dict[str, int | float]:
    """Build dashboard-level metrics."""
    recently_synced = 0
    for connection_status in connection_statuses:
        if connection_status.latest_sync_time is None:
            continue
        latest_sync_time = datetime.fromisoformat(connection_status.latest_sync_time)
        if latest_sync_time.tzinfo is None:
            latest_sync_time = latest_sync_time.replace(tzinfo=timezone.utc)
        age_hours = (now - latest_sync_time).total_seconds() / 3600
        if age_hours <= recent_hours:
            recently_synced += 1

    completed_jobs = sum(
        connection_status.recent_successes + connection_status.recent_failures
        for connection_status in connection_statuses
    )
    recent_successes = sum(
        connection_status.recent_successes for connection_status in connection_statuses
    )
    success_rate = recent_successes / completed_jobs * 100 if completed_jobs else 0.0
    return {
        "total_connections": len(connection_statuses),
        "recently_synced_connections": recently_synced,
        "problem_connections": sum(
            1 for connection_status in connection_statuses if connection_status.is_problem
        ),
        "running_connections": sum(
            1 for connection_status in connection_statuses if connection_status.running_job_id
        ),
        "recent_success_rate": round(success_rate, 1),
        "recent_completed_jobs": completed_jobs,
        "recent_records_synced": sum(
            connection_status.recent_records_synced for connection_status in connection_statuses
        ),
        "recent_bytes_synced": sum(
            connection_status.recent_bytes_synced for connection_status in connection_statuses
        ),
    }


def _build_agent_text(
    *,
    agent_context: Literal["verbose", "summary", "min"],
    workspace_id: str,
    connection_statuses: list[WorkspaceConnectionSyncStatus],
    metric_summary: dict[str, int | float],
) -> str:
    """Build the bounded text response returned to the agent."""
    header = (
        "The user has already been shown an interactive workspace sync status "
        f"dashboard for workspace '{workspace_id}'. Do not reprint the full table."
    )
    summary = (
        f"Summary: {metric_summary['total_connections']} connections, "
        f"{metric_summary['recently_synced_connections']} recently synced, "
        f"{metric_summary['problem_connections']} problem connections, "
        f"{metric_summary['recent_success_rate']}% recent success rate, "
        f"{_format_records(int(metric_summary['recent_records_synced']))} records, "
        f"{_format_bytes(int(metric_summary['recent_bytes_synced']))} synced."
    )

    if agent_context == "min":
        return (
            f"{header}\n\n{summary}\n\n"
            "For drill-down, ask the user to select a row or call "
            "`show_connection_sync_history(connection_id=...)` "
            "with a connection ID from the dashboard."
        )

    if agent_context == "summary":
        problem_connections = [
            connection_status.connection_name
            for connection_status in connection_statuses
            if connection_status.is_problem
        ]
        return (
            f"{header}\n\n{summary}\n\n"
            f"Problem connection names: {', '.join(problem_connections[:10]) or 'None'}."
        )

    preview = [
        asdict(connection_status)
        for connection_status in connection_statuses[:WORKSPACE_SYNC_STATUS_AGENT_PREVIEW_LIMIT]
    ]
    return (
        f"{header}\n\n{summary}\n\n"
        "Agent-only capped connection preview:\n"
        f"{json.dumps(preview, indent=2)}"
    )


def _build_workspace_sync_status_app(
    *,
    workspace_id: str,
    workspace_url: str,
    connection_statuses: list[WorkspaceConnectionSyncStatus],
    metric_summary: dict[str, int | float],
    recent_hours: int,
) -> PrefabApp:
    """Build the interactive workspace sync status Prefab UI."""
    rows = [
        _connection_status_to_row(connection_status) for connection_status in connection_statuses
    ]
    status_pie_rows = _status_pie_rows(connection_statuses)
    problem_rows = [row for row in rows if row["is_problem"]]
    rows_by_status = {
        str(status_row["status"]): [
            row for row in rows if row["latest_status_label"] == status_row["status"]
        ]
        for status_row in status_pie_rows
    }

    with (
        PrefabApp(
            title="Workspace sync status",
            state={
                "workspace_id": workspace_id,
                "workspace_url": workspace_url,
                "connection_rows": rows,
                "filtered_connection_rows": rows,
                "active_status_filter": "All",
                "selected_connection": rows[0] if rows else None,
            },
        ) as app,
        Column(gap=4, css_class="p-6"),
    ):
        Heading("Workspace sync status")
        Text(
            "Overview of recent Airbyte Cloud sync health across workspace connections. "
            "Click a row for drill-down options.",
            css_class="text-sm text-muted-foreground",
        )
        with Row(gap=2):
            Button(
                "Open workspace",
                variant="outline",
                icon="external-link",
                onClick=OpenLink(workspace_url),
            )
        if connection_statuses:
            with Grid(columns=2, gap=4, css_class="mt-4 items-start"):
                _workspace_metric_cards(
                    metric_summary=metric_summary,
                    recent_hours=recent_hours,
                )
                _status_pie_section(
                    status_pie_rows=status_pie_rows,
                    rows_by_status=rows_by_status,
                    rows=rows,
                )
        else:
            with Div(css_class="mt-4"):
                _workspace_metric_cards(
                    metric_summary=metric_summary,
                    recent_hours=recent_hours,
                )
        if problem_rows:
            with Alert(variant="warning", icon="triangle-alert"):
                AlertTitle("Connections needing attention")
                AlertDescription(
                    f"{len(problem_rows)} connection(s) have failed, cancelled, or unknown status."
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
                    DataTableColumn(key="connection_name", header="Connection", sortable=True),
                    DataTableColumn(key="latest_status", header="Latest Status", sortable=True),
                    DataTableColumn(key="last_sync", header="Last Sync", sortable=True),
                    DataTableColumn(
                        key="recent_success_rate", header="Recent Success", sortable=True
                    ),
                    DataTableColumn(key="recent_failures", header="Failures", sortable=True),
                    DataTableColumn(key="source_name", header="Source", sortable=True),
                    DataTableColumn(key="destination_name", header="Destination", sortable=True),
                    DataTableColumn(key="recent_records", header="Records", sortable=True),
                    DataTableColumn(key="recent_bytes", header="Bytes", sortable=True),
                ],
                rows="{{ filtered_connection_rows }}",
                search=True,
                paginated=False,
                on_row_click=SetState("selected_connection", "{{ $event }}"),
            )
        with If("selected_connection"), Card():
            with CardHeader():
                CardTitle("{{ selected_connection.connection_name }}")
                Text(
                    "{{ selected_connection.connection_id }}",
                    css_class="text-sm text-muted-foreground font-mono",
                )
            with CardContent():
                with Grid(columns=2, gap=4):
                    _connection_detail("Latest status", "{{ selected_connection.latest_status }}")
                    _connection_detail("Last sync", "{{ selected_connection.last_sync }}")
                    _connection_detail("Source", "{{ selected_connection.source_name }}")
                    _connection_detail("Destination", "{{ selected_connection.destination_name }}")
                    _connection_detail(
                        "Suggested tool call",
                        "{{ selected_connection.suggested_tool_call }}",
                    )
                with Row(gap=2, css_class="mt-4"):
                    Button(
                        "Open connection",
                        variant="outline",
                        icon="external-link",
                        onClick=OpenLink("{{ selected_connection.connection_url }}"),
                    )
                    Button(
                        "Ask for sync history",
                        variant="secondary",
                        icon="message-square",
                        onClick=SendMessage(
                            "Show sync history for connection "
                            "{{ selected_connection.connection_id }} using "
                            '`show_connection_sync_history(connection_id="'
                            '{{ selected_connection.connection_id }}")`.'
                        ),
                    )

    return app


def _connection_status_to_row(
    connection_status: WorkspaceConnectionSyncStatus,
) -> _ConnectionStatusRow:
    """Convert a connection summary into a table row."""
    latest_status_label, _, _ = _status_pie_style(connection_status.latest_status)
    return {
        "connection_id": connection_status.connection_id,
        "connection_name": connection_status.connection_name,
        "connection_url": connection_status.connection_url,
        "source_name": connection_status.source_name,
        "source_url": connection_status.source_url,
        "destination_name": connection_status.destination_name,
        "destination_url": connection_status.destination_url,
        "latest_status": connection_status.latest_status,
        "latest_status_label": latest_status_label,
        "latest_job_id": connection_status.latest_job_id,
        "last_sync": connection_status.latest_sync_time or "No syncs found",
        "latest_records": _format_records(connection_status.latest_records_synced),
        "latest_bytes": _format_bytes(connection_status.latest_bytes_synced),
        "recent_jobs": connection_status.recent_jobs,
        "recent_success_rate": round(connection_status.success_rate, 1),
        "recent_failures": connection_status.recent_failures,
        "recent_records": _format_records(connection_status.recent_records_synced),
        "recent_bytes": _format_bytes(connection_status.recent_bytes_synced),
        "running_job_id": connection_status.running_job_id,
        "suggested_tool_call": connection_status.suggested_tool_call,
        "is_problem": connection_status.is_problem,
    }


def _workspace_metric_cards(
    *,
    metric_summary: dict[str, int | float],
    recent_hours: int,
) -> None:
    """Render workspace summary metrics."""
    with Column(gap=4):
        with Grid(columns=2, gap=4):
            Metric(label="Connections", value=str(metric_summary["total_connections"]))
            Metric(
                label=f"Synced in {recent_hours}h",
                value=str(metric_summary["recently_synced_connections"]),
            )
            Metric(
                label="Problem Connections",
                value=str(metric_summary["problem_connections"]),
                trend="down" if metric_summary["problem_connections"] else "neutral",
                trend_sentiment=(
                    "negative" if metric_summary["problem_connections"] else "neutral"
                ),
            )
            Metric(
                label="Recent Success Rate",
                value=f"{metric_summary['recent_success_rate']}%",
                trend=(
                    "up"
                    if metric_summary["recent_success_rate"] >= _SUCCESS_HIGH
                    else (
                        "down"
                        if metric_summary["recent_success_rate"] < _SUCCESS_LOW
                        else "neutral"
                    )
                ),
                trend_sentiment=(
                    "positive"
                    if metric_summary["recent_success_rate"] >= _SUCCESS_HIGH
                    else (
                        "negative"
                        if metric_summary["recent_success_rate"] < _SUCCESS_LOW
                        else "neutral"
                    )
                ),
            )
        with Grid(columns=2, gap=4):
            Metric(
                label="Recent Records Synced",
                value=_format_records(int(metric_summary["recent_records_synced"])),
            )
            Metric(
                label="Recent Bytes Synced",
                value=_format_bytes(int(metric_summary["recent_bytes_synced"])),
            )


def _status_pie_section(
    *,
    status_pie_rows: list[dict[str, int | str]],
    rows_by_status: dict[str, list[_ConnectionStatusRow]],
    rows: list[_ConnectionStatusRow],
) -> None:
    """Render status pie chart and table filters."""
    with Column(gap=3):
        with Div(style=_status_pie_chart_style(status_pie_rows)):
            PieChart(
                data=status_pie_rows,
                data_key="connections",
                name_key="status",
                height=360,
                inner_radius=87,
                padding_angle=2,
                show_label=True,
                show_legend=True,
                show_tooltip=True,
            )
        with Row(gap=3, css_class="flex-wrap"):
            for status_row in status_pie_rows:
                with Row(gap=1, css_class="items-center"):
                    Div(
                        style={
                            "width": "0.5rem",
                            "height": "0.5rem",
                            "borderRadius": "0.125rem",
                            "backgroundColor": str(status_row["color"]),
                            "flexShrink": "0",
                        },
                    )
                    Text(
                        f"{status_row['status']}: {status_row['connections']}",
                        css_class="text-xs text-muted-foreground",
                    )
        _status_filter_controls(
            status_pie_rows=status_pie_rows,
            rows_by_status=rows_by_status,
            rows=rows,
        )


def _status_filter_controls(
    *,
    status_pie_rows: list[dict[str, int | str]],
    rows_by_status: dict[str, list[_ConnectionStatusRow]],
    rows: list[_ConnectionStatusRow],
) -> None:
    """Render controls that filter the table by latest status."""
    with Row(gap=2, css_class="flex-wrap items-center"):
        Text("Filter table:", css_class="text-xs font-semibold text-muted-foreground")
        Button(
            "All",
            variant="outline",
            size="sm",
            onClick=[
                SetState("active_status_filter", "All"),
                SetState("filtered_connection_rows", rows),
                SetState("selected_connection", rows[0] if rows else None),
            ],
        )
        for status_row in status_pie_rows:
            status_label = str(status_row["status"])
            filtered_rows = rows_by_status[status_label]
            Button(
                f"{status_label} ({status_row['connections']})",
                variant=str(status_row["variant"]),
                size="sm",
                onClick=[
                    SetState("active_status_filter", status_label),
                    SetState("filtered_connection_rows", filtered_rows),
                    SetState(
                        "selected_connection",
                        filtered_rows[0] if filtered_rows else None,
                    ),
                ],
            )
    Text(
        "Active table filter: {{ active_status_filter }}",
        css_class="text-xs text-muted-foreground",
    )


def _status_pie_rows(
    connection_statuses: list[WorkspaceConnectionSyncStatus],
) -> list[dict[str, int | str]]:
    """Build connection counts by latest status for a pie chart."""
    status_counts = {label: 0 for label, _, _ in _STATUS_PIE_CATEGORIES}
    for connection_status in connection_statuses:
        label, _, _ = _status_pie_style(connection_status.latest_status)
        status_counts[label] += 1
    return [
        {
            "status": label,
            "connections": status_counts[label],
            "color": color,
            "fill": color,
            "variant": variant,
        }
        for label, color, variant in _STATUS_PIE_CATEGORIES
        if status_counts[label] or label in {"Succeeded", "Canceled", "No syncs"}
    ]


def _status_pie_chart_style(status_pie_rows: list[dict[str, int | str]]) -> dict[str, str]:
    """Build chart palette variables in the rendered slice order."""
    return {
        f"--color-chart-{index}": str(status_row["color"])
        for index, status_row in enumerate(status_pie_rows, start=1)
    }


def _status_pie_style(status: str) -> tuple[str, str, str]:
    """Return the display label, color, and dot variant for a sync status."""
    return _STATUS_PIE_STYLE_BY_STATUS.get(status, ("Other", "#64748b", "secondary"))


def _connection_detail(label: str, value: str) -> None:
    """Render a small label/value pair."""
    with Column(gap=1):
        Text(label, css_class="text-xs font-semibold uppercase text-muted-foreground")
        Text(value, css_class="text-sm break-all")
