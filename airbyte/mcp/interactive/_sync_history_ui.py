# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Interactive sync history MCP tool with Prefab UI charts."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Annotated, Literal

from fastmcp import Context  # noqa: TC002 - required at runtime for FastMCP tool registration
from fastmcp.apps import PrefabAppConfig
from fastmcp.tools.base import ToolResult
from prefab_ui.app import PrefabApp
from prefab_ui.components import (
    Accordion,
    AccordionItem,
    Column,
    DataTable,
    DataTableColumn,
    Grid,
    Heading,
    Link,
    Metric,
    Tab,
    Tabs,
    Text,
)
from prefab_ui.components.charts import BarChart, ChartSeries, LineChart
from pydantic import Field

from airbyte.mcp._tool_utils import INTERACTIVE_UI_ANNOTATION, mcp_tool
from airbyte.mcp.cloud import CLOUD_AUTH_TIP_TEXT, WORKSPACE_ID_TIP_TEXT, _get_cloud_workspace


if TYPE_CHECKING:
    from datetime import datetime

    from airbyte.cloud.workspaces import CloudWorkspace

_BYTES_GB = 1_000_000_000
_BYTES_MB = 1_000_000
_BYTES_KB = 1_000
_RECORDS_M = 1_000_000
_RECORDS_K = 1_000
_SUCCESS_HIGH = 90
_SUCCESS_LOW = 50


def _format_bytes(n: int) -> str:
    """Format byte count to human-readable string."""
    if n >= _BYTES_GB:
        return f"{n / _BYTES_GB:.1f} GB"
    if n >= _BYTES_MB:
        return f"{n / _BYTES_MB:.1f} MB"
    if n >= _BYTES_KB:
        return f"{n / _BYTES_KB:.1f} KB"
    return f"{n} B"


def _format_records(n: int) -> str:
    """Format record count to human-readable string."""
    if n >= _RECORDS_M:
        return f"{n / _RECORDS_M:.1f}M"
    if n >= _RECORDS_K:
        return f"{n / _RECORDS_K:.1f}K"
    return str(n)


def _time_label(dt: datetime, *, include_date: bool = False) -> str:
    """Format a datetime as a concise chart axis label."""
    if include_date:
        return dt.strftime("%m/%d %H:%M")
    return dt.strftime("%H:%M")


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
def show_connection_sync_history(  # noqa: PLR0914
    ctx: Context,
    connection_id: Annotated[
        str,
        Field(description="The ID of the Airbyte Cloud connection to show sync history for."),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ] = None,
    max_jobs: Annotated[
        int,
        Field(
            description=(
                "Maximum number of recent sync jobs to display. "
                "Defaults to 30. Maximum allowed value is 100."
            ),
            default=30,
            ge=1,
            le=100,
        ),
    ] = 30,
    agent_context: Annotated[
        Literal["verbose", "summary", "min"],
        Field(
            description=(
                "Controls how much context is returned to the agent in the text response. "
                "'verbose': full job-level data for detailed follow-up analysis. "
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
    """Show interactive sync history dashboard for an Airbyte Cloud connection.

    Renders a rich UI with metrics (success rate, total records, total bytes),
    charts (success/fail by date, records over time, bytes over time), and
    a detailed job history table.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)
    resolved_name = connection.name or connection_id
    job_history_url = connection.job_history_url
    source = connection.source
    destination = connection.destination

    sync_results = connection.get_previous_sync_logs(
        limit=max_jobs,
        from_tail=True,
    )

    jobs_data: list[dict[str, object]] = []
    for sr in sync_results:
        status = str(sr.get_job_status())
        jobs_data.append(
            {
                "job_id": sr.job_id,
                "status": status,
                "bytes_synced": sr.bytes_synced,
                "records_synced": sr.records_synced,
                "start_time": sr.start_time.isoformat(),
                "start_time_dt": sr.start_time,
                "job_url": sr.job_url,
            }
        )

    total_jobs = len(jobs_data)
    succeeded = sum(1 for j in jobs_data if "succeeded" in str(j["status"]).lower())
    success_rate = (succeeded / total_jobs * 100) if total_jobs > 0 else 0.0
    total_records = sum(int(j["records_synced"]) for j in jobs_data)  # type: ignore[arg-type]
    total_bytes = sum(int(j["bytes_synced"]) for j in jobs_data)  # type: ignore[arg-type]

    # Per-job chart data (oldest first) for continuous timeline
    jobs_chronological = list(reversed(jobs_data))
    multi_day = len({str(j["start_time_dt"])[:10] for j in jobs_chronological}) > 1  # type: ignore[index]
    chart_data: list[dict[str, int | str]] = [
        {
            "time": _time_label(j["start_time_dt"], include_date=multi_day),  # type: ignore[arg-type]
            "succeeded": 1 if "succeeded" in str(j["status"]).lower() else 0,
            "failed": 0 if "succeeded" in str(j["status"]).lower() else 1,
            "records": int(j["records_synced"]),  # type: ignore[arg-type]
            "bytes": int(j["bytes_synced"]),  # type: ignore[arg-type]
        }
        for j in jobs_chronological
    ]

    agent_text = _build_agent_text(
        agent_context=agent_context,
        connection_id=connection_id,
        connection_name=resolved_name,
        total_jobs=total_jobs,
        succeeded=succeeded,
        success_rate=success_rate,
        total_records=total_records,
        total_bytes=total_bytes,
        jobs_data=jobs_data,
    )

    if suppress_ui:
        return ToolResult(content=agent_text)

    return ToolResult(
        content=agent_text,
        structured_content=_build_sync_history_app(
            connection_name=resolved_name,
            job_history_url=job_history_url or "",
            source_name=source.name or "Source",
            source_url=source.connector_url or "",
            destination_name=destination.name or "Destination",
            destination_url=destination.connector_url or "",
            jobs_data=jobs_data,
            chart_data=chart_data,
            succeeded=succeeded,
            success_rate=success_rate,
            total_records=total_records,
            total_bytes=total_bytes,
        ),
    )


def _build_agent_text(  # noqa: PLR0913
    *,
    agent_context: Literal["verbose", "summary", "min"],
    connection_id: str,
    connection_name: str,
    total_jobs: int,
    succeeded: int,
    success_rate: float,
    total_records: int,
    total_bytes: int,
    jobs_data: list[dict[str, object]],
) -> str:
    """Build the text response returned to the agent (not shown to the user).

    The user has already been shown the interactive dashboard. This text is
    strictly for the agent's own context when answering follow-up questions.
    """
    header = (
        f"The user has already been shown an interactive sync history dashboard "
        f"for connection '{connection_name}' ({connection_id}). "
        f"Do not re-summarize or reprint this data — the user can already see it."
    )

    if agent_context == "min":
        followup_hint = (
            "To retrieve more detail without re-rendering the UI, call this tool again "
            "with suppress_ui=True and agent_context='verbose' or agent_context='summary'."
        )
        return (
            f"{header}\n\n"
            f"Summary: {total_jobs} jobs, {round(success_rate, 1)}% success rate.\n\n"
            f"{followup_hint}"
        )

    summary = (
        f"What the user sees: {total_jobs} total sync jobs, "
        f"{succeeded} succeeded, {round(success_rate, 1)}% success rate, "
        f"{_format_records(total_records)} records synced, "
        f"{_format_bytes(total_bytes)} bytes synced. "
        f"Charts show per-job success/failure, records over time, and bytes over time. "
        f"A data table lists all {total_jobs} jobs with IDs, statuses, and timestamps."
    )

    if agent_context == "summary":
        followup_hint = (
            "To retrieve more detail without re-rendering the UI, call this tool again "
            "with suppress_ui=True and agent_context='verbose'."
        )
        return f"{header}\n\n{summary}\n\n{followup_hint}"

    # verbose: include per-job data for detailed follow-up analysis
    preview_limit = 10
    jobs_preview = [
        {k: v for k, v in j.items() if k != "start_time_dt"} for j in jobs_data[:preview_limit]
    ]
    detail = (
        f"\n\nAgent-only context (first {min(total_jobs, preview_limit)} jobs "
        f"for follow-up analysis):\n{json.dumps(jobs_preview, indent=2)}"
    )
    return f"{header}\n\n{summary}{detail}"


def _build_sync_history_app(  # noqa: PLR0913
    *,
    connection_name: str,
    job_history_url: str,
    source_name: str,
    source_url: str,
    destination_name: str,
    destination_url: str,
    jobs_data: list[dict[str, object]],
    chart_data: list[dict[str, int | str]],
    succeeded: int,
    success_rate: float,
    total_records: int,
    total_bytes: int,
) -> PrefabApp:
    """Build the interactive sync history Prefab UI."""
    total_jobs = len(jobs_data)
    table_rows = [
        {
            "job_id": j["job_id"],
            "status": j["status"],
            "records": _format_records(int(j["records_synced"])),  # type: ignore[arg-type]
            "bytes": _format_bytes(int(j["bytes_synced"])),  # type: ignore[arg-type]
            "date": j["start_time"],
        }
        for j in jobs_data
    ]

    with (
        PrefabApp(
            title=f"Sync History \u2014 {connection_name}",
            state={"connection_name": connection_name},
        ) as app,
        Column(gap=4, css_class="p-6"),
    ):
        Heading(connection_name)
        Text(
            Link(connection_name, href=job_history_url),
            " (",
            Link(source_name, href=source_url),
            " \u2192 ",
            Link(destination_name, href=destination_url),
            ")",
            css_class="text-muted-foreground text-sm",
        )

        with Grid(columns=4, gap=4, css_class="mt-4"):
            Metric(
                label="Total Syncs",
                value=str(total_jobs),
            )
            Metric(
                label="Success Rate",
                value=f"{success_rate:.1f}%",
                delta=f"{succeeded} succeeded" if succeeded else None,
                trend=(
                    "up"
                    if success_rate >= _SUCCESS_HIGH
                    else ("down" if success_rate < _SUCCESS_LOW else "neutral")
                ),
                trend_sentiment=(
                    "positive"
                    if success_rate >= _SUCCESS_HIGH
                    else ("negative" if success_rate < _SUCCESS_LOW else "neutral")
                ),
            )
            Metric(
                label="Records Synced",
                value=_format_records(total_records),
            )
            Metric(
                label="Bytes Synced",
                value=_format_bytes(total_bytes),
            )

        with Tabs(css_class="mt-6"):
            with Tab(title="Success / Failure"):
                BarChart(
                    data=chart_data,
                    series=[
                        ChartSeries(data_key="succeeded", label="Succeeded", color="#22c55e"),
                        ChartSeries(data_key="failed", label="Failed", color="#ef4444"),
                    ],
                    x_axis="time",
                    stacked=True,
                    height=280,
                    show_legend=True,
                    show_tooltip=True,
                )
            with Tab(title="Records Synced"):
                LineChart(
                    data=chart_data,
                    series=[
                        ChartSeries(data_key="records", label="Records", color="#3b82f6"),
                    ],
                    x_axis="time",
                    height=280,
                    show_dots=True,
                    show_legend=True,
                    show_tooltip=True,
                    value_format="compact",
                )
            with Tab(title="Bytes Synced"):
                LineChart(
                    data=chart_data,
                    series=[
                        ChartSeries(data_key="bytes", label="Bytes", color="#8b5cf6"),
                    ],
                    x_axis="time",
                    height=280,
                    show_dots=True,
                    show_legend=True,
                    show_tooltip=True,
                    value_format="compact",
                )

        with Accordion(collapsible=True, css_class="mt-6"), AccordionItem("Job History"):
            DataTable(
                rows=table_rows,
                columns=[
                    DataTableColumn(key="job_id", header="Job ID"),
                    DataTableColumn(key="status", header="Status"),
                    DataTableColumn(key="records", header="Records"),
                    DataTableColumn(key="bytes", header="Bytes"),
                    DataTableColumn(key="date", header="Started"),
                ],
            )

    return app
