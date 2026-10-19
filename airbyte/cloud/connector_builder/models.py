# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Result models for Connector Builder projects in Airbyte Cloud."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


_HTTP_ERROR_STATUS_MIN = 400
_PROBLEM_LOG_LEVELS = frozenset({"WARN", "ERROR", "FATAL"})
_FAILING_LOG_LEVELS = frozenset({"ERROR", "FATAL"})
ManifestTextEditMode = Literal["replace_full", "replace_text", "replace_all"]
"""How `update_draft_manifest_text()` edits the manifest text.

- `replace_full`: `new_text` is the whole manifest.
- `replace_text`: replace the single exact match of `old_text`; fails on zero or several matches.
- `replace_all`: replace every exact match of `old_text`; fails on zero matches.
"""


class ConnectorBuilderPageSummary(BaseModel):
    """One HTTP page fetched during a Connector Builder test read."""

    url: str | None = None
    http_method: str | None = None
    status: int | None = None
    record_count: int = 0
    response_body: str | None = Field(
        default=None,
        description="The response body, included only for HTTP error responses.",
    )


def _record_count_warnings(
    record_count: int,
    pages: list[ConnectorBuilderPageSummary],
    *,
    test_read_limit_reached: bool,
) -> list[str]:
    """Return informational hints for record counts that may indicate a partial read."""
    if test_read_limit_reached:
        return []
    if record_count == 0:
        return ["Read 0 records. Check the request, record selector, and testing values."]

    warnings = []
    if record_count % 10 == 0:
        warnings.append(
            f"Read {record_count:,} records, a multiple of 10. A page size or API limit may "
            "have cut the read short; check the paginator."
        )

    record_pages = [page for page in pages if page.record_count > 0]
    if (
        record_pages
        and record_pages[0].record_count > 1
        and pages[-1].record_count > 0
        and pages[-1].record_count == record_pages[0].record_count
    ):
        record_count_per_page = record_pages[0].record_count
        warnings.append(
            f"The last page returned a full page of {record_count_per_page:,} records, the "
            "same as the first page, so more pages may exist; check the paginator."
        )
    return warnings


class ConnectorBuilderStreamReadResult(BaseModel):
    """Result of a Connector Builder test read of one stream."""

    stream_name: str
    record_count: int
    slice_count: int
    page_count: int
    test_read_limit_reached: bool
    records: list[dict[str, Any]]
    pages: list[ConnectorBuilderPageSummary]
    logs: list[dict[str, Any]]
    inferred_schema: dict[str, Any] | None = None
    warnings: list[str] = Field(
        default_factory=list,
        description=(
            "Hints that the read may be incomplete, such as round record counts. "
            "They don't fail the read."
        ),
    )

    @classmethod
    def from_api_response(
        cls,
        stream_name: str,
        response: dict[str, Any],
    ) -> ConnectorBuilderStreamReadResult:
        """Build a result from a `read_stream` API response."""
        records: list[dict[str, Any]] = []
        pages: list[ConnectorBuilderPageSummary] = []
        slices = response.get("slices") or []
        for slice_ in slices:
            for page in slice_.get("pages") or []:
                page_records = page.get("records") or []
                records.extend(page_records)
                request = page.get("request") or {}
                http_response = page.get("response") or {}
                status = http_response.get("status")
                pages.append(
                    ConnectorBuilderPageSummary(
                        url=request.get("url"),
                        http_method=request.get("http_method"),
                        status=status,
                        record_count=len(page_records),
                        response_body=(
                            http_response.get("body")
                            if isinstance(status, int) and status >= _HTTP_ERROR_STATUS_MIN
                            else None
                        ),
                    )
                )

        test_read_limit_reached = bool(response.get("test_read_limit_reached"))
        return cls(
            stream_name=stream_name,
            record_count=len(records),
            slice_count=len(slices),
            page_count=len(pages),
            test_read_limit_reached=test_read_limit_reached,
            records=records,
            pages=pages,
            logs=[
                {"level": log.get("level"), "message": log.get("message")}
                for log in response.get("logs") or []
            ],
            inferred_schema=response.get("inferred_schema"),
            warnings=_record_count_warnings(
                len(records),
                pages,
                test_read_limit_reached=test_read_limit_reached,
            ),
        )


class ConnectorBuilderResolvedManifest(BaseModel):
    """A manifest resolved by Airbyte Cloud, with dynamic streams expanded."""

    manifest: dict[str, Any]
    stream_names: list[str]

    @classmethod
    def from_manifest(cls, manifest: dict[str, Any]) -> ConnectorBuilderResolvedManifest:
        """Build a result from a resolved manifest."""
        return cls(
            manifest=manifest,
            stream_names=[
                stream["name"]
                for stream in manifest.get("streams") or []
                if isinstance(stream, dict) and isinstance(stream.get("name"), str)
            ],
        )


class ConnectorBuilderProjectInfo(BaseModel):
    """A snapshot of a Connector Builder project, read with a single project request."""

    builder_project_id: str
    definition_id: str | None
    connector_builder_project_url: str
    name: str
    has_draft: bool
    draft_manifest: dict[str, Any] | None
    draft_manifest_text: str | None
    published_manifest: dict[str, Any] | None


class ConnectorBuilderStreamReadiness(BaseModel):
    """Readiness of one stream, from a Connector Builder test read."""

    stream_name: str
    failed: bool = Field(
        description=(
            "True when the test read raised an error, returned HTTP errors, or logged errors."
        ),
    )
    record_count: int = 0
    page_count: int = 0
    read_to_end: bool = Field(
        default=False,
        description=(
            "True when the stream ended within the test-read caps without HTTP errors. False "
            "means a record, page, or slice cap was hit, an HTTP error occurred, or the read "
            "did not run (see `error`)."
        ),
    )
    http_errors: list[ConnectorBuilderPageSummary] = Field(default_factory=list)
    problem_logs: list[dict[str, Any]] = Field(default_factory=list)
    inferred_schema: dict[str, Any] | None = None
    warnings: list[str] = Field(
        default_factory=list,
        description=(
            "Hints that the read may be incomplete, such as round record counts. "
            "They don't fail the read."
        ),
    )
    error: str | None = Field(
        default=None,
        description="The error raised by the test read itself, if it failed to run.",
    )

    @classmethod
    def from_stream_read(
        cls, result: ConnectorBuilderStreamReadResult
    ) -> ConnectorBuilderStreamReadiness:
        """Summarize errors and results from a stream test read."""
        http_errors = [
            page
            for page in result.pages
            if page.status is not None and page.status >= _HTTP_ERROR_STATUS_MIN
        ]
        problem_logs = [log for log in result.logs if log.get("level") in _PROBLEM_LOG_LEVELS]
        has_errors = bool(http_errors) or any(
            log.get("level") in _FAILING_LOG_LEVELS for log in problem_logs
        )
        return cls(
            stream_name=result.stream_name,
            failed=has_errors,
            record_count=result.record_count,
            page_count=result.page_count,
            read_to_end=not result.test_read_limit_reached and not http_errors,
            http_errors=http_errors,
            problem_logs=problem_logs,
            inferred_schema=result.inferred_schema,
            warnings=result.warnings,
        )


ReadinessCheckResult = Literal["passed", "failed", "unknown"]
"""Outcome of a readiness check; `unknown` when the report can't tell."""


class ConnectorBuilderReadinessCheck(BaseModel):
    """One readiness check toward a finished custom source."""

    description: str
    result: ReadinessCheckResult
    detail: str | None = None


class ConnectorBuilderFullSync(BaseModel):
    """A full sync started for a published Connector Builder project."""

    source_id: str
    destination_id: str
    destination_created: bool
    connection_id: str
    connection_created: bool
    job_id: int
    job_url: str


class ConnectorBuilderReadinessReport(BaseModel):
    """Readiness checks and per-stream test reads for a Connector Builder draft."""

    failed_streams: list[str] = Field(default_factory=list)
    pending_streams: list[str] = Field(
        default_factory=list,
        description=(
            "Streams still reading when `timeout_seconds` ran out. Their results are dropped, "
            "and Cloud may still finish those reads."
        ),
    )
    not_attempted_streams: list[str] = Field(
        default_factory=list,
        description="Streams not started before `timeout_seconds` ran out.",
    )
    checks: list[ConnectorBuilderReadinessCheck]
    next_steps: list[str]
    streams: list[ConnectorBuilderStreamReadiness]
    full_sync: ConnectorBuilderFullSync | None = Field(
        default=None,
        description="The full sync started with `run_full_sync=True`, if any.",
    )
