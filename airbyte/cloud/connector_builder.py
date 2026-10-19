# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Connector Builder projects in Airbyte Cloud.

A Connector Builder project holds an unpublished draft manifest and the testing values
(including secrets) used to test it. Test reads and manifest resolution run on Airbyte Cloud,
so no connector code runs on the caller's machine.
"""

from __future__ import annotations

import difflib
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import requests
import yaml
from pydantic import BaseModel, Field

from airbyte import exceptions as exc
from airbyte._util import api_util
from airbyte._util.api_util import get_web_url_root


if TYPE_CHECKING:
    from airbyte.cloud.connectors import CloudSource, CustomCloudSourceDefinition
    from airbyte.cloud.workspaces import CloudWorkspace


_HTTP_ERROR_STATUS_MIN = 400
_PROBLEM_LOG_LEVELS = frozenset({"WARN", "ERROR", "FATAL"})
_FAILING_LOG_LEVELS = frozenset({"ERROR", "FATAL"})
_MANIFEST_KEYS = frozenset({"streams", "definitions", "check"})
_YAML_LINE_WIDTH = 2**31 - 1
_READINESS_MAX_WORKERS = 5
"""Most streams `run_readiness_checks()` reads at once."""
_MIN_READ_TIMEOUT = 1.0
"""Shortest HTTP timeout for a test read started just before the readiness deadline."""
_SECRETS_MASK = "**********"
"""Placeholder Cloud returns instead of a stored secret value."""
_FULL_SYNC_STREAM_LIMIT = 100
"""Most streams per dynamic stream selected for the full sync (Cloud's resolve maximum)."""
CONNECTOR_BUILDER_TEST_DESTINATION_NAME = "Connector Builder Test (No-op)"
"""Name of the no-op destination that full syncs find or create by default."""

ManifestTextEditMode = Literal["replace_full", "replace_text", "replace_all"]
"""How `update_draft_manifest_text()` edits the manifest text.

- `replace_full`: `new_text` is the whole manifest.
- `replace_text`: replace the single exact match of `old_text`; fails on zero or several matches.
- `replace_all`: replace every exact match of `old_text`; fails on zero matches.
"""


def _unwrap_builder_manifest(value: object) -> dict[str, Any] | None:
    """Return the connector manifest from a builder manifest payload.

    Some builder projects store their draft as a `DeclarativeSourceManifest`
    envelope (`{"manifest": {...}, "spec": ..., "version": ...}`) rather than
    the bare manifest; unwrap that envelope so the result matches the shape of
    `manifest`.
    """
    if not isinstance(value, dict):
        return None
    nested = value.get("manifest")
    if isinstance(nested, dict) and not any(key in value for key in _MANIFEST_KEYS):
        return nested
    return value


def render_manifest_text(manifest: dict[str, Any]) -> str:
    """Render a manifest as deterministic YAML text, without line wrapping.

    Airbyte Cloud stores the parsed manifest, not its original text, so comments and
    formatting are not preserved. Text edits match against this rendering.
    """
    return yaml.safe_dump(
        manifest,
        sort_keys=False,
        allow_unicode=True,
        width=_YAML_LINE_WIDTH,
    )


def apply_manifest_text_edit(
    current_text: str,
    *,
    edit_mode: ManifestTextEditMode,
    new_text: str,
    old_text: str | None = None,
) -> str:
    """Return `current_text` after applying one text edit.

    Args:
        current_text: The text to edit.
        edit_mode: See `ManifestTextEditMode`.
        new_text: The whole new text for `replace_full`, else the replacement for `old_text`.
        old_text: The exact text to replace. Required unless `edit_mode` is `replace_full`.
    """
    if edit_mode == "replace_full":
        if old_text is not None:
            raise exc.AirbyteLibInputError(
                message="`old_text` is not allowed when `edit_mode` is `replace_full`.",
            )
        return new_text

    if not old_text:
        raise exc.AirbyteLibInputError(
            message=f"`old_text` is required when `edit_mode` is `{edit_mode}`.",
        )
    match_count = current_text.count(old_text)
    if match_count == 0:
        raise exc.AirbyteLibInputError(
            message="`old_text` was not found in the draft manifest text.",
            guidance="Copy `old_text` verbatim from the latest draft manifest text.",
        )
    if edit_mode == "replace_text" and match_count > 1:
        raise exc.AirbyteLibInputError(
            message=f"`old_text` matches {match_count} places in the draft manifest text.",
            guidance=(
                "Include more surrounding text in `old_text` so it matches once, "
                "or use `replace_all` to replace every match."
            ),
            context={"match_count": match_count},
        )
    return current_text.replace(old_text, new_text)


def parse_manifest_yaml(manifest_yaml: dict[str, Any] | Path | str) -> dict[str, Any]:
    """Return a manifest dict from a dict, a YAML file path, or YAML text."""
    if isinstance(manifest_yaml, Path):
        manifest_yaml = manifest_yaml.read_text()
    if isinstance(manifest_yaml, str):
        manifest_yaml = yaml.safe_load(manifest_yaml)
    if not isinstance(manifest_yaml, dict):
        raise exc.AirbyteLibInputError(message="The manifest must be a YAML mapping.")
    return manifest_yaml


def _builder_testing_values_spec(manifest: dict[str, Any]) -> dict[str, Any]:
    """Build the `spec` object that the testing-values API expects, as the Builder UI does."""
    manifest_spec = manifest.get("spec") or {}
    return {
        "documentationUrl": manifest_spec.get("documentation_url"),
        "connectionSpecification": manifest_spec.get("connection_specification") or {},
        "advancedAuth": manifest_spec.get("advanced_auth"),
    }


def _publish_spec(manifest: dict[str, Any]) -> dict[str, Any]:
    """Build the connector spec for publishing, with the API's camelCase field names."""
    return {**(manifest.get("spec") or {}), **_builder_testing_values_spec(manifest)}


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

        return cls(
            stream_name=stream_name,
            record_count=len(records),
            slice_count=len(slices),
            page_count=len(pages),
            test_read_limit_reached=bool(response.get("test_read_limit_reached")),
            records=records,
            pages=pages,
            logs=[
                {"level": log.get("level"), "message": log.get("message")}
                for log in response.get("logs") or []
            ],
            inferred_schema=response.get("inferred_schema"),
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


def _draft_from_project_data(project_data: dict[str, Any]) -> dict[str, Any] | None:
    if not project_data["builderProject"].get("hasDraft"):
        return None
    declarative_manifest = project_data.get("declarativeManifest") or {}
    return _unwrap_builder_manifest(declarative_manifest.get("manifest"))


class ConnectorBuilderStreamReadiness(BaseModel):
    """Readiness of one stream, from a Connector Builder test read."""

    stream_name: str
    passed: bool
    record_count: int = 0
    page_count: int = 0
    read_to_end: bool = Field(
        default=False,
        description=(
            "True when the stream ended within the test-read caps. False means the read hit a "
            "record, page, or slice cap, so later pages were never read."
        ),
    )
    http_errors: list[ConnectorBuilderPageSummary] = Field(default_factory=list)
    problem_logs: list[dict[str, Any]] = Field(default_factory=list)
    inferred_schema: dict[str, Any] | None = None
    error: str | None = Field(
        default=None,
        description="The error raised by the test read itself, if it failed to run.",
    )

    @classmethod
    def from_stream_read(
        cls, result: ConnectorBuilderStreamReadResult
    ) -> ConnectorBuilderStreamReadiness:
        """Judge a stream from its test read: records returned, no HTTP or error logs."""
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
            passed=result.record_count > 0 and not has_errors,
            record_count=result.record_count,
            page_count=result.page_count,
            read_to_end=not result.test_read_limit_reached,
            http_errors=http_errors,
            problem_logs=problem_logs,
            inferred_schema=result.inferred_schema,
        )


class ConnectorBuilderChecklistItem(BaseModel):
    """One step toward a finished custom source."""

    step: str
    done: bool | None = Field(
        description="True or False when known; None when this report can't tell.",
    )
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
    """Checklist and per-stream test reads for a Connector Builder draft."""

    passed: bool = Field(
        description=(
            "True when every named stream passed and was read to the end. Covers only the "
            "named streams; a full sync validates the whole connector."
        ),
    )
    passed_streams: list[str] = Field(default_factory=list)
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
    checklist: list[ConnectorBuilderChecklistItem]
    next_steps: list[str]
    streams: list[ConnectorBuilderStreamReadiness]
    full_sync: ConnectorBuilderFullSync | None = Field(
        default=None,
        description="The full sync started with `run_full_sync=True`, if any.",
    )


def _readiness_next_steps(
    checklist: list[ConnectorBuilderChecklistItem],
    streams: list[ConnectorBuilderStreamReadiness],
    pending_streams: list[str],
    not_attempted_streams: list[str],
) -> list[str]:
    steps = [item.step for item in checklist[:4] if item.done is not True]
    failed = [stream.stream_name for stream in streams if not stream.passed]
    capped = [stream.stream_name for stream in streams if stream.passed and not stream.read_to_end]
    if failed:
        steps.append(f"Fix the failed streams and test them again: {', '.join(failed)}")
    if pending_streams:
        steps.append(
            "These streams were still reading at the timeout, so their results were dropped. "
            "Retry them with a longer `timeout_seconds`, or check them with the full sync: "
            f"{', '.join(pending_streams)}"
        )
    if not_attempted_streams and not failed:
        steps.append(
            "Run the report again with the not-attempted streams, skipping any already "
            f"validated or unchanged since: {', '.join(not_attempted_streams)}"
        )
    if not (failed or pending_streams or not_attempted_streams):
        steps.append(
            "All named streams passed. Once every stream is validated, publish the custom "
            "source definition (if not yet) and run a full sync of every stream to a no-op "
            "destination."
        )
    if capped:
        steps.append(
            f"These streams hit a test-read cap; check them with the full sync: {', '.join(capped)}"
        )
    return steps


def _readiness_checklist(
    project_data: dict[str, Any],
    draft: dict[str, Any],
    streams: list[ConnectorBuilderStreamReadiness],
    pending_streams: list[str],
    url: str,
) -> list[ConnectorBuilderChecklistItem]:
    spec = (draft.get("spec") or {}).get("connection_specification")
    takes_no_inputs = isinstance(spec, dict) and not spec.get("properties")
    testing_values = project_data.get("testingValues") or {}
    source_definition_id = project_data["builderProject"].get("sourceDefinitionId")
    failed = [stream.stream_name for stream in streams if not stream.passed]
    not_read_to_end = [stream.stream_name for stream in streams if not stream.read_to_end]
    return [
        ConnectorBuilderChecklistItem(
            step="Save a draft manifest",
            done=bool(project_data["builderProject"].get("hasDraft")),
        ),
        ConnectorBuilderChecklistItem(
            step="Define the connection spec (user inputs)",
            done=isinstance(spec, dict),
        ),
        ConnectorBuilderChecklistItem(
            step="Define at least one stream",
            done=bool(draft.get("streams")) or bool(draft.get("dynamic_streams")),
        ),
        ConnectorBuilderChecklistItem(
            step="Set testing values (enter secrets in the Connector Builder UI)",
            done=bool(testing_values) or takes_no_inputs,
            detail=url,
        ),
        ConnectorBuilderChecklistItem(
            step="Pass a test read on the named streams",
            done=not failed and not pending_streams,
            detail="; ".join(
                part
                for part in (
                    f"failed: {', '.join(failed)}" if failed else "",
                    f"pending: {', '.join(pending_streams)}" if pending_streams else "",
                )
                if part
            )
            or None,
        ),
        ConnectorBuilderChecklistItem(
            step="Read the named streams to the end within the test-read caps",
            done=not not_read_to_end and not pending_streams,
            detail=(
                f"hit a cap: {', '.join(not_read_to_end)}. Check these with a full sync "
                "after publishing."
                if not_read_to_end
                else None
            ),
        ),
        ConnectorBuilderChecklistItem(
            step="Publish the custom source definition",
            done=source_definition_id is not None,
            detail=source_definition_id,
        ),
        ConnectorBuilderChecklistItem(
            step="Run a full sync of every stream to a no-op destination",
            done=None,
            detail="Not tracked by this report. Run it after publishing.",
        ),
    ]


def _drop_masked_secrets(value: Any) -> Any:  # noqa: ANN401
    """Return a copy of `value` without the masked secret fields Cloud returns."""
    if isinstance(value, dict):
        return {
            key: _drop_masked_secrets(nested)
            for key, nested in value.items()
            if nested != _SECRETS_MASK
        }
    if isinstance(value, list):
        return [_drop_masked_secrets(nested) for nested in value]
    return value


def _capped_dynamic_streams(resolved_manifest: dict[str, Any], stream_limit: int) -> list[str]:
    """Return the dynamic streams whose generated streams reached `stream_limit` when resolved."""
    counts: Counter[str] = Counter(
        stream["dynamic_stream_name"]
        for stream in resolved_manifest.get("streams") or []
        if isinstance(stream, dict) and isinstance(stream.get("dynamic_stream_name"), str)
    )
    return sorted(name for name, count in counts.items() if count >= stream_limit)


def _spec_missing_required(schema: Any, config: dict[str, Any]) -> bool:  # noqa: ANN401
    """Return whether `config` lacks a required top-level field that has no default."""
    if not isinstance(schema, dict):
        return False
    properties = schema.get("properties") or {}
    return any(
        name not in config and "default" not in (properties.get(name) or {})
        for name in schema.get("required") or []
    )


def _spec_has_secrets(schema: Any) -> bool:  # noqa: ANN401
    """Return whether a connection spec marks any field `airbyte_secret`."""
    if isinstance(schema, dict):
        return schema.get("airbyte_secret") is True or any(
            _spec_has_secrets(nested) for nested in schema.values()
        )
    if isinstance(schema, list):
        return any(_spec_has_secrets(nested) for nested in schema)
    return False


class CloudConnectorBuilderProject:  # noqa: PLR0904  # Builder project API facade.
    """A Connector Builder project in an Airbyte Cloud workspace."""

    def __init__(
        self,
        workspace: CloudWorkspace,
        builder_project_id: str,
        *,
        owner_workspace_id: str | None = None,
    ) -> None:
        """Initialize a Connector Builder project object. Does not call the API.

        Args:
            workspace: The workspace whose credentials are used.
            builder_project_id: The Connector Builder project ID.
            owner_workspace_id: The workspace that owns the project, if it differs from
                `workspace` (for example, a definition shared from another workspace).
        """
        self.workspace = workspace
        self.builder_project_id = builder_project_id
        self.owner_workspace_id = owner_workspace_id or workspace.workspace_id

    @property
    def url(self) -> str:
        """The project's Connector Builder URL, where users enter testing values and secrets."""
        return (
            f"{get_web_url_root(self.workspace.api_root)}/workspaces/"
            f"{self.owner_workspace_id}/connector-builder/edit/{self.builder_project_id}"
        )

    def get_project_data(self) -> dict[str, Any]:
        """Fetch the project metadata and draft manifest from the API."""
        return api_util.get_connector_builder_project(
            workspace_id=self.owner_workspace_id,
            builder_project_id=self.builder_project_id,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )

    @property
    def name(self) -> str:
        """The project name."""
        return self.get_project_data()["builderProject"]["name"]

    @property
    def source_definition_id(self) -> str | None:
        """The ID of the custom source definition, or `None` if never published."""
        return self.get_project_data()["builderProject"].get("sourceDefinitionId")

    def _published_manifest(self, source_definition_id: str | None) -> dict[str, Any] | None:
        if source_definition_id is None:
            return None
        return self.workspace.get_custom_source_definition(
            source_definition_id,
            definition_type="yaml",
        ).manifest

    @property
    def published_manifest(self) -> dict[str, Any] | None:
        """The active manifest of the published definition, or `None` if never published."""
        return self._published_manifest(self.source_definition_id)

    def get_info(self) -> ConnectorBuilderProjectInfo:
        """Read the project once and return its IDs, URL, draft, and published manifest."""
        project_data = self.get_project_data()
        draft_manifest = _draft_from_project_data(project_data)
        source_definition_id = project_data["builderProject"].get("sourceDefinitionId")
        return ConnectorBuilderProjectInfo(
            builder_project_id=self.builder_project_id,
            definition_id=source_definition_id,
            connector_builder_project_url=self.url,
            name=project_data["builderProject"]["name"],
            has_draft=draft_manifest is not None,
            draft_manifest=draft_manifest,
            draft_manifest_text=(
                render_manifest_text(draft_manifest) if draft_manifest is not None else None
            ),
            published_manifest=self._published_manifest(source_definition_id),
        )

    @property
    def draft_manifest(self) -> dict[str, Any] | None:
        """The unpublished draft manifest, or `None` if the project has no draft."""
        return _draft_from_project_data(self.get_project_data())

    def _require_draft_manifest(self, project_data: dict[str, Any] | None = None) -> dict[str, Any]:
        manifest = _draft_from_project_data(project_data or self.get_project_data())
        if manifest is None:
            raise exc.AirbyteLibInputError(
                message="This Connector Builder project has no draft manifest.",
                guidance="Save a draft manifest to the project first.",
                context={"builder_project_id": self.builder_project_id},
            )
        return manifest

    def set_draft_manifest(
        self,
        manifest_yaml: dict[str, Any] | Path | str,
        *,
        pre_validate: bool = True,
    ) -> None:
        """Replace the project's draft manifest. Does not publish anything.

        Args:
            manifest_yaml: The manifest, as a dict, a YAML file path, or YAML text.
            pre_validate: Whether to validate the manifest client-side before saving.
        """
        manifest = parse_manifest_yaml(manifest_yaml)
        if pre_validate:
            api_util.validate_yaml_manifest(manifest, raise_on_error=True)

        api_util.update_connector_builder_project(
            workspace_id=self.owner_workspace_id,
            builder_project_id=self.builder_project_id,
            name=self.name,
            draft_manifest=manifest,
            components_file_content=None,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )

    @property
    def draft_manifest_text(self) -> str | None:
        """The draft manifest rendered by `render_manifest_text()`, or `None` if no draft."""
        manifest = self.draft_manifest
        return render_manifest_text(manifest) if manifest is not None else None

    def update_draft_manifest_text(
        self,
        *,
        edit_mode: ManifestTextEditMode,
        new_text: str,
        old_text: str | None = None,
        pre_validate: bool = True,
    ) -> str:
        """Edit the draft manifest as text and save it. Does not publish anything.

        Edits match against `draft_manifest_text`. Returns a unified diff from the previous
        rendering to the rendering of the saved draft.

        Args:
            edit_mode: See `ManifestTextEditMode`.
            new_text: The whole new manifest for `replace_full`, else the replacement text.
            old_text: The exact text to replace. Required unless `edit_mode` is `replace_full`.
            pre_validate: Whether to validate the edited manifest client-side before saving.
        """
        current_text = self.draft_manifest_text
        if current_text is None and edit_mode != "replace_full":
            raise exc.AirbyteLibInputError(
                message="This Connector Builder project has no draft manifest to edit.",
                guidance="Use `replace_full` to save a whole manifest first.",
                context={"builder_project_id": self.builder_project_id},
            )
        edited_text = apply_manifest_text_edit(
            current_text or "",
            edit_mode=edit_mode,
            new_text=new_text,
            old_text=old_text,
        )
        self.set_draft_manifest(edited_text, pre_validate=pre_validate)

        saved_text = self.draft_manifest_text or ""
        return "".join(
            difflib.unified_diff(
                (current_text or "").splitlines(keepends=True),
                saved_text.splitlines(keepends=True),
                fromfile="previous",
                tofile="saved",
            )
        )

    def set_testing_values(
        self,
        testing_values: dict[str, Any],
    ) -> None:
        """Replace the project's testing values, which Cloud stores in its secret storage.

        The spec comes from the draft manifest, so save a draft first. Fields marked
        `airbyte_secret` in the spec are stored as secrets.
        """
        manifest = self._require_draft_manifest()
        api_util.update_connector_builder_project_testing_values(
            workspace_id=self.owner_workspace_id,
            builder_project_id=self.builder_project_id,
            testing_values=testing_values,
            spec=_builder_testing_values_spec(manifest),
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )

    def test_read_stream(
        self,
        stream_name: str,
        *,
        record_limit: int = 100,
        page_limit: int = 5,
        slice_limit: int = 5,
    ) -> ConnectorBuilderStreamReadResult:
        """Test-read one stream of the draft manifest on Airbyte Cloud.

        Uses the project's saved testing values as the connector config.

        Args:
            stream_name: The stream to read.
            record_limit: Maximum records to return (1 to 10,000).
            page_limit: Maximum pages to fetch per slice (1 to 20).
            slice_limit: Maximum slices to read (1 to 20).
        """
        return self._test_read_stream(
            self._require_draft_manifest(),
            stream_name,
            record_limit=record_limit,
            page_limit=page_limit,
            slice_limit=slice_limit,
        )

    def _test_read_stream(
        self,
        manifest: dict[str, Any],
        stream_name: str,
        *,
        record_limit: int,
        page_limit: int,
        slice_limit: int,
        timeout_seconds: float | None = None,
    ) -> ConnectorBuilderStreamReadResult:
        response = api_util.read_connector_builder_project_stream(
            workspace_id=self.owner_workspace_id,
            builder_project_id=self.builder_project_id,
            manifest=manifest,
            stream_name=stream_name,
            record_limit=record_limit,
            page_limit=page_limit,
            slice_limit=slice_limit,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
            timeout_seconds=timeout_seconds,
        )
        return ConnectorBuilderStreamReadResult.from_api_response(stream_name, response)

    def resolve_manifest(
        self,
        *,
        stream_limit: int = 5,
        manifest: dict[str, Any] | None = None,
    ) -> ConnectorBuilderResolvedManifest:
        """Resolve the draft manifest on Airbyte Cloud, including dynamic streams.

        Uses the project's saved testing values to generate dynamic streams.

        Args:
            stream_limit: Maximum generated streams to return per dynamic stream (1 to 100).
            manifest: Manifest to resolve instead of the draft.
        """
        resolved_manifest = api_util.full_resolve_connector_builder_project_manifest(
            workspace_id=self.owner_workspace_id,
            builder_project_id=self.builder_project_id,
            manifest=manifest if manifest is not None else self._require_draft_manifest(),
            stream_limit=stream_limit,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )
        return ConnectorBuilderResolvedManifest.from_manifest(resolved_manifest)

    def run_readiness_checks(
        self,
        stream_names: list[str],
        *,
        record_limit: int = 10_000,
        page_limit: int = 20,
        slice_limit: int = 20,
        timeout_seconds: float | None = None,
        run_full_sync: bool = False,
        test_source_id: str | None = None,
        test_destination_id: str | None = None,
    ) -> ConnectorBuilderReadinessReport:
        """Check the draft's progress and test-read the given streams on Airbyte Cloud.

        Reads run in parallel. A stream passes when its test read returns records with no
        HTTP errors or error logs, and is `read_to_end` when it ended within the test-read
        caps (which default to Cloud's maximums). The checklist is derived from the project,
        so nothing is stored. `passed` covers only the named streams: each must pass and be
        read to the end. A full sync after publishing validates the whole connector.

        Args:
            stream_names: Streams to test-read (at least 1). For
                dynamic streams, get the generated names from `resolve_manifest()`.
            record_limit: Maximum records to read per stream (1 to 10,000).
            page_limit: Maximum pages to fetch per slice (1 to 20).
            slice_limit: Maximum slices to read (1 to 20).
            timeout_seconds: Return after this many seconds with the streams finished so far;
                the rest are listed in `pending_streams` and `not_attempted_streams`. No limit
                by default.
            run_full_sync: If every named stream finished and passed (or none are named),
                publish the draft and start a full sync with `run_full_sync()`.
            test_source_id: The source for the full sync. See `run_full_sync()`.
            test_destination_id: The destination for the full sync. See `run_full_sync()`.
        """
        stream_names = list(dict.fromkeys(stream_names))
        if not stream_names and not run_full_sync:
            raise exc.AirbyteLibInputError(
                message="Pass at least 1 stream name, or set `run_full_sync`.",
            )
        project_data = self.get_project_data()
        if not stream_names:
            # A full sync alone can run on the published manifest; the first publish clears
            # the draft.
            manifest = _draft_from_project_data(project_data) or self._published_manifest(
                project_data["builderProject"].get("sourceDefinitionId")
            )
            if manifest is None:
                manifest = self._require_draft_manifest(project_data)
            return self._readiness_report(project_data, manifest, [], [], []).model_copy(
                update={
                    "full_sync": self.run_full_sync(
                        test_source_id=test_source_id,
                        test_destination_id=test_destination_id,
                    ),
                },
            )

        draft = self._require_draft_manifest(project_data)
        deadline = None if timeout_seconds is None else time.monotonic() + timeout_seconds

        def read(stream_name: str) -> ConnectorBuilderStreamReadiness:
            # Bound each HTTP request by the deadline so worker threads end with the call.
            request_timeout = (
                None if deadline is None else max(deadline - time.monotonic(), _MIN_READ_TIMEOUT)
            )
            try:
                result = self._test_read_stream(
                    draft,
                    stream_name,
                    record_limit=record_limit,
                    page_limit=page_limit,
                    slice_limit=slice_limit,
                    timeout_seconds=request_timeout,
                )
            except exc.AirbyteLibError as ex:
                error = ex.get_message()
                if ex.guidance:
                    error += f" {ex.guidance}"
                return ConnectorBuilderStreamReadiness(
                    stream_name=stream_name, passed=False, error=error
                )
            except requests.RequestException as ex:
                return ConnectorBuilderStreamReadiness(
                    stream_name=stream_name,
                    passed=False,
                    error=f"Request to Airbyte Cloud failed: {type(ex).__name__}",
                )
            return ConnectorBuilderStreamReadiness.from_stream_read(result)

        pool = ThreadPoolExecutor(max_workers=min(_READINESS_MAX_WORKERS, len(stream_names)))
        try:
            futures = [pool.submit(read, name) for name in stream_names]
            done, _ = wait(futures, timeout=timeout_seconds)
        finally:
            pool.shutdown(wait=False, cancel_futures=True)
        streams = [future.result() for future in futures if future in done]
        unfinished = [
            (name, future)
            for name, future in zip(stream_names, futures, strict=True)
            if future not in done
        ]
        pending_streams = [name for name, future in unfinished if not future.cancelled()]
        not_attempted_streams = [name for name, future in unfinished if future.cancelled()]
        report = self._readiness_report(
            project_data, draft, streams, pending_streams, not_attempted_streams
        )
        if run_full_sync and not (
            report.failed_streams or report.pending_streams or report.not_attempted_streams
        ):
            report.full_sync = self.run_full_sync(
                test_source_id=test_source_id,
                test_destination_id=test_destination_id,
            )
        return report

    def _readiness_report(
        self,
        project_data: dict[str, Any],
        draft: dict[str, Any],
        streams: list[ConnectorBuilderStreamReadiness],
        pending_streams: list[str],
        not_attempted_streams: list[str],
    ) -> ConnectorBuilderReadinessReport:
        remaining = pending_streams + not_attempted_streams
        checklist = _readiness_checklist(project_data, draft, streams, remaining, self.url)
        return ConnectorBuilderReadinessReport(
            passed=not remaining
            and all(stream.passed and stream.read_to_end for stream in streams),
            passed_streams=[stream.stream_name for stream in streams if stream.passed],
            failed_streams=[stream.stream_name for stream in streams if not stream.passed],
            pending_streams=pending_streams,
            not_attempted_streams=not_attempted_streams,
            checklist=checklist,
            next_steps=_readiness_next_steps(
                checklist, streams, pending_streams, not_attempted_streams
            ),
            streams=streams,
        )

    def publish(
        self,
        name: str,
        *,
        pre_validate: bool = True,
    ) -> CustomCloudSourceDefinition:
        """Publish the draft manifest as a custom source definition.

        The first publish creates the definition with `name`. If the project is already
        published, the draft becomes a new active version of the existing definition.
        """
        manifest = self._require_draft_manifest()
        source_definition_id = self.source_definition_id
        if source_definition_id is not None:
            definition = self.workspace.get_custom_source_definition(
                source_definition_id,
                definition_type="yaml",
            )
            return definition.update_definition(manifest_yaml=manifest, pre_validate=pre_validate)

        if pre_validate:
            api_util.validate_yaml_manifest(manifest, raise_on_error=True)

        source_definition_id = api_util.publish_connector_builder_project(
            workspace_id=self.owner_workspace_id,
            builder_project_id=self.builder_project_id,
            name=name,
            manifest=manifest,
            spec=_publish_spec(manifest),
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )
        return self.workspace.get_custom_source_definition(
            source_definition_id,
            definition_type="yaml",
        )

    @property
    def test_source_name(self) -> str:
        """Name of the source that `create_test_source()` creates for full syncs."""
        return f"{self.name} (Connector Builder test)"

    def find_test_source(self) -> CloudSource | None:
        """Return the source made by `create_test_source()` for this project, if it exists."""
        source_definition_id = self.source_definition_id
        if source_definition_id is None:
            return None
        return next(
            (
                source
                for source in self.workspace.list_sources(name=self.test_source_name)
                if source.definition_id == source_definition_id
            ),
            None,
        )

    def create_test_source(self) -> CloudSource:
        """Create a source on the published definition for full syncs.

        The source gets the project's non-secret testing values. When the spec has secret
        fields, the source is a draft (deferred credentials): a person enters the secrets at
        its `connector_url`, then tests and saves. Publish the project first.
        """
        project_data = self.get_project_data()
        source_definition_id = project_data["builderProject"].get("sourceDefinitionId")
        if source_definition_id is None:
            raise exc.AirbyteLibInputError(
                message="Publish the Connector Builder project before creating a test source.",
            )
        # The first publish clears the draft, so fall back to the published manifest.
        manifest = _draft_from_project_data(project_data) or self._published_manifest(
            source_definition_id
        )
        spec = ((manifest or {}).get("spec") or {}).get("connection_specification")
        config = _drop_masked_secrets(project_data.get("testingValues") or {})
        return self.workspace.deploy_source(
            name=self.test_source_name,
            source=config,
            definition_id=source_definition_id,
            defer_credentials=_spec_has_secrets(spec) or _spec_missing_required(spec, config),
        )

    def run_full_sync(
        self,
        *,
        test_source_id: str | None = None,
        test_destination_id: str | None = None,
        pre_validate: bool = True,
    ) -> ConnectorBuilderFullSync:
        """Publish the draft and start a full sync of every stream to a no-op destination.

        Without a draft, syncs the published version. Fails if a dynamic stream generates more
        streams than resolve returns (100), since the sync couldn't select them all. Returns
        without waiting for the sync to finish.

        Args:
            test_source_id: Source on the published definition to sync from. Defaults to the
                source from `create_test_source()`.
            test_destination_id: Destination to sync to. Defaults to the workspace's
                `CONNECTOR_BUILDER_TEST_DESTINATION_NAME` destination, created as a no-op
                destination if missing.
            pre_validate: Whether to validate the manifest client-side before publishing.
        """
        project_data = self.get_project_data()
        manifest = _draft_from_project_data(project_data)
        if manifest is None:
            manifest = self._published_manifest(
                project_data["builderProject"].get("sourceDefinitionId")
            )
            if manifest is None:
                self._require_draft_manifest(project_data)
        # Resolve before publishing: the first publish clears the draft.
        resolved = self.resolve_manifest(stream_limit=_FULL_SYNC_STREAM_LIMIT, manifest=manifest)
        capped = _capped_dynamic_streams(resolved.manifest, _FULL_SYNC_STREAM_LIMIT)
        if capped:
            raise exc.AirbyteLibInputError(
                message=(
                    f"Dynamic streams generate {_FULL_SYNC_STREAM_LIMIT} or more streams, "
                    "more than a full sync can select."
                ),
                guidance="Sync the published source with `run_cloud_sync` instead.",
                context={"dynamic_streams": capped},
            )
        if _draft_from_project_data(project_data) is not None:
            self.publish(self.name, pre_validate=pre_validate)
        source_definition_id = self.source_definition_id
        if test_source_id is None:
            test_source = self.find_test_source()
            if test_source is None:
                raise exc.AirbyteLibInputError(
                    message="No test source found for this Connector Builder project.",
                    guidance=(
                        "Pass `test_source_id`, or create one with `create_test_source()` "
                        "and have the user complete its settings."
                    ),
                )
            test_source_id = test_source.connector_id
        elif self.workspace.get_source(test_source_id).definition_id != source_definition_id:
            raise exc.AirbyteLibInputError(
                message="`test_source_id` isn't a source of this project's published definition.",
                context={"test_source_id": test_source_id, "definition_id": source_definition_id},
            )
        destination_created = False
        if test_destination_id is None:
            test_destination_id, destination_created = self._get_or_create_test_destination()

        stream_names = resolved.stream_names
        connection_name = f"{self.name} (Connector Builder full sync)"
        connection = next(
            (
                connection
                for connection in self.workspace.list_connections(name=connection_name)
                if connection.source_id == test_source_id
                and connection.destination_id == test_destination_id
            ),
            None,
        )
        connection_created = connection is None
        if connection is None:
            connection = self.workspace.deploy_connection(
                connection_name,
                source=test_source_id,
                destination=test_destination_id,
                selected_streams=stream_names,
            )
        else:
            connection.set_selected_streams(stream_names)
        sync_result = connection.run_sync(wait=False)
        return ConnectorBuilderFullSync(
            source_id=test_source_id,
            destination_id=test_destination_id,
            destination_created=destination_created,
            connection_id=connection.connection_id,
            connection_created=connection_created,
            job_id=sync_result.job_id,
            job_url=sync_result.job_url,
        )

    def _get_or_create_test_destination(self) -> tuple[str, bool]:
        """Return the test destination's ID and whether this call created it."""
        existing = self.workspace.list_destinations(name=CONNECTOR_BUILDER_TEST_DESTINATION_NAME)
        noop = [
            destination
            for destination in existing
            if destination.definition_id == api_util.DEV_NULL_DESTINATION_DEFINITION_ID
        ]
        if noop:
            return noop[0].connector_id, False
        if existing:
            raise exc.AirbyteLibInputError(
                message=(
                    f"Destination `{CONNECTOR_BUILDER_TEST_DESTINATION_NAME}` exists "
                    "but isn't a no-op (dev-null) destination."
                ),
                guidance="Pass `test_destination_id`, or rename that destination.",
                context={"destination_ids": [destination.connector_id for destination in existing]},
            )
        from airbyte.destinations.util import get_noop_destination  # noqa: PLC0415

        return self.workspace.deploy_destination(
            name=CONNECTOR_BUILDER_TEST_DESTINATION_NAME,
            destination=get_noop_destination(),
        ).connector_id, True

    def __repr__(self) -> str:
        """Return a string representation of the project."""
        return f"CloudConnectorBuilderProject(builder_project_id={self.builder_project_id!r})"


__all__ = [
    "CONNECTOR_BUILDER_TEST_DESTINATION_NAME",
    "CloudConnectorBuilderProject",
    "ConnectorBuilderFullSync",
    "ConnectorBuilderChecklistItem",
    "ConnectorBuilderPageSummary",
    "ConnectorBuilderReadinessReport",
    "ConnectorBuilderStreamReadiness",
    "ConnectorBuilderProjectInfo",
    "ConnectorBuilderResolvedManifest",
    "ConnectorBuilderStreamReadResult",
    "ManifestTextEditMode",
    "apply_manifest_text_edit",
    "parse_manifest_yaml",
    "render_manifest_text",
]
