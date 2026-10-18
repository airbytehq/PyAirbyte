# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Connector Builder projects in Airbyte Cloud.

A Connector Builder project holds an unpublished draft manifest and the testing values
(including secrets) used to test it. Test reads and manifest resolution run on Airbyte Cloud,
so no connector code runs on the caller's machine.
"""

from __future__ import annotations

import difflib
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import yaml
from pydantic import BaseModel, Field

from airbyte import exceptions as exc
from airbyte._util import api_util
from airbyte._util.api_util import get_web_url_root


if TYPE_CHECKING:
    from airbyte.cloud.connectors import CustomCloudSourceDefinition
    from airbyte.cloud.workspaces import CloudWorkspace


_HTTP_ERROR_STATUS_MIN = 400
_MANIFEST_KEYS = frozenset({"streams", "definitions", "check"})
_YAML_LINE_WIDTH = 2**31 - 1

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
    """Build the connector spec for publishing, with camelCase `connectionSpecification`."""
    spec = dict(manifest.get("spec") or {})
    spec["connectionSpecification"] = spec.get("connection_specification") or {}
    return spec


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


class CloudConnectorBuilderProject:
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

    def _require_draft_manifest(self) -> dict[str, Any]:
        manifest = self.draft_manifest
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
        response = api_util.read_connector_builder_project_stream(
            workspace_id=self.owner_workspace_id,
            builder_project_id=self.builder_project_id,
            manifest=self._require_draft_manifest(),
            stream_name=stream_name,
            record_limit=record_limit,
            page_limit=page_limit,
            slice_limit=slice_limit,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )
        return ConnectorBuilderStreamReadResult.from_api_response(stream_name, response)

    def resolve_manifest(
        self,
        *,
        stream_limit: int = 5,
    ) -> ConnectorBuilderResolvedManifest:
        """Resolve the draft manifest on Airbyte Cloud, including dynamic streams.

        Uses the project's saved testing values to generate dynamic streams.

        Args:
            stream_limit: Maximum generated streams to return per dynamic stream (1 to 100).
        """
        resolved_manifest = api_util.full_resolve_connector_builder_project_manifest(
            workspace_id=self.owner_workspace_id,
            builder_project_id=self.builder_project_id,
            manifest=self._require_draft_manifest(),
            stream_limit=stream_limit,
            api_root=self.workspace.api_root,
            client_id=self.workspace.client_id,
            client_secret=self.workspace.client_secret,
            bearer_token=self.workspace.bearer_token,
            config_api_root=self.workspace.config_api_root,
        )
        return ConnectorBuilderResolvedManifest.from_manifest(resolved_manifest)

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

    def __repr__(self) -> str:
        """Return a string representation of the project."""
        return f"CloudConnectorBuilderProject(builder_project_id={self.builder_project_id!r})"


__all__ = [
    "CloudConnectorBuilderProject",
    "ConnectorBuilderPageSummary",
    "ConnectorBuilderProjectInfo",
    "ConnectorBuilderResolvedManifest",
    "ConnectorBuilderStreamReadResult",
    "ManifestTextEditMode",
    "apply_manifest_text_edit",
    "parse_manifest_yaml",
    "render_manifest_text",
]
