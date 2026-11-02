# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Airbyte Cloud Connector Builder MCP operations.

.. include:: ../../docs/mcp-generated/connector_builder.md
"""

# No public Python API — MCP primitives are registered via decorators and
# documented via the generated Markdown include above.
__all__: list[str] = []

from typing import TYPE_CHECKING, Annotated, Any

from fastmcp import Context, FastMCP
from fastmcp_extensions import TraceArg, register_mcp_tools
from pydantic import BaseModel, Field

from airbyte.cloud.connector_builder._models import (
    ConnectorBuilderPublishedVersion,
    ConnectorBuilderReadinessReport,
    ConnectorBuilderStreamReadResult,
    ManifestTextEditMode,
)
from airbyte.cloud.connector_builder._util import (
    TEST_READ_MAX_PAGES,
    TEST_READ_MAX_RECORDS,
    TEST_READ_MAX_SLICES,
)
from airbyte.exceptions import (
    AirbyteLibInputError,
)
from airbyte.mcp._arg_resolvers import (
    resolve_connector_config,
    resolve_list_of_strings,
    resolve_manifest_yaml,
)
from airbyte.mcp._error_handling import redact_agent_text
from airbyte.mcp._guards import is_trusted_execution_enabled
from airbyte.mcp._tool_utils import (
    AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET,
    mcp_tool,
    register_guid_created_in_session,
)


if TYPE_CHECKING:
    from airbyte.cloud.workspaces import CloudWorkspace

from airbyte.mcp.cloud import (
    WORKSPACE_ID_TIP_TEXT,
    _add_defaults_for_exclude_args,
    _check_builder_project_created_in_session,
    _get_cloud_workspace,
    _manifest_yaml_source_hint,
)


_BUILDER_PROJECT_ID_DESCRIPTION = (
    "The Connector Builder project ID. Provide exactly one of `builder_project_id` or "
    "`definition_id`."
)
_BUILDER_DEFINITION_ID_DESCRIPTION = (
    "The custom source definition ID, for a project that has been published. Provide exactly "
    "one of `builder_project_id` or `definition_id`."
)
_TEST_READ_RECORDS_PREVIEW_LIMIT = 10


class ConnectorBuilderDraftManifestResult(BaseModel):
    """Draft and published manifests of a Connector Builder project."""

    builder_project_id: str
    """The Connector Builder project ID."""
    definition_id: str | None
    """The published custom source definition ID, or `None` if never published."""
    connector_builder_project_url: str
    """The project's Connector Builder URL."""
    name: str
    """The project name."""
    has_draft: bool
    """Whether the project has a saved draft manifest."""
    draft_manifest: dict[str, Any] | None
    """The draft manifest, or `None` without a draft."""
    draft_manifest_yaml: str | None
    """The draft manifest as YAML text, or `None` without a draft."""
    published_manifest: dict[str, Any] | None
    """The published manifest, or `None` if never published."""


class ConnectorBuilderProjectCreateResult(BaseModel):
    """Result of creating a Connector Builder project."""

    builder_project_id: str
    """The new Connector Builder project ID."""
    definition_id: str | None
    """The published custom source definition ID, set only with `create_test_source`."""
    connector_builder_project_url: str
    """The project's Connector Builder URL."""
    name: str
    """The project name."""
    test_source_id: str | None = None
    """The test source ID, set only with `create_test_source`."""
    test_source_settings_url: str | None = None
    """Cloud settings page of the test source, set only with `create_test_source`."""
    next_steps: list[str] = Field(default_factory=list)
    """Credential-entry steps, or an empty list if no test source was created."""


class ConnectorBuilderStreamTestReadToolResult(ConnectorBuilderStreamReadResult):
    """Test read of one stream, with credentials redacted and a capped record preview."""

    builder_project_id: str
    """The Connector Builder project ID."""


class ConnectorBuilderResolvedManifestResult(BaseModel):
    """Resolved draft manifest of a Connector Builder project."""

    builder_project_id: str
    """The Connector Builder project ID."""
    stream_names: list[str]
    """Stream names in the resolved manifest, including generated dynamic streams."""
    resolved_manifest: dict[str, Any]
    """The resolved manifest."""


class ConnectorBuilderPublishedVersionsResult(BaseModel):
    """Published manifest versions of a Connector Builder project."""

    builder_project_id: str
    """The Connector Builder project ID."""
    definition_id: str | None
    """The published custom source definition ID, or `None` if never published."""
    versions: list[ConnectorBuilderPublishedVersion]
    """Published versions, oldest first. Empty if never published."""


class ConnectorBuilderPublishedManifestResult(BaseModel):
    """One published manifest version of a Connector Builder project."""

    builder_project_id: str
    """The Connector Builder project ID."""
    definition_id: str
    """The published custom source definition ID."""
    version: int | None
    """The returned version; `None` when the active version was requested."""
    manifest_yaml: str
    """The published manifest as YAML text."""


class ConnectorBuilderRevertResult(BaseModel):
    """Result of reverting a Connector Builder project to a published version."""

    builder_project_id: str
    """The Connector Builder project ID."""
    definition_id: str
    """The published custom source definition ID."""
    reverted_to_version: int
    """The published version whose manifest was restored."""
    as_draft: bool
    """Whether the version was saved as the draft instead of published."""
    active_version: int | None
    """The active published version after the revert."""


class ConnectorBuilderReadinessReportResult(ConnectorBuilderReadinessReport):
    """Readiness report of a Connector Builder project, with credentials redacted."""

    builder_project_id: str
    """The Connector Builder project ID."""
    connector_builder_project_url: str
    """The project's Connector Builder URL."""


@mcp_tool(
    read_only=True,
    idempotent=True,
)
def get_connector_builder_draft_manifest(
    ctx: Context,
    definition_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_DEFINITION_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    *,
    builder_project_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_PROJECT_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
        TraceArg.OMIT,
    ],
) -> ConnectorBuilderDraftManifestResult:
    """Get the Connector Builder draft manifest of a project or custom source definition.

    Returns the working draft manifest that has been saved but not yet published, both as
    a dict and as `draft_manifest_yaml`. Airbyte Cloud stores the parsed manifest, so the text
    is a normalized YAML rendering without comments; text edits with
    `update_connector_builder_manifest_text` match against it.

    If no draft exists, 'has_draft' will be False and the draft fields will be None.
    The published manifest is included for comparison, or None if never published.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    project = workspace.get_connector_builder_project(
        builder_project_id,
        definition_id=definition_id,
    )
    return ConnectorBuilderDraftManifestResult.model_validate(
        project._get_info().model_dump(by_alias=True),  # noqa: SLF001
    )


@mcp_tool(
    extra_help_text=_manifest_yaml_source_hint,
    insiders_only=True,
)
def create_connector_builder_project(
    ctx: Context,
    name: Annotated[
        str,
        Field(description="The name for the Connector Builder project."),
        TraceArg.FINGERPRINT,
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
        TraceArg.OMIT,
    ],
    manifest_yaml_text: Annotated[
        str | None,
        Field(
            description=(
                "Optional initial draft. Full YAML contents of the declarative (low-code) "
                "connector manifest: the file's text, not a filename or path."
            ),
            default=None,
        ),
    ] = None,
    manifest_yaml_file: Annotated[
        str | None,
        Field(
            description=(
                "Optional initial draft. Absolute path to a manifest YAML file on the machine "
                "running this MCP server."
            ),
            default=None,
        ),
    ] = None,
    pre_validate: Annotated[
        bool,
        Field(
            description="Whether to validate the manifest client-side before saving.",
            default=True,
        ),
    ] = True,
    create_test_source: Annotated[
        bool,
        Field(
            description=(
                "Optional. Publish the initial manifest (which needs its spec) and create a "
                "test source on it for the full sync. Use this for connectors that need "
                "credentials."
            ),
            default=False,
        ),
    ] = False,
) -> ConnectorBuilderProjectCreateResult:
    """Create a Connector Builder project to develop a custom YAML source.

    The project holds the draft manifest and the testing values used for test reads.
    Nothing appears in the workspace's source list until the project is published with
    `publish_custom_source_definition`.

    For a connector that needs credentials, include the spec in the initial manifest, with
    secret fields marked `airbyte_secret`, and set `create_test_source`. Put the credentials
    under one `credentials` object whose `oneOf` lists the auth options the API supports.
    Each option has a constant `auth_type` and its own fields, for example `api_key`
    (`api_key`), `access_token` (`access_token`), or `oauth2.0` (`client_id`,
    `client_secret`, `refresh_token`). With more than one option, use a
    `SelectiveAuthenticator` with `authenticator_selection_path: ["credentials", "auth_type"]`.
    The test source is saved as a draft, so auth processes not tested yet don't block it.
    Then ask the user to enter their credentials in both places listed in `next_steps`. Never
    ask for secrets in the chat.
    """
    processed_manifest = resolve_manifest_yaml(
        manifest_yaml_text=manifest_yaml_text,
        manifest_yaml_file=manifest_yaml_file,
    )
    if create_test_source and processed_manifest is None:
        raise AirbyteLibInputError(
            message="`create_test_source` needs an initial manifest to publish.",
            guidance="Pass `manifest_yaml_text` or `manifest_yaml_file`.",
        )
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    project = workspace.create_connector_builder_project(
        name,
        manifest_yaml=processed_manifest,
        pre_validate=pre_validate,
    )
    register_guid_created_in_session(project.builder_project_id)
    result = ConnectorBuilderProjectCreateResult(
        builder_project_id=project.builder_project_id,
        definition_id=None,
        connector_builder_project_url=project.connector_builder_project_url,
        name=name,
    )
    if create_test_source:
        definition = project.publish(name, pre_validate=pre_validate)
        register_guid_created_in_session(definition.definition_id)
        test_source = project._get_or_create_default_test_source()  # noqa: SLF001
        register_guid_created_in_session(test_source.connector_id)
        result.definition_id = definition.definition_id
        result.test_source_id = test_source.connector_id
        result.test_source_settings_url = test_source.connector_url
        result.next_steps = [
            "Ask the user to enter their credentials in the Connector Builder testing "
            f"values: {project.connector_builder_project_url}",
            "Ask the user to enter the same credentials in the test source, then test and "
            f"save it: {test_source.connector_url}",
        ]
    return result


@mcp_tool(
    destructive=True,
    insiders_only=True,
)
def update_connector_builder_manifest_text(
    ctx: Context,
    edit_mode: Annotated[
        ManifestTextEditMode,
        Field(
            description=(
                "`replace_full`: `new_text` is the whole manifest. "
                "`replace_text`: replace the single exact match of `old_text`; fails if "
                "`old_text` matches zero or several places (add surrounding text to make it "
                "unique). `replace_all`: replace every exact match of `old_text`."
            ),
        ),
    ],
    new_text: Annotated[
        str,
        Field(
            description=(
                "The whole manifest YAML for `replace_full`, else the text that replaces "
                "`old_text`."
            ),
        ),
    ],
    *,
    old_text: Annotated[
        str | None,
        Field(
            description=(
                "The exact text to replace, copied verbatim from `draft_manifest_yaml`. "
                "Required unless `edit_mode` is `replace_full`."
            ),
            default=None,
        ),
    ] = None,
    builder_project_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_PROJECT_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    definition_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_DEFINITION_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
        TraceArg.OMIT,
    ],
    pre_validate: Annotated[
        bool,
        Field(
            description="Whether to validate the edited manifest client-side before saving.",
            default=True,
        ),
    ] = True,
) -> str:
    """Edit and save a Connector Builder project's draft manifest. Does not publish anything.

    Text edits match against `draft_manifest_yaml` from `get_connector_builder_draft_manifest`.
    Returns a unified diff from the previous text to the saved text; base later edits on the
    saved text.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    project = workspace.get_connector_builder_project(
        builder_project_id,
        definition_id=definition_id,
    )
    _check_builder_project_created_in_session(project)
    diff = project._update_draft_manifest_yaml(  # noqa: SLF001
        edit_mode=edit_mode,
        new_text=new_text,
        old_text=old_text,
        pre_validate=pre_validate,
    )
    return (
        f"Saved the draft manifest of Connector Builder project {project.builder_project_id}."
        "\n\n" + (diff or "No changes.")
    )


@mcp_tool(
    destructive=True,
    idempotent=True,
    insiders_only=True,
)
def set_connector_builder_testing_values(
    ctx: Context,
    *,
    builder_project_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_PROJECT_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    definition_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_DEFINITION_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
        TraceArg.OMIT,
    ],
    nonsecret_testing_values: Annotated[
        dict | str,
        Field(
            description=(
                "Non-secret configuration values for test reads, as a JSON object or JSON "
                "string. Merged onto the saved testing values, so secrets the user entered in "
                "the Connector Builder are kept. Values for `airbyte_secret` fields are rejected."
            ),
        ),
    ],
) -> str:
    """Update the testing values (connector config) of a Connector Builder project.

    Test reads use these values. Airbyte Cloud stores fields marked `airbyte_secret` in the
    draft manifest's spec in its secret storage. Requires a saved draft manifest.
    Don't ask users to paste secrets into the chat: send them the returned project URL to
    enter secrets in the Connector Builder's testing values form instead.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    project = workspace.get_connector_builder_project(
        builder_project_id,
        definition_id=definition_id,
    )
    _check_builder_project_created_in_session(project)
    testing_values_dict = resolve_connector_config(
        config=nonsecret_testing_values,
        config_spec_jsonschema=project._testing_values_connection_spec(),  # noqa: SLF001
    )
    project._merge_testing_values(testing_values_dict)  # noqa: SLF001
    return (
        f"Saved testing values for Connector Builder project {project.builder_project_id}. "
        f"Users can review them or enter secrets at: {project.connector_builder_project_url}"
    )


def _redact_stream_diagnostics(stream_data: dict[str, Any]) -> dict[str, Any]:
    """Redact credentials from a test read's page URLs, error bodies, logs, and error text."""
    for page in [*stream_data.get("pages", []), *stream_data.get("http_errors", [])]:
        for key in ("url", "response_body"):
            if page.get(key):
                page[key] = redact_agent_text(page[key])
    for log in [*stream_data.get("logs", []), *stream_data.get("problem_logs", [])]:
        if isinstance(log.get("message"), str):
            log["message"] = redact_agent_text(log["message"])
    if stream_data.get("error"):
        stream_data["error"] = redact_agent_text(stream_data["error"])
    return stream_data


@mcp_tool(
    read_only=True,
    external_access=True,
    insiders_only=True,
)
def run_connector_builder_stream_test_read(
    ctx: Context,
    stream_name: Annotated[
        str,
        Field(description="The name of the stream to test-read."),
        TraceArg.FINGERPRINT,
    ],
    *,
    builder_project_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_PROJECT_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    definition_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_DEFINITION_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
        TraceArg.OMIT,
    ],
    record_limit: Annotated[
        int,
        Field(
            description=f"Maximum records to read (1 to {TEST_READ_MAX_RECORDS:,}).",
            default=100,
            ge=1,
            le=TEST_READ_MAX_RECORDS,
        ),
        TraceArg.VALUE,
    ] = 100,
    page_limit: Annotated[
        int,
        Field(
            description=f"Maximum pages to fetch per slice (1 to {TEST_READ_MAX_PAGES}).",
            default=5,
            ge=1,
            le=TEST_READ_MAX_PAGES,
        ),
        TraceArg.VALUE,
    ] = 5,
    slice_limit: Annotated[
        int,
        Field(
            description=f"Maximum slices to read (1 to {TEST_READ_MAX_SLICES}).",
            default=5,
            ge=1,
            le=TEST_READ_MAX_SLICES,
        ),
        TraceArg.VALUE,
    ] = 5,
) -> ConnectorBuilderStreamTestReadToolResult:
    """Test-read one stream of a Connector Builder project's draft manifest on Airbyte Cloud.

    Uses the project's saved testing values as the connector config. Returns counts, a
    per-page summary of HTTP requests (with response bodies for HTTP errors), connector logs,
    the inferred schema, and a preview of the first records (`records` holds at most
    10 records; `record_count` is the total read). `test_read_limit_reached` true means later
    records weren't read. Cloud stops at page boundaries, so `record_count` can exceed
    `record_limit`.
    `warnings` flags reads that may be incomplete (round record counts, a full last page);
    check them, but they don't fail the read.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    project = workspace.get_connector_builder_project(
        builder_project_id,
        definition_id=definition_id,
    )
    result = project.run_stream_test_read(
        stream_name,
        record_limit=record_limit,
        page_limit=page_limit,
        slice_limit=slice_limit,
    )
    return ConnectorBuilderStreamTestReadToolResult.model_validate(
        {
            **_redact_stream_diagnostics(result.model_dump()),
            "records": result.records[:_TEST_READ_RECORDS_PREVIEW_LIMIT],
            "builder_project_id": project.builder_project_id,
        }
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    external_access=True,
    insiders_only=True,
)
def get_connector_builder_resolved_manifest(
    ctx: Context,
    *,
    builder_project_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_PROJECT_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    definition_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_DEFINITION_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
        TraceArg.OMIT,
    ],
    stream_limit: Annotated[
        int,
        Field(
            description="Maximum generated streams per dynamic stream (1 to 100).",
            default=5,
            ge=1,
            le=100,
        ),
        TraceArg.VALUE,
    ] = 5,
) -> ConnectorBuilderResolvedManifestResult:
    """Resolve a Connector Builder project's draft manifest on Airbyte Cloud.

    Expands `$ref`s, `$parameters`, and dynamic streams (using the project's testing values),
    and lists the resulting stream names.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    project = workspace.get_connector_builder_project(
        builder_project_id,
        definition_id=definition_id,
    )
    resolved = project.get_resolved_manifest(stream_limit=stream_limit)
    return ConnectorBuilderResolvedManifestResult(
        builder_project_id=project.builder_project_id,
        stream_names=resolved.stream_names,
        resolved_manifest=resolved.manifest_dict,
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    insiders_only=True,
)
def list_connector_builder_published_versions(
    ctx: Context,
    *,
    builder_project_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_PROJECT_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    definition_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_DEFINITION_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
        TraceArg.OMIT,
    ],
) -> ConnectorBuilderPublishedVersionsResult:
    """List the published manifest versions of a Connector Builder project, oldest first.

    Each version has `version`, `is_active` and `description`. Get a version's manifest with
    `get_connector_builder_published_manifest`.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    project = workspace.get_connector_builder_project(
        builder_project_id,
        definition_id=definition_id,
    )
    return ConnectorBuilderPublishedVersionsResult(
        builder_project_id=project.builder_project_id,
        definition_id=project.source_definition_id,
        versions=project.list_published_versions(),
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    insiders_only=True,
)
def get_connector_builder_published_manifest(
    ctx: Context,
    *,
    builder_project_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_PROJECT_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    definition_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_DEFINITION_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
        TraceArg.OMIT,
    ],
    version: Annotated[
        int | None,
        Field(
            description="Optional published version. Defaults to the active version.",
            default=None,
            ge=1,
        ),
        TraceArg.VALUE,
    ] = None,
) -> ConnectorBuilderPublishedManifestResult:
    """Get a published manifest version of a Connector Builder project as YAML text."""
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    project = workspace.get_connector_builder_project(
        builder_project_id,
        definition_id=definition_id,
    )
    source_definition_id = project._require_source_definition_id()  # noqa: SLF001
    return ConnectorBuilderPublishedManifestResult(
        builder_project_id=project.builder_project_id,
        definition_id=source_definition_id,
        version=version,
        manifest_yaml=project.get_published_manifest_yaml(version) or "",
    )


@mcp_tool(
    destructive=True,
    insiders_only=True,
)
def revert_connector_builder_published_version(
    ctx: Context,
    *,
    version: Annotated[
        int,
        Field(
            description="The published version to revert to.",
            ge=1,
        ),
        TraceArg.VALUE,
    ],
    builder_project_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_PROJECT_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    definition_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_DEFINITION_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
        TraceArg.OMIT,
    ],
    as_draft: Annotated[
        bool,
        Field(
            description=(
                "True saves the version as the pending draft (overwriting unpublished edits) "
                "and leaves the active version alone. False publishes it as a new active "
                "version, which fails if the draft has unpublished edits."
            ),
            default=True,
        ),
        TraceArg.VALUE,
    ] = True,
    description: Annotated[
        str | None,
        Field(
            description=(
                "Optional release note for the new version when `as_draft` is false. "
                "Defaults to a generic note with the reverted version and the UTC time."
            ),
            default=None,
            max_length=256,
        ),
        TraceArg.FINGERPRINT,
    ] = None,
) -> ConnectorBuilderRevertResult:
    """Revert a Connector Builder project to a published manifest version.

    List versions with `list_connector_builder_published_versions`.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    project = workspace.get_connector_builder_project(
        builder_project_id,
        definition_id=definition_id,
    )
    _check_builder_project_created_in_session(project)
    source_definition_id = project._require_source_definition_id()  # noqa: SLF001
    project.revert_to_published_version(
        version,
        as_draft=as_draft,
        description=description,
    )
    active_versions = [item.version for item in project.list_published_versions() if item.is_active]
    return ConnectorBuilderRevertResult(
        builder_project_id=project.builder_project_id,
        definition_id=source_definition_id,
        reverted_to_version=version,
        as_draft=as_draft,
        active_version=active_versions[0] if active_versions else None,
    )


@mcp_tool(
    external_access=True,
    insiders_only=True,
)
def run_connector_builder_readiness_checks(
    ctx: Context,
    stream_names: Annotated[
        str | list[str] | None,
        Field(
            description=(
                "Streams to test-read. The report covers only these streams. Required unless "
                "`run_full_sync` is set. For dynamic streams, get the generated names from "
                "`get_connector_builder_resolved_manifest`."
            ),
            default=None,
        ),
        TraceArg.FINGERPRINT,
    ] = None,
    *,
    builder_project_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_PROJECT_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    definition_id: Annotated[
        str | None,
        Field(
            description=_BUILDER_DEFINITION_ID_DESCRIPTION,
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
        TraceArg.OMIT,
    ],
    timeout_seconds: Annotated[
        float | None,
        Field(
            description=(
                "Optional. Return after this many seconds with the streams finished so far. "
                "Streams still reading are dropped (`pending_streams`); streams not started are "
                "listed in `not_attempted_streams` to pass in another call. No limit by default."
            ),
            default=None,
        ),
    ] = None,
    run_full_sync: Annotated[
        bool,
        Field(
            description=(
                "Optional. If no named stream failed or is still pending (or none are named), "
                "publish the draft and start a full sync of every stream to a no-op destination. "
                "Returns the job in `full_sync`; poll it with `get_connection_sync_status`."
            ),
            default=False,
        ),
    ] = False,
    test_source_id: Annotated[
        str | None,
        Field(
            description=(
                "Optional. Source on the published definition for the full sync. Defaults to "
                "the test source from `create_connector_builder_project(create_test_source=True)`."
            ),
            default=None,
        ),
    ] = None,
) -> ConnectorBuilderReadinessReportResult:
    """Check a Connector Builder draft's progress and test-read its streams on Airbyte Cloud.

    Returns readiness checks for the steps to a finished custom source, with next steps, plus
    per-stream results from parallel test reads using the project's testing values: `failed`,
    `read_to_end`, record and page counts, HTTP errors, warning and error logs, and inferred
    schemas. A stream is `failed` when its read errored, returned HTTP errors, or logged
    errors. Cloud test reads stop at 5,000 records, 20 pages per slice, and 20 slices, so a
    stream that hits a cap isn't `read_to_end`. The report calls out failures and warnings
    without judging readiness; report every failure and warning to the user. A full sync
    validates the whole connector. With `run_full_sync`, the tool publishes the draft and
    starts that sync once no named stream failed or is pending.
    `warnings` flags reads that may be incomplete (round record counts, a full last page);
    check them, but they don't fail the read.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    project = workspace.get_connector_builder_project(
        builder_project_id,
        definition_id=definition_id,
    )
    if run_full_sync:
        _check_builder_project_created_in_session(project)
    report = project._run_readiness_checks(  # noqa: SLF001
        resolve_list_of_strings(stream_names) or [],
        timeout_seconds=timeout_seconds,
        run_full_sync=run_full_sync,
        test_source_id=test_source_id,
        on_resource_created=register_guid_created_in_session,
    )
    report_data = report.model_dump()
    report_data["streams"] = [
        _redact_stream_diagnostics(stream) for stream in report_data["streams"]
    ]
    return ConnectorBuilderReadinessReportResult.model_validate(
        {
            "builder_project_id": project.builder_project_id,
            "connector_builder_project_url": project.connector_builder_project_url,
            **report_data,
        }
    )


def register_connector_builder_tools(app: FastMCP) -> None:
    """Register Connector Builder tools with the FastMCP app.

    Args:
        app: FastMCP application instance
    """
    exclude_args = ["workspace_id"] if AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET else []
    if not is_trusted_execution_enabled():
        exclude_args.extend(("manifest_yaml_file", "testing_values_secret_name"))
    if exclude_args:
        _add_defaults_for_exclude_args(exclude_args)
    register_mcp_tools(
        app,
        mcp_module=__name__,
        exclude_args=exclude_args or None,
    )
