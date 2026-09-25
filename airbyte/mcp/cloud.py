# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Airbyte Cloud MCP operations.

.. include:: ../../docs/mcp-generated/cloud.md
"""

# No public Python API — MCP primitives are registered via decorators and
# documented via the generated Markdown include above. Setting `__all__` to an
# empty list tells pdoc (and other doc tools) not to surface the individual
# tool / helper definitions as a redundant "API Documentation" list.
__all__: list[str] = []

from collections.abc import Callable
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Final, Literal, TypeVar, cast

import requests
from fastmcp import Context, FastMCP
from fastmcp_extensions import get_mcp_config, mcp_tool, register_mcp_tools
from pydantic import BaseModel, ConfigDict, Field

from airbyte import Destination, Source, get_destination, get_source
from airbyte._direct_connectors import connector_docs
from airbyte._direct_connectors.models import (
    CloudConnectorConnectionInfo,
    ExternalApiExecuteResult,
    ExternalApiReadOnlyAction,
    ExternalApiWriteAction,
)
from airbyte._util import api_util
from airbyte.cloud.client import MAX_WORKSPACES_TO_VALIDATE, CloudClient
from airbyte.cloud.connectors import (
    CheckResult,
    CloudConnector,
    CloudDestination,
    CloudSource,
    CustomCloudSourceDefinition,
)
from airbyte.cloud.constants import FAILED_STATUSES
from airbyte.cloud.models import (
    CloudDefaultContextInfo,
    CloudDefaultWorkspaceUpdateInfo,
    CloudOrganizationInfo,
    ConnectorFeature,
    ConnectorType,
    JobTypeEnum,
    OrganizationFeature,
    WorkspacePrivilegeScope,
)
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.constants import (
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CLIENT_SECRET_ENV_VAR,
    CLOUD_WORKSPACE_ID_ENV_VAR,
    MCP_BEARER_TOKEN_HEADER,
    MCP_CONFIG_API_URL,
    MCP_CONFIG_BEARER_TOKEN,
    MCP_CONFIG_CLIENT_ID,
    MCP_CONFIG_CLIENT_SECRET,
    MCP_CONFIG_CONFIG_API_URL,
    MCP_CONFIG_ORGANIZATION_ID,
    MCP_CONFIG_WORKSPACE_ID,
    MCP_WORKSPACE_ID_HEADER,
)
from airbyte.destinations.util import get_noop_destination
from airbyte.exceptions import (
    AirbyteCloudApiError,
    AirbyteConnectorNotRegisteredError,
    AirbyteDeferredSetupError,
    AirbyteError,
    AirbyteMissingResourceError,
    AirbyteMissingWorkspaceContextError,
    PyAirbyteError,
    PyAirbyteInputError,
)
from airbyte.mcp._arg_resolvers import (
    resolve_api_args,
    resolve_connector_config,
    resolve_list_of_strings,
)
from airbyte.mcp._docs_results import (
    AgentSkillDocsResult,
    CloudConnectorDocsResult,
    render_agent_skill_docs_result,
    render_connector_docs_result,
)
from airbyte.mcp._tool_utils import (
    AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET,
    check_guid_created_in_session,
    register_guid_created_in_session,
)
from airbyte.registry import (
    ApiDocsUrl,  # Needed at runtime for Pydantic field types.
    get_connector_metadata,
)


if TYPE_CHECKING:
    from airbyte.cloud.sync_results import SyncResult


CLOUD_AUTH_TIP_TEXT = (
    f"When connecting to a hosted MCP server, provide a bearer token via the "
    f"`{MCP_BEARER_TOKEN_HEADER}` header, or client credentials via the transport "
    f"`Client-Id` and `Client-Secret` headers. When no workspace ID is provided, "
    f"the authenticated user's default workspace (and its organization) is used "
    f"automatically. Call `get_default_cloud_context` to inspect the resolved "
    f"context. To discover other workspaces, call `list_cloud_workspaces` "
    f"with an organization ID or broader privilege scope. Only call "
    f"`list_cloud_organizations` when you need to search organizations by name, "
    f"passing `name_contains`. For local or "
    f"stdio connections, set the `{CLOUD_BEARER_TOKEN_ENV_VAR}` environment "
    f"variable, or both `{CLOUD_CLIENT_ID_ENV_VAR}` and "
    f"`{CLOUD_CLIENT_SECRET_ENV_VAR}`. If discovery returns multiple candidates, "
    f"ask the user to choose one; do not select automatically."
)
WORKSPACE_ID_TIP_TEXT = (
    f"Workspace ID. Hosted MCP connections pass it via the "
    f"`{MCP_WORKSPACE_ID_HEADER}` header; local or stdio connections use the "
    f"`{CLOUD_WORKSPACE_ID_ENV_VAR}` environment variable."
)
CONNECTOR_CHECK_FAILURE_FALLBACK = "Connector check failed without a failure message."
DEFER_CREDENTIALS_TIP_TEXT = (
    "Create a draft connector so a person can complete OAuth or enter "
    "secrets in Airbyte Cloud. Pass only non-secret configuration in `config` (no credentials, "
    "secret references or `config_secret_name`). When the connector offers several "
    "authentication methods, include the method's selector field in `config`. The result "
    "includes a settings link for the person to complete any missing fields and test the draft. "
    "After they report a successful test and save, call "
    "`check_cloud_connector` with the `connector_id` (optionally `connector_type` and "
    "`workspace_id`)."
)
DEFERRED_SETUP_GUIDANCE = (
    "Share `settings_url` with the user. They must open it, complete authentication and any "
    "missing settings, then test and save the draft. A successful test makes it ready to use. "
    "Then call `check_cloud_connector` with `connector_id` (optionally `connector_type` "
    "and `workspace_id`)."
)

_DiscoveryResult = TypeVar("_DiscoveryResult")


def _handle_discovery_permission_error(
    error: AirbyteError,
    *,
    make_result: Callable[[str], _DiscoveryResult],
) -> _DiscoveryResult:
    """Return a graceful result for discovery permission errors."""
    status_code = (error.context or {}).get("status_code")
    if status_code not in {HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN}:
        raise error
    return make_result(
        "Organization or workspace discovery is unavailable because these credentials "
        "do not have the required permission or access. Provide an organization or "
        "workspace ID, or use credentials with the needed access."
    )


def _get_connector_check_message(check_result: CheckResult) -> str | None:
    """Return the check failure message, if applicable."""
    if check_result.success:
        return None
    return (
        check_result.error_message
        or check_result.internal_error
        or CONNECTOR_CHECK_FAILURE_FALLBACK
    )


FEATURE_FILTER_TIP_TEXT = (
    "Optional feature filter: `direct_access` returns only connectors AI agents can use "
    "through the Airbyte Context layer; `direct_api_query` narrows to sources agents can "
    "query. `enabled_features` is only resolved and returned when this filter is set; "
    "use `direct_access` to find every connector with any external-access feature "
    "enabled and see its full feature list. Omit to list every connector with "
    "`enabled_features='not_checked'` (no feature check performed). Connectors whose "
    "feature lookup fails are returned with `enabled_features='unknown'` and a "
    "`warnings` entry, so they can still be inspected or tried."
)

FEATURES_NOT_CHECKED: Final = "not_checked"
FeaturesNotChecked = Literal["not_checked"]
FEATURES_UNKNOWN: Final = "unknown"
FeaturesUnknown = Literal["unknown"]


CONNECTOR_TYPE_TIP_TEXT = (
    "Optional: `source` or `destination`. When omitted, the connector type is "
    "resolved automatically from the connector ID (one extra API call)."
)


def _get_cloud_connector(
    workspace: CloudWorkspace,
    connector_id: str,
    connector_type: ConnectorType | None,
) -> CloudConnector:
    """Return a typed `CloudConnector`, resolving the type lazily when not provided."""
    if connector_type == ConnectorType.SOURCE:
        return workspace.get_source(source_id=connector_id)
    if connector_type == ConnectorType.DESTINATION:
        return workspace.get_destination(destination_id=connector_id)
    return workspace.get_connector(connector_id=connector_id)


def _get_typed_cloud_connector(
    workspace: CloudWorkspace,
    connector_id: str,
    connector_type: ConnectorType | None,
) -> CloudSource | CloudDestination:
    """Return a `CloudSource` or `CloudDestination`, resolving the type if not provided."""
    connector = _get_cloud_connector(workspace, connector_id, connector_type)
    if connector.connector_type == ConnectorType.SOURCE:
        return connector.as_cloud_source()
    return connector.as_cloud_destination()


def _infer_connector_type_from_name(connector_name: str) -> ConnectorType:
    """Infer the connector type from a canonical connector name like `source-faker`."""
    if connector_name.startswith("source-"):
        return ConnectorType.SOURCE
    if connector_name.startswith("destination-"):
        return ConnectorType.DESTINATION
    raise PyAirbyteInputError(
        message=(
            f"Cannot infer connector type from connector name '{connector_name}'. "
            "Pass `connector_type` explicitly or use a canonical name with a "
            "`source-` or `destination-` prefix."
        ),
        context={"connector_name": connector_name},
    )


class CloudConnectionResult(BaseModel):
    """Information about a deployed connection in Airbyte Cloud."""

    id: str
    """The connection ID."""
    name: str
    """Display name of the connection."""
    url: str
    """Web URL for managing this connection in Airbyte Cloud."""
    source_id: str
    """ID of the source used by this connection."""
    destination_id: str
    """ID of the destination used by this connection."""
    last_job_status: str | None = None
    """Status of the most recent completed sync job (e.g., 'succeeded', 'failed', 'cancelled').
    Only populated when with_connection_status=True."""
    last_job_id: int | None = None
    """Job ID of the most recent completed sync. Only populated when with_connection_status=True."""
    last_job_time: str | None = None
    """ISO 8601 timestamp of the most recent completed sync.
    Only populated when with_connection_status=True."""
    currently_running_job_id: int | None = None
    """Job ID of a currently running sync, if any.
    Only populated when with_connection_status=True."""
    currently_running_job_start_time: str | None = None
    """ISO 8601 timestamp of when the currently running sync started.
    Only populated when with_connection_status=True."""


class CloudConnectionDetails(BaseModel):
    """Detailed information about a deployed connection in Airbyte Cloud."""

    connection_id: str
    """The connection ID."""
    connection_name: str
    """Display name of the connection."""
    connection_url: str
    """Web URL for managing this connection in Airbyte Cloud."""
    source_id: str
    """ID of the source used by this connection."""
    source_name: str
    """Display name of the source."""
    destination_id: str
    """ID of the destination used by this connection."""
    destination_name: str
    """Display name of the destination."""
    selected_streams: list[str]
    """List of stream names selected for syncing."""
    table_prefix: str | None
    """Table prefix applied when syncing to the destination."""


class CloudOrganizationResult(BaseModel):
    """Information about an organization in Airbyte Cloud."""

    id: str
    """The organization ID."""
    name: str | None = None
    """Display name of the organization, when available."""
    email: str | None = None
    """Email associated with the organization, when available."""
    enabled_features: list[OrganizationFeature]
    """Features enabled for this organization; see `OrganizationFeature`."""


class CloudOrganizationListResult(BaseModel):
    """Result of discovering organizations in Airbyte Cloud."""

    organizations: list[CloudOrganizationResult]
    """Organizations visible to the authenticated credentials."""

    message: str | None = None
    """Additional guidance when discovery returns no results or is unavailable."""


class CloudWorkspaceResult(BaseModel):
    """Information about a workspace in Airbyte Cloud."""

    workspace_id: str
    """The workspace ID."""
    workspace_name: str
    """Display name of the workspace."""
    workspace_url: str | None = None
    """URL to access the workspace in Airbyte Cloud."""
    organization_id: str | None
    """ID of the organization, if known and available."""
    organization_name: str | None = None
    """Name of the organization (requires ORGANIZATION_READER permission)."""


class CloudOrganizationBillingStatusResult(BaseModel):
    """Billing and account status for an Airbyte organization."""

    organization_id: str
    organization_name: str | None = None
    billing_info_available: bool
    """False when billing info could not be retrieved."""
    payment_status: str | None = None
    subscription_status: str | None = None
    is_account_locked: bool = False
    message: str | None = None


class CloudWorkspaceListResult(BaseModel):
    """Result of discovering workspaces in Airbyte Cloud."""

    workspaces: list[CloudWorkspaceResult]
    """Workspaces visible to the authenticated credentials."""

    message: str | None = None
    """Additional guidance when discovery returns no results."""

    available_organizations: list[CloudOrganizationResult] | None = None
    """Organizations to choose from when the credentials match multiple organizations."""


class CloudDefaultContextResult(BaseModel):
    """Explicit authenticated Cloud affinities and discovery guidance."""

    user_id: str | None
    """The Airbyte user ID, if available."""

    user_name: str | None
    """The authenticated user's name, if available."""

    user_email: str | None
    """The authenticated user's email, if available."""

    default_workspace_id: str | None
    """The resolved default workspace ID, if available."""

    default_workspace_name: str | None
    """The resolved default workspace name, if available."""

    default_workspace_verified: bool
    """Whether the resolved default workspace was verified as accessible."""

    unvalidated_workspace_count: int = 0
    """Number of direct workspace grants not validated due to the validation cap."""

    default_organization_id: str | None
    """The organization containing the resolved default workspace, if available."""

    default_organization_name: str | None
    """The name of the organization containing the resolved default workspace, if available."""

    configured_workspace_id: str | None
    """The explicitly configured workspace ID, if available."""

    configured_organization_id: str | None
    """The configured organization ID, if available."""

    member_organizations: list[CloudOrganizationInfo]
    """Organizations identified by explicit organization membership grants."""

    member_workspaces: list[CloudWorkspaceResult]
    """Summary of workspace memberships without notification settings."""

    member_organizations_truncated: bool
    """True if organization memberships beyond the returned list were omitted."""

    member_workspaces_truncated: bool
    """True if workspace memberships beyond the returned list were omitted."""

    discovery_hints: list[str]
    """Hints for discovering additional organizations or workspaces."""

    message: str
    """Guidance for selecting a workspace or organization context."""


class CloudDefaultWorkspaceUpdateResult(BaseModel):
    """Result of durably updating the authenticated user's default workspace."""

    user_id: str
    """The Airbyte user ID the update applied to."""

    user_email: str | None
    """The authenticated user's email, if available."""

    previous_default_workspace_id: str | None
    """The user's previous default workspace ID, if one was set."""

    default_workspace_id: str
    """The new default workspace ID."""

    default_workspace_name: str | None
    """The new default workspace name, if available."""

    organization_id: str | None
    """The ID of the organization containing the new default workspace, if available."""

    organization_name: str | None
    """The name of the organization containing the new default workspace, if available."""

    membership_basis: Literal["workspace", "organization"]
    """Whether access was established via a direct workspace grant or an organization grant."""

    message: str
    """Summary of the persistent change and where it applies."""


class LogReadResult(BaseModel):
    """Result of reading sync logs with pagination support."""

    job_id: int
    """The job ID the logs belong to."""
    attempt_number: int
    """The attempt number the logs belong to."""
    log_text: str
    """The string containing the log text we are returning."""
    log_text_start_line: int
    """1-based line index of the first line returned."""
    log_text_line_count: int
    """Count of lines we are returning."""
    total_log_lines_available: int
    """Total number of log lines available, shows if any lines were missed due to the limit."""


class SyncJobResult(BaseModel):
    """Information about a sync job."""

    job_id: int
    """The job ID."""
    status: str
    """The job status (e.g., 'succeeded', 'failed', 'running', 'pending')."""
    bytes_synced: int
    """Number of bytes synced in this job."""
    records_synced: int
    """Number of records synced in this job."""
    start_time: str
    """ISO 8601 timestamp of when the job started."""
    job_url: str
    """URL to view the job in Airbyte Cloud."""


class ConnectorCheckResult(BaseModel):
    """Result of a connection check against a deployed Cloud connector."""

    connector_id: str
    """The deployed connector ID."""
    connector_type: Literal["source", "destination"]
    """The connector type: 'source' or 'destination'."""
    succeeded: bool
    """Whether the connector check succeeded."""
    message: str | None
    """The failure message when the check failed, otherwise None."""


class DeferredDeployResult(BaseModel):
    """Result of deploying a connector with `defer_credentials=True`."""

    connector_id: str
    """The deployed connector ID."""
    connector_type: ConnectorType
    """The connector type: 'source' or 'destination'."""
    name: str
    """The connector name in Airbyte Cloud."""
    workspace_id: str
    """The workspace the connector was created in; pass it to `check_cloud_connector`."""
    settings_url: str
    """Cloud settings page where a person completes the credentials."""
    guidance: str = DEFERRED_SETUP_GUIDANCE
    """What the agent should do next."""


class SyncJobListResult(BaseModel):
    """Result of listing sync jobs with limit support."""

    jobs: list[SyncJobResult]
    """List of sync jobs."""
    jobs_count: int
    """Number of jobs returned in this response."""
    from_tail: bool
    """Whether jobs are ordered newest-first (True) or oldest-first (False)."""


def _get_cloud_workspace(
    ctx: Context,
    workspace_id: str | None = None,
    organization_id: str | None = None,
) -> CloudWorkspace:
    """Get an authenticated CloudWorkspace.

    Resolves credentials from multiple sources via MCP config args in order:
    1. HTTP headers (when running as MCP server with HTTP/SSE transport)
    2. Environment variables

    The ctx parameter provides access to MCP config values that are resolved
    from HTTP headers or environment variables based on the config args
    defined in server.py.
    """
    client = _get_cloud_client(ctx, organization_id=organization_id)
    resolved_workspace_id = workspace_id or client.resolve_default_workspace_id()
    if not resolved_workspace_id:
        raise AirbyteMissingWorkspaceContextError

    return client.get_workspace(resolved_workspace_id)


def _get_cloud_client(
    ctx: Context,
    *,
    organization_id: str | None = None,
) -> CloudClient:
    """Get an authenticated `CloudClient` from MCP config."""
    bearer_token = get_mcp_config(ctx, MCP_CONFIG_BEARER_TOKEN)
    client_id = get_mcp_config(ctx, MCP_CONFIG_CLIENT_ID)
    client_secret = get_mcp_config(ctx, MCP_CONFIG_CLIENT_SECRET)
    api_url = get_mcp_config(ctx, MCP_CONFIG_API_URL)
    config_api_url = get_mcp_config(ctx, MCP_CONFIG_CONFIG_API_URL)
    workspace_id = get_mcp_config(ctx, MCP_CONFIG_WORKSPACE_ID)

    return CloudClient(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        public_api_root=api_url,
        config_api_root=config_api_url,
        workspace_id=workspace_id,
        organization_id=organization_id or get_mcp_config(ctx, MCP_CONFIG_ORGANIZATION_ID),
    )


@mcp_tool(
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def deploy_connector_to_cloud(  # noqa: PLR0913  # Mirrors the API surface.
    ctx: Context,
    name: Annotated[
        str,
        Field(description="The name to use when deploying the connector."),
    ],
    connector_name: Annotated[
        str,
        Field(
            description=(
                "The canonical name of the connector (e.g., 'source-faker' or "
                "'destination-postgres')."
            ),
        ),
    ],
    *,
    connector_type: Annotated[
        ConnectorType | None,
        Field(
            description=(
                "Optional: `source` or `destination`. When omitted, inferred from the "
                "`source-`/`destination-` prefix of `connector_name`."
            ),
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
    config: Annotated[
        dict | str | None,
        Field(
            description="The configuration for the connector.",
            default=None,
        ),
    ],
    config_secret_name: Annotated[
        str | None,
        Field(
            description="The name of the secret containing the configuration.",
            default=None,
        ),
    ],
    unique: Annotated[
        bool,
        Field(
            description="Whether to require a unique name.",
            default=True,
        ),
    ],
    defer_credentials: Annotated[
        bool,
        Field(
            description=DEFER_CREDENTIALS_TIP_TEXT,
            default=False,
        ),
    ] = False,
) -> str:
    """Deploy a source or destination connector to Airbyte Cloud.

    With `defer_credentials=True`, returns a JSON `DeferredDeployResult` whose `settings_url`
    is where a person completes the credentials.
    """
    resolved_type = connector_type or _infer_connector_type_from_name(connector_name)
    if defer_credentials:
        return _deploy_deferred_to_cloud(
            ctx,
            connector_type=resolved_type,
            name=name,
            connector_name=connector_name,
            workspace_id=workspace_id,
            config=config,
            config_secret_name=config_secret_name,
            unique=unique,
        ).model_dump_json(indent=2)

    connector: Source | Destination = (
        get_source(connector_name, no_executor=True)
        if resolved_type == ConnectorType.SOURCE
        else get_destination(connector_name, no_executor=True)
    )
    config_dict = resolve_connector_config(
        config=config,
        config_secret_name=config_secret_name,
        config_spec_jsonschema=connector.config_spec,
    )
    connector.set_config(config_dict, validate=True)

    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    deployed: CloudConnector = (
        workspace.deploy_source(name=name, source=connector, unique=unique)
        if isinstance(connector, Source)
        else workspace.deploy_destination(name=name, destination=connector, unique=unique)
    )

    register_guid_created_in_session(deployed.connector_id)
    return (
        f"Successfully deployed {resolved_type.value} '{name}' with ID "
        f"'{deployed.connector_id}' and URL: {deployed.connector_url}"
    )


def _deploy_deferred_to_cloud(
    ctx: Context,
    *,
    connector_type: ConnectorType,
    name: str,
    connector_name: str,
    workspace_id: str | None,
    config: dict | str | None,
    config_secret_name: str | None,
    unique: bool,
) -> DeferredDeployResult:
    """Create a connector from non-secret configuration; a person completes it in Cloud."""
    if config_secret_name is not None:
        raise PyAirbyteInputError(
            message="`config_secret_name` cannot be used with `defer_credentials=True`.",
            guidance="Pass non-secret configuration in `config`; credentials are entered in Cloud.",
        )
    metadata = get_connector_metadata(connector_name)
    if metadata is None or metadata.definition_id is None:
        raise AirbyteConnectorNotRegisteredError(connector_name=connector_name)
    if metadata.connector_type != connector_type:
        raise PyAirbyteInputError(
            message=f"`{connector_name}` is not a {connector_type} connector.",
            guidance=f"Pass a `{connector_type}-*` connector name.",
        )
    config_dict = resolve_connector_config(config=config)

    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    deployed: CloudSource | CloudDestination
    try:
        if connector_type == ConnectorType.SOURCE:
            deployed = workspace.deploy_source(
                name=name,
                source=config_dict,
                unique=unique,
                definition_id=metadata.definition_id,
                defer_credentials=True,
            )
        else:
            deployed = workspace.deploy_destination(
                name=name,
                destination=config_dict,
                unique=unique,
                definition_id=metadata.definition_id,
                defer_credentials=True,
            )
    except AirbyteDeferredSetupError as ex:
        if ex.actor_id is not None:
            register_guid_created_in_session(ex.actor_id)
        raise
    register_guid_created_in_session(deployed.connector_id)
    return DeferredDeployResult(
        connector_id=deployed.connector_id,
        connector_type=connector_type,
        name=name,
        workspace_id=workspace.workspace_id,
        settings_url=deployed.connector_url,
    )


@mcp_tool(
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def create_connection_on_cloud(
    ctx: Context,
    connection_name: Annotated[
        str,
        Field(description="The name of the connection."),
    ],
    source_id: Annotated[
        str,
        Field(description="The ID of the deployed source."),
    ],
    destination_id: Annotated[
        str,
        Field(description="The ID of the deployed destination."),
    ],
    selected_streams: Annotated[
        str | list[str],
        Field(
            description=(
                "The selected stream names to sync within the connection. "
                "Must be an explicit stream name or list of streams. "
                "Cannot be empty or '*'."
            )
        ),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
    table_prefix: Annotated[
        str | None,
        Field(
            description="Optional table prefix to use when syncing to the destination.",
            default=None,
        ),
    ],
) -> str:
    """Create a connection between a deployed source and destination on Airbyte Cloud."""
    resolved_streams_list: list[str] = resolve_list_of_strings(selected_streams)
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    deployed_connection = workspace.deploy_connection(
        connection_name=connection_name,
        source=source_id,
        destination=destination_id,
        selected_streams=resolved_streams_list,
        table_prefix=table_prefix,
    )

    register_guid_created_in_session(deployed_connection.connection_id)
    return (
        f"Successfully created connection '{connection_name}' "
        f"with ID '{deployed_connection.connection_id}' and "
        f"URL: {deployed_connection.connection_url}"
    )


@mcp_tool(
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def run_cloud_sync(
    ctx: Context,
    connection_id: Annotated[
        str,
        Field(description="The ID of the Airbyte Cloud connection."),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
    wait: Annotated[
        bool,
        Field(
            description=(
                "Whether to wait for the sync to complete. Since a sync can take between several "
                "minutes and several hours, this option is not recommended for most "
                "scenarios."
            ),
            default=False,
        ),
    ],
    wait_timeout: Annotated[
        int,
        Field(
            description="Maximum time to wait for sync completion (seconds).",
            default=300,
        ),
    ],
) -> str:
    """Run a sync job on Airbyte Cloud."""
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)
    sync_result = connection.run_sync(wait=wait, wait_timeout=wait_timeout)

    if wait:
        status = sync_result.get_job_status()
        return (
            f"Sync completed with status: {status}. "
            f"Job ID is '{sync_result.job_id}' and "
            f"job URL is: {sync_result.job_url}"
        )
    return f"Sync started. Job ID is '{sync_result.job_id}' and job URL is: {sync_result.job_url}"


@mcp_tool(
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def deploy_noop_destination_to_cloud(
    ctx: Context,
    name: str = "No-op Destination",
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
    unique: bool = True,
) -> str:
    """Deploy the No-op destination to Airbyte Cloud for testing purposes."""
    destination = get_noop_destination()
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    deployed_destination = workspace.deploy_destination(
        name=name,
        destination=destination,
        unique=unique,
    )
    register_guid_created_in_session(deployed_destination.connector_id)
    return (
        f"Successfully deployed No-op Destination "
        f"with ID '{deployed_destination.connector_id}' and "
        f"URL: {deployed_destination.connector_url}"
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def get_cloud_sync_status(
    ctx: Context,
    connection_id: Annotated[
        str,
        Field(
            description="The ID of the Airbyte Cloud connection.",
        ),
    ],
    job_id: Annotated[
        int | None,
        Field(
            description="Optional job ID. If not provided, the latest job will be used.",
            default=None,
        ),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
    include_attempts: Annotated[
        bool,
        Field(
            description="Whether to include detailed attempts information.",
            default=False,
        ),
    ],
) -> dict[str, Any]:
    """Get the status of a sync job from the Airbyte Cloud."""
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)

    # If a job ID is provided, get the job by ID.
    sync_result: SyncResult | None = connection.get_sync_result(job_id=job_id)

    if not sync_result:
        return {"status": None, "job_id": None, "attempts": []}

    result = {
        "status": sync_result.get_job_status(),
        "job_id": sync_result.job_id,
        "bytes_synced": sync_result.bytes_synced,
        "records_synced": sync_result.records_synced,
        "start_time": sync_result.start_time.isoformat(),
        "job_url": sync_result.job_url,
        "attempts": [],
    }

    if include_attempts:
        attempts = sync_result.get_attempts()
        result["attempts"] = [
            {
                "attempt_number": attempt.attempt_number,
                "attempt_id": attempt.attempt_id,
                "status": attempt.status,
                "bytes_synced": attempt.bytes_synced,
                "records_synced": attempt.records_synced,
                "created_at": attempt.created_at.isoformat(),
            }
            for attempt in attempts
        ]

    return result


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def list_cloud_sync_jobs(
    ctx: Context,
    connection_id: Annotated[
        str,
        Field(description="The ID of the Airbyte Cloud connection."),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
    max_jobs: Annotated[
        int,
        Field(
            description=(
                "Maximum number of jobs to return. "
                "Defaults to 20 if not specified. "
                "Maximum allowed value is 500."
            ),
            default=20,
        ),
    ],
    from_tail: Annotated[
        bool | None,
        Field(
            description=(
                "When True, jobs are ordered newest-first (createdAt DESC). "
                "When False, jobs are ordered oldest-first (createdAt ASC). "
                "Defaults to True."
            ),
            default=None,
        ),
    ],
    job_type: Annotated[
        JobTypeEnum | None,
        Field(
            description=(
                "Filter by job type. Options: 'sync', 'reset', 'refresh', 'clear'. "
                "If not specified, defaults to sync and reset jobs only (API default). "
                "Use 'refresh' to find refresh jobs or 'clear' to find clear jobs."
            ),
            default=None,
        ),
    ],
) -> SyncJobListResult:
    """List sync jobs for a connection with limit support.

    This tool allows you to retrieve a list of sync jobs for a connection,
    with control over ordering and result limit. By default, jobs are returned
    newest-first (`from_tail=True`).
    """
    if from_tail is None:
        from_tail = True

    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)

    # Cap at 500 to avoid overloading agent context
    effective_limit = min(max_jobs, 500) if max_jobs > 0 else 20

    sync_results = connection.get_previous_sync_logs(
        limit=effective_limit,
        from_tail=from_tail,
        job_type=job_type,
    )

    jobs = [
        SyncJobResult(
            job_id=sync_result.job_id,
            status=sync_result.get_job_status().value,
            bytes_synced=sync_result.bytes_synced,
            records_synced=sync_result.records_synced,
            start_time=sync_result.start_time.isoformat(),
            job_url=sync_result.job_url,
        )
        for sync_result in sync_results
    ]

    return SyncJobListResult(
        jobs=jobs,
        jobs_count=len(jobs),
        from_tail=from_tail,
    )


@mcp_tool(
    destructive=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def cancel_cloud_sync(
    ctx: Context,
    connection_id: Annotated[
        str,
        Field(description="The ID of the Airbyte Cloud connection."),
    ],
    job_id: Annotated[
        int | None,
        Field(
            description=(
                "Optional job ID to cancel. If not provided, the connection's most recent "
                "sync job will be cancelled. Other job types require an explicit job ID."
            ),
            default=None,
        ),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> SyncJobResult:
    """Cancel a running sync job on an Airbyte Cloud connection."""
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)
    # Deliberately omit check_guid_created_in_session: cancelling a sync is reversible.
    sync_result = connection.cancel_sync(job_id=job_id)
    return SyncJobResult(
        job_id=sync_result.job_id,
        status=sync_result.get_job_status().value,
        bytes_synced=sync_result.bytes_synced,
        records_synced=sync_result.records_synced,
        start_time=sync_result.start_time.isoformat(),
        job_url=sync_result.job_url,
    )


class CloudConnectorResult(BaseModel):
    """Information about a deployed connector in Airbyte Cloud."""

    id: str
    """The connector ID."""
    connector_type: Literal["source", "destination"]
    """Whether the connector is a source or a destination."""
    name: str
    """The connector's display name."""
    url: str
    """The connector's page in the Airbyte Cloud UI."""
    enabled_features: list[ConnectorFeature] | FeaturesNotChecked | FeaturesUnknown = (
        FEATURES_NOT_CHECKED
    )
    """Features enabled for this connector. `"not_checked"` means no feature check was
    performed (no `feature_filter`); `"unknown"` means the feature lookup failed (see
    `warnings`); an empty list means checked with nothing enabled."""

    warnings: list[str] = Field(default_factory=list)
    """Non-fatal issues encountered while resolving this connector's features."""


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def list_cloud_connectors(
    ctx: Context,
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
    connector_type: Annotated[
        ConnectorType | None,
        Field(
            description=("Optional: return only sources or only destinations. Omit for both."),
            default=None,
        ),
    ] = None,
    name_contains: Annotated[
        str | None,
        Field(
            description="Optional case-insensitive substring to filter connectors by name",
            default=None,
        ),
    ],
    limit: Annotated[
        int | None,
        Field(
            description="Optional maximum number of items to return (default: no limit)",
            default=None,
        ),
    ],
    feature_filter: Annotated[
        ConnectorFeature | None,
        Field(
            description=FEATURE_FILTER_TIP_TEXT,
            default=None,
        ),
    ] = None,
) -> list[CloudConnectorResult]:
    """List deployed source and destination connectors in the Airbyte Cloud workspace.

    Pass `feature_filter` (for example `direct_api_query` for sources,
    `direct_sql_query` for destinations, or `direct_access` for either) to return only
    matching connectors with their `enabled_features` resolved; without it,
    `enabled_features` is `"not_checked"`. A returned `"not_checked"` means no
    feature check was performed; `[]` means checked and no features enabled.

    When a connector's feature lookup fails for a reason other than "not enabled",
    it is returned with `enabled_features="unknown"` plus a `warnings` entry. A
    502/503/504 or transport failure means the endpoint is down for the whole
    workspace, so remaining connectors are returned as `"unknown"` without further
    probes; other failures mark only that connector `"unknown"`. `"unknown"`
    results can still be inspected or tried via `describe_cloud_connector` or the
    execute tools.
    """
    if limit is not None and limit <= 0:
        raise PyAirbyteInputError(message="`limit` must be greater than 0.")
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connectors = workspace.list_connectors(
        connector_type=connector_type,
        name_contains=name_contains,
        limit=None if feature_filter is not None else limit,
    )
    if feature_filter is None:
        # Note: name and url are guaranteed non-null from list API responses
        return [
            CloudConnectorResult(
                id=connector.connector_id,
                connector_type=connector.connector_type.value,
                name=cast(str, connector.name),
                url=connector.connector_url,
            )
            for connector in connectors
        ]

    results: list[CloudConnectorResult] = []
    probe_failure: str | None = None
    for connector in connectors:
        features: frozenset[ConnectorFeature] | None = None
        if probe_failure is None:
            try:
                features = connector.enabled_features
            except (AirbyteError, requests.RequestException) as error:
                warning = f"Connector feature lookup failed; enabled features are unknown: {error}"
                if isinstance(error, requests.RequestException) or (
                    isinstance(error, AirbyteCloudApiError)
                    and error.status_code
                    in {
                        HTTPStatus.BAD_GATEWAY,
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        HTTPStatus.GATEWAY_TIMEOUT,
                    }
                ):
                    probe_failure = warning
                else:
                    results.append(
                        CloudConnectorResult(
                            id=connector.connector_id,
                            connector_type=connector.connector_type.value,
                            name=cast(str, connector.name),
                            url=connector.connector_url,
                            enabled_features=FEATURES_UNKNOWN,
                            warnings=[warning],
                        )
                    )
                    if limit is not None and len(results) >= limit:
                        break
                    continue
        if probe_failure is not None:
            enabled_features: list[ConnectorFeature] | FeaturesUnknown = FEATURES_UNKNOWN
            connector_warnings = [probe_failure]
        elif features is not None and feature_filter in features:
            enabled_features = sorted(features)
            connector_warnings = []
        else:
            continue
        results.append(
            CloudConnectorResult(
                id=connector.connector_id,
                connector_type=connector.connector_type.value,
                name=cast(str, connector.name),
                url=connector.connector_url,
                enabled_features=enabled_features,
                warnings=connector_warnings,
            )
        )
        if limit is not None and len(results) >= limit:
            break
    return results


class CloudConnectorDetailsResult(BaseModel):
    """A description of a deployed Cloud connector.

    As returned by the `describe_cloud_*` MCP tools.
    """

    model_config = ConfigDict(extra="allow")

    connector_id: str
    """The connector ID."""

    connector_type: Literal["source", "destination"]
    """Whether the connector is a source or a destination."""

    connector_name: str
    """The connector's display name."""

    connector_url: str
    """The connector's web URL."""

    connector_definition_id: str
    """The connector definition ID (for example, the ID for `source-postgres`)."""

    integration_name: str | None = None
    """Name of the underlying integration, for example `GitHub` or `Snowflake`."""

    enabled_features: list[ConnectorFeature] | FeaturesUnknown = Field(default_factory=list)
    """Features enabled for this connector; see `ConnectorFeature`. `"unknown"` means
    the feature lookup failed (see `warnings`)."""

    config: dict[str, Any] | None = None
    """The connector configuration, populated only by `with_config`.

    Secret values are redacted by the Cloud API. Always `None` for sources, which the
    API does not expose configuration for."""

    replication_details: list[CloudConnectorConnectionInfo] | None = None
    """Connections touching this connector, populated only by `with_replication_details`."""

    direct_access_guidance: CloudConnectorDocsResult | None = None
    """Direct-access docs rendered as Markdown, populated only by `with_direct_access_guidance`."""

    data_replication_docs: list[ApiDocsUrl] | None = None
    """Upstream API documentation links, populated only by `with_data_replication_docs`."""

    warnings: list[str] = Field(default_factory=list)
    """Non-fatal issues encountered while describing the connector."""

    errors: list[str] = Field(default_factory=list)
    """Fatal issues encountered while describing optional connector details."""


def _describe_cloud_connector(
    connector: CloudConnector,
    *,
    with_config: bool,
    with_replication_details: bool,
    with_direct_access_guidance: bool,
    with_data_replication_docs: bool,
) -> CloudConnectorDetailsResult:
    """Assemble the `describe_cloud_*` MCP tools' result for a deployed connector."""
    connector_type = connector.connector_type
    warnings: list[str] = []
    try:
        integration_name = connector.integration_name
    except AirbyteError as error:
        warnings.append(f"Integration name lookup failed: {error}")
        integration_name = None

    result = CloudConnectorDetailsResult(
        connector_id=connector.connector_id,
        connector_type=connector_type.value,
        connector_name=connector.name or "",
        connector_url=connector.connector_url,
        connector_definition_id=connector.definition_id,
        integration_name=integration_name,
    )

    try:
        result.enabled_features = sorted(connector.enabled_features)
        if (
            connector.is_feature_enabled(ConnectorFeature.DIRECT_ACCESS)
            and connector.workspace._has_context_layer_api()  # noqa: SLF001
        ):
            context_layer = connector._context_layer_inspect(  # noqa: SLF001
                warnings=warnings,
            )
            if context_layer is not None:
                warnings.extend(str(warning) for warning in context_layer.warnings)
    except (AirbyteError, requests.RequestException) as error:
        result.enabled_features = FEATURES_UNKNOWN
        warnings.append(f"Connector feature lookup failed; enabled features are unknown: {error}")

    if with_config and connector_type == ConnectorType.DESTINATION:
        try:
            result.config = connector.as_cloud_destination().configuration
        except AirbyteError as error:
            warnings.append(f"Connector configuration lookup failed: {error}")

    if with_replication_details:
        try:
            result.replication_details = connector_docs.build_connection_details(connector)
        except AirbyteError as error:
            warnings.append(f"Connection listing failed: {error}")

    if with_direct_access_guidance:
        try:
            docs = connector.get_direct_access_guidance()
        except (PyAirbyteError, requests.RequestException) as error:
            warnings.append(f"Direct access docs are unavailable: {error}")
        else:
            result.direct_access_guidance = render_connector_docs_result(docs)

    if with_data_replication_docs:
        try:
            result.data_replication_docs = connector.get_data_replication_docs()
        except (PyAirbyteError, requests.RequestException) as error:
            warnings.append(f"Data replication docs are unavailable: {error}")

    result.warnings = warnings
    return result


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def describe_cloud_connector(
    ctx: Context,
    connector_id: Annotated[
        str,
        Field(
            description=(
                "The ID of the deployed connector to describe. Works for both sources "
                "and destinations; the kind is resolved automatically."
            ),
        ),
    ],
    *,
    connector_type: Annotated[
        ConnectorType | None,
        Field(
            description=CONNECTOR_TYPE_TIP_TEXT,
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
    with_config: Annotated[
        bool,
        Field(
            description=(
                "Include the connector configuration (secrets are redacted by the " "Cloud API)."
            ),
            default=False,
        ),
    ],
    with_replication_details: Annotated[
        bool,
        Field(
            description="Include the connections that read from or write to this " "connector.",
            default=False,
        ),
    ],
    with_direct_access_guidance: Annotated[
        bool,
        Field(
            description="Include the connector's direct-access usage docs rendered " "as Markdown.",
            default=False,
        ),
    ],
    with_data_replication_docs: Annotated[
        bool,
        Field(
            description="Include links to the connector's upstream API documentation.",
            default=False,
        ),
    ],
) -> CloudConnectorDetailsResult:
    """Get detailed information about a deployed source or destination connector.

    Always returns identity fields and enabled features. The `with_*` toggles add the
    connector's configuration, its connections, its direct-access docs, and links to
    its upstream API documentation.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    return _describe_cloud_connector(
        _get_cloud_connector(workspace, connector_id, connector_type),
        with_config=with_config,
        with_replication_details=with_replication_details,
        with_direct_access_guidance=with_direct_access_guidance,
        with_data_replication_docs=with_data_replication_docs,
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def get_agent_skill_docs(
    ctx: Context,
    *,
    docs_skill_id: Annotated[
        str | None,
        Field(
            description=(
                "Fully-qualified skill ID, e.g. from `describe_cloud_*` `skill_id`. "
                "Provide this or `connector_id`."
            ),
            default=None,
        ),
    ] = None,
    connector_id: Annotated[
        str | None,
        Field(
            description=(
                "Deployed source or destination ID; resolves that connector's skill docs. "
                "Provide this or `docs_skill_id`."
            ),
            default=None,
        ),
    ] = None,
    section: Annotated[
        str | None,
        Field(
            description=(
                "Optional exact section ID from the guidance's outline to read a single "
                "section. Omit for the overview, metadata, and outline."
            ),
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> AgentSkillDocsResult:
    """Returns the requested skill document by ID for an AI agent.

    Pass either a fully-qualified `docs_skill_id` or a `connector_id` (source or
    destination); exactly one is required.

    `section` is optional; if omitted, the summary overview is returned along with
    the list of available sections.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    return render_agent_skill_docs_result(
        workspace.get_agent_skill_docs(docs_skill_id, connector_id=connector_id, section=section)
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def execute_external_api_query(  # noqa: PLR0913  # Explicit args mirror the connector API.
    ctx: Context,
    *,
    connector_id: Annotated[
        str,
        Field(description="The ID of the deployed source connector to query."),
    ],
    entity_type: Annotated[
        str,
        Field(
            description=(
                "The type of entity to query, for example 'issues'. Call "
                "`describe_cloud_*` or `get_agent_skill_docs` for supported entity types."
            ),
        ),
    ],
    action: Annotated[
        ExternalApiReadOnlyAction,
        Field(
            description="The read action to run: `list`, `get`, or `search`.",
            default=ExternalApiReadOnlyAction.LIST,
        ),
    ] = ExternalApiReadOnlyAction.LIST,
    api_args: Annotated[
        dict[str, Any] | str | None,
        Field(
            description=(
                "Connector-specific arguments for the action, as an object or a JSON "
                "object string. For example {'repository': 'airbytehq/PyAirbyte'}."
            ),
            default=None,
        ),
    ] = None,
    select_fields: Annotated[
        list[str] | str | None,
        Field(
            description="Fields to keep in the response, as a list or a CSV string.",
            default=None,
        ),
    ] = None,
    exclude_fields: Annotated[
        list[str] | str | None,
        Field(
            description="Fields to drop from the response, as a list or a CSV string.",
            default=None,
        ),
    ] = None,
    page_size: Annotated[
        int | None,
        Field(
            description="Maximum number of entities to return in this page.",
            default=None,
        ),
    ] = None,
    cursor: Annotated[
        str | None,
        Field(
            description="Pagination cursor from a previous response.",
            default=None,
        ),
    ] = None,
    skip_truncation: Annotated[
        bool,
        Field(
            description="Skip truncating long field values in the response.",
            default=True,
        ),
    ] = True,
    intent: Annotated[
        str | None,
        Field(
            description="Optional free-text intent recorded with the request.",
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ] = None,
) -> ExternalApiExecuteResult:
    """Read data from an external system through a deployed Cloud connector's direct API.

    Use `describe_cloud_*` (with `with_direct_access_guidance=True`) or
    `get_agent_skill_docs` to learn the entity types, actions, and `api_args` a
    connector supports.
    """
    connector = _get_cloud_workspace(ctx, workspace_id).get_connector(connector_id)
    return connector.execute_api_query(
        entity_type,
        action,
        resolve_api_args(api_args),
        select_fields=resolve_list_of_strings(select_fields),
        exclude_fields=resolve_list_of_strings(exclude_fields),
        page_size=page_size,
        cursor=cursor,
        skip_truncation=skip_truncation,
        intent=intent,
    )


@mcp_tool(
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def execute_external_api_action(  # noqa: PLR0913  # Explicit args mirror the connector API.
    ctx: Context,
    *,
    connector_id: Annotated[
        str,
        Field(description="The ID of the deployed source connector to act on."),
    ],
    entity_type: Annotated[
        str,
        Field(
            description=(
                "The type of entity to act on, for example 'issues'. Call "
                "`describe_cloud_*` or `get_agent_skill_docs` for supported entity types."
            ),
        ),
    ],
    action: Annotated[
        ExternalApiWriteAction,
        Field(description="The write action to run: `create`, `update`, or `delete`."),
    ],
    api_args: Annotated[
        dict[str, Any] | str | None,
        Field(
            description=(
                "Connector-specific arguments for the action, as an object or a JSON "
                "object string. For example {'repository': 'airbytehq/PyAirbyte'}."
            ),
            default=None,
        ),
    ] = None,
    select_fields: Annotated[
        list[str] | str | None,
        Field(
            description="Fields to keep in the response, as a list or a CSV string.",
            default=None,
        ),
    ] = None,
    exclude_fields: Annotated[
        list[str] | str | None,
        Field(
            description="Fields to drop from the response, as a list or a CSV string.",
            default=None,
        ),
    ] = None,
    skip_truncation: Annotated[
        bool,
        Field(
            description="Skip truncating long field values in the response.",
            default=True,
        ),
    ] = True,
    intent: Annotated[
        str | None,
        Field(
            description="Optional free-text intent recorded with the request.",
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ] = None,
) -> ExternalApiExecuteResult:
    """Run a write action through a deployed Cloud connector's direct API.

    Creates, updates, or deletes data in the external system.

    Use `describe_cloud_*` (with `with_direct_access_guidance=True`) or
    `get_agent_skill_docs` to learn the entity types, actions, and `api_args` a
    connector supports.
    """
    connector = _get_cloud_workspace(ctx, workspace_id).get_connector(connector_id)
    return connector.execute_api_action(
        entity_type,
        action,
        resolve_api_args(api_args),
        select_fields=resolve_list_of_strings(select_fields),
        exclude_fields=resolve_list_of_strings(exclude_fields),
        skip_truncation=skip_truncation,
        intent=intent,
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def execute_external_sql_query(
    ctx: Context,
    *,
    connector_id: Annotated[
        str,
        Field(description="The ID of the deployed SQL-passthrough destination to query."),
    ],
    sql: Annotated[
        str,
        Field(description="The read-only SQL statement to run, for example `SHOW TABLES`."),
    ],
    sql_dialect: Annotated[
        str | None,
        Field(
            description=(
                "The SQL dialect (`snowflake` or `bigquery`). Defaults to the "
                "destination's registered dialect."
            ),
            default=None,
        ),
    ] = None,
    page_size: Annotated[
        int | None,
        Field(
            description="Maximum number of rows to return in this page.",
            default=None,
        ),
    ] = None,
    cursor: Annotated[
        str | None,
        Field(
            description="Pagination cursor from a previous response.",
            default=None,
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        Field(
            description=(
                "Validate the SQL and return only the result columns without "
                "executing a scan. Cannot be combined with `cursor`."
            ),
            default=False,
        ),
    ] = False,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ] = None,
) -> ExternalApiExecuteResult:
    """Run a read-only SQL query against a deployed SQL-passthrough destination.

    Only SQL-passthrough destinations (Snowflake/BigQuery) support this tool.

    Run `SHOW TABLES` first to discover tables; `sql_dialect` defaults to the
    destination's registered dialect. Use `dry_run=True` with
    `SELECT * FROM <table> LIMIT 1` to discover a table's columns.

    `SHOW TABLES` is the only non-`SELECT` statement accepted and takes no
    `LIMIT`; add a `LIMIT` to every `SELECT`. On Snowflake prefer unquoted
    identifiers: double-quoting makes them case-sensitive.
    """
    connector = _get_cloud_workspace(ctx, workspace_id).get_connector(connector_id)
    return connector.execute_sql_query(
        sql,
        sql_dialect=sql_dialect,
        page_size=page_size,
        cursor=cursor,
        dry_run=dry_run,
    )


@mcp_tool(
    read_only=False,
    idempotent=False,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def check_cloud_connector(
    ctx: Context,
    connector_id: Annotated[
        str,
        Field(description="The ID of the deployed connector to check."),
    ],
    *,
    connector_type: Annotated[
        ConnectorType | None,
        Field(
            description=CONNECTOR_TYPE_TIP_TEXT,
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> ConnectorCheckResult:
    """Check the configuration and credentials of a deployed source or destination."""
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connector = _get_cloud_connector(workspace, connector_id, connector_type)
    check_result = connector.check(raise_on_error=False)
    return ConnectorCheckResult(
        connector_id=connector_id,
        connector_type=connector.connector_type,
        succeeded=check_result.success,
        message=_get_connector_check_message(check_result),
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def describe_cloud_connection(
    ctx: Context,
    connection_id: Annotated[
        str,
        Field(description="The ID of the connection to describe."),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> CloudConnectionDetails:
    """Get detailed information about a specific deployed connection."""
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)

    return CloudConnectionDetails(
        connection_id=connection.connection_id,
        connection_name=cast(str, connection.name),
        connection_url=cast(str, connection.connection_url),
        source_id=connection.source_id,
        source_name=cast(str, connection.source.name),
        destination_id=connection.destination_id,
        destination_name=cast(str, connection.destination.name),
        selected_streams=connection.stream_names,
        table_prefix=connection.table_prefix,
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def get_cloud_sync_logs(
    ctx: Context,
    connection_id: Annotated[
        str,
        Field(description="The ID of the Airbyte Cloud connection."),
    ],
    job_id: Annotated[
        int | None,
        Field(description="Optional job ID. If not provided, the latest job will be used."),
    ] = None,
    attempt_number: Annotated[
        int | None,
        Field(
            description="Optional attempt number. If not provided, the latest attempt will be used."
        ),
    ] = None,
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
    max_lines: Annotated[
        int,
        Field(
            description=(
                "Maximum number of lines to return. "
                "Defaults to 4000 if not specified. "
                "If '0' is provided, no limit is applied."
            ),
            default=4000,
        ),
    ],
    from_tail: Annotated[
        bool | None,
        Field(
            description=(
                "Pull from the end of the log text if total lines is greater than 'max_lines'. "
                "Defaults to True if `line_offset` is not specified. "
                "Cannot combine `from_tail=True` with `line_offset`."
            ),
            default=None,
        ),
    ],
    line_offset: Annotated[
        int | None,
        Field(
            description=(
                "Number of lines to skip from the beginning of the logs. "
                "Cannot be combined with `from_tail=True`."
            ),
            default=None,
        ),
    ],
) -> LogReadResult:
    """Get the logs from a sync job attempt on Airbyte Cloud."""
    # Validate that line_offset and from_tail are not both set
    if line_offset is not None and from_tail:
        raise PyAirbyteInputError(
            message="Cannot specify both 'line_offset' and 'from_tail' parameters.",
            context={"line_offset": line_offset, "from_tail": from_tail},
        )

    if from_tail is None and line_offset is None:
        from_tail = True
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)

    sync_result: SyncResult | None = connection.get_sync_result(job_id=job_id)

    if not sync_result:
        raise AirbyteMissingResourceError(
            resource_type="sync job",
            resource_name_or_id=connection_id,
        )

    attempts = sync_result.get_attempts()

    if not attempts:
        raise AirbyteMissingResourceError(
            resource_type="sync attempt",
            resource_name_or_id=str(sync_result.job_id),
        )

    if attempt_number is not None:
        target_attempt = None
        for attempt in attempts:
            if attempt.attempt_number == attempt_number:
                target_attempt = attempt
                break

        if target_attempt is None:
            raise AirbyteMissingResourceError(
                resource_type="sync attempt",
                resource_name_or_id=f"job {sync_result.job_id}, attempt {attempt_number}",
            )
    else:
        target_attempt = max(attempts, key=lambda a: a.attempt_number)

    logs = target_attempt.get_full_log_text()

    if not logs:
        # Return empty result with zero lines
        return LogReadResult(
            log_text=(
                f"[No logs available for job '{sync_result.job_id}', "
                f"attempt {target_attempt.attempt_number}.]"
            ),
            log_text_start_line=1,
            log_text_line_count=0,
            total_log_lines_available=0,
            job_id=sync_result.job_id,
            attempt_number=target_attempt.attempt_number,
        )

    # Apply line limiting
    log_lines = logs.splitlines()
    total_lines = len(log_lines)

    # Determine effective max_lines (0 means no limit)
    effective_max = total_lines if max_lines == 0 else max_lines

    # Calculate start_index and slice based on from_tail or line_offset
    if from_tail:
        start_index = max(0, total_lines - effective_max)
        selected_lines = log_lines[start_index:][:effective_max]
    else:
        start_index = line_offset or 0
        selected_lines = log_lines[start_index : start_index + effective_max]

    return LogReadResult(
        log_text="\n".join(selected_lines),
        log_text_start_line=start_index + 1,  # Convert to 1-based index
        log_text_line_count=len(selected_lines),
        total_log_lines_available=total_lines,
        job_id=sync_result.job_id,
        attempt_number=target_attempt.attempt_number,
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def list_cloud_connections(
    ctx: Context,
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
    name_contains: Annotated[
        str | None,
        Field(
            description="Optional case-insensitive substring to filter connections by name",
            default=None,
        ),
    ],
    limit: Annotated[
        int | None,
        Field(
            description="Optional maximum number of items to return (default: no limit)",
            default=None,
        ),
    ],
    with_connection_status: Annotated[
        bool | None,
        Field(
            description="If True, include status info for each connection's most recent sync job",
            default=False,
        ),
    ],
    failing_connections_only: Annotated[
        bool | None,
        Field(
            description="If True, only return connections with failed/cancelled last sync",
            default=False,
        ),
    ],
) -> list[CloudConnectionResult]:
    """List all deployed connections in the Airbyte Cloud workspace.

    When with_connection_status is True, each connection result will include
    information about the most recent sync job status, skipping over any
    currently in-progress syncs to find the last completed job.

    When failing_connections_only is True, only connections where the most
    recent completed sync job failed or was cancelled will be returned.
    This implicitly enables with_connection_status.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connections = workspace.list_connections(
        limit=None if name_contains or failing_connections_only else limit
    )

    # Filter by name if requested
    if name_contains:
        needle = name_contains.lower()
        connections = [c for c in connections if c.name is not None and needle in c.name.lower()]

    # If failing_connections_only is True, implicitly enable with_connection_status
    if failing_connections_only:
        with_connection_status = True

    results: list[CloudConnectionResult] = []

    for connection in connections:
        last_job_status: str | None = None
        last_job_id: int | None = None
        last_job_time: str | None = None
        currently_running_job_id: int | None = None
        currently_running_job_start_time: str | None = None

        if with_connection_status:
            sync_logs = connection.get_previous_sync_logs(limit=5)
            last_completed_job_status = None  # Keep enum for comparison

            for sync_result in sync_logs:
                job_status = sync_result.get_job_status()

                if not sync_result.is_job_complete():
                    currently_running_job_id = sync_result.job_id
                    currently_running_job_start_time = sync_result.start_time.isoformat()
                    continue

                last_completed_job_status = job_status
                last_job_status = job_status.value
                last_job_id = sync_result.job_id
                last_job_time = sync_result.start_time.isoformat()
                break

            if failing_connections_only and (
                last_completed_job_status is None
                or last_completed_job_status not in FAILED_STATUSES
            ):
                continue

        results.append(
            CloudConnectionResult(
                id=connection.connection_id,
                name=cast(str, connection.name),
                url=cast(str, connection.connection_url),
                source_id=connection.source_id,
                destination_id=connection.destination_id,
                last_job_status=last_job_status,
                last_job_id=last_job_id,
                last_job_time=last_job_time,
                currently_running_job_id=currently_running_job_id,
                currently_running_job_start_time=currently_running_job_start_time,
            )
        )

        if limit is not None and len(results) >= limit:
            break

    return results


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def list_cloud_workspaces(
    ctx: Context,
    *,
    organization_id: Annotated[
        str | None,
        Field(
            description="Optional organization ID to list workspaces within.",
            default=None,
        ),
    ],
    organization_name: Annotated[
        str | None,
        Field(
            description=("Optional organization name (exact match) to list workspaces within."),
            default=None,
        ),
    ],
    name_contains: Annotated[
        str | None,
        Field(
            description="Optional substring to filter workspaces by name (server-side filtering)",
            default=None,
        ),
    ],
    limit: Annotated[
        int | None,
        Field(
            description="Optional maximum number of items to return (default: no limit)",
            default=None,
        ),
    ],
    privilege_scope: Annotated[
        WorkspacePrivilegeScope,
        Field(
            description=(
                "How broadly to search: direct memberships by default, organization "
                "memberships, instance-wide admin access, or any available scope."
            ),
            default=WorkspacePrivilegeScope.MEMBER_OF,
        ),
    ],
) -> CloudWorkspaceListResult:
    """List all workspaces visible to the authenticated credentials.

    The default returns direct workspace memberships. Use `organization_id` or a broader
    `privilege_scope` to discover more workspaces.
    """
    client = _get_cloud_client(ctx)

    try:
        workspaces = client.list_workspaces(
            organization_id=organization_id,
            organization_name=organization_name,
            name_contains=name_contains,
            limit=limit,
            privilege_scope=privilege_scope,
        )
    except AirbyteError as error:
        return _handle_discovery_permission_error(
            error,
            make_result=lambda message: CloudWorkspaceListResult(
                workspaces=[],
                message=message,
            ),
        )

    results = [
        CloudWorkspaceResult(
            workspace_id=ws.workspace_id,
            workspace_name=ws.name,
            organization_id=ws.organization_id,
        )
        for ws in workspaces
    ]
    organization_ids = {
        result.organization_id for result in results if result.organization_id is not None
    }
    message = (
        "No workspaces were returned for these credentials. By default only direct "
        "workspace memberships are listed; pass `organization_id` or a broader "
        "`privilege_scope` to discover organization-wide workspaces, or call "
        "`get_default_cloud_context` to inspect your memberships."
        if not results
        else None
    )
    if len(organization_ids) == 1:
        resolved_organization_id = next(iter(organization_ids))
        try:
            organization = client.get_organization(organization_id=resolved_organization_id)
        except AirbyteError:
            pass
        else:
            for result in results:
                if result.organization_id == resolved_organization_id:
                    result.organization_name = organization.organization_name
            if organization_id is None and organization_name is None:
                resolved_organization = (
                    f"{organization.organization_name} ({resolved_organization_id})"
                    if organization.organization_name is not None
                    else resolved_organization_id
                )
                message = f"Resolved organization {resolved_organization} for these credentials."
    return CloudWorkspaceListResult(
        workspaces=results,
        message=message,
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def get_default_cloud_context(ctx: Context) -> CloudDefaultContextResult:
    """Return the authenticated user's default Cloud context.

    This is the one-call orientation entry point: it resolves the default
    workspace and its parent organization in a single call, along with the
    user's explicit workspace and organization memberships.
    """
    context: CloudDefaultContextInfo = _get_cloud_client(ctx).get_default_context_for_user()
    truncated_memberships: list[str] = []
    if context.member_organizations_truncated:
        truncated_memberships.append(
            f"{len(context.member_organizations)} organization memberships"
        )
    if context.member_workspaces_truncated:
        truncated_memberships.append(f"{len(context.member_workspaces)} workspace memberships")
    resolved_default_workspace = None
    if context.default_workspace_id is not None:
        if not context.default_workspace_verified:
            resolved_default_workspace = (
                f"Default workspace ID {context.default_workspace_id} could not be verified "
                "(it may have been deleted or is not accessible with these credentials)"
            )
        else:
            workspace_detail = context.default_workspace_id
            if context.default_workspace_name is not None:
                workspace_detail = (
                    f"{context.default_workspace_name} ({context.default_workspace_id})"
                )
            resolved_default_workspace = f"Resolved default workspace {workspace_detail}"
        if context.default_workspace_verified and context.default_organization_id is not None:
            organization_detail = context.default_organization_id
            if context.default_organization_name is not None:
                organization_detail = (
                    f"{context.default_organization_name} " f"({context.default_organization_id})"
                )
            resolved_default_workspace += f" in organization {organization_detail}"
        resolved_default_workspace += ". "
    message = (
        "These lists are membership-based, not access-based: they show explicit "
        "organization and workspace memberships only. Use default_workspace_id, "
        "pass workspace_id from member_workspaces, or pick an organization from "
        "member_organizations."
    )
    if truncated_memberships:
        message += (
            f" Only the first {' and '.join(truncated_memberships)} are shown; use "
            "list_cloud_organizations or list_cloud_workspaces to see the rest."
        )
    if context.unvalidated_workspace_count > 0:
        message += (
            f" {context.unvalidated_workspace_count} additional direct workspace grant(s) were "
            f"not validated because this call checks at most {MAX_WORKSPACES_TO_VALIDATE}; use "
            "list_cloud_workspaces to see them."
        )
    if resolved_default_workspace is not None:
        message = resolved_default_workspace + message
    return CloudDefaultContextResult(
        user_id=context.user_id,
        user_name=context.user_name,
        user_email=context.user_email,
        default_workspace_id=context.default_workspace_id,
        default_workspace_name=context.default_workspace_name,
        default_workspace_verified=context.default_workspace_verified,
        unvalidated_workspace_count=context.unvalidated_workspace_count,
        default_organization_id=context.default_organization_id,
        default_organization_name=context.default_organization_name,
        configured_workspace_id=context.configured_workspace_id,
        configured_organization_id=context.configured_organization_id,
        member_organizations=context.member_organizations,
        member_workspaces=[
            CloudWorkspaceResult(
                workspace_id=ws.workspace_id,
                workspace_name=ws.name,
                organization_id=ws.organization_id,
                organization_name=ws.organization_name,
            )
            for ws in context.member_workspaces
        ],
        member_organizations_truncated=context.member_organizations_truncated,
        member_workspaces_truncated=context.member_workspaces_truncated,
        discovery_hints=context.discovery_hints,
        message=message,
    )


@mcp_tool(
    idempotent=True,
    destructive=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def set_default_cloud_workspace(
    ctx: Context,
    user_email: Annotated[
        str,
        Field(
            description=(
                "Email of the authenticated Airbyte Cloud user this change applies to. "
                "Must match the current credentials' user (compared case-insensitively, "
                "ignoring surrounding whitespace; see get_default_cloud_context); "
                "mismatches fail with a validation error. "
                "Required as a safety confirmation."
            ),
        ),
    ],
    workspace_id: Annotated[
        str,
        Field(
            description=(
                "ID of the workspace to make the durable default. The user must be an "
                "explicit member of the workspace or its organization; tombstoned "
                "workspaces are rejected."
            ),
        ),
    ],
) -> CloudDefaultWorkspaceUpdateResult:
    """Durably set the authenticated user's default Airbyte Cloud workspace.

    WARNING: This is a persistent, account-level change. It updates the user's
    stored default workspace in Airbyte Cloud, which affects both future MCP
    sessions (default_workspace_id in get_default_cloud_context and every tool
    that falls back to the default workspace) AND the Airbyte Cloud web app,
    where this workspace becomes the user's default landing workspace.
    Call get_default_cloud_context first to confirm the current user and to
    discover member workspaces.
    """
    result: CloudDefaultWorkspaceUpdateInfo = _get_cloud_client(ctx).set_default_workspace_for_user(
        user_email=user_email,
        workspace_id=workspace_id,
    )
    workspace_detail = result.default_workspace_id
    if result.default_workspace_name is not None:
        workspace_detail = f"{result.default_workspace_name} ({result.default_workspace_id})"
    return CloudDefaultWorkspaceUpdateResult(
        **result.model_dump(),
        message=(
            f"Default workspace durably set to {workspace_detail} for "
            f"{result.user_email}. This applies to future MCP sessions and the "
            "Airbyte Cloud web app."
        ),
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def list_cloud_organizations(
    ctx: Context,
    name_contains: Annotated[
        str | None,
        Field(
            description="Optional case-insensitive substring to filter organization names.",
            default=None,
        ),
    ] = None,
    limit: Annotated[
        int | None,
        Field(
            description="Optional maximum number of organizations to return (default: 100).",
            default=None,
        ),
    ] = None,
    feature_filter: Annotated[
        OrganizationFeature | None,
        Field(
            description=(
                "Optional feature filter: `direct_access` returns only organizations enabled "
                "for AI agents through the Airbyte Context layer; `search_indexing` returns "
                "only organizations where search indexing is available. Omit to list every "
                "organization along with its enabled features."
            ),
            default=None,
        ),
    ] = None,
) -> CloudOrganizationListResult:
    """List organizations visible to the authenticated Airbyte Cloud credentials.

    Each organization reports `enabled_features`; pass `feature_filter` to return only
    organizations with a given feature.
    """
    effective_limit = 100 if limit is None else limit
    try:
        organizations = _get_cloud_client(ctx).list_organizations(
            name_contains=name_contains,
            feature_filter=feature_filter,
            limit=effective_limit,
        )
    except AirbyteError as error:
        return _handle_discovery_permission_error(
            error,
            make_result=lambda message: CloudOrganizationListResult(
                organizations=[],
                message=message,
            ),
        )

    if not organizations and feature_filter is not None:
        return CloudOrganizationListResult(
            organizations=[],
            message=(
                f"No organizations visible to these credentials have `{feature_filter.value}` "
                "enabled. Omit `feature_filter` to list every organization with its feature flags."
            ),
        )

    if not organizations:
        return CloudOrganizationListResult(
            organizations=[],
            message=(
                "No organizations were returned for these credentials. Verify the credentials "
                "or ask the user to provide an organization ID. Call "
                "`get_default_cloud_context` to inspect your memberships."
            ),
        )

    return CloudOrganizationListResult(
        organizations=[
            CloudOrganizationResult(
                id=organization.organization_id,
                name=organization.organization_name,
                email=organization.email,
                enabled_features=sorted(organization.enabled_features),
            )
            for organization in organizations
        ],
        message=(
            f"Showing the first {effective_limit} organizations; more may exist. "
            "Pass `name_contains` to narrow the search, or a larger `limit`."
            if len(organizations) == effective_limit
            else None
        ),
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def describe_cloud_workspace(
    ctx: Context,
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=(
                "Workspace ID. With no argument, resolves the configured default or the "
                "authenticated user's default workspace."
            ),
            default=None,
        ),
    ],
) -> CloudWorkspaceResult:
    """Get basic details about a workspace (ID, name, URL, parent organization).

    Does not include billing/account status; use `get_cloud_organization_billing_status` for that.
    """
    workspace = _get_cloud_workspace(ctx, workspace_id)
    workspace_response = api_util.get_workspace(
        workspace_id=workspace.workspace_id,
        api_root=workspace.api_root,
        client_id=workspace.client_id,
        client_secret=workspace.client_secret,
        bearer_token=workspace.bearer_token,
    )
    organization = workspace.get_organization(raise_on_error=False)
    return CloudWorkspaceResult(
        workspace_id=workspace_response.workspace_id,
        workspace_name=workspace_response.name,
        workspace_url=workspace.workspace_url,
        organization_id=organization.organization_id if organization else None,
        organization_name=organization.organization_name if organization else None,
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def describe_cloud_organization(
    ctx: Context,
    *,
    organization_id: Annotated[
        str | None,
        Field(
            description=(
                "Organization ID. With no arguments, resolves from the configured "
                "default or the authenticated user's sole membership."
            ),
            default=None,
        ),
    ],
    organization_name: Annotated[
        str | None,
        Field(
            description=(
                "Organization name (exact match). With no arguments, resolves from the "
                "configured default or the authenticated user's sole membership. With "
                "multiple memberships, the error lists candidate organization IDs."
            ),
            default=None,
        ),
    ],
) -> CloudOrganizationResult:
    """Get basic details about an organization (ID, name, email, feature flags).

    Billing/account status is available via `get_cloud_organization_billing_status`.

    With no arguments, resolves the organization from the configured default or the
    authenticated user's sole membership. With multiple memberships, the error lists
    candidate organization IDs. Use organization_id or organization_name (exact match)
    to look up a specific organization.
    """
    org = _get_cloud_client(ctx).get_organization(
        organization_id=organization_id,
        organization_name=organization_name,
    )

    return CloudOrganizationResult(
        id=org.organization_id,
        name=org.organization_name,
        email=org.email,
        enabled_features=sorted(org.enabled_features),
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def get_cloud_organization_billing_status(
    ctx: Context,
    *,
    organization_id: Annotated[
        str | None,
        Field(
            description="Organization ID, when known.",
            default=None,
        ),
    ],
    organization_name: Annotated[
        str | None,
        Field(
            description="Organization name for an exact match, when ID is not provided.",
            default=None,
        ),
    ],
) -> CloudOrganizationBillingStatusResult:
    """Get billing and account status for an organization.

    This generally requires elevated `ORGANIZATION_READER` or administrator permissions.
    """
    org = _get_cloud_client(ctx).get_organization(
        organization_id=organization_id,
        organization_name=organization_name,
    )
    try:
        info = org.get_billing_status()
    except (AirbyteError, NotImplementedError) as error:
        reason = error.message if isinstance(error, AirbyteError) and error.message else str(error)
        return CloudOrganizationBillingStatusResult(
            organization_id=org.organization_id,
            organization_name=org.organization_name,
            billing_info_available=False,
            message=f"Billing information could not be retrieved: {reason}",
        )
    return CloudOrganizationBillingStatusResult(
        organization_id=org.organization_id,
        organization_name=org.organization_name,
        billing_info_available=True,
        payment_status=info.payment_status,
        subscription_status=info.subscription_status,
        is_account_locked=info.is_account_locked,
    )


def _get_custom_source_definition_description(
    custom_source: CustomCloudSourceDefinition,
) -> str:
    return "\n".join(
        [
            f" - Custom Source Name: {custom_source.name}",
            f" - Definition ID: {custom_source.definition_id}",
            f" - Definition Version: {custom_source.version}",
            f" - Connector Builder Project ID: {custom_source.connector_builder_project_id}",
            f" - Connector Builder Project URL: {custom_source.connector_builder_project_url}",
        ]
    )


@mcp_tool(
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def publish_custom_source_definition(
    ctx: Context,
    name: Annotated[
        str,
        Field(description="The name for the custom connector definition."),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
    manifest_yaml: Annotated[
        str | Path | None,
        Field(
            description=(
                "The Low-code CDK manifest as a YAML string or file path. "
                "Required for YAML connectors."
            ),
            default=None,
        ),
    ] = None,
    unique: Annotated[
        bool,
        Field(
            description="Whether to require a unique name.",
            default=True,
        ),
    ] = True,
    pre_validate: Annotated[
        bool,
        Field(
            description="Whether to validate the manifest client-side before publishing.",
            default=True,
        ),
    ] = True,
    testing_values: Annotated[
        dict | str | None,
        Field(
            description=(
                "Optional testing configuration values for the Builder UI. "
                "Can be provided as a JSON object or JSON string. "
                "Supports inline secret refs via 'secret_reference::ENV_VAR_NAME' syntax. "
                "If provided, these values replace any existing testing values "
                "for the connector builder project, allowing immediate test read operations."
            ),
            default=None,
        ),
    ],
    testing_values_secret_name: Annotated[
        str | None,
        Field(
            description=(
                "Optional name of a secret containing testing configuration values "
                "in JSON or YAML format. The secret will be resolved by the MCP "
                "server and merged into testing_values, with secret values taking "
                "precedence. This lets the agent reference secrets without sending "
                "raw values as tool arguments."
            ),
            default=None,
        ),
    ],
) -> str:
    """Publish a custom YAML source connector definition to Airbyte Cloud.

    Note: Only YAML (declarative) connectors are currently supported.
    Docker-based custom sources are not yet available.
    """
    processed_manifest = manifest_yaml
    if isinstance(manifest_yaml, str) and "\n" not in manifest_yaml:
        processed_manifest = Path(manifest_yaml)

    # Resolve testing values from inline config and/or secret
    testing_values_dict: dict[str, Any] | None = None
    if testing_values is not None or testing_values_secret_name is not None:
        testing_values_dict = (
            resolve_connector_config(
                config=testing_values,
                config_secret_name=testing_values_secret_name,
            )
            or None
        )

    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    custom_source = workspace.publish_custom_source_definition(
        name=name,
        manifest_yaml=processed_manifest,
        unique=unique,
        pre_validate=pre_validate,
        testing_values=testing_values_dict,
    )
    register_guid_created_in_session(custom_source.definition_id)
    return (
        "Successfully published custom YAML source definition:\n"
        + _get_custom_source_definition_description(
            custom_source=custom_source,
        )
        + "\n"
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
)
def list_custom_source_definitions(
    ctx: Context,
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> list[dict[str, Any]]:
    """List custom YAML source definitions in the Airbyte Cloud workspace.

    Note: Only YAML (declarative) connectors are currently supported.
    Docker-based custom sources are not yet available.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    definitions = workspace.list_custom_source_definitions(
        definition_type="yaml",
    )

    return [
        {
            "definition_id": d.definition_id,
            "name": d.name,
            "version": d.version,
            "connector_builder_project_url": d.connector_builder_project_url,
        }
        for d in definitions
    ]


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
)
def get_custom_source_definition(
    ctx: Context,
    definition_id: Annotated[
        str,
        Field(description="The ID of the custom source definition to retrieve."),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
    include_draft: Annotated[
        bool,
        Field(
            description=(
                "Whether to include the Connector Builder draft manifest in the response. "
                "If True and a draft exists, the response will include 'has_draft' and "
                "'draft_manifest' fields. Defaults to False."
            ),
            default=False,
        ),
    ] = False,
) -> dict[str, Any]:
    """Get a custom YAML source definition from Airbyte Cloud, including its manifest.

    Returns the full definition details including the published manifest YAML content.
    Optionally includes the Connector Builder draft manifest (unpublished changes)
    when include_draft=True.

    Note: Only YAML (declarative) connectors are currently supported.
    Docker-based custom sources are not yet available.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    definition = workspace.get_custom_source_definition(
        definition_id=definition_id,
        definition_type="yaml",
    )

    result: dict[str, Any] = {
        "definition_id": definition.definition_id,
        "name": definition.name,
        "version": definition.version,
        "connector_builder_project_id": definition.connector_builder_project_id,
        "connector_builder_project_url": definition.connector_builder_project_url,
        "manifest": definition.manifest,
    }

    if include_draft:
        result["has_draft"] = definition.has_draft
        result["draft_manifest"] = definition.draft_manifest

    return result


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
)
def get_connector_builder_draft_manifest(
    ctx: Context,
    definition_id: Annotated[
        str,
        Field(description="The ID of the custom source definition to retrieve the draft for."),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> dict[str, Any]:
    """Get the Connector Builder draft manifest for a custom source definition.

    Returns the working draft manifest that has been saved in the Connector Builder UI
    but not yet published. This is useful for inspecting what a user is currently working
    on before they publish their changes.

    If no draft exists, 'has_draft' will be False and 'draft_manifest' will be None.
    The published manifest is always included for comparison.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    definition = workspace.get_custom_source_definition(
        definition_id=definition_id,
        definition_type="yaml",
    )

    return {
        "definition_id": definition.definition_id,
        "name": definition.name,
        "connector_builder_project_id": definition.connector_builder_project_id,
        "connector_builder_project_url": definition.connector_builder_project_url,
        "has_draft": definition.has_draft,
        "draft_manifest": definition.draft_manifest,
        "published_manifest": definition.manifest,
    }


@mcp_tool(
    destructive=True,
    open_world=True,
)
def update_custom_source_definition(
    ctx: Context,
    definition_id: Annotated[
        str,
        Field(description="The ID of the definition to update."),
    ],
    manifest_yaml: Annotated[
        str | Path | None,
        Field(
            description=(
                "New manifest as YAML string or file path. "
                "Optional; omit to update only testing values."
            ),
            default=None,
        ),
    ] = None,
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
    pre_validate: Annotated[
        bool,
        Field(
            description="Whether to validate the manifest client-side before updating.",
            default=True,
        ),
    ] = True,
    testing_values: Annotated[
        dict | str | None,
        Field(
            description=(
                "Optional testing configuration values for the Builder UI. "
                "Can be provided as a JSON object or JSON string. "
                "Supports inline secret refs via 'secret_reference::ENV_VAR_NAME' syntax. "
                "If provided, these values replace any existing testing values "
                "for the connector builder project. The entire testing values object "
                "is overwritten, so pass the full set of values you want to persist."
            ),
            default=None,
        ),
    ],
    testing_values_secret_name: Annotated[
        str | None,
        Field(
            description=(
                "Optional name of a secret containing testing configuration values "
                "in JSON or YAML format. The secret will be resolved by the MCP "
                "server and merged into testing_values, with secret values taking "
                "precedence. This lets the agent reference secrets without sending "
                "raw values as tool arguments."
            ),
            default=None,
        ),
    ],
) -> str:
    """Update a custom YAML source definition in Airbyte Cloud.

    Updates the manifest and/or testing values for an existing custom source definition.
    At least one of manifest_yaml, testing_values, or testing_values_secret_name must be provided.
    """
    check_guid_created_in_session(definition_id)

    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)

    if manifest_yaml is None and testing_values is None and testing_values_secret_name is None:
        raise PyAirbyteInputError(
            message=(
                "At least one of manifest_yaml, testing_values, or testing_values_secret_name "
                "must be provided to update a custom source definition."
            ),
            context={
                "definition_id": definition_id,
                "workspace_id": workspace.workspace_id,
            },
        )

    processed_manifest: str | Path | None = manifest_yaml
    if isinstance(manifest_yaml, str) and "\n" not in manifest_yaml:
        processed_manifest = Path(manifest_yaml)

    # Resolve testing values from inline config and/or secret
    testing_values_dict: dict[str, Any] | None = None
    if testing_values is not None or testing_values_secret_name is not None:
        testing_values_dict = (
            resolve_connector_config(
                config=testing_values,
                config_secret_name=testing_values_secret_name,
            )
            or None
        )

    definition = workspace.get_custom_source_definition(
        definition_id=definition_id,
        definition_type="yaml",
    )
    custom_source: CustomCloudSourceDefinition = definition

    if processed_manifest is not None:
        custom_source = definition.update_definition(
            manifest_yaml=processed_manifest,
            pre_validate=pre_validate,
        )

    if testing_values_dict is not None:
        custom_source.set_testing_values(testing_values_dict)

    return (
        "Successfully updated custom YAML source definition:\n"
        + _get_custom_source_definition_description(
            custom_source=custom_source,
        )
    )


@mcp_tool(
    destructive=True,
    open_world=True,
)
def permanently_delete_custom_source_definition(
    ctx: Context,
    definition_id: Annotated[
        str,
        Field(description="The ID of the custom source definition to delete."),
    ],
    name: Annotated[
        str,
        Field(description="The expected name of the custom source definition (for verification)."),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> str:
    """Permanently delete a custom YAML source definition from Airbyte Cloud.

    IMPORTANT: This operation requires the connector name to contain "delete-me" or "deleteme"
    (case insensitive).

    If the connector does not meet this requirement, the deletion will be rejected with a
    helpful error message. Instruct the user to rename the connector appropriately to authorize
    the deletion.

    The provided name must match the actual name of the definition for the operation to proceed.
    This is a safety measure to ensure you are deleting the correct resource.

    Note: Only YAML (declarative) connectors are currently supported.
    Docker-based custom sources are not yet available.
    """
    check_guid_created_in_session(definition_id)
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    definition = workspace.get_custom_source_definition(
        definition_id=definition_id,
        definition_type="yaml",
    )
    actual_name: str = definition.name

    # Verify the name matches
    if actual_name != name:
        raise PyAirbyteInputError(
            message=(
                f"Name mismatch: expected '{name}' but found '{actual_name}'. "
                "The provided name must exactly match the definition's actual name. "
                "This is a safety measure to prevent accidental deletion."
            ),
            context={
                "definition_id": definition_id,
                "expected_name": name,
                "actual_name": actual_name,
            },
        )

    definition.permanently_delete(
        safe_mode=True,  # Hard-coded safe mode for extra protection when running in LLM agents.
    )
    return f"Successfully deleted custom source definition '{actual_name}' (ID: {definition_id})"


@mcp_tool(
    destructive=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def permanently_delete_cloud_connector(
    ctx: Context,
    connector_id: Annotated[
        str,
        Field(description="The ID of the deployed source or destination to delete."),
    ],
    name: Annotated[
        str,
        Field(description="The expected name of the connector (for verification)."),
    ],
    *,
    connector_type: Annotated[
        ConnectorType | None,
        Field(
            description=CONNECTOR_TYPE_TIP_TEXT,
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> str:
    """Permanently delete a deployed source or destination connector from Airbyte Cloud.

    IMPORTANT: This operation requires the connector name to contain "delete-me" or "deleteme"
    (case insensitive).

    If the connector does not meet this requirement, the deletion will be rejected with a
    helpful error message. Instruct the user to rename the connector appropriately to authorize
    the deletion.

    The provided name must match the actual name of the connector for the operation to proceed.
    This is a safety measure to ensure you are deleting the correct resource.
    """
    check_guid_created_in_session(connector_id)
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connector = _get_cloud_connector(workspace, connector_id, connector_type)
    actual_name: str = cast(str, connector.name)
    resolved_type = connector.connector_type

    # Verify the name matches
    if actual_name != name:
        raise PyAirbyteInputError(
            message=(
                f"Name mismatch: expected '{name}' but found '{actual_name}'. "
                f"The provided name must exactly match the {resolved_type.value}'s actual name. "
                "This is a safety measure to prevent accidental deletion."
            ),
            context={
                "connector_id": connector_id,
                "connector_type": resolved_type.value,
                "expected_name": name,
                "actual_name": actual_name,
            },
        )

    # Safe mode is hard-coded to True for extra protection when running in LLM agents
    if resolved_type == ConnectorType.SOURCE:
        workspace.permanently_delete_source(
            source=connector_id,
            safe_mode=True,  # Requires name to contain "delete-me" or "deleteme" (case insensitive)
        )
    else:
        workspace.permanently_delete_destination(
            destination=connector_id,
            safe_mode=True,  # Requires name to contain "delete-me" or "deleteme" (case insensitive)
        )
    return f"Successfully deleted {resolved_type.value} '{actual_name}' (ID: {connector_id})"


@mcp_tool(
    destructive=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def permanently_delete_cloud_connection(
    ctx: Context,
    connection_id: Annotated[
        str,
        Field(description="The ID of the connection to delete."),
    ],
    name: Annotated[
        str,
        Field(description="The expected name of the connection (for verification)."),
    ],
    *,
    cascade_delete_source: Annotated[
        bool,
        Field(
            description=(
                "Whether to also delete the source connector associated with this connection."
            ),
            default=False,
        ),
    ] = False,
    cascade_delete_destination: Annotated[
        bool,
        Field(
            description=(
                "Whether to also delete the destination connector associated with this connection."
            ),
            default=False,
        ),
    ] = False,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> str:
    """Permanently delete a connection from Airbyte Cloud.

    IMPORTANT: This operation requires the connection name to contain "delete-me" or "deleteme"
    (case insensitive).

    If the connection does not meet this requirement, the deletion will be rejected with a
    helpful error message. Instruct the user to rename the connection appropriately to authorize
    the deletion.

    The provided name must match the actual name of the connection for the operation to proceed.
    This is a safety measure to ensure you are deleting the correct resource.
    """
    check_guid_created_in_session(connection_id)
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)
    actual_name: str = cast(str, connection.name)

    # Verify the name matches
    if actual_name != name:
        raise PyAirbyteInputError(
            message=(
                f"Name mismatch: expected '{name}' but found '{actual_name}'. "
                "The provided name must exactly match the connection's actual name. "
                "This is a safety measure to prevent accidental deletion."
            ),
            context={
                "connection_id": connection_id,
                "expected_name": name,
                "actual_name": actual_name,
            },
        )

    # Safe mode is hard-coded to True for extra protection when running in LLM agents
    workspace.permanently_delete_connection(
        safe_mode=True,  # Requires name-based delete disposition ("delete-me" or "deleteme")
        connection=connection_id,
        cascade_delete_source=cascade_delete_source,
        cascade_delete_destination=cascade_delete_destination,
    )
    return f"Successfully deleted connection '{actual_name}' (ID: {connection_id})"


@mcp_tool(
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def rename_cloud_connector(
    ctx: Context,
    connector_id: Annotated[
        str,
        Field(description="The ID of the deployed source or destination to rename."),
    ],
    name: Annotated[
        str,
        Field(description="New name for the connector."),
    ],
    *,
    connector_type: Annotated[
        ConnectorType | None,
        Field(
            description=CONNECTOR_TYPE_TIP_TEXT,
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> str:
    """Rename a deployed source or destination connector on Airbyte Cloud."""
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connector = _get_typed_cloud_connector(workspace, connector_id, connector_type)
    connector.rename(name=name)
    return (
        f"Successfully renamed {connector.connector_type.value} '{connector_id}' to '{name}'. "
        f"URL: {connector.connector_url}"
    )


@mcp_tool(
    destructive=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def update_cloud_connector_config(
    ctx: Context,
    connector_id: Annotated[
        str,
        Field(description="The ID of the deployed source or destination to update."),
    ],
    config: Annotated[
        dict | str,
        Field(
            description="New configuration for the connector.",
        ),
    ],
    config_secret_name: Annotated[
        str | None,
        Field(
            description="The name of the secret containing the configuration.",
            default=None,
        ),
    ] = None,
    *,
    connector_type: Annotated[
        ConnectorType | None,
        Field(
            description=CONNECTOR_TYPE_TIP_TEXT,
            default=None,
        ),
    ] = None,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> str:
    """Update a deployed source or destination connector's configuration on Airbyte Cloud.

    This is a destructive operation that can break existing connections if the
    configuration is changed incorrectly. Use with caution.
    """
    check_guid_created_in_session(connector_id)
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connector = _get_typed_cloud_connector(workspace, connector_id, connector_type)

    config_dict = resolve_connector_config(
        config=config,
        config_secret_name=config_secret_name,
        config_spec_jsonschema=None,  # We don't have the spec here
    )

    connector.update_config(config=config_dict)
    return (
        f"Successfully updated {connector.connector_type.value} '{connector_id}'. "
        f"URL: {connector.connector_url}"
    )


@mcp_tool(
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def rename_cloud_connection(
    ctx: Context,
    connection_id: Annotated[
        str,
        Field(description="The ID of the connection to rename."),
    ],
    name: Annotated[
        str,
        Field(description="New name for the connection."),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> str:
    """Rename a connection on Airbyte Cloud."""
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)
    connection.rename(name=name)
    return (
        f"Successfully renamed connection '{connection_id}' to '{name}'. "
        f"URL: {connection.connection_url}"
    )


@mcp_tool(
    destructive=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def set_cloud_connection_table_prefix(
    ctx: Context,
    connection_id: Annotated[
        str,
        Field(description="The ID of the connection to update."),
    ],
    prefix: Annotated[
        str,
        Field(description="New table prefix to use when syncing to the destination."),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> str:
    """Set the table prefix for a connection on Airbyte Cloud.

    This is a destructive operation that can break downstream dependencies if the
    table prefix is changed incorrectly. Use with caution.
    """
    check_guid_created_in_session(connection_id)
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)
    connection.set_table_prefix(prefix=prefix)
    return (
        f"Successfully set table prefix for connection '{connection_id}' to '{prefix}'. "
        f"URL: {connection.connection_url}"
    )


@mcp_tool(
    destructive=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def set_cloud_connection_selected_streams(
    ctx: Context,
    connection_id: Annotated[
        str,
        Field(description="The ID of the connection to update."),
    ],
    stream_names: Annotated[
        str | list[str],
        Field(
            description=(
                "The selected stream names to sync within the connection. "
                "Must be an explicit stream name or list of streams."
            )
        ),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> str:
    """Set the selected streams for a connection on Airbyte Cloud.

    This is a destructive operation that can break existing connections if the
    stream selection is changed incorrectly. Use with caution.
    """
    check_guid_created_in_session(connection_id)
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)

    resolved_streams_list: list[str] = resolve_list_of_strings(stream_names)
    connection.set_selected_streams(stream_names=resolved_streams_list)

    return (
        f"Successfully set selected streams for connection '{connection_id}' "
        f"to {resolved_streams_list}. URL: {connection.connection_url}"
    )


@mcp_tool(
    open_world=True,
    destructive=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def update_cloud_connection(
    ctx: Context,
    connection_id: Annotated[
        str,
        Field(description="The ID of the connection to update."),
    ],
    *,
    enabled: Annotated[
        bool | None,
        Field(
            description=(
                "Set the connection's enabled status. "
                "True enables the connection (status='active'), "
                "False disables it (status='inactive'). "
                "Leave unset to keep the current status."
            ),
            default=None,
        ),
    ],
    cron_expression: Annotated[
        str | None,
        Field(
            description=(
                "A Quartz cron expression defining when syncs should run. "
                "Must have 6 or 7 space-separated fields "
                "(seconds, minutes, hours, day-of-month, month, day-of-week[, year]), "
                "optionally followed by a timezone ID. Standard 5-field Unix cron "
                "expressions are rejected by the API, and schedules may run at most "
                "once per hour (seconds and minutes cannot be '*'). "
                "Examples: '0 0 0 * * ?' (daily at midnight UTC), "
                "'0 0 */6 * * ?' (every 6 hours), "
                "'0 0 0 ? * SUN' (weekly on Sunday at midnight UTC), "
                "'0 0 9 ? * MON-FRI US/Pacific' (weekdays at 9am Pacific). "
                "Leave unset to keep the current schedule. "
                "Cannot be used together with 'manual_schedule'."
            ),
            default=None,
        ),
    ],
    manual_schedule: Annotated[
        bool | None,
        Field(
            description=(
                "Set to True to disable automatic syncs (manual scheduling only). "
                "Syncs will only run when manually triggered. "
                "Cannot be used together with 'cron_expression'."
            ),
            default=None,
        ),
    ],
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> str:
    """Update a connection's settings on Airbyte Cloud.

    This tool allows updating multiple connection settings in a single call:
    - Enable or disable the connection
    - Set a cron schedule for automatic syncs
    - Switch to manual scheduling (no automatic syncs)

    At least one setting must be provided. The 'cron_expression' and 'manual_schedule'
    parameters are mutually exclusive.
    """
    check_guid_created_in_session(connection_id)

    # Validate that at least one setting is provided
    if enabled is None and cron_expression is None and manual_schedule is None:
        raise ValueError(
            "At least one setting must be provided: 'enabled', 'cron_expression', "
            "or 'manual_schedule'."
        )

    # Validate mutually exclusive schedule options
    if cron_expression is not None and manual_schedule is True:
        raise ValueError(
            "Cannot specify both 'cron_expression' and 'manual_schedule=True'. "
            "Use 'cron_expression' for scheduled syncs or 'manual_schedule=True' "
            "for manual-only syncs."
        )

    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)

    changes_made: list[str] = []

    # Apply enabled status change
    if enabled is not None:
        connection.set_enabled(enabled=enabled)
        status_str = "enabled" if enabled else "disabled"
        changes_made.append(f"status set to '{status_str}'")

    # Apply schedule change
    if cron_expression is not None:
        connection.set_schedule(cron_expression=cron_expression)
        changes_made.append(f"schedule set to '{cron_expression}'")
    elif manual_schedule is True:
        connection.set_manual_schedule()
        changes_made.append("schedule set to 'manual'")

    changes_summary = ", ".join(changes_made)
    return (
        f"Successfully updated connection '{connection_id}': {changes_summary}. "
        f"URL: {connection.connection_url}"
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def get_connection_artifact(
    ctx: Context,
    connection_id: Annotated[
        str,
        Field(description="The ID of the Airbyte Cloud connection."),
    ],
    artifact_type: Annotated[
        Literal["state", "catalog"],
        Field(description="The type of artifact to retrieve: 'state' or 'catalog'."),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> dict[str, Any] | list[dict[str, Any]]:
    """Get a connection artifact (state or catalog) from Airbyte Cloud.

    By default, returns artifacts in Airbyte protocol format (snake_case,
    suitable for passing to connector CLI flags like `--state` or `--catalog`).

    Retrieves the specified artifact for a connection:
    - `state`: Returns a list of protocol-format `AirbyteStateMessage` dicts,
      or `{"ERROR": "..."}` if no state is set.
    - `catalog`: Returns the protocol-format `ConfiguredAirbyteCatalog` dict,
      or `{"ERROR": "..."}` if not found.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(ctx, workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)

    if artifact_type == "state":
        state = connection.dump_raw_state()
        if not state:
            return {"ERROR": "No state is set for this connection (stateType: not_set)"}
        return state

    # artifact_type == "catalog"
    catalog = connection.dump_raw_catalog()
    if catalog is None:
        return {"ERROR": "No catalog found for this connection"}
    return catalog


def _add_defaults_for_exclude_args(
    exclude_args: list[str],
) -> None:
    """Patch registered tool functions to add Python-level defaults for excluded args.

    FastMCP requires that excluded args have Python-level default values, but MCP tool
    functions should only use Field(default=...) in their Annotated type hints (not
    Python-level `= None`). This function bridges the gap by dynamically adding Python
    defaults to the function signatures at registration time, so the source code stays
    clean while satisfying FastMCP's requirement.

    Args:
        exclude_args: List of argument names that will be excluded from the tool schema.
    """
    import inspect  # noqa: PLC0415  # Local import for optional patching logic

    from fastmcp_extensions.decorators import (  # noqa: PLC0415
        _REGISTERED_TOOLS,  # noqa: PLC2701
    )

    for func, _annotations in _REGISTERED_TOOLS:
        sig = inspect.signature(func)
        needs_patch = any(
            arg_name in sig.parameters
            and sig.parameters[arg_name].default is inspect.Parameter.empty
            for arg_name in exclude_args
        )
        if needs_patch:
            new_params = [
                p.replace(default=None)
                if name in exclude_args and p.default is inspect.Parameter.empty
                else p
                for name, p in sig.parameters.items()
            ]
            func.__signature__ = sig.replace(parameters=new_params)  # type: ignore[attr-defined]


def register_cloud_tools(app: FastMCP) -> None:
    """Register cloud tools with the FastMCP app.

    Args:
        app: FastMCP application instance
    """
    exclude_args = ["workspace_id"] if AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET else None
    if exclude_args:
        _add_defaults_for_exclude_args(exclude_args)
    register_mcp_tools(
        app,
        mcp_module=__name__,
        exclude_args=exclude_args,
    )
