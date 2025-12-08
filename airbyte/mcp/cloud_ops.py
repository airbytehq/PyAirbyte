# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Airbyte Cloud MCP operations."""

from pathlib import Path
from typing import Annotated, Any, cast

from fastmcp import FastMCP
from pydantic import BaseModel, Field

from airbyte import cloud, get_destination, get_source
from airbyte._util import api_util
from airbyte.cloud.auth import (
    resolve_cloud_api_url,
    resolve_cloud_client_id,
    resolve_cloud_client_secret,
    resolve_cloud_workspace_id,
)
from airbyte.cloud.connectors import CustomCloudSourceDefinition
from airbyte.cloud.constants import FAILED_STATUSES
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.destinations.util import get_noop_destination
from airbyte.exceptions import AirbyteMissingResourceError, PyAirbyteInputError
from airbyte.mcp._tool_utils import (
    check_guid_created_in_session,
    mcp_tool,
    register_guid_created_in_session,
    register_tools,
)
from airbyte.mcp._util import resolve_config, resolve_list_of_strings
from airbyte.secrets import SecretString


CLOUD_AUTH_TIP_TEXT = (
    "By default, the `AIRBYTE_CLOUD_CLIENT_ID`, `AIRBYTE_CLOUD_CLIENT_SECRET`, "
    "and `AIRBYTE_CLOUD_WORKSPACE_ID` environment variables "
    "will be used to authenticate with the Airbyte Cloud API."
)
WORKSPACE_ID_TIP_TEXT = "Workspace ID. Defaults to `AIRBYTE_CLOUD_WORKSPACE_ID` env var."


class CloudSourceResult(BaseModel):
    """Information about a deployed source connector in Airbyte Cloud."""

    id: str
    """The source ID."""
    name: str
    """Display name of the source."""
    url: str
    """Web URL for managing this source in Airbyte Cloud."""


class CloudDestinationResult(BaseModel):
    """Information about a deployed destination connector in Airbyte Cloud."""

    id: str
    """The destination ID."""
    name: str
    """Display name of the destination."""
    url: str
    """Web URL for managing this destination in Airbyte Cloud."""


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


class CloudSourceDetails(BaseModel):
    """Detailed information about a deployed source connector in Airbyte Cloud."""

    source_id: str
    """The source ID."""
    source_name: str
    """Display name of the source."""
    source_url: str
    """Web URL for managing this source in Airbyte Cloud."""
    connector_definition_id: str
    """The connector definition ID (e.g., the ID for 'source-postgres')."""


class CloudDestinationDetails(BaseModel):
    """Detailed information about a deployed destination connector in Airbyte Cloud."""

    destination_id: str
    """The destination ID."""
    destination_name: str
    """Display name of the destination."""
    destination_url: str
    """Web URL for managing this destination in Airbyte Cloud."""
    connector_definition_id: str
    """The connector definition ID (e.g., the ID for 'destination-snowflake')."""


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
    name: str
    """Display name of the organization."""
    email: str
    """Email associated with the organization."""


class CloudWorkspaceResult(BaseModel):
    """Information about a workspace in Airbyte Cloud."""

    id: str
    """The workspace ID."""
    name: str
    """Display name of the workspace."""
    organization_id: str
    """ID of the organization this workspace belongs to."""
    organization_name: str | None = None
    """Name of the organization this workspace belongs to."""


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


def _get_cloud_workspace(workspace_id: str | None = None) -> CloudWorkspace:
    """Get an authenticated CloudWorkspace.

    Args:
        workspace_id: Optional workspace ID. If not provided, uses the
            AIRBYTE_CLOUD_WORKSPACE_ID environment variable.
    """
    return CloudWorkspace(
        workspace_id=resolve_cloud_workspace_id(workspace_id),
        client_id=resolve_cloud_client_id(),
        client_secret=resolve_cloud_client_secret(),
        api_root=resolve_cloud_api_url(),
    )


@mcp_tool(
    domain="cloud",
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def deploy_source_to_cloud(
    source_name: Annotated[
        str,
        Field(description="The name to use when deploying the source."),
    ],
    source_connector_name: Annotated[
        str,
        Field(description="The name of the source connector (e.g., 'source-faker')."),
    ],
    *,
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
            description="The configuration for the source connector.",
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
) -> str:
    """Deploy a source connector to Airbyte Cloud."""
    source = get_source(
        source_connector_name,
        no_executor=True,
    )
    config_dict = resolve_config(
        config=config,
        config_secret_name=config_secret_name,
        config_spec_jsonschema=source.config_spec,
    )
    source.set_config(config_dict, validate=True)

    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    deployed_source = workspace.deploy_source(
        name=source_name,
        source=source,
        unique=unique,
    )

    register_guid_created_in_session(deployed_source.connector_id)
    return (
        f"Successfully deployed source '{source_name}' with ID '{deployed_source.connector_id}'"
        f" and URL: {deployed_source.connector_url}"
    )


@mcp_tool(
    domain="cloud",
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def deploy_destination_to_cloud(
    destination_name: Annotated[
        str,
        Field(description="The name to use when deploying the destination."),
    ],
    destination_connector_name: Annotated[
        str,
        Field(description="The name of the destination connector (e.g., 'destination-postgres')."),
    ],
    *,
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
            description="The configuration for the destination connector.",
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
) -> str:
    """Deploy a destination connector to Airbyte Cloud."""
    destination = get_destination(
        destination_connector_name,
        no_executor=True,
    )
    config_dict = resolve_config(
        config=config,
        config_secret_name=config_secret_name,
        config_spec_jsonschema=destination.config_spec,
    )
    destination.set_config(config_dict, validate=True)

    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    deployed_destination = workspace.deploy_destination(
        name=destination_name,
        destination=destination,
        unique=unique,
    )

    register_guid_created_in_session(deployed_destination.connector_id)
    return (
        f"Successfully deployed destination '{destination_name}' "
        f"with ID: {deployed_destination.connector_id}"
    )


@mcp_tool(
    domain="cloud",
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def create_connection_on_cloud(
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
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
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
    domain="cloud",
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def run_cloud_sync(
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
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
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
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def check_airbyte_cloud_workspace(
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> CloudWorkspaceResult:
    """Check if we have a valid Airbyte Cloud connection and return workspace info.

    Returns workspace details including workspace ID, name, organization ID, and organization name.
    """
    resolved_workspace_id = resolve_cloud_workspace_id(workspace_id)
    api_root = resolve_cloud_api_url()
    client_id = resolve_cloud_client_id()
    client_secret = resolve_cloud_client_secret()

    # Get workspace details including organization info
    workspace_info = api_util.get_workspace_with_org_info(
        workspace_id=resolved_workspace_id,
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )

    return CloudWorkspaceResult(
        id=workspace_info["workspace_id"],
        name=workspace_info["workspace_name"],
        organization_id=workspace_info["organization_id"],
        organization_name=workspace_info["organization_name"],
    )


@mcp_tool(
    domain="cloud",
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def deploy_noop_destination_to_cloud(
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
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
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
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def get_cloud_sync_status(
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
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)

    # If a job ID is provided, get the job by ID.
    sync_result: cloud.SyncResult | None = connection.get_sync_result(job_id=job_id)

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
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def list_deployed_cloud_source_connectors(
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
            description="Optional case-insensitive substring to filter sources by name",
            default=None,
        ),
    ],
    max_items_limit: Annotated[
        int | None,
        Field(
            description="Optional maximum number of items to return (default: no limit)",
            default=None,
        ),
    ],
) -> list[CloudSourceResult]:
    """List all deployed source connectors in the Airbyte Cloud workspace."""
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    sources = workspace.list_sources()

    # Filter by name if requested
    if name_contains:
        needle = name_contains.lower()
        sources = [s for s in sources if s.name is not None and needle in s.name.lower()]

    # Apply limit if requested
    if max_items_limit is not None:
        sources = sources[:max_items_limit]

    # Note: name and url are guaranteed non-null from list API responses
    return [
        CloudSourceResult(
            id=source.source_id,
            name=cast(str, source.name),
            url=cast(str, source.connector_url),
        )
        for source in sources
    ]


@mcp_tool(
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def list_deployed_cloud_destination_connectors(
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
            description="Optional case-insensitive substring to filter destinations by name",
            default=None,
        ),
    ],
    max_items_limit: Annotated[
        int | None,
        Field(
            description="Optional maximum number of items to return (default: no limit)",
            default=None,
        ),
    ],
) -> list[CloudDestinationResult]:
    """List all deployed destination connectors in the Airbyte Cloud workspace."""
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    destinations = workspace.list_destinations()

    # Filter by name if requested
    if name_contains:
        needle = name_contains.lower()
        destinations = [d for d in destinations if d.name is not None and needle in d.name.lower()]

    # Apply limit if requested
    if max_items_limit is not None:
        destinations = destinations[:max_items_limit]

    # Note: name and url are guaranteed non-null from list API responses
    return [
        CloudDestinationResult(
            id=destination.destination_id,
            name=cast(str, destination.name),
            url=cast(str, destination.connector_url),
        )
        for destination in destinations
    ]


@mcp_tool(
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def describe_cloud_source(
    source_id: Annotated[
        str,
        Field(description="The ID of the source to describe."),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> CloudSourceDetails:
    """Get detailed information about a specific deployed source connector."""
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    source = workspace.get_source(source_id=source_id)

    # Access name property to ensure _connector_info is populated
    source_name = cast(str, source.name)

    return CloudSourceDetails(
        source_id=source.source_id,
        source_name=source_name,
        source_url=source.connector_url,
        connector_definition_id=source._connector_info.definition_id,  # noqa: SLF001  # type: ignore[union-attr]
    )


@mcp_tool(
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def describe_cloud_destination(
    destination_id: Annotated[
        str,
        Field(description="The ID of the destination to describe."),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> CloudDestinationDetails:
    """Get detailed information about a specific deployed destination connector."""
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    destination = workspace.get_destination(destination_id=destination_id)

    # Access name property to ensure _connector_info is populated
    destination_name = cast(str, destination.name)

    return CloudDestinationDetails(
        destination_id=destination.destination_id,
        destination_name=destination_name,
        destination_url=destination.connector_url,
        connector_definition_id=destination._connector_info.definition_id,  # noqa: SLF001  # type: ignore[union-attr]
    )


@mcp_tool(
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def describe_cloud_connection(
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
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
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
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def get_cloud_sync_logs(
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
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)

    sync_result: cloud.SyncResult | None = connection.get_sync_result(job_id=job_id)

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
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def list_deployed_cloud_connections(
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
    max_items_limit: Annotated[
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
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    connections = workspace.list_connections()

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
                last_job_status = str(job_status.value) if job_status else None
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

        if max_items_limit is not None and len(results) >= max_items_limit:
            break

    return results


def _resolve_organization(
    organization_id: str | None,
    organization_name: str | None,
    *,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> api_util.models.OrganizationResponse:
    """Resolve organization from either ID or exact name match.

    Args:
        organization_id: The organization ID (if provided directly)
        organization_name: The organization name (exact match required)
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret

    Returns:
        The resolved OrganizationResponse object

    Raises:
        PyAirbyteInputError: If neither or both parameters are provided,
            or if no organization matches the exact name
        AirbyteMissingResourceError: If the organization is not found
    """
    if organization_id and organization_name:
        raise PyAirbyteInputError(
            message="Provide either 'organization_id' or 'organization_name', not both."
        )
    if not organization_id and not organization_name:
        raise PyAirbyteInputError(
            message="Either 'organization_id' or 'organization_name' must be provided."
        )

    # Get all organizations for the user
    orgs = api_util.list_organizations_for_user(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )

    if organization_id:
        # Find by ID
        matching_orgs = [org for org in orgs if org.organization_id == organization_id]
        if not matching_orgs:
            raise AirbyteMissingResourceError(
                resource_type="organization",
                context={
                    "organization_id": organization_id,
                    "message": f"No organization found with ID '{organization_id}' "
                    "for the current user.",
                },
            )
        return matching_orgs[0]

    # Find by exact name match (case-sensitive)
    matching_orgs = [org for org in orgs if org.organization_name == organization_name]

    if not matching_orgs:
        raise AirbyteMissingResourceError(
            resource_type="organization",
            context={
                "organization_name": organization_name,
                "message": f"No organization found with exact name '{organization_name}' "
                "for the current user.",
            },
        )

    if len(matching_orgs) > 1:
        raise PyAirbyteInputError(
            message=f"Multiple organizations found with name '{organization_name}'. "
            "Please use 'organization_id' instead to specify the exact organization."
        )

    return matching_orgs[0]


def _resolve_organization_id(
    organization_id: str | None,
    organization_name: str | None,
    *,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> str:
    """Resolve organization ID from either ID or exact name match.

    This is a convenience wrapper around _resolve_organization that returns just the ID.
    """
    org = _resolve_organization(
        organization_id=organization_id,
        organization_name=organization_name,
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )
    return org.organization_id


@mcp_tool(
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def list_cloud_workspaces(
    *,
    organization_id: Annotated[
        str | None,
        Field(
            description="Organization ID. Required if organization_name is not provided.",
            default=None,
        ),
    ],
    organization_name: Annotated[
        str | None,
        Field(
            description=(
                "Organization name (exact match). " "Required if organization_id is not provided."
            ),
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
    max_items_limit: Annotated[
        int | None,
        Field(
            description="Optional maximum number of items to return (default: no limit)",
            default=None,
        ),
    ],
) -> list[CloudWorkspaceResult]:
    """List all workspaces in a specific organization.

    Requires either organization_id OR organization_name (exact match) to be provided.
    This tool will NOT list workspaces across all organizations - you must specify
    which organization to list workspaces from.
    """
    api_root = resolve_cloud_api_url()
    client_id = resolve_cloud_client_id()
    client_secret = resolve_cloud_client_secret()

    resolved_org_id = _resolve_organization_id(
        organization_id=organization_id,
        organization_name=organization_name,
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )

    workspaces = api_util.list_workspaces_in_organization(
        organization_id=resolved_org_id,
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        name_contains=name_contains,
        max_items_limit=max_items_limit,
    )

    return [
        CloudWorkspaceResult(
            id=ws.get("workspaceId", ""),
            name=ws.get("name", ""),
            organization_id=ws.get("organizationId", ""),
        )
        for ws in workspaces
    ]


@mcp_tool(
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def describe_cloud_organization(
    *,
    organization_id: Annotated[
        str | None,
        Field(
            description="Organization ID. Required if organization_name is not provided.",
            default=None,
        ),
    ],
    organization_name: Annotated[
        str | None,
        Field(
            description=(
                "Organization name (exact match). " "Required if organization_id is not provided."
            ),
            default=None,
        ),
    ],
) -> CloudOrganizationResult:
    """Get details about a specific organization.

    Requires either organization_id OR organization_name (exact match) to be provided.
    This tool is useful for looking up an organization's ID from its name, or vice versa.
    """
    api_root = resolve_cloud_api_url()
    client_id = resolve_cloud_client_id()
    client_secret = resolve_cloud_client_secret()

    org = _resolve_organization(
        organization_id=organization_id,
        organization_name=organization_name,
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )

    return CloudOrganizationResult(
        id=org.organization_id,
        name=org.organization_name,
        email=org.email,
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
    domain="cloud",
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def publish_custom_source_definition(
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
) -> str:
    """Publish a custom YAML source connector definition to Airbyte Cloud.

    Note: Only YAML (declarative) connectors are currently supported.
    Docker-based custom sources are not yet available.
    """
    processed_manifest = manifest_yaml
    if isinstance(manifest_yaml, str) and "\n" not in manifest_yaml:
        processed_manifest = Path(manifest_yaml)

    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    custom_source = workspace.publish_custom_source_definition(
        name=name,
        manifest_yaml=processed_manifest,
        unique=unique,
        pre_validate=pre_validate,
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
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
)
def list_custom_source_definitions(
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
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
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
    domain="cloud",
    destructive=True,
    open_world=True,
)
def update_custom_source_definition(
    definition_id: Annotated[
        str,
        Field(description="The ID of the definition to update."),
    ],
    manifest_yaml: Annotated[
        str | Path,
        Field(
            description="New manifest as YAML string or file path.",
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
    pre_validate: Annotated[
        bool,
        Field(
            description="Whether to validate the manifest client-side before updating.",
            default=True,
        ),
    ] = True,
) -> str:
    """Update a custom YAML source definition in Airbyte Cloud.

    Note: Only YAML (declarative) connectors are currently supported.
    Docker-based custom sources are not yet available.
    """
    check_guid_created_in_session(definition_id)
    processed_manifest = manifest_yaml
    if isinstance(manifest_yaml, str) and "\n" not in manifest_yaml:
        processed_manifest = Path(manifest_yaml)

    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    definition = workspace.get_custom_source_definition(
        definition_id=definition_id,
        definition_type="yaml",
    )
    custom_source: CustomCloudSourceDefinition = definition.update_definition(
        manifest_yaml=processed_manifest,
        pre_validate=pre_validate,
    )
    return (
        "Successfully updated custom YAML source definition:\n"
        + _get_custom_source_definition_description(
            custom_source=custom_source,
        )
    )


@mcp_tool(
    domain="cloud",
    destructive=True,
    open_world=True,
)
def permanently_delete_custom_source_definition(
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
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
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
    domain="cloud",
    destructive=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def permanently_delete_cloud_source(
    source_id: Annotated[
        str,
        Field(description="The ID of the deployed source to delete."),
    ],
    name: Annotated[
        str,
        Field(description="The expected name of the source (for verification)."),
    ],
) -> str:
    """Permanently delete a deployed source connector from Airbyte Cloud.

    IMPORTANT: This operation requires the source name to contain "delete-me" or "deleteme"
    (case insensitive).

    If the source does not meet this requirement, the deletion will be rejected with a
    helpful error message. Instruct the user to rename the source appropriately to authorize
    the deletion.

    The provided name must match the actual name of the source for the operation to proceed.
    This is a safety measure to ensure you are deleting the correct resource.
    """
    check_guid_created_in_session(source_id)
    workspace: CloudWorkspace = _get_cloud_workspace()
    source = workspace.get_source(source_id=source_id)
    actual_name: str = cast(str, source.name)

    # Verify the name matches
    if actual_name != name:
        raise PyAirbyteInputError(
            message=(
                f"Name mismatch: expected '{name}' but found '{actual_name}'. "
                "The provided name must exactly match the source's actual name. "
                "This is a safety measure to prevent accidental deletion."
            ),
            context={
                "source_id": source_id,
                "expected_name": name,
                "actual_name": actual_name,
            },
        )

    # Safe mode is hard-coded to True for extra protection when running in LLM agents
    workspace.permanently_delete_source(
        source=source_id,
        safe_mode=True,  # Requires name to contain "delete-me" or "deleteme" (case insensitive)
    )
    return f"Successfully deleted source '{actual_name}' (ID: {source_id})"


@mcp_tool(
    domain="cloud",
    destructive=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def permanently_delete_cloud_destination(
    destination_id: Annotated[
        str,
        Field(description="The ID of the deployed destination to delete."),
    ],
    name: Annotated[
        str,
        Field(description="The expected name of the destination (for verification)."),
    ],
) -> str:
    """Permanently delete a deployed destination connector from Airbyte Cloud.

    IMPORTANT: This operation requires the destination name to contain "delete-me" or "deleteme"
    (case insensitive).

    If the destination does not meet this requirement, the deletion will be rejected with a
    helpful error message. Instruct the user to rename the destination appropriately to authorize
    the deletion.

    The provided name must match the actual name of the destination for the operation to proceed.
    This is a safety measure to ensure you are deleting the correct resource.
    """
    check_guid_created_in_session(destination_id)
    workspace: CloudWorkspace = _get_cloud_workspace()
    destination = workspace.get_destination(destination_id=destination_id)
    actual_name: str = cast(str, destination.name)

    # Verify the name matches
    if actual_name != name:
        raise PyAirbyteInputError(
            message=(
                f"Name mismatch: expected '{name}' but found '{actual_name}'. "
                "The provided name must exactly match the destination's actual name. "
                "This is a safety measure to prevent accidental deletion."
            ),
            context={
                "destination_id": destination_id,
                "expected_name": name,
                "actual_name": actual_name,
            },
        )

    # Safe mode is hard-coded to True for extra protection when running in LLM agents
    workspace.permanently_delete_destination(
        destination=destination_id,
        safe_mode=True,  # Requires name-based delete disposition ("delete-me" or "deleteme")
    )
    return f"Successfully deleted destination '{actual_name}' (ID: {destination_id})"


@mcp_tool(
    domain="cloud",
    destructive=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def permanently_delete_cloud_connection(
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
    workspace: CloudWorkspace = _get_cloud_workspace()
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
    domain="cloud",
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def rename_cloud_source(
    source_id: Annotated[
        str,
        Field(description="The ID of the deployed source to rename."),
    ],
    name: Annotated[
        str,
        Field(description="New name for the source."),
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
    """Rename a deployed source connector on Airbyte Cloud."""
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    source = workspace.get_source(source_id=source_id)
    source.rename(name=name)
    return f"Successfully renamed source '{source_id}' to '{name}'. URL: {source.connector_url}"


@mcp_tool(
    domain="cloud",
    destructive=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def update_cloud_source_config(
    source_id: Annotated[
        str,
        Field(description="The ID of the deployed source to update."),
    ],
    config: Annotated[
        dict | str,
        Field(
            description="New configuration for the source connector.",
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
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> str:
    """Update a deployed source connector's configuration on Airbyte Cloud.

    This is a destructive operation that can break existing connections if the
    configuration is changed incorrectly. Use with caution.
    """
    check_guid_created_in_session(source_id)
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    source = workspace.get_source(source_id=source_id)

    config_dict = resolve_config(
        config=config,
        config_secret_name=config_secret_name,
        config_spec_jsonschema=None,  # We don't have the spec here
    )

    source.update_config(config=config_dict)
    return f"Successfully updated source '{source_id}'. URL: {source.connector_url}"


@mcp_tool(
    domain="cloud",
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def rename_cloud_destination(
    destination_id: Annotated[
        str,
        Field(description="The ID of the deployed destination to rename."),
    ],
    name: Annotated[
        str,
        Field(description="New name for the destination."),
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
    """Rename a deployed destination connector on Airbyte Cloud."""
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    destination = workspace.get_destination(destination_id=destination_id)
    destination.rename(name=name)
    return (
        f"Successfully renamed destination '{destination_id}' to '{name}'. "
        f"URL: {destination.connector_url}"
    )


@mcp_tool(
    domain="cloud",
    destructive=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def update_cloud_destination_config(
    destination_id: Annotated[
        str,
        Field(description="The ID of the deployed destination to update."),
    ],
    config: Annotated[
        dict | str,
        Field(
            description="New configuration for the destination connector.",
        ),
    ],
    config_secret_name: Annotated[
        str | None,
        Field(
            description="The name of the secret containing the configuration.",
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
) -> str:
    """Update a deployed destination connector's configuration on Airbyte Cloud.

    This is a destructive operation that can break existing connections if the
    configuration is changed incorrectly. Use with caution.
    """
    check_guid_created_in_session(destination_id)
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    destination = workspace.get_destination(destination_id=destination_id)

    config_dict = resolve_config(
        config=config,
        config_secret_name=config_secret_name,
        config_spec_jsonschema=None,  # We don't have the spec here
    )

    destination.update_config(config=config_dict)
    return (
        f"Successfully updated destination '{destination_id}'. " f"URL: {destination.connector_url}"
    )


@mcp_tool(
    domain="cloud",
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def rename_cloud_connection(
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
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)
    connection.rename(name=name)
    return (
        f"Successfully renamed connection '{connection_id}' to '{name}'. "
        f"URL: {connection.connection_url}"
    )


@mcp_tool(
    domain="cloud",
    destructive=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def set_cloud_connection_table_prefix(
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
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)
    connection.set_table_prefix(prefix=prefix)
    return (
        f"Successfully set table prefix for connection '{connection_id}' to '{prefix}'. "
        f"URL: {connection.connection_url}"
    )


@mcp_tool(
    domain="cloud",
    destructive=True,
    open_world=True,
    extra_help_text=CLOUD_AUTH_TIP_TEXT,
)
def set_cloud_connection_selected_streams(
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
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)

    resolved_streams_list: list[str] = resolve_list_of_strings(stream_names)
    connection.set_selected_streams(stream_names=resolved_streams_list)

    return (
        f"Successfully set selected streams for connection '{connection_id}' "
        f"to {resolved_streams_list}. URL: {connection.connection_url}"
    )


def register_cloud_ops_tools(app: FastMCP) -> None:
    """@private Register tools with the FastMCP app.

    This is an internal function and should not be called directly.

    Tools are filtered based on mode settings:
    - AIRBYTE_CLOUD_MCP_READONLY_MODE=1: Only read-only tools are registered
    - AIRBYTE_CLOUD_MCP_SAFE_MODE=1: All tools are registered, but destructive
      operations are protected by runtime session checks
    """
    register_tools(app, domain="cloud")
