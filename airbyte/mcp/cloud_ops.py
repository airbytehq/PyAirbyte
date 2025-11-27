# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Airbyte Cloud MCP operations."""

from pathlib import Path
from typing import Annotated, Any, cast

from fastmcp import FastMCP
from pydantic import BaseModel, Field

from airbyte import cloud, get_destination, get_source
from airbyte._util.api_util import PyAirbyteInputError
from airbyte.cloud.auth import (
    resolve_cloud_api_url,
    resolve_cloud_client_id,
    resolve_cloud_client_secret,
    resolve_cloud_workspace_id,
)
from airbyte.cloud.connectors import CustomCloudSourceDefinition
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.destinations.util import get_noop_destination
from airbyte.mcp._tool_utils import (
    check_guid_created_in_session,
    mcp_tool,
    register_guid_created_in_session,
    register_tools,
)
from airbyte.mcp._util import resolve_config, resolve_list_of_strings


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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
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
    """Deploy a source connector to Airbyte Cloud.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
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
    """Deploy a destination connector to Airbyte Cloud.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
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
    """Create a connection between a deployed source and destination on Airbyte Cloud.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
            default=None,
        ),
    ],
    wait: Annotated[
        bool,
        Field(
            description="Whether to wait for the sync to complete.",
            default=True,
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
    """Run a sync job on Airbyte Cloud.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
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
)
def check_airbyte_cloud_workspace(
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
            default=None,
        ),
    ],
) -> str:
    """Check if we have a valid Airbyte Cloud connection and return workspace info.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.

    Returns workspace ID and workspace URL for verification.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    workspace.connect()

    return (
        f"✅ Successfully connected to Airbyte Cloud workspace.\n"
        f"Workspace ID: {workspace.workspace_id}\n"
        f"Workspace URL: {workspace.workspace_url}"
    )


@mcp_tool(
    domain="cloud",
    open_world=True,
)
def deploy_noop_destination_to_cloud(
    name: str = "No-op Destination",
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
            default=None,
        ),
    ],
    unique: bool = True,
) -> str:
    """Deploy the No-op destination to Airbyte Cloud for testing purposes.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
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
    """Get the status of a sync job from the Airbyte Cloud.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
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
)
def list_deployed_cloud_source_connectors(
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
            default=None,
        ),
    ],
    name_contains: Annotated[
        str | None,
        "Optional case-insensitive substring to filter sources by name",
    ] = None,
    max_items_limit: Annotated[
        int | None,
        "Optional maximum number of items to return (default: no limit)",
    ] = None,
) -> list[CloudSourceResult]:
    """List all deployed source connectors in the Airbyte Cloud workspace.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
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
)
def list_deployed_cloud_destination_connectors(
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
            default=None,
        ),
    ],
    name_contains: Annotated[
        str | None,
        "Optional case-insensitive substring to filter destinations by name",
    ] = None,
    max_items_limit: Annotated[
        int | None,
        "Optional maximum number of items to return (default: no limit)",
    ] = None,
) -> list[CloudDestinationResult]:
    """List all deployed destination connectors in the Airbyte Cloud workspace.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
            default=None,
        ),
    ],
) -> str:
    """Get the logs from a sync job attempt on Airbyte Cloud.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    connection = workspace.get_connection(connection_id=connection_id)

    sync_result: cloud.SyncResult | None = connection.get_sync_result(job_id=job_id)

    if not sync_result:
        return f"No sync job found for connection '{connection_id}'"

    attempts = sync_result.get_attempts()

    if not attempts:
        return f"No attempts found for job '{sync_result.job_id}'"

    if attempt_number is not None:
        target_attempt = None
        for attempt in attempts:
            if attempt.attempt_number == attempt_number:
                target_attempt = attempt
                break

        if target_attempt is None:
            return f"Attempt number {attempt_number} not found for job '{sync_result.job_id}'"
    else:
        target_attempt = max(attempts, key=lambda a: a.attempt_number)

    logs = target_attempt.get_full_log_text()

    if not logs:
        return (
            f"No logs available for job '{sync_result.job_id}', "
            f"attempt {target_attempt.attempt_number}"
        )

    return logs


@mcp_tool(
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
)
def list_deployed_cloud_connections(
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
            default=None,
        ),
    ],
    name_contains: Annotated[
        str | None,
        "Optional case-insensitive substring to filter connections by name",
    ] = None,
    max_items_limit: Annotated[
        int | None,
        "Optional maximum number of items to return (default: no limit)",
    ] = None,
) -> list[CloudConnectionResult]:
    """List all deployed connections in the Airbyte Cloud workspace.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    connections = workspace.list_connections()

    # Filter by name if requested
    if name_contains:
        needle = name_contains.lower()
        connections = [c for c in connections if c.name is not None and needle in c.name.lower()]

    # Apply limit if requested
    if max_items_limit is not None:
        connections = connections[:max_items_limit]

    # Note: name and url are guaranteed non-null from list API responses
    return [
        CloudConnectionResult(
            id=connection.connection_id,
            name=cast(str, connection.name),
            url=cast(str, connection.connection_url),
            source_id=connection.source_id,
            destination_id=connection.destination_id,
        )
        for connection in connections
    ]


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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
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

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
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

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
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

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
            default=None,
        ),
    ],
) -> str:
    """Rename a deployed source connector on Airbyte Cloud.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
    workspace: CloudWorkspace = _get_cloud_workspace(workspace_id)
    source = workspace.get_source(source_id=source_id)
    source.rename(name=name)
    return f"Successfully renamed source '{source_id}' to '{name}'. URL: {source.connector_url}"


@mcp_tool(
    domain="cloud",
    destructive=True,
    open_world=True,
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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
            default=None,
        ),
    ],
) -> str:
    """Update a deployed source connector's configuration on Airbyte Cloud.

    This is a destructive operation that can break existing connections if the
    configuration is changed incorrectly. Use with caution.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
            default=None,
        ),
    ],
) -> str:
    """Rename a deployed destination connector on Airbyte Cloud.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
            default=None,
        ),
    ],
) -> str:
    """Update a deployed destination connector's configuration on Airbyte Cloud.

    This is a destructive operation that can break existing connections if the
    configuration is changed incorrectly. Use with caution.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
            default=None,
        ),
    ],
) -> str:
    """Rename a connection on Airbyte Cloud.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
            default=None,
        ),
    ],
) -> str:
    """Set the table prefix for a connection on Airbyte Cloud.

    This is a destructive operation that can break downstream dependencies if the
    table prefix is changed incorrectly. Use with caution.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
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
            description="Workspace ID. Defaults to AIRBYTE_CLOUD_WORKSPACE_ID env var.",
            default=None,
        ),
    ],
) -> str:
    """Set the selected streams for a connection on Airbyte Cloud.

    This is a destructive operation that can break existing connections if the
    stream selection is changed incorrectly. Use with caution.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
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
