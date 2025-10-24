# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Airbyte Cloud MCP operations."""

from pathlib import Path
from typing import Annotated, Any

from fastmcp import FastMCP
from pydantic import Field

from airbyte import cloud, get_destination, get_source
from airbyte.cloud.auth import (
    resolve_cloud_api_url,
    resolve_cloud_client_id,
    resolve_cloud_client_secret,
    resolve_cloud_workspace_id,
)
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.connectors import CloudDestination, CloudSource, CustomCloudSourceDefinition
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.destinations.util import get_noop_destination
from airbyte.mcp._tool_utils import mcp_tool, register_tools
from airbyte.mcp._util import resolve_config, resolve_list_of_strings


def _get_cloud_workspace() -> CloudWorkspace:
    """Get an authenticated CloudWorkspace using environment variables."""
    return CloudWorkspace(
        workspace_id=resolve_cloud_workspace_id(),
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
    try:
        source = get_source(
            source_connector_name,
            install_if_missing=False,
        )
        config_dict = resolve_config(
            config=config,
            config_secret_name=config_secret_name,
            config_spec_jsonschema=source.config_spec,
        )
        source.set_config(config_dict)

        workspace: CloudWorkspace = _get_cloud_workspace()
        deployed_source = workspace.deploy_source(
            name=source_name,
            source=source,
            unique=unique,
        )

    except Exception as ex:
        return f"Failed to deploy source '{source_name}': {ex}"
    else:
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
    try:
        destination = get_destination(
            destination_connector_name,
            install_if_missing=False,
        )
        config_dict = resolve_config(
            config=config,
            config_secret_name=config_secret_name,
            config_spec_jsonschema=destination.config_spec,
        )
        destination.set_config(config_dict)

        workspace: CloudWorkspace = _get_cloud_workspace()
        deployed_destination = workspace.deploy_destination(
            name=destination_name,
            destination=destination,
            unique=unique,
        )

    except Exception as ex:
        return f"Failed to deploy destination '{destination_name}': {ex}"
    else:
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
    try:
        workspace: CloudWorkspace = _get_cloud_workspace()
        deployed_connection = workspace.deploy_connection(
            connection_name=connection_name,
            source=source_id,
            destination=destination_id,
            selected_streams=resolved_streams_list,
            table_prefix=table_prefix,
        )

    except Exception as ex:
        return f"Failed to create connection '{connection_name}': {ex}"
    else:
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
    try:
        workspace: CloudWorkspace = _get_cloud_workspace()
        connection = workspace.get_connection(connection_id=connection_id)
        sync_result = connection.run_sync(wait=wait, wait_timeout=wait_timeout)

    except Exception as ex:
        return f"Failed to run sync for connection '{connection_id}': {ex}"
    else:
        if wait:
            status = sync_result.get_job_status()
            return (
                f"Sync completed with status: {status}. "  # Sync completed.
                f"Job ID is '{sync_result.job_id}' and "
                f"job URL is: {sync_result.job_url}"
            )
        return (
            f"Sync started. "  # Sync started.
            f"Job ID is '{sync_result.job_id}' and "
            f"job URL is: {sync_result.job_url}"
        )


@mcp_tool(
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
)
def check_airbyte_cloud_workspace() -> str:
    """Check if we have a valid Airbyte Cloud connection and return workspace info.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.

    Returns workspace ID and workspace URL for verification.
    """
    try:
        workspace: CloudWorkspace = _get_cloud_workspace()
        workspace.connect()

    except Exception as ex:
        return f"❌ Failed to connect to Airbyte Cloud workspace: {ex}"
    else:
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
    unique: bool = True,
) -> str:
    """Deploy the No-op destination to Airbyte Cloud for testing purposes.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
    try:
        destination = get_noop_destination()
        workspace: CloudWorkspace = _get_cloud_workspace()
        deployed_destination = workspace.deploy_destination(
            name=name,
            destination=destination,
            unique=unique,
        )
    except Exception as ex:
        return f"Failed to deploy No-op Destination: {ex}"
    else:
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
    try:
        workspace: CloudWorkspace = _get_cloud_workspace()
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

        return result  # noqa: TRY300

    except Exception as ex:
        return {
            "status": None,
            "job_id": job_id,
            "error": f"Failed to get sync status for connection '{connection_id}': {ex}",
            "attempts": [],
        }


@mcp_tool(
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
)
def list_deployed_cloud_source_connectors() -> list[CloudSource]:
    """List all deployed source connectors in the Airbyte Cloud workspace.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
    workspace: CloudWorkspace = _get_cloud_workspace()
    return workspace.list_sources()


@mcp_tool(
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
)
def list_deployed_cloud_destination_connectors() -> list[CloudDestination]:
    """List all deployed destination connectors in the Airbyte Cloud workspace.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
    workspace: CloudWorkspace = _get_cloud_workspace()
    return workspace.list_destinations()


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
) -> str:
    """Get the logs from a sync job attempt on Airbyte Cloud.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
    try:
        workspace: CloudWorkspace = _get_cloud_workspace()
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

        return logs  # noqa: TRY300

    except Exception as ex:
        return f"Failed to get logs for connection '{connection_id}': {ex}"


@mcp_tool(
    domain="cloud",
    read_only=True,
    idempotent=True,
    open_world=True,
)
def list_deployed_cloud_connections() -> list[CloudConnection]:
    """List all deployed connections in the Airbyte Cloud workspace.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
    workspace: CloudWorkspace = _get_cloud_workspace()
    return workspace.list_connections()


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
    try:
        processed_manifest = manifest_yaml
        if isinstance(manifest_yaml, str) and "\n" not in manifest_yaml:
            processed_manifest = Path(manifest_yaml)

        workspace: CloudWorkspace = _get_cloud_workspace()
        custom_source = workspace.publish_custom_source_definition(
            name=name,
            manifest_yaml=processed_manifest,
            unique=unique,
            pre_validate=pre_validate,
        )
    except Exception as ex:
        return f"Failed to publish custom source definition '{name}': {ex}"
    else:
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
def list_custom_source_definitions() -> list[dict[str, Any]]:
    """List custom YAML source definitions in the Airbyte Cloud workspace.

    Note: Only YAML (declarative) connectors are currently supported.
    Docker-based custom sources are not yet available.
    """
    workspace: CloudWorkspace = _get_cloud_workspace()
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
    try:
        processed_manifest = manifest_yaml
        if isinstance(manifest_yaml, str) and "\n" not in manifest_yaml:
            processed_manifest = Path(manifest_yaml)

        workspace: CloudWorkspace = _get_cloud_workspace()
        definition = workspace.get_custom_source_definition(
            definition_id=definition_id,
            definition_type="yaml",
        )
        custom_source: CustomCloudSourceDefinition = definition.update_definition(
            manifest_yaml=processed_manifest,
            pre_validate=pre_validate,
        )
    except Exception as ex:
        return f"Failed to update custom source definition '{definition_id}': {ex}"
    else:
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
) -> str:
    """Permanently delete a custom YAML source definition from Airbyte Cloud.

    IMPORTANT: This operation requires the connector name to either:
    1. Start with "delete:" (case insensitive), OR
    2. Contain "delete-me" (case insensitive)

    If the connector does not meet these requirements, the deletion will be rejected with a
    helpful error message. Instruct the user to rename the connector appropriately to authorize
    the deletion.

    Note: Only YAML (declarative) connectors are currently supported.
    Docker-based custom sources are not yet available.
    """
    workspace: CloudWorkspace = _get_cloud_workspace()
    definition = workspace.get_custom_source_definition(
        definition_id=definition_id,
        definition_type="yaml",
    )
    definition_name: str = definition.name  # Capture name before deletion
    definition.permanently_delete(
        safe_mode=True,  # Hard-coded safe mode for extra protection when running in LLM agents.
    )
    return (
        f"Successfully deleted custom source definition '{definition_name}' (ID: {definition_id})"
    )


def register_cloud_ops_tools(app: FastMCP) -> None:
    """@private Register tools with the FastMCP app.

    This is an internal function and should not be called directly.

    Tools are filtered based on safe mode settings:
    - AIRBYTE_CLOUD_MCP_READONLY_MODE=1: Only read-only tools are registered
    - AIRBYTE_CLOUD_MCP_SAFE_MODE=1: Destructive tools are not registered
    """
    register_tools(app, domain="cloud")
