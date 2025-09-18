# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Airbyte Cloud MCP operations."""

from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field

from airbyte import cloud, get_destination, get_source
from airbyte._util.api_imports import JobStatusEnum
from airbyte.cloud.auth import (
    resolve_cloud_api_url,
    resolve_cloud_client_id,
    resolve_cloud_client_secret,
    resolve_cloud_workspace_id,
)
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.connectors import CloudDestination, CloudSource
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.destinations.util import get_noop_destination
from airbyte.mcp._util import resolve_config, resolve_list_of_strings


def _get_cloud_workspace() -> CloudWorkspace:
    """Get an authenticated CloudWorkspace using environment variables."""
    return CloudWorkspace(
        workspace_id=resolve_cloud_workspace_id(),
        client_id=resolve_cloud_client_id(),
        client_secret=resolve_cloud_client_secret(),
        api_root=resolve_cloud_api_url(),
    )


# @app.tool()  # << deferred
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
        Field(description="The configuration for the source connector."),
    ] = None,
    config_secret_name: Annotated[
        str | None,
        Field(description="The name of the secret containing the configuration."),
    ] = None,
    unique: Annotated[
        bool,
        Field(description="Whether to require a unique name."),
    ] = True,
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


# @app.tool()  # << deferred
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
        Field(description="The configuration for the destination connector."),
    ] = None,
    config_secret_name: Annotated[
        str | None,
        Field(description="The name of the secret containing the configuration."),
    ] = None,
    unique: Annotated[
        bool,
        Field(description="Whether to require a unique name."),
    ] = True,
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


# @app.tool()  # << deferred
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
        Field(description="Optional table prefix to use when syncing to the destination."),
    ] = None,
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


# @app.tool()  # << deferred
def run_cloud_sync(
    connection_id: Annotated[
        str,
        Field(description="The ID of the Airbyte Cloud connection."),
    ],
    *,
    wait: Annotated[
        bool,
        Field(description="Whether to wait for the sync to complete."),
    ] = True,
    wait_timeout: Annotated[
        int,
        Field(description="Maximum time to wait for sync completion (seconds)."),
    ] = 300,
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


# @app.tool()  # << deferred
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


# @app.tool()  # << deferred
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


# @app.tool()  # << deferred
def get_cloud_sync_status(
    connection_id: Annotated[
        str,
        Field(
            description="The ID of the Airbyte Cloud connection.",
        ),
    ],
    job_id: Annotated[
        int | None,
        Field(description="Optional job ID. If not provided, the latest job will be used."),
    ] = None,
) -> JobStatusEnum | None:
    """Get the status of a sync job from the Airbyte Cloud.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
    workspace: CloudWorkspace = _get_cloud_workspace()
    connection = workspace.get_connection(connection_id=connection_id)

    # If a job ID is provided, get the job by ID.
    sync_result: cloud.SyncResult | None = connection.get_sync_result(job_id=job_id)
    return sync_result.get_job_status() if sync_result else None


# @app.tool()  # << deferred
def list_deployed_cloud_source_connectors() -> list[CloudSource]:
    """List all deployed source connectors in the Airbyte Cloud workspace.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
    workspace: CloudWorkspace = _get_cloud_workspace()
    return workspace.list_sources()


# @app.tool()  # << deferred
def list_deployed_cloud_destination_connectors() -> list[CloudDestination]:
    """List all deployed destination connectors in the Airbyte Cloud workspace.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
    workspace: CloudWorkspace = _get_cloud_workspace()
    return workspace.list_destinations()


# @app.tool()  # << deferred
def list_deployed_cloud_connections() -> list[CloudConnection]:
    """List all deployed connections in the Airbyte Cloud workspace.

    By default, the `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`,
    and `AIRBYTE_API_ROOT` environment variables will be used to authenticate with the
    Airbyte Cloud API.
    """
    workspace: CloudWorkspace = _get_cloud_workspace()
    return workspace.list_connections()


def register_cloud_ops_tools(app: FastMCP) -> None:
    """Register tools with the FastMCP app."""
    app.tool(check_airbyte_cloud_workspace)
    app.tool(deploy_source_to_cloud)
    app.tool(deploy_destination_to_cloud)
    app.tool(deploy_noop_destination_to_cloud)
    app.tool(create_connection_on_cloud)
    app.tool(run_cloud_sync)
    app.tool(get_cloud_sync_status)
    app.tool(list_deployed_cloud_source_connectors)
    app.tool(list_deployed_cloud_destination_connectors)
    app.tool(list_deployed_cloud_connections)
