# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Airbyte Cloud MCP operations."""

from pathlib import Path
from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field

from airbyte import cloud, get_destination, get_source, secrets
from airbyte._util.api_imports import JobStatusEnum
from airbyte._util.api_util import CLOUD_API_ROOT
from airbyte.mcp._util import resolve_config


def deploy_source_to_cloud(
    workspace_id: Annotated[
        str,
        Field(description="The ID of the Airbyte Cloud workspace."),
    ],
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
        dict | Path | None,
        Field(description="The configuration for the source connector."),
    ] = None,
    config_secret_name: Annotated[
        str | None,
        Field(description="The name of the secret containing the configuration."),
    ] = None,
    api_root: Annotated[
        str | None,
        Field(description="Optional Cloud API root URL override."),
    ] = None,
    unique: Annotated[
        bool,
        Field(description="Whether to require a unique name."),
    ] = True,
) -> str:
    """Deploy a source connector to Airbyte Cloud.

    By default, the `AIRBYTE_CLIENT_ID` and `AIRBYTE_CLIENT_SECRET` environment variables will be
    used to authenticate with the Airbyte Cloud API.
    """
    try:
        source = get_source(source_connector_name)
        config_dict = resolve_config(
            config=config,
            config_secret_name=config_secret_name,
            config_spec_jsonschema=source.config_spec,
        )
        source.set_config(config_dict)

        workspace = cloud.CloudWorkspace(
            workspace_id,
            client_id=secrets.get_secret("AIRBYTE_CLIENT_ID"),
            client_secret=secrets.get_secret("AIRBYTE_CLIENT_SECRET"),
            api_root=api_root or CLOUD_API_ROOT,
        )

        deployed_source = workspace.deploy_source(
            name=source_name,
            source=source,
            unique=unique,
        )

    except Exception as ex:
        return f"Failed to deploy source '{source_name}': {ex}"
    else:
        return (
            f"Successfully deployed source '{source_name}' with ID: {deployed_source.connector_id}"
        )


def deploy_destination_to_cloud(
    workspace_id: Annotated[
        str,
        Field(description="The ID of the Airbyte Cloud workspace."),
    ],
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
        dict | Path | None,
        Field(description="The configuration for the destination connector."),
    ] = None,
    config_secret_name: Annotated[
        str | None,
        Field(description="The name of the secret containing the configuration."),
    ] = None,
    api_root: Annotated[
        str | None,
        Field(description="Optional Cloud API root URL override."),
    ] = None,
    unique: Annotated[
        bool,
        Field(description="Whether to require a unique name."),
    ] = True,
) -> str:
    """Deploy a destination connector to Airbyte Cloud.

    By default, the `AIRBYTE_CLIENT_ID` and `AIRBYTE_CLIENT_SECRET` environment variables will be
    used to authenticate with the Airbyte Cloud API.
    """
    try:
        destination = get_destination(destination_connector_name)
        config_dict = resolve_config(
            config=config,
            config_secret_name=config_secret_name,
            config_spec_jsonschema=destination.config_spec,
        )
        destination.set_config(config_dict)

        workspace = cloud.CloudWorkspace(
            workspace_id,
            client_id=secrets.get_secret("AIRBYTE_CLIENT_ID"),
            client_secret=secrets.get_secret("AIRBYTE_CLIENT_SECRET"),
            api_root=api_root or CLOUD_API_ROOT,
        )

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


def create_connection_on_cloud(
    workspace_id: Annotated[
        str,
        Field(description="The ID of the Airbyte Cloud workspace."),
    ],
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
        list[str],
        Field(description="The selected stream names to sync within the connection."),
    ],
    api_root: Annotated[
        str | None,
        Field(description="Optional Cloud API root URL override."),
    ] = None,
    table_prefix: Annotated[
        str | None,
        Field(description="Optional table prefix to use when syncing to the destination."),
    ] = None,
) -> str:
    """Create a connection between a deployed source and destination on Airbyte Cloud.

    By default, the `AIRBYTE_CLIENT_ID` and `AIRBYTE_CLIENT_SECRET` environment variables will be
    used to authenticate with the Airbyte Cloud API.
    """
    try:
        workspace = cloud.CloudWorkspace(
            workspace_id,
            client_id=secrets.get_secret("AIRBYTE_CLIENT_ID"),
            client_secret=secrets.get_secret("AIRBYTE_CLIENT_SECRET"),
            api_root=api_root or CLOUD_API_ROOT,
        )

        deployed_connection = workspace.deploy_connection(
            connection_name=connection_name,
            source=source_id,
            destination=destination_id,
            selected_streams=selected_streams,
            table_prefix=table_prefix,
        )

    except Exception as ex:
        return f"Failed to create connection '{connection_name}': {ex}"
    else:
        return (
            f"Successfully created connection '{connection_name}' "
            f"with ID: {deployed_connection.connection_id}"
        )


def run_cloud_sync(
    workspace_id: Annotated[
        str,
        Field(description="The ID of the Airbyte Cloud workspace."),
    ],
    connection_id: Annotated[
        str,
        Field(description="The ID of the Airbyte Cloud connection."),
    ],
    *,
    api_root: Annotated[
        str | None,
        Field(description="Optional Cloud API root URL override."),
    ] = None,
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

    By default, the `AIRBYTE_CLIENT_ID` and `AIRBYTE_CLIENT_SECRET` environment variables will be
    used to authenticate with the Airbyte Cloud API.
    """
    try:
        workspace = cloud.CloudWorkspace(
            workspace_id,
            client_id=secrets.get_secret("AIRBYTE_CLIENT_ID"),
            client_secret=secrets.get_secret("AIRBYTE_CLIENT_SECRET"),
            api_root=api_root or CLOUD_API_ROOT,
        )

        connection = workspace.get_connection(connection_id=connection_id)
        sync_result = connection.run_sync(wait=wait, wait_timeout=wait_timeout)

    except Exception as ex:
        return f"Failed to run sync for connection '{connection_id}': {ex}"
    else:
        if wait:
            status = sync_result.get_job_status()
            return f"Sync completed with status: {status}. Job ID: {sync_result.job_id}"
        return f"Sync started. Job ID: {sync_result.job_id}"


# @app.tool()  # << deferred
def get_cloud_sync_status(
    workspace_id: Annotated[
        str,
        Field(
            description="The ID of the Airbyte Cloud workspace.",
        ),
    ],
    connection_id: Annotated[
        str,
        Field(
            description="The ID of the Airbyte Cloud connection.",
        ),
    ],
    api_root: Annotated[
        str | None,
        Field(
            description="Optional Cloud API root URL override.",
        ),
    ],
    job_id: Annotated[
        int | None,
        Field(description="Optional job ID. If not provided, the latest job will be used."),
    ] = None,
) -> JobStatusEnum | None:
    """Get the status of a sync job from the Airbyte Cloud.

    By default, the `AIRBYTE_CLIENT_ID` and `AIRBYTE_CLIENT_SECRET` environment variables will be
    used to authenticate with the Airbyte Cloud API.
    """
    workspace = cloud.CloudWorkspace(
        workspace_id,
        # We'll attempt any configured secrets managers to retrieve the client ID and secret.
        # If no other secret manager is defined, this normally comes from environment variables.
        client_id=secrets.get_secret("AIRBYTE_CLIENT_ID"),
        client_secret=secrets.get_secret("AIRBYTE_CLIENT_SECRET"),
        api_root=api_root or CLOUD_API_ROOT,  # Defaults to the Airbyte Cloud API root if None.
    )
    connection = workspace.get_connection(connection_id=connection_id)

    # If a job ID is provided, get the job by ID.
    sync_result: cloud.SyncResult | None = connection.get_sync_result(job_id=job_id)
    return sync_result.get_job_status() if sync_result else None


def register_cloud_ops_tools(app: FastMCP) -> None:
    """Register tools with the FastMCP app."""
    app.tool(get_cloud_sync_status)
    app.tool(deploy_source_to_cloud)
    app.tool(deploy_destination_to_cloud)
    app.tool(create_connection_on_cloud)
    app.tool(run_cloud_sync)
