# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Airbyte Cloud MCP operations."""

from typing import Annotated

from fastmcp import FastMCP
from pydantic import Field

from airbyte import cloud, secrets
from airbyte._util.api_imports import JobStatusEnum
from airbyte._util.api_util import CLOUD_API_ROOT


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
