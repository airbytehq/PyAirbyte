# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""These internal functions are used to interact with the Airbyte API (module named `airbyte`).

In order to insulate users from breaking changes and to avoid general confusion around naming
and design inconsistencies, we do not expose these functions or other Airbyte API classes within
PyAirbyte. Classes and functions from the Airbyte API external library should always be wrapped in
PyAirbyte classes - unless there's a very compelling reason to surface these models intentionally.
"""

from __future__ import annotations

import json
import os
from typing import Any

import airbyte_api
from airbyte_api.models import operations as api_operations
from airbyte_api.models import shared as api_models
from airbyte_api.models.shared.jobcreaterequest import JobCreateRequest, JobTypeEnum

from airbyte.exceptions import (
    AirbyteConnectionSyncError,
    AirbyteError,
    AirbyteMissingResourceError,
    AirbyteMultipleResourcesError,
)


JOB_WAIT_INTERVAL_SECS = 2.0
JOB_WAIT_TIMEOUT_SECS_DEFAULT = 60 * 60  # 1 hour
CLOUD_API_ROOT = "https://api.airbyte.com/v1"

AIRBYTE_API_KEY_SECRET_NAME = "AIRBYTE_CLOUD_API_KEY"

# Helper functions


def status_ok(status_code: int) -> bool:
    """Check if a status code is OK."""
    return status_code >= 200 and status_code < 300  # noqa: PLR2004  # allow inline magic numbers


def get_default_bearer_token() -> str | None:
    """Get the default bearer token from env variables."""
    return os.environ.get(AIRBYTE_API_KEY_SECRET_NAME, None)


def get_airbyte_server_instance(
    *,
    api_key: str,
    api_root: str,
) -> airbyte_api.Airbyte:
    """Get an Airbyte instance."""
    return airbyte_api.Airbyte(
        security=api_models.Security(
            bearer_auth=api_key,
        ),
        server_url=api_root,
    )


# Get workspace


def get_workspace(
    workspace_id: str,
    *,
    api_root: str,
    api_key: str,
) -> api_models.WorkspaceResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.workspaces.get_workspace(
        api_operations.GetWorkspaceRequest(
            workspace_id=workspace_id,
        ),
    )
    if status_ok(response.status_code) and response.workspace_response:
        return response.workspace_response

    raise AirbyteMissingResourceError(
        resource_type="workspace",
        context={
            "workspace_id": workspace_id,
            "response": response,
        },
    )


# List, get, and run connections


def list_connections(
    workspace_id: str,
    *,
    api_root: str,
    api_key: str,
) -> list[api_models.ConnectionResponse]:
    """Get a connection."""
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.connections.list_connections(
        api_operations.ListConnectionsRequest()(
            workspace_ids=[workspace_id],
        ),
    )

    if status_ok(response.status_code) and response.connections_response:
        return response.connections_response.data

    raise AirbyteError(
        context={
            "workspace_id": workspace_id,
            "response": response,
        }
    )


def get_connection(
    workspace_id: str,
    connection_id: str,
    *,
    api_root: str,
    api_key: str,
) -> api_models.ConnectionResponse:
    """Get a connection."""
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.connections.get_connection(
        api_operations.GetConnectionRequest(
            connection_id=connection_id,
        ),
    )
    if status_ok(response.status_code) and response.connection_response:
        return response.connection_response

    raise AirbyteMissingResourceError(connection_id, "connection", response.text)


def run_connection(
    workspace_id: str,
    connection_id: str,
    *,
    api_root: str,
    api_key: str,
) -> api_models.ConnectionResponse:
    """Get a connection.

    If block is True, this will block until the connection is finished running.

    If raise_on_failure is True, this will raise an exception if the connection fails.
    """
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.jobs.create_job(
        JobCreateRequest(
            connection_id=connection_id,
            job_type=JobTypeEnum.SYNC,
        ),
    )
    if status_ok(response.status_code) and response.job_response:
        return response.job_response

    raise AirbyteConnectionSyncError(
        connection_id=connection_id,
        context={
            "workspace_id": workspace_id,
        },
        response=response,
    )


# Get job info (logs)


def get_job_logs(
    workspace_id: str,
    connection_id: str,
    limit: int = 20,
    *,
    api_root: str,
    api_key: str,
) -> list[api_models.JobResponse]:
    """Get a job's logs."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response: api_operations.ListJobsResponse = airbyte_instance.jobs.list_jobs(
        api_operations.ListJobsRequest(
            workspace_ids=[workspace_id],
            connection_id=connection_id,
            limit=limit,
        ),
    )
    if status_ok(response.status_code) and response.jobs_response:
        return response.jobs_response.data

    raise AirbyteMissingResourceError(
        response=response,
        resource_type="job",
        context={
            "workspace_id": workspace_id,
            "connection_id": connection_id,
        },
    )


def get_job_info(
    job_id: str,
    *,
    api_root: str,
    api_key: str,
) -> api_models.JobResponse:
    """Get a job."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.jobs.get_job(
        api_operations.GetJobRequest(
            job_id=job_id,
        ),
    )
    if status_ok(response.status_code) and response.job_response:
        return response.job_response

    raise AirbyteMissingResourceError(job_id, "job", response.text)


# Create, get, and delete sources


def create_source(
    name: str,
    *,
    workspace_id: str,
    config: dict[str, Any],
    api_root: str,
    api_key: str,
) -> api_models.SourceResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response: api_operations.CreateSourceResponse = airbyte_instance.sources.create_source(
        api_models.SourceCreateRequest(
            name=name,
            workspace_id=workspace_id,
            configuration=config,  # TODO: wrap in a proper configuration object
            definition_id=None,  # Not used alternative to config.sourceType.
            secret_id=None,  # For OAuth, not yet supported
        ),
    )
    if status_ok(response.status_code) and response.source_response:
        return response.source_response

    raise AirbyteError(
        message="Could not create source.",
        response=response,
    )


def get_source(
    source_id: str,
    *,
    api_root: str,
    api_key: str,
) -> api_models.SourceResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.sources.get_source(
        api_operations.GetSourceRequest(
            source_id=source_id,
        ),
    )
    if status_ok(response.status_code) and response.connection_response:
        return response.connection_response

    raise AirbyteMissingResourceError(source_id, "source", response.text)


def delete_source(
    source_id: str,
    *,
    api_root: str,
    api_key: str,
    workspace_id: str | None = None,
) -> None:
    """Delete a source."""
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.sources.delete_source(
        api_operations.DeleteSourceRequest(
            source_id=source_id,
        ),
    )
    if not status_ok(response.status_code):
        raise AirbyteError(
            context={
                "source_id": source_id,
                "response": response,
            },
        )


# Create, get, and delete destinations


def create_destination(
    name: str,
    *,
    workspace_id: str,
    config: dict[str, Any],
    api_root: str,
    api_key: str,
) -> api_models.DestinationResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response: api_operations.CreateDestinationResponse = (
        airbyte_instance.destinations.create_destination(
            api_models.DestinationCreateRequest(
                name=name,
                workspace_id=workspace_id,
                configuration=config,  # TODO: wrap in a proper configuration object
            ),
        )
    )
    if status_ok(response.status_code) and response.destination_response:
        return response.destination_response

    raise AirbyteError(
        message="Could not create destination.",
        response=response,
    )


def get_destination(
    destination_id: str,
    *,
    api_root: str,
    api_key: str,
) -> api_models.DestinationResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.destinations.get_destination(
        api_operations.GetDestinationRequest(
            destination_id=destination_id,
        ),
    )
    if status_ok(response.status_code):
        # TODO: This is a temporary workaround to resolve an issue where
        # the destination API response is of the wrong type.
        raw_response: dict[str, Any] = json.loads(response.raw_response.text)
        raw_configuration: dict[str, Any] = raw_response["configuration"]
        destination_type = raw_response.get("destinationType")
        if destination_type == "snowflake":
            response.destination_response.configuration = api_models.DestinationSnowflake.from_dict(
                raw_configuration,
            )
        if destination_type == "bigquery":
            response.destination_response.configuration = api_models.DestinationBigquery.from_dict(
                raw_configuration,
            )
        if destination_type == "postgres":
            response.destination_response.configuration = api_models.DestinationPostgres.from_dict(
                raw_configuration,
            )
        if destination_type == "duckdb":
            response.destination_response.configuration = api_models.DestinationDuckdb.from_dict(
                raw_configuration,
            )

        return response.destination_response

    raise AirbyteMissingResourceError(destination_id, "destination", response.text)


def delete_destination(
    destination_id: str,
    *,
    api_root: str,
    api_key: str,
    workspace_id: str | None = None,
) -> None:
    """Delete a destination."""
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.destinations.delete_destination(
        api_operations.DeleteDestinationRequest(
            destination_id=destination_id,
        ),
    )
    if not status_ok(response.status_code):
        raise AirbyteError(
            context={
                "destination_id": destination_id,
                "response": response,
            },
        )


# Create and delete connections


def create_connection(
    name: str,
    *,
    source_id: str,
    destination_id: str,
    api_root: str,
    api_key: str,
    workspace_id: str | None = None,
    prefix: str,
    selected_stream_names: list[str],
) -> api_models.ConnectionResponse:
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    stream_configurations: list[api_models.StreamConfiguration] = []
    if selected_stream_names:
        for stream_name in selected_stream_names:
            stream_configuration = api_models.StreamConfiguration(
                name=stream_name,
            )
            stream_configurations.append(stream_configuration)

    stream_configurations = api_models.StreamConfigurations(stream_configurations)
    response = airbyte_instance.connections.create_connection(
        api_models.ConnectionCreateRequest(
            name=name,
            source_id=source_id,
            destination_id=destination_id,
            configurations=stream_configurations,
            prefix=prefix,
        ),
    )
    if not status_ok(response.status_code):
        raise AirbyteError(
            context={
                "source_id": source_id,
                "destination_id": destination_id,
                "response": response,
            },
        )

    return response.connection_response


def get_connection_by_name(
    workspace_id: str,
    connection_name: str,
    *,
    api_root: str,
    api_key: str,
) -> api_models.ConnectionResponse:
    """Get a connection."""
    connections = list_connections(
        workspace_id=workspace_id,
        api_key=api_key,
        api_root=api_root,
    )
    found: list[api_models.ConnectionResponse] = [
        connection for connection in connections if connection.name == connection_name
    ]
    if len(found) == 0:
        raise AirbyteMissingResourceError(
            connection_name, "connection", f"Workspace: {workspace_id}"
        )

    if len(found) > 1:
        raise AirbyteMultipleResourcesError(
            resource_type="connection",
            resource_name_or_id=connection_name,
            context={
                "workspace_id": workspace_id,
                "multiples": found,
            },
        )

    return found[0]


def delete_connection(
    connection_id: str,
    api_root: str,
    workspace_id: str,
    api_key: str,
) -> None:
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.connections.delete_connection(
        api_operations.DeleteConnectionRequest(
            connection_id=connection_id,
        ),
    )
    if not status_ok(response.status_code):
        raise AirbyteError(
            context={
                "connection_id": connection_id,
                "response": response,
            },
        )


# Not yet implemented


def check_source(
    source_id: str,
    *,
    api_root: str,
    api_key: str,
    workspace_id: str | None = None,
) -> api_models.SourceCheckResponse:
    """Check a source.

    # TODO: Need to use legacy Configuration API for this:
    # https://airbyte-public-api-docs.s3.us-east-2.amazonaws.com/rapidoc-api-docs.html#post-/v1/sources/check_connection
    """
    _ = source_id, workspace_id, api_root, api_key
    raise NotImplementedError
