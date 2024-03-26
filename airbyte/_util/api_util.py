# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""These internal functions are used to interact with the Airbyte API (module named `airbyte`).

In order to insulate users from breaking changes and to avoid general confusion around naming
and design inconsistencies, we do not expose these functions or other Airbyte API classes within
AirbyteLib. Classes and functions from the Airbyte API external library should always be wrapped in
AirbyteLib classes - unless there's a very compelling reason to surface these models intentionally.
"""

from __future__ import annotations

import os
from time import sleep
from typing import Any

import airbyte_api
from airbyte_api.models import operations as api_operations
from airbyte_api.models import shared as api_models
from airbyte_api.models.shared.jobcreaterequest import JobCreateRequest, JobTypeEnum

from airbyte.exceptions import (
    HostedAirbyteError,
    HostedConnectionSyncError,
    MissingResourceError,
    MultipleResourcesError,
)


JOB_WAIT_INTERVAL_SECS = 2.0
CLOUD_API_ROOT = "https://api.airbyte.com/v1"


def status_ok(status_code: int) -> bool:
    """Check if a status code is OK."""
    return status_code >= 200 and status_code < 300  # noqa: PLR2004  # allow inline magic numbers


def get_default_bearer_token() -> str | None:
    """Get the default bearer token from env variables."""
    return os.environ.get("AIRBYTE_API_KEY", None)


def get_airbyte_server_instance(
    *,
    api_key: str | None = None,
    api_root: str = CLOUD_API_ROOT,
) -> airbyte_api.Airbyte:
    """Get an Airbyte instance."""
    api_key = api_key or get_default_bearer_token()
    return airbyte_api.Airbyte(
        security=api_models.Security(
            bearer_auth=api_key,
        ),
        server_url=api_root,
    )


def get_workspace(
    workspace_id: str,
    *,
    api_root: str = CLOUD_API_ROOT,
    api_key: str | None = None,
) -> api_models.WorkspaceResponse:
    """Get a connection."""
    api_key = api_key or get_default_bearer_token()
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

    raise MissingResourceError(
        resource_type="workspace",
        context={
            "workspace_id": workspace_id,
            "response": response,
        },
    )


def list_connections(
    workspace_id: str,
    *,
    api_root: str = CLOUD_API_ROOT,
    api_key: str | None = None,
) -> list[api_models.ConnectionResponse]:
    """Get a connection."""
    _ = workspace_id  # Not used (yet)
    api_key = api_key or get_default_bearer_token()
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

    raise HostedAirbyteError(
        context={
            "workspace_id": workspace_id,
            "response": response,
        }
    )


def get_connection(
    workspace_id: str,
    connection_id: str,
    *,
    api_root: str = CLOUD_API_ROOT,
    api_key: str | None = None,
) -> api_models.ConnectionResponse:
    """Get a connection."""
    _ = workspace_id  # Not used (yet)
    api_key = api_key or get_default_bearer_token()
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

    raise MissingResourceError(connection_id, "connection", response.text)


def run_connection(
    workspace_id: str,
    connection_id: str,
    *,
    api_root: str = CLOUD_API_ROOT,
    api_key: str | None = None,
    wait_for_job: bool = True,
    raise_on_failure: bool = True,
) -> api_models.ConnectionResponse:
    """Get a connection.

    If block is True, this will block until the connection is finished running.

    If raise_on_failure is True, this will raise an exception if the connection fails.
    """
    _ = workspace_id  # Not used (yet)
    api_key = api_key or get_default_bearer_token()
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
        if wait_for_job:
            job_info = wait_for_airbyte_job(
                workspace_id=workspace_id,
                job_id=response.job_response.job_id,
                api_key=api_key,
                api_root=api_root,
                raise_on_failure=raise_on_failure,
            )

        return job_info

    raise HostedConnectionSyncError(
        context={
            "workspace_id": workspace_id,
            "connection_id": connection_id,
        },
        response=response,
    )


def wait_for_airbyte_job(
    workspace_id: str,
    job_id: str,
    *,
    api_root: str = CLOUD_API_ROOT,
    api_key: str | None = None,
    raise_on_failure: bool = True,
) -> api_models.JobInfo:
    """Wait for a job to finish running."""
    _ = workspace_id  # Not used (yet)
    api_key = api_key or get_default_bearer_token()
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    while True:
        sleep(JOB_WAIT_INTERVAL_SECS)
        response = airbyte_instance.jobs.get_job(
            api_operations.GetJobRequest(
                job_id=job_id,
            ),
        )
        if status_ok(response.status_code) and response.job_info:
            job_info = response.job_info
            if job_info.status == api_models.StatusEnum.succeeded:
                return job_info

            if job_info.status == api_models.StatusEnum.failed:
                if raise_on_failure:
                    raise HostedConnectionSyncError(
                        context={
                            "job_status": job_info.status,
                            "workspace_id": workspace_id,
                            "job_id": job_id,
                            "message": job_info.message,
                        },
                    )

                return job_info

            # Else: Job is still running
            pass
        else:
            raise MissingResourceError(job_id, "job", response.text)


def get_connection_by_name(
    workspace_id: str,
    connection_name: str,
    *,
    api_root: str = CLOUD_API_ROOT,
    api_key: str | None = None,
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
        raise MissingResourceError(connection_name, "connection", f"Workspace: {workspace_id}")

    if len(found) > 1:
        raise MultipleResourcesError(
            resource_type="connection",
            resource_name_or_id=connection_name,
            context={
                "workspace_id": workspace_id,
                "multiples": found,
            },
        )

    return found[0]


def get_source(
    source_id: str,
    *,
    api_root: str = CLOUD_API_ROOT,
    api_key: str | None = None,
) -> api_models.SourceResponse:
    """Get a connection."""
    api_key = api_key or get_default_bearer_token()
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

    raise MissingResourceError(source_id, "source", response.text)


def create_source(
    name: str,
    *,
    workspace_id: str,
    config: dict[str, Any],
    api_root: str = CLOUD_API_ROOT,
    api_key: str | None = None,
) -> api_models.SourceResponse:
    """Get a connection."""
    api_key = api_key or get_default_bearer_token()
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

    raise HostedAirbyteError(
        message="Could not create source.",
        response=response,
    )


def delete_source(
    source_id: str,
    *,
    api_root: str = CLOUD_API_ROOT,
    api_key: str | None = None,
    workspace_id: str | None = None,
) -> None:
    """Delete a source."""
    _ = workspace_id  # Not used (yet)
    api_key = api_key or get_default_bearer_token()
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
        raise HostedAirbyteError(
            context={
                "source_id": source_id,
                "response": response,
            },
        )


def create_destination(
    name: str,
    *,
    workspace_id: str,
    config: dict[str, Any],
    api_root: str = CLOUD_API_ROOT,
    api_key: str | None = None,
) -> api_models.DestinationResponse:
    """Get a connection."""
    api_key = api_key or get_default_bearer_token()
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
                # definition_id="a7bcc9d8-13b3-4e49-b80d-d020b90045e3",  # Not used alternative to config.destinationType.
            ),
        )
    )
    if status_ok(response.status_code) and response.destination_response:
        return response.destination_response

    raise HostedAirbyteError(
        message="Could not create destination.",
        response=response,
    )


def delete_destination(
    destination_id: str,
    *,
    api_root: str = CLOUD_API_ROOT,
    api_key: str | None = None,
    workspace_id: str | None = None,
) -> None:
    """Delete a destination."""
    _ = workspace_id  # Not used (yet)
    api_key = api_key or get_default_bearer_token()
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
        raise HostedAirbyteError(
            context={
                "destination_id": destination_id,
                "response": response,
            },
        )


def create_connection(
    name: str,
    *,
    source_id: str,
    destination_id: str,
    api_root: str,
    api_key: str | None = None,
    workspace_id: str | None = None,
) -> api_models.ConnectionResponse:
    _ = workspace_id  # Not used (yet)
    api_key = api_key or get_default_bearer_token()
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    stream_configuration = api_models.StreamConfiguration(
        name="users",
    )
    stream_configurations = api_models.StreamConfigurations([stream_configuration])
    response = airbyte_instance.connections.create_connection(
        api_models.ConnectionCreateRequest(
            name=name,
            source_id=source_id,
            destination_id=destination_id,
            configurations=stream_configurations,
        ),
    )
    if not status_ok(response.status_code):
        raise HostedAirbyteError(
            context={
                "source_id": source_id,
                "destination_id": destination_id,
                "response": response,
            },
        )

    return response.connection_response


def delete_connection(
    connection_id: str,
    api_root: str,
    workspace_id: str | None = None,
    api_key: str | None = None,
) -> None:
    _ = workspace_id  # Not used (yet)
    api_key = api_key or get_default_bearer_token()
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
        raise HostedAirbyteError(
            context={
                "connection_id": connection_id,
                "response": response,
            },
        )


def check_source(
    source_id: str,
    *,
    api_root: str = CLOUD_API_ROOT,
    api_key: str | None = None,
    workspace_id: str | None = None,
) -> api_models.SourceCheckResponse:
    """Check a source.

    # TODO: Need to use legacy Configuration API for this:
    # https://airbyte-public-api-docs.s3.us-east-2.amazonaws.com/rapidoc-api-docs.html#post-/v1/sources/check_connection
    """
    _ = source_id, workspace_id, api_root, api_key
    raise NotImplementedError


def get_destination(
    destination_id: str,
    *,
    api_root: str = CLOUD_API_ROOT,
    api_key: str | None = None,
) -> api_models.DestinationResponse:
    """Get a connection."""
    api_key = api_key or get_default_bearer_token()
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.sources.get_destination(
        api_operations.GetDestinationRequest(
            destination_id=destination_id,
        ),
    )
    if status_ok(response.status_code) and response.connection_response:
        return response.connection_response

    raise MissingResourceError(destination_id, "destination", response.text)
