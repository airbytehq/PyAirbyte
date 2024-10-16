# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""These internal functions are used to interact with the Airbyte API (module named `airbyte`).

In order to insulate users from breaking changes and to avoid general confusion around naming
and design inconsistencies, we do not expose these functions or other Airbyte API classes within
PyAirbyte. Classes and functions from the Airbyte API external library should always be wrapped in
PyAirbyte classes - unless there's a very compelling reason to surface these models intentionally.

Similarly, modules outside of this file should try to avoid interfacing with `airbyte_api` library
directly. This will ensure a single source of truth when mapping between the `airbyte` and
`airbyte_api` libraries.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

import airbyte_api
import requests
from airbyte_api import api, models

from airbyte.exceptions import (
    AirbyteConnectionSyncError,
    AirbyteError,
    AirbyteMissingResourceError,
    AirbyteMultipleResourcesError,
    PyAirbyteInputError,
)
from airbyte.secrets.base import SecretString
from airbyte.sources.util import get_connector


if TYPE_CHECKING:
    from collections.abc import Callable


JOB_WAIT_INTERVAL_SECS = 2.0
JOB_WAIT_TIMEOUT_SECS_DEFAULT = 60 * 60  # 1 hour
CLOUD_API_ROOT = "https://api.airbyte.com/v1"
"""The Airbyte Cloud API root URL.

This is the root URL for the Airbyte Cloud API. It is used to interact with the Airbyte Cloud API
and is the default API root for the `CloudWorkspace` class.
- https://reference.airbyte.com/reference/getting-started
"""
CLOUD_CONFIG_API_ROOT = "https://cloud.airbyte.com/api/v1"
"""Internal-Use API Root, aka Airbyte "Config API".

Documentation:
- https://docs.airbyte.com/api-documentation#configuration-api-deprecated
- https://github.com/airbytehq/airbyte-platform-internal/blob/master/oss/airbyte-api/server-api/src/main/openapi/config.yaml
"""

# Helper functions


def status_ok(status_code: int) -> bool:
    """Check if a status code is OK."""
    return status_code >= 200 and status_code < 300  # noqa: PLR2004  # allow inline magic numbers


def get_airbyte_server_instance(
    *,
    api_key: str,
    api_root: str,
) -> airbyte_api.AirbyteAPI:
    """Get an Airbyte instance."""
    return airbyte_api.AirbyteAPI(
        security=models.Security(
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
) -> models.WorkspaceResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.workspaces.get_workspace(
        api.GetWorkspaceRequest(
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


# List resources


def list_connections(
    workspace_id: str,
    *,
    api_root: str,
    api_key: str,
    name: str | None = None,
    name_filter: Callable[[str], bool] | None = None,
) -> list[models.ConnectionResponse]:
    """Get a connection."""
    if name and name_filter:
        raise PyAirbyteInputError(message="You can provide name or name_filter, but not both.")

    name_filter = (lambda n: n == name) if name else name_filter or (lambda _: True)

    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.connections.list_connections(
        api.ListConnectionsRequest(
            workspace_ids=[workspace_id],
        ),
    )

    if not status_ok(response.status_code) and response.connections_response:
        raise AirbyteError(
            context={
                "workspace_id": workspace_id,
                "response": response,
            }
        )
    assert response.connections_response is not None
    return [
        connection
        for connection in response.connections_response.data
        if name_filter(connection.name)
    ]


def list_sources(
    workspace_id: str,
    *,
    api_root: str,
    api_key: str,
    name: str | None = None,
    name_filter: Callable[[str], bool] | None = None,
) -> list[models.SourceResponse]:
    """Get a connection."""
    if name and name_filter:
        raise PyAirbyteInputError(message="You can provide name or name_filter, but not both.")

    name_filter = (lambda n: n == name) if name else name_filter or (lambda _: True)

    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.sources.list_sources(
        api.ListSourcesRequest(
            workspace_ids=[workspace_id],
        ),
    )

    if not status_ok(response.status_code) and response.sources_response:
        raise AirbyteError(
            context={
                "workspace_id": workspace_id,
                "response": response,
            }
        )
    assert response.sources_response is not None
    return [source for source in response.sources_response.data if name_filter(source.name)]


def list_destinations(
    workspace_id: str,
    *,
    api_root: str,
    api_key: str,
    name: str | None = None,
    name_filter: Callable[[str], bool] | None = None,
) -> list[models.DestinationResponse]:
    """Get a connection."""
    if name and name_filter:
        raise PyAirbyteInputError(message="You can provide name or name_filter, but not both.")

    name_filter = (lambda n: n == name) if name else name_filter or (lambda _: True)

    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.destinations.list_destinations(
        api.ListDestinationsRequest(
            workspace_ids=[workspace_id],
        ),
    )

    if not status_ok(response.status_code) and response.destinations_response:
        raise AirbyteError(
            context={
                "workspace_id": workspace_id,
                "response": response,
            }
        )
    assert response.destinations_response is not None
    return [
        destination
        for destination in response.destinations_response.data
        if name_filter(destination.name)
    ]


# Get and run connections


def get_connection(
    workspace_id: str,
    connection_id: str,
    *,
    api_root: str,
    api_key: str,
) -> models.ConnectionResponse:
    """Get a connection."""
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.connections.get_connection(
        api.GetConnectionRequest(
            connection_id=connection_id,
        ),
    )
    if status_ok(response.status_code) and response.connection_response:
        return response.connection_response

    raise AirbyteMissingResourceError(
        resource_name_or_id=connection_id,
        resource_type="connection",
        log_text=response.raw_response.text,
    )


def run_connection(
    workspace_id: str,
    connection_id: str,
    *,
    api_root: str,
    api_key: str,
) -> models.JobResponse:
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
        models.JobCreateRequest(
            connection_id=connection_id,
            job_type=models.JobTypeEnum.SYNC,
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
) -> list[models.JobResponse]:
    """Get a job's logs."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response: api.ListJobsResponse = airbyte_instance.jobs.list_jobs(
        api.ListJobsRequest(
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
    job_id: int,
    *,
    api_root: str,
    api_key: str,
) -> models.JobResponse:
    """Get a job."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.jobs.get_job(
        api.GetJobRequest(
            job_id=job_id,
        ),
    )
    if status_ok(response.status_code) and response.job_response:
        return response.job_response

    raise AirbyteMissingResourceError(
        resource_name_or_id=str(job_id),
        resource_type="job",
        log_text=response.raw_response.text,
    )


# Create, get, and delete sources


def create_source(
    name: str,
    *,
    workspace_id: str,
    config: dict[str, Any],
    api_root: str,
    api_key: str,
) -> models.SourceResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response: api.CreateSourceResponse = airbyte_instance.sources.create_source(
        models.SourceCreateRequest(
            name=name,
            workspace_id=workspace_id,
            configuration=config,
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
) -> models.SourceResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.sources.get_source(
        api.GetSourceRequest(
            source_id=source_id,
        ),
    )
    if status_ok(response.status_code) and response.source_response:
        return response.source_response

    raise AirbyteMissingResourceError(
        resource_name_or_id=source_id,
        resource_type="source",
        log_text=response.raw_response.text,
    )


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
        api.DeleteSourceRequest(
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
) -> models.DestinationResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response: api.CreateDestinationResponse = airbyte_instance.destinations.create_destination(
        models.DestinationCreateRequest(
            name=name,
            workspace_id=workspace_id,
            configuration=config,
        ),
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
) -> models.DestinationResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response = airbyte_instance.destinations.get_destination(
        api.GetDestinationRequest(
            destination_id=destination_id,
        ),
    )
    if status_ok(response.status_code) and response.destination_response:
        # TODO: This is a temporary workaround to resolve an issue where
        # the destination API response is of the wrong type.
        # https://github.com/airbytehq/pyairbyte/issues/320
        raw_response: dict[str, Any] = json.loads(response.raw_response.text)
        raw_configuration: dict[str, Any] = raw_response["configuration"]

        destination_type = raw_response.get("destinationType")
        if destination_type == "snowflake":
            response.destination_response.configuration = models.DestinationSnowflake(
                **raw_configuration,
            )
        if destination_type == "bigquery":
            response.destination_response.configuration = models.DestinationBigquery(
                **raw_configuration,
            )
        if destination_type == "postgres":
            response.destination_response.configuration = models.DestinationPostgres(
                **raw_configuration,
            )
        if destination_type == "duckdb":
            response.destination_response.configuration = models.DestinationDuckdb(
                **raw_configuration,
            )

        return response.destination_response

    raise AirbyteMissingResourceError(
        resource_name_or_id=destination_id,
        resource_type="destination",
        log_text=response.raw_response.text,
    )


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
        api.DeleteDestinationRequest(
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
) -> models.ConnectionResponse:
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    stream_configurations: list[models.StreamConfiguration] = []
    if selected_stream_names:
        for stream_name in selected_stream_names:
            stream_configuration = models.StreamConfiguration(
                name=stream_name,
            )
            stream_configurations.append(stream_configuration)

    stream_configurations_obj = models.StreamConfigurations(stream_configurations)
    response = airbyte_instance.connections.create_connection(
        models.ConnectionCreateRequest(
            name=name,
            source_id=source_id,
            destination_id=destination_id,
            configurations=stream_configurations_obj,
            prefix=prefix,
        ),
    )
    if not status_ok(response.status_code) or response.connection_response is None:
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
) -> models.ConnectionResponse:
    """Get a connection."""
    connections = list_connections(
        workspace_id=workspace_id,
        api_key=api_key,
        api_root=api_root,
    )
    found: list[models.ConnectionResponse] = [
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
        api.DeleteConnectionRequest(
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


# Functions for leveraging the Airbyte Config API (may not be supported or stable)


@dataclass
class DockerImageOverride:
    """Defines a connector image override."""

    docker_image_override: str
    override_level: Literal["workspace", "actor"] = "actor"


def set_actor_override(
    *,
    workspace_id: str,
    actor_id: str,
    actor_type: Literal["source", "destination"],
    override: DockerImageOverride,
    config_api_root: str = CLOUD_CONFIG_API_ROOT,
    api_key: str | SecretString,
) -> None:
    """Override the docker image and tag for a specific connector.

    https://github.com/airbytehq/airbyte-platform-internal/blob/master/oss/airbyte-api/server-api/src/main/openapi/config.yaml#L7234

    """
    path = config_api_root + "/v1/scoped_configuration/create"
    headers: dict[str, Any] = {
        "Content-Type": "application",
        "Authorization": SecretString(f"Bearer {api_key}"),
    }
    request_body: dict[str, str] = {
        "config_key": "docker_image",  # TODO: Fix this.
        "value": override.docker_image_override,
        "scope_id": actor_id,
        "scope_type": actor_type,
        "resource_id": "",  # TODO: Need to call something like get_actor_definition
        "resource_type": "ACTOR_DEFINITION",
        "origin": "",  # TODO: Need to get user ID somehow or use another origin type
        "origin_type": "USER",
    }
    response = requests.request(
        method="POST",
        url=path,
        headers=headers,
        json=request_body,
    )
    if not status_ok(response.status_code):
        raise AirbyteError(
            context={
                "workspace_id": workspace_id,
                "actor_id": actor_id,
                "actor_type": actor_type,
                "response": response,
            },
        )


def get_connector_image_override(
    *,
    workspace_id: str,
    actor_id: str,
    actor_type: Literal["source", "destination"],
    config_api_root: str = CLOUD_CONFIG_API_ROOT,
    api_key: str,
) -> DockerImageOverride | None:
    """Get the docker image and tag for a specific connector.

    Result is a tuple of two values:
    - A boolean indicating if an override is set.
    - The docker image and tag, either from the override if set, or from the .
    """
    path = config_api_root + "/v1/scoped_configuration/list"
    headers: dict[str, Any] = {
        "Content-Type": "application",
        "Authorization": SecretString(f"Bearer {api_key}"),
    }
    request_body: dict[str, str] = {
        "config_key": "docker_image",  # TODO: Fix this.
    }
    response = requests.request(
        method="GET",
        url=path,
        headers=headers,
        json=request_body,
    )
    if not status_ok(response.status_code):
        raise AirbyteError(
            context={
                "workspace_id": workspace_id,
                "actor_id": actor_id,
                "actor_type": actor_type,
                "response": response,
            },
        )
    if not response.json():
        return None

    overrides = [
        DockerImageOverride(
            docker_image_override=entry["value"],
            override_level=entry["scope_type"],
        )
        for entry in response.json()
    ]
    if not overrides:
        return None
    if len(overrides) > 1:
        raise NotImplementedError(
            "Multiple overrides found. This is not yet supported.",
        )
    return overrides[0]


# Not yet implemented


# def check_source(
#     source_id: str,
#     *,
#     api_root: str,
#     api_key: str,
#     workspace_id: str | None = None,
# ) -> api.SourceCheckResponse:
#     """Check a source."""
#     _ = source_id, workspace_id, api_root, api_key
#     raise NotImplementedError
