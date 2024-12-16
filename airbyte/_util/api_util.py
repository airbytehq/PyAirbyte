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


if TYPE_CHECKING:
    from collections.abc import Callable


if TYPE_CHECKING:
    from collections.abc import Callable

    from airbyte_api.models import (
        DestinationConfiguration,
    )


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


def status_ok(status_code: int) -> bool:
    """Check if a status code is OK."""
    return status_code >= 200 and status_code < 300  # noqa: PLR2004  # allow inline magic numbers


def get_config_api_root(api_root: str) -> str:
    """Get the configuration API root from the main API root."""
    if api_root == CLOUD_API_ROOT:
        return CLOUD_CONFIG_API_ROOT

    raise NotImplementedError("Configuration API root not implemented for this API root.")


def get_airbyte_server_instance(
    *,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> airbyte_api.AirbyteAPI:
    """Get an Airbyte instance."""
    return airbyte_api.AirbyteAPI(
        security=models.Security(
            client_credentials=models.SchemeClientCredentials(
                client_id=client_id,
                client_secret=client_secret,
                token_url=api_root + "/applications/token",
                # e.g. https://api.airbyte.com/v1/applications/token
            ),
        ),
        server_url=api_root,
    )


# Get workspace


def get_workspace(
    workspace_id: str,
    *,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> models.WorkspaceResponse:
    """Get a workspace object."""
    airbyte_instance = get_airbyte_server_instance(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
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
    client_id: SecretString,
    client_secret: SecretString,
    name: str | None = None,
    name_filter: Callable[[str], bool] | None = None,
) -> list[models.ConnectionResponse]:
    """List connections."""
    if name and name_filter:
        raise PyAirbyteInputError(message="You can provide name or name_filter, but not both.")

    name_filter = (lambda n: n == name) if name else name_filter or (lambda _: True)

    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
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


def list_workspaces(
    workspace_id: str,
    *,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
    name: str | None = None,
    name_filter: Callable[[str], bool] | None = None,
) -> list[models.WorkspaceResponse]:
    """List workspaces."""
    if name and name_filter:
        raise PyAirbyteInputError(message="You can provide name or name_filter, but not both.")

    name_filter = (lambda n: n == name) if name else name_filter or (lambda _: True)

    _ = workspace_id  # Not used (yet)
    airbyte_instance: airbyte_api.AirbyteAPI = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        api_root=api_root,
    )

    response: api.ListWorkspacesResponse = airbyte_instance.workspaces.list_workspaces(
        api.ListWorkspacesRequest(
            workspace_ids=[workspace_id],
        ),
    )

    if not status_ok(response.status_code) and response.workspaces_response:
        raise AirbyteError(
            context={
                "workspace_id": workspace_id,
                "response": response,
            }
        )
    assert response.workspaces_response is not None
    return [
        workspace for workspace in response.workspaces_response.data if name_filter(workspace.name)
    ]


def list_sources(
    workspace_id: str,
    *,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
    name: str | None = None,
    name_filter: Callable[[str], bool] | None = None,
) -> list[models.SourceResponse]:
    """List sources."""
    if name and name_filter:
        raise PyAirbyteInputError(message="You can provide name or name_filter, but not both.")

    name_filter = (lambda n: n == name) if name else name_filter or (lambda _: True)

    _ = workspace_id  # Not used (yet)
    airbyte_instance: airbyte_api.AirbyteAPI = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        api_root=api_root,
    )
    response: api.ListSourcesResponse = airbyte_instance.sources.list_sources(
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
    client_id: SecretString,
    client_secret: SecretString,
    name: str | None = None,
    name_filter: Callable[[str], bool] | None = None,
) -> list[models.DestinationResponse]:
    """List destinations."""
    if name and name_filter:
        raise PyAirbyteInputError(message="You can provide name or name_filter, but not both.")

    name_filter = (lambda n: n == name) if name else name_filter or (lambda _: True)

    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
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
    client_id: SecretString,
    client_secret: SecretString,
) -> models.ConnectionResponse:
    """Get a connection."""
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
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
    client_id: SecretString,
    client_secret: SecretString,
) -> models.JobResponse:
    """Get a connection.

    If block is True, this will block until the connection is finished running.

    If raise_on_failure is True, this will raise an exception if the connection fails.
    """
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
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
    client_id: SecretString,
    client_secret: SecretString,
) -> list[models.JobResponse]:
    """Get a job's logs."""
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
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
    client_id: SecretString,
    client_secret: SecretString,
) -> models.JobResponse:
    """Get a job."""
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
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
    config: models.SourceConfiguration | dict[str, Any],
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> models.SourceResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        api_root=api_root,
    )
    response: api.CreateSourceResponse = airbyte_instance.sources.create_source(
        models.SourceCreateRequest(
            name=name,
            workspace_id=workspace_id,
            configuration=config,  # Speakeasy API wants a dataclass, not a dict
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
    client_id: SecretString,
    client_secret: SecretString,
) -> models.SourceResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
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
    client_id: SecretString,
    client_secret: SecretString,
    workspace_id: str | None = None,
) -> None:
    """Delete a source."""
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
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
    config: DestinationConfiguration | dict[str, Any],
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> models.DestinationResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        api_root=api_root,
    )
    response: api.CreateDestinationResponse = airbyte_instance.destinations.create_destination(
        models.DestinationCreateRequest(
            name=name,
            workspace_id=workspace_id,
            configuration=config,  # Speakeasy API wants a dataclass, not a dict
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
    client_id: SecretString,
    client_secret: SecretString,
) -> models.DestinationResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
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
        destination_mapping = {
            "snowflake": models.DestinationSnowflake,
            "bigquery": models.DestinationBigquery,
            "postgres": models.DestinationPostgres,
            "duckdb": models.DestinationDuckdb,
        }

        if destination_type in destination_mapping:
            response.destination_response.configuration = destination_mapping[destination_type](
                **raw_configuration
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
    client_id: SecretString,
    client_secret: SecretString,
    workspace_id: str | None = None,
) -> None:
    """Delete a destination."""
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
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


def create_connection(  # noqa: PLR0913  # Too many arguments
    name: str,
    *,
    source_id: str,
    destination_id: str,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
    workspace_id: str | None = None,
    prefix: str,
    selected_stream_names: list[str],
) -> models.ConnectionResponse:
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
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
    client_id: SecretString,
    client_secret: SecretString,
) -> models.ConnectionResponse:
    """Get a connection."""
    connections = list_connections(
        workspace_id=workspace_id,
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
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
    client_id: SecretString,
    client_secret: SecretString,
) -> None:
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
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


def get_bearer_token(
    *,
    client_id: SecretString,
    client_secret: SecretString,
    api_root: str = CLOUD_API_ROOT,
) -> SecretString:
    """Get a bearer token.

    https://reference.airbyte.com/reference/createaccesstoken

    """
    response = requests.post(
        url=api_root + "/applications/token",
        headers={
            "content-type": "application/json",
            "accept": "application/json",
        },
        json={
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    if not status_ok(response.status_code):
        response.raise_for_status()

    return SecretString(response.json()["access_token"])


def _make_config_api_request(
    *,
    api_root: str,
    path: str,
    json: dict[str, Any],
    client_id: SecretString,
    client_secret: SecretString,
) -> dict[str, Any]:
    config_api_root = get_config_api_root(api_root)
    bearer_token = get_bearer_token(
        client_id=client_id,
        client_secret=client_secret,
        api_root=api_root,
    )
    headers: dict[str, Any] = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "PyAirbyte Client",
    }
    response = requests.request(
        method="POST",
        url=config_api_root + path,
        headers=headers,
        json=json,
    )
    if not status_ok(response.status_code):
        try:
            response.raise_for_status()
        except requests.HTTPError as ex:
            raise AirbyteError(
                context={
                    "url": response.request.url,
                    "body": response.request.body,
                    "response": response.__dict__,
                },
            ) from ex

    return response.json()


def check_connector(
    *,
    actor_id: str,
    connector_type: Literal["source", "destination"],
    client_id: SecretString,
    client_secret: SecretString,
    workspace_id: str | None = None,
    api_root: str = CLOUD_API_ROOT,
) -> tuple[bool, str | None]:
    """Check a source.

    Raises an exception if the check fails. Uses one of these endpoints:

    - /v1/sources/check_connection: https://github.com/airbytehq/airbyte-platform-internal/blob/10bb92e1745a282e785eedfcbed1ba72654c4e4e/oss/airbyte-api/server-api/src/main/openapi/config.yaml#L1409
    - /v1/destinations/check_connection: https://github.com/airbytehq/airbyte-platform-internal/blob/10bb92e1745a282e785eedfcbed1ba72654c4e4e/oss/airbyte-api/server-api/src/main/openapi/config.yaml#L1995
    """
    _ = workspace_id  # Not used (yet)

    json_result = _make_config_api_request(
        path=f"/{connector_type}s/check_connection",
        json={
            f"{connector_type}Id": actor_id,
        },
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )
    result, message = json_result.get("status"), json_result.get("message")

    if result == "succeeded":
        return True, None

    if result == "failed":
        return False, message

    raise AirbyteError(
        context={
            "actor_id": actor_id,
            "connector_type": connector_type,
            "response": json_result,
        },
    )
