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
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Literal

import airbyte_api
import requests
from airbyte_api import api, models

from airbyte.constants import CLOUD_API_ROOT, CLOUD_CONFIG_API_ROOT
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


def status_ok(status_code: int) -> bool:
    """Check if a status code is OK."""
    return status_code >= 200 and status_code < 300  # noqa: PLR2004  # allow inline magic numbers


def get_config_api_root(api_root: str) -> str:
    """Get the configuration API root from the main API root."""
    if api_root == CLOUD_API_ROOT:
        return CLOUD_CONFIG_API_ROOT

    raise NotImplementedError("Configuration API root not implemented for this API root.")


def get_web_url_root(api_root: str) -> str:
    """Get the web URL root from the main API root.

    # TODO: This does not return a valid URL for self-managed instances, due to not knowing the
    # web URL root. Logged here:
    # - https://github.com/airbytehq/PyAirbyte/issues/563
    """
    if api_root == CLOUD_API_ROOT:
        return "https://cloud.airbyte.com"

    return api_root


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
    result: list[models.ConnectionResponse] = []
    has_more = True
    offset, page_size = 0, 100
    while has_more:
        response = airbyte_instance.connections.list_connections(
            api.ListConnectionsRequest(
                workspace_ids=[workspace_id],
                offset=offset,
                limit=page_size,
            ),
        )
        has_more = bool(response.connections_response and response.connections_response.next)
        offset += page_size

        if not status_ok(response.status_code) and response.connections_response:
            raise AirbyteError(
                context={
                    "workspace_id": workspace_id,
                    "response": response,
                }
            )
        assert response.connections_response is not None
        result += [
            connection
            for connection in response.connections_response.data
            if name_filter(connection.name)
        ]
    return result


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
    result: list[models.WorkspaceResponse] = []
    has_more = True
    offset, page_size = 0, 100
    while has_more:
        response: api.ListWorkspacesResponse = airbyte_instance.workspaces.list_workspaces(
            api.ListWorkspacesRequest(workspace_ids=[workspace_id], offset=offset, limit=page_size),
        )
        has_more = bool(response.workspaces_response and response.workspaces_response.next)
        offset += page_size

        if not status_ok(response.status_code) and response.workspaces_response:
            raise AirbyteError(
                context={
                    "workspace_id": workspace_id,
                    "response": response,
                }
            )

        assert response.workspaces_response is not None
        result += [
            workspace
            for workspace in response.workspaces_response.data
            if name_filter(workspace.name)
        ]

    return result


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
    result: list[models.SourceResponse] = []
    has_more = True
    offset, page_size = 0, 100
    while has_more:
        response: api.ListSourcesResponse = airbyte_instance.sources.list_sources(
            api.ListSourcesRequest(
                workspace_ids=[workspace_id],
                offset=offset,
                limit=page_size,
            ),
        )
        has_more = bool(response.sources_response and response.sources_response.next)
        offset += page_size

        if not status_ok(response.status_code) and response.sources_response:
            raise AirbyteError(
                context={
                    "workspace_id": workspace_id,
                    "response": response,
                }
            )
        assert response.sources_response is not None
        result += [source for source in response.sources_response.data if name_filter(source.name)]

    return result


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
    result: list[models.DestinationResponse] = []
    has_more = True
    offset, page_size = 0, 100
    while has_more:
        response = airbyte_instance.destinations.list_destinations(
            api.ListDestinationsRequest(
                workspace_ids=[workspace_id],
                offset=offset,
                limit=page_size,
            ),
        )
        has_more = bool(response.destinations_response and response.destinations_response.next)
        offset += page_size

        if not status_ok(response.status_code) and response.destinations_response:
            raise AirbyteError(
                context={
                    "workspace_id": workspace_id,
                    "response": response,
                }
            )
        assert response.destinations_response is not None
        result += [
            destination
            for destination in response.destinations_response.data
            if name_filter(destination.name)
        ]

    return result


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
    limit: int = 100,
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
    definition_id: str | None = None,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> models.SourceResponse:
    """Create a source connector instance.

    Either `definition_id` or `config[sourceType]` must be provided.
    """
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
            definition_id=definition_id or None,  # Only used for custom sources
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


# Utility function


def _get_destination_type_str(
    destination: DestinationConfiguration | dict[str, Any],
) -> str:
    if isinstance(destination, dict):
        destination_type = destination.get("destinationType")
    else:
        destination_type = getattr(destination, "DESTINATION_TYPE", None)

    if not destination_type or not isinstance(destination_type, str):
        raise PyAirbyteInputError(
            message="Could not determine destination type from configuration.",
            context={
                "destination": destination,
            },
        )

    return destination_type


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
    definition_id_override: str | None = None
    if _get_destination_type_str(config) == "dev-null":
        # TODO: We have to hard-code the definition ID for dev-null destination.
        #  https://github.com/airbytehq/PyAirbyte/issues/743
        definition_id_override = "a7bcc9d8-13b3-4e49-b80d-d020b90045e3"
    response: api.CreateDestinationResponse = airbyte_instance.destinations.create_destination(
        models.DestinationCreateRequest(
            definition_id=definition_id_override,
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
            response.destination_response.configuration = destination_mapping[
                destination_type  # pyrefly: ignore[index-error]
            ](**raw_configuration)
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
            "client_id": str(client_id),
            "client_secret": str(client_secret),
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
    full_url = config_api_root + path
    response = requests.request(
        method="POST",
        url=full_url,
        headers=headers,
        json=json,
    )
    if not status_ok(response.status_code):
        try:
            response.raise_for_status()
        except requests.HTTPError as ex:
            error_message = f"API request failed with status {response.status_code}"
            if response.status_code == HTTPStatus.FORBIDDEN:  # 403 error
                error_message += f" (Forbidden) when accessing: {full_url}"
            raise AirbyteError(
                message=error_message,
                context={
                    "full_url": full_url,
                    "config_api_root": config_api_root,
                    "path": path,
                    "status_code": response.status_code,
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


def validate_yaml_manifest(
    manifest: Any,  # noqa: ANN401
    *,
    raise_on_error: bool = True,
) -> tuple[bool, str | None]:
    """Validate a YAML connector manifest structure.

    Performs basic client-side validation before sending to API.

    Args:
        manifest: The manifest to validate (should be a dictionary).
        raise_on_error: Whether to raise an exception on validation failure.

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not isinstance(manifest, dict):
        error = "Manifest must be a dictionary"
        if raise_on_error:
            raise PyAirbyteInputError(message=error, context={"manifest": manifest})
        return False, error

    required_fields = ["version", "type"]
    missing = [f for f in required_fields if f not in manifest]
    if missing:
        error = f"Manifest missing required fields: {', '.join(missing)}"
        if raise_on_error:
            raise PyAirbyteInputError(message=error, context={"manifest": manifest})
        return False, error

    if manifest.get("type") != "DeclarativeSource":
        error = f"Manifest type must be 'DeclarativeSource', got '{manifest.get('type')}'"
        if raise_on_error:
            raise PyAirbyteInputError(message=error, context={"manifest": manifest})
        return False, error

    return True, None


def create_custom_yaml_source_definition(
    name: str,
    *,
    workspace_id: str,
    manifest: dict[str, Any],
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> models.DeclarativeSourceDefinitionResponse:
    """Create a custom YAML source definition."""
    airbyte_instance = get_airbyte_server_instance(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )

    request_body = models.CreateDeclarativeSourceDefinitionRequest(
        name=name,
        manifest=manifest,
    )
    request = api.CreateDeclarativeSourceDefinitionRequest(
        workspace_id=workspace_id,
        create_declarative_source_definition_request=request_body,
    )
    response = airbyte_instance.declarative_source_definitions.create_declarative_source_definition(
        request
    )
    if response.declarative_source_definition_response is None:
        raise AirbyteError(
            message="Failed to create custom YAML source definition",
            context={"name": name, "workspace_id": workspace_id},
        )
    return response.declarative_source_definition_response


def list_custom_yaml_source_definitions(
    workspace_id: str,
    *,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> list[models.DeclarativeSourceDefinitionResponse]:
    """List all custom YAML source definitions in a workspace."""
    airbyte_instance = get_airbyte_server_instance(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )

    request = api.ListDeclarativeSourceDefinitionsRequest(
        workspace_id=workspace_id,
    )
    response = airbyte_instance.declarative_source_definitions.list_declarative_source_definitions(
        request
    )
    if (
        not status_ok(response.status_code)
        or response.declarative_source_definitions_response is None
    ):
        raise AirbyteError(
            message="Failed to list custom YAML source definitions",
            context={
                "workspace_id": workspace_id,
                "response": response,
            },
        )
    return response.declarative_source_definitions_response.data


def get_custom_yaml_source_definition(
    workspace_id: str,
    definition_id: str,
    *,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> models.DeclarativeSourceDefinitionResponse:
    """Get a specific custom YAML source definition."""
    airbyte_instance = get_airbyte_server_instance(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )

    request = api.GetDeclarativeSourceDefinitionRequest(
        workspace_id=workspace_id,
        definition_id=definition_id,
    )
    response = airbyte_instance.declarative_source_definitions.get_declarative_source_definition(
        request
    )
    if (
        not status_ok(response.status_code)
        or response.declarative_source_definition_response is None
    ):
        raise AirbyteError(
            message="Failed to get custom YAML source definition",
            context={
                "workspace_id": workspace_id,
                "definition_id": definition_id,
                "response": response,
            },
        )
    return response.declarative_source_definition_response


def update_custom_yaml_source_definition(
    workspace_id: str,
    definition_id: str,
    *,
    manifest: dict[str, Any],
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> models.DeclarativeSourceDefinitionResponse:
    """Update a custom YAML source definition."""
    airbyte_instance = get_airbyte_server_instance(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )

    request_body = models.UpdateDeclarativeSourceDefinitionRequest(
        manifest=manifest,
    )
    request = api.UpdateDeclarativeSourceDefinitionRequest(
        workspace_id=workspace_id,
        definition_id=definition_id,
        update_declarative_source_definition_request=request_body,
    )
    response = airbyte_instance.declarative_source_definitions.update_declarative_source_definition(
        request
    )
    if (
        not status_ok(response.status_code)
        or response.declarative_source_definition_response is None
    ):
        raise AirbyteError(
            message="Failed to update custom YAML source definition",
            context={
                "workspace_id": workspace_id,
                "definition_id": definition_id,
                "response": response,
            },
        )
    return response.declarative_source_definition_response


def delete_custom_yaml_source_definition(
    workspace_id: str,
    definition_id: str,
    *,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
    safe_mode: bool = True,
) -> None:
    """Delete a custom YAML source definition.

    Args:
        workspace_id: The workspace ID
        definition_id: The definition ID to delete
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        safe_mode: If True, requires the connector name to either start with "delete:"
            or contain "delete-me" (case insensitive) to prevent accidental deletion.
            Defaults to True.

    Raises:
        PyAirbyteInputError: If safe_mode is True and the connector name does not meet
            the safety requirements.
    """
    if safe_mode:
        definition_info = get_custom_yaml_source_definition(
            workspace_id=workspace_id,
            definition_id=definition_id,
            api_root=api_root,
            client_id=client_id,
            client_secret=client_secret,
        )
        connector_name = definition_info.name

        def is_safe_to_delete(name: str) -> bool:
            name_lower = name.lower()
            return name_lower.startswith("delete:") or "delete-me" in name_lower

        if not is_safe_to_delete(definition_info.name):
            raise PyAirbyteInputError(
                message=(
                    f"Cannot delete custom connector definition '{connector_name}' "
                    "with safe_mode enabled. "
                    "To authorize deletion, the connector name must either:\n"
                    "  1. Start with 'delete:' (case insensitive), OR\n"
                    "  2. Contain 'delete-me' (case insensitive)\n\n"
                    "Please rename the connector to meet one of these requirements "
                    "before attempting deletion."
                ),
                context={
                    "definition_id": definition_id,
                    "connector_name": connector_name,
                    "safe_mode": True,
                },
            )

    # Else proceed with deletion

    airbyte_instance = get_airbyte_server_instance(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )

    request = api.DeleteDeclarativeSourceDefinitionRequest(
        workspace_id=workspace_id,
        definition_id=definition_id,
    )
    response = airbyte_instance.declarative_source_definitions.delete_declarative_source_definition(
        request
    )
    if not status_ok(response.status_code):
        raise AirbyteError(
            context={
                "workspace_id": workspace_id,
                "definition_id": definition_id,
                "response": response,
            },
        )


def get_connector_builder_project_for_definition_id(
    *,
    workspace_id: str,
    definition_id: str,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> str | None:
    """Get the connector builder project ID for a declarative source definition.

    Uses the Config API endpoint:
    /v1/connector_builder_projects/get_for_definition_id

    See: https://github.com/airbytehq/airbyte-platform-internal/blob/master/oss/airbyte-api/server-api/src/main/openapi/config.yaml#L1268

    Args:
        workspace_id: The workspace ID
        definition_id: The declarative source definition ID (actorDefinitionId)
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret

    Returns:
        The builder project ID if found, None otherwise (can be null in API response)
    """
    json_result = _make_config_api_request(
        path="/connector_builder_projects/get_for_definition_id",
        json={
            "actorDefinitionId": definition_id,
            "workspaceId": workspace_id,
        },
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )
    return json_result.get("builderProjectId")


def get_connector_version(
    *,
    connector_id: str,
    connector_type: Literal["source", "destination"],
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> dict[str, Any]:
    """Get the current version for a source or destination connector.

    Uses the Config API endpoint:
    - /v1/actor_definition_versions/get_for_source
    - /v1/actor_definition_versions/get_for_destination

    See: https://github.com/airbytehq/airbyte-platform-internal/blob/master/oss/airbyte-api/server-api/src/main/openapi/config.yaml

    Args:
        connector_id: The source or destination ID
        connector_type: Either "source" or "destination"
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret

    Returns:
        A dictionary containing version information including:
        - dockerImageTag: The version string (e.g., "0.1.0")
        - actorDefinitionId: The connector definition ID
        - actorDefinitionVersionId: The specific version ID
        - isOverrideApplied: Whether a version override is active
    """
    endpoint = f"/actor_definition_versions/get_for_{connector_type}"
    return _make_config_api_request(
        path=endpoint,
        json={
            f"{connector_type}Id": connector_id,
        },
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )


def resolve_connector_version(
    *,
    actor_definition_id: str,
    connector_type: Literal["source", "destination"],
    version: str,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> str:
    """Resolve a semver version string to an actor_definition_version_id.

    Uses the Config API endpoint:
    /v1/actor_definition_versions/resolve

    See: https://github.com/airbytehq/airbyte-platform-internal/blob/master/oss/airbyte-api/server-api/src/main/openapi/config.yaml

    Args:
        actor_definition_id: The connector definition ID
        connector_type: Either "source" or "destination"
        version: The semver version string (e.g., "0.1.0")
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret

    Returns:
        The actor_definition_version_id (UUID as string)

    Raises:
        AirbyteError: If the version cannot be resolved
    """
    json_result = _make_config_api_request(
        path="/actor_definition_versions/resolve",
        json={
            "actorDefinitionId": actor_definition_id,
            "actorType": connector_type,
            "dockerImageTag": version,
        },
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )
    version_id = json_result.get("versionId")
    if not version_id:
        raise AirbyteError(
            message=f"Could not resolve version '{version}' for connector",
            context={
                "actor_definition_id": actor_definition_id,
                "connector_type": connector_type,
                "version": version,
                "response": json_result,
            },
        )
    return version_id


def _find_connector_version_override_id(
    *,
    connector_id: str,
    actor_definition_id: str,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> str | None:
    """Find the scoped configuration ID for a connector version override.

    Uses the /v1/scoped_configuration/get_context endpoint to retrieve the active
    configuration for the given scope without needing to list all configurations.

    Args:
        connector_id: The source or destination ID
        actor_definition_id: The connector definition ID
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret

    Returns:
        The scoped configuration ID if found, None otherwise
    """
    json_result = _make_config_api_request(
        path="/scoped_configuration/get_context",
        json={
            "config_key": "connector_version",
            "scope_type": "actor",
            "scope_id": connector_id,
            "resource_type": "actor_definition",
            "resource_id": actor_definition_id,
        },
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )

    active_config = json_result.get("activeConfiguration")
    if active_config:
        return active_config.get("id")

    return None


def clear_connector_version_override(
    *,
    connector_id: str,
    actor_definition_id: str,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> bool:
    """Clear a version override for a source or destination connector.

    Deletes the scoped configuration that pins the connector to a specific version.

    Uses the Config API endpoints:
    - /v1/scoped_configuration/get_context (to find the override)
    - /v1/scoped_configuration/delete (to remove it)

    See: https://github.com/airbytehq/airbyte-platform-internal/blob/master/oss/airbyte-api/server-api/src/main/openapi/config.yaml

    Args:
        connector_id: The source or destination ID
        actor_definition_id: The connector definition ID
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret

    Returns:
        True if an override was found and deleted, False if no override existed
    """
    config_id = _find_connector_version_override_id(
        connector_id=connector_id,
        actor_definition_id=actor_definition_id,
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )

    if not config_id:
        return False

    _make_config_api_request(
        path="/scoped_configuration/delete",
        json={
            "scopedConfigurationId": config_id,
        },
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )

    return True


def get_user_id_by_email(
    *,
    email: str,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
) -> str:
    """Get a user's UUID by their email address.

    Uses the Config API endpoint:
    /v1/users/list_instance_admin

    Args:
        email: The user's email address
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret

    Returns:
        The user's UUID as a string

    Raises:
        ValueError: If no user with the given email is found
    """
    response = _make_config_api_request(
        path="/users/list_instance_admin",
        json={},
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )

    users = response.get("users", [])
    for user in users:
        if user.get("email") == email:
            return user["userId"]

    raise ValueError(f"No user found with email: {email}")


def set_connector_version_override(  # noqa: PLR0913
    *,
    connector_id: str,
    connector_type: Literal["source", "destination"],
    actor_definition_id: str,
    actor_definition_version_id: str,
    override_reason: str,
    user_email: str,
    api_root: str,
    client_id: SecretString,
    client_secret: SecretString,
    override_reason_reference_url: str | None = None,
) -> dict[str, Any]:
    """Set a version override for a source or destination connector.

    Creates a scoped configuration at the ACTOR level to pin the connector
    to a specific version.

    Uses the Config API endpoint:
    /v1/scoped_configuration/create

    See: https://github.com/airbytehq/airbyte-platform-internal/blob/master/oss/airbyte-api/server-api/src/main/openapi/config.yaml

    Args:
        connector_id: The source or destination ID
        connector_type: Either "source" or "destination"
        actor_definition_id: The connector definition ID
        actor_definition_version_id: The version ID to pin to
        override_reason: Explanation for why the version override is being set
        user_email: Email address of the user creating the override
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        override_reason_reference_url: Optional URL with more context (e.g., issue link)

    Returns:
        The created scoped configuration response
    """
    user_id = get_user_id_by_email(
        email=user_email,
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )

    request_body: dict[str, Any] = {
        "config_key": "connector_version",
        "value": actor_definition_version_id,
        "resource_type": "actor_definition",
        "resource_id": actor_definition_id,
        "scope_type": "actor",
        "scope_id": connector_id,
        "origin": user_id,
        "origin_type": "user",
        "description": override_reason,
    }
    if override_reason_reference_url:
        request_body["reference_url"] = override_reason_reference_url

    # Note: The ScopedConfiguration API also supports an optional "expiresAt" field
    # (ISO 8601 datetime string) to automatically expire the override after a certain date.
    # This could be added in the future if there are business cases for time-limited overrides.

    return _make_config_api_request(
        path="/scoped_configuration/create",
        json=request_body,
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
    )
