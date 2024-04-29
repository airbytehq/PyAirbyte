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
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable

import airbyte_api
import requests
from airbyte_api import api, models

from airbyte import exceptions as exc
from airbyte._util.iter import exactly_one_resource
from airbyte.exceptions import (
    AirbyteConnectionSyncError,
    AirbyteError,
    AirbyteMissingResourceError,
    PyAirbyteInternalError,
)


if TYPE_CHECKING:
    from collections.abc import Iterator

    from airbyte.cloud.constants import ConnectorTypeEnum


JOB_WAIT_INTERVAL_SECS = 2.0
JOB_WAIT_TIMEOUT_SECS_DEFAULT = 60 * 60  # 1 hour
CLOUD_API_ROOT = "https://api.airbyte.com/v1"
LEGACY_API_ROOT = "https://cloud.airbyte.com/api/v1"
DEFAULT_PAGE_SIZE = 30

# Helper functions


class ResourceTypeEnum(str, Enum):
    """Resource types."""

    CONNECTION = "connection"
    SOURCE = "source"
    DESTINATION = "destination"
    JOB = "job"
    WORKSPACE = "workspace"


def status_ok(status_code: int) -> bool:
    """Check if a status code is OK."""
    return status_code >= 200 and status_code < 300  # noqa: PLR2004  # allow inline magic numbers


def get_airbyte_server_instance(
    *,
    api_key: str,
    api_root: str,
) -> airbyte_api.Airbyte:
    """Get an Airbyte instance."""
    return airbyte_api.AirbyteAPI(
        security=models.Security(
            bearer_auth=api_key,
        ),
        server_url=api_root,
    )


# Fetch Resource (Generic)


def fetch_resource_info(
    resource_id: str,
    resource_type: ResourceTypeEnum,
    *,
    api_root: str,
    api_key: str,
) -> (
    None
    | models.WorkspaceResponse
    | models.ConnectionResponse
    | models.SourceResponse
    | models.DestinationResponse
    | models.JobResponse
):
    """Get a resource."""
    airbyte_instance: airbyte_api.AirbyteAPI = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    if resource_type == ResourceTypeEnum.CONNECTION:
        raw_response = airbyte_instance.connections.get_connection(
            api.GetConnectionRequest(
                connection_id=resource_id,
            ),
        )
        if status_ok(raw_response.status_code) and raw_response.connection_response:
            return raw_response.connection_response

    elif resource_type == ResourceTypeEnum.SOURCE:
        raw_response = airbyte_instance.sources.get_source(
            api.GetSourceRequest(
                source_id=resource_id,
            ),
        )
        if status_ok(raw_response.status_code) and raw_response.source_response:
            return raw_response.source_response

    elif resource_type == ResourceTypeEnum.DESTINATION:
        raw_response = airbyte_instance.destinations.get_destination(
            api.GetDestinationRequest(
                destination_id=resource_id,
            ),
        )
        if status_ok(raw_response.status_code) and raw_response.destination_response:
            # TODO: Remove this "fix" once the Airbyte API library is fixed.
            raw_response = _fix_destination_info(raw_response)
            return raw_response.destination_response

    elif resource_type == ResourceTypeEnum.JOB:
        raw_response = airbyte_instance.jobs.get_job(
            api.GetJobRequest(
                job_id=resource_id,
            ),
        )
        if status_ok(raw_response.status_code) and raw_response.job_response:
            return raw_response.job_response

    elif resource_type == ResourceTypeEnum.WORKSPACE:
        raw_response = airbyte_instance.workspaces.get_workspace(
            api.GetWorkspaceRequest(
                workspace_id=resource_id,
            ),
        )
        if status_ok(raw_response.status_code) and raw_response.workspace_response:
            return raw_response.workspace_response

    else:
        raise PyAirbyteInternalError(
            message="Invalid resource type.",
            context={
                "resource_type": resource_type,
            },
        )

    # If we reach this point, the resource was not found.
    raise AirbyteMissingResourceError(
        response=raw_response,
        resource_name_or_id=resource_id,
        resource_type=resource_type.value,
        log_text=raw_response.text,
    )


def fetch_workspace_info(
    workspace_id: str,
    *,
    api_root: str,
    api_key: str,
) -> models.WorkspaceResponse:
    """Get a connection."""
    return fetch_resource_info(
        resource_id=workspace_id,
        resource_type=ResourceTypeEnum.WORKSPACE,
        api_key=api_key,
        api_root=api_root,
    )


def fetch_connection_info(
    connection_id: str,
    *,
    api_root: str,
    api_key: str,
) -> models.ConnectionResponse:
    """Get a connection."""
    return fetch_resource_info(
        resource_id=connection_id,
        resource_type=ResourceTypeEnum.CONNECTION,
        api_key=api_key,
        api_root=api_root,
    )


def fetch_source_info(
    source_id: str,
    *,
    api_root: str,
    api_key: str,
) -> models.SourceResponse:
    """Get a source."""
    return fetch_resource_info(
        resource_id=source_id,
        resource_type=ResourceTypeEnum.SOURCE,
        api_key=api_key,
        api_root=api_root,
    )


def fetch_destination_info(
    destination_id: str,
    *,
    api_root: str,
    api_key: str,
) -> models.DestinationResponse:
    """Get a destination."""
    return fetch_resource_info(
        resource_id=destination_id,
        resource_type=ResourceTypeEnum.DESTINATION,
        api_key=api_key,
        api_root=api_root,
    )


def _fix_destination_info(
    response: models.DestinationResponse,
) -> models.DestinationResponse:
    if status_ok(response.status_code):
        # TODO: This is a temporary workaround to resolve an issue where
        # the destination API response is of the wrong type.
        raw_response: dict[str, Any] = json.loads(response.raw_response.text)
        raw_configuration: dict[str, Any] = raw_response["configuration"]
        destination_type = raw_response.get("destinationType")
        if destination_type == "snowflake":
            response.destination_response.configuration = models.DestinationSnowflake.from_dict(
                raw_configuration,
            )
        if destination_type == "bigquery":
            response.destination_response.configuration = models.DestinationBigquery.from_dict(
                raw_configuration,
            )
        if destination_type == "postgres":
            response.destination_response.configuration = models.DestinationPostgres.from_dict(
                raw_configuration,
            )
        if destination_type == "duckdb":
            response.destination_response.configuration = models.DestinationDuckdb.from_dict(
                raw_configuration,
            )

        return response.destination_response

    raise NotImplementedError  # TODO: Replace with a custom exception for this case.


def fetch_job_info(
    job_id: str,
    *,
    api_root: str,
    api_key: str,
) -> models.JobResponse:
    """Get a job."""
    return fetch_resource_info(
        resource_id=job_id,
        resource_type=ResourceTypeEnum.JOB,
        api_key=api_key,
        api_root=api_root,
    )


# List connections, sources, and destinations


def list_resources(
    resource_type: ResourceTypeEnum,
    *,
    workspace_id: str,
    parent_resource_id: str | None = None,
    name_filter: str | Callable[[str], bool] | None = None,
    api_root: str,
    api_key: str,
    page_size: int,
    limit: int | None,
) -> Iterator[
    api.ListConnectionsResponse
    | api.ListSourcesResponse
    | api.ListDestinationsResponse
    | api.ListJobsResponse
]:
    """Get a connection.

    If name_filter is a string, only connections containing that name will be returned. If
    name_filter is a function, it will be called with the connection name and should return a
    boolean.
    """
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    offset = 0
    returned_count = 0

    if isinstance(name_filter, str):
        # Redefine name_filter as a function

        def name_filter(name: str) -> bool:
            return name_filter in name
    elif name_filter is None:
        # "Always True" filter

        def name_filter(name: str) -> bool:
            _ = name
            return True

    if resource_type == ResourceTypeEnum.CONNECTION:
        list_function = airbyte_instance.connections.list_connections
        request = api.ListConnectionsRequest(
            workspace_ids=[workspace_id],
            include_deleted=False,
            limit=page_size,
            offset=offset,
        )

        def get_resources_from_response(
            response: api.ListConnectionsResponse,
        ) -> list[api.ConnectionResponse]:
            return response.connections_response.data

    elif resource_type == ResourceTypeEnum.SOURCE:
        list_function = airbyte_instance.sources.list_sources
        request = api.ListSourcesRequest(
            workspace_ids=[workspace_id],
            limit=page_size,
            offset=offset,
        )

        def get_resources_from_response(
            response: api.ListSourcesResponse,
        ) -> list[api.SourceResponse]:
            return response.sources_response.data

    elif resource_type == ResourceTypeEnum.DESTINATION:
        list_function = airbyte_instance.destinations.list_destinations
        request = api.ListDestinationsRequest(
            workspace_id=[workspace_id],
            limit=page_size,
            offset=offset,
        )

        def get_resources_from_response(
            response: api.ListConnectionsResponse,
        ) -> list[api.ConnectionResponse]:
            return response.connections_response.data

    elif resource_type == ResourceTypeEnum.JOB:
        list_function = airbyte_instance.jobs.list_jobs
        request = api.ListJobsRequest(
            workspace_ids=[workspace_id],
            connection_id=parent_resource_id,
            limit=page_size,
            offset=offset,
        )

        def get_resources_from_response(
            response: api.ListJobsResponse,
        ) -> list[api.JobResponse]:
            return response.jobs_response.data

    else:
        raise PyAirbyteInternalError(
            message="Invalid resource type.",
            context={
                "resource_type": resource_type,
            },
        )

    while limit is None or returned_count < limit:
        response = list_function(request)

        if not status_ok(response.status_code):
            raise AirbyteError(
                context={
                    "workspace_id": workspace_id,
                    "response": response,
                }
            )

        resources = get_resources_from_response(response)
        if not resources:
            # No more resources to list
            break

        for resource in resources:
            if name_filter(resource.name):
                yield resource

            returned_count += 1
            if limit is not None and returned_count >= limit:
                break

        offset += page_size

    # Finished paging
    return


def list_sources(
    workspace_id: str,
    *,
    name_filter: str | Callable[[str], bool] | None = None,
    api_root: str,
    api_key: str,
    limit: int | None = None,
) -> Iterator[api.SourceResponse]:
    """List sources."""
    return list_resources(
        ResourceTypeEnum.SOURCE,
        workspace_id=workspace_id,
        name_filter=name_filter,
        limit=limit,
        api_key=api_key,
        api_root=api_root,
        page_size=DEFAULT_PAGE_SIZE,
    )


def list_destinations(
    workspace_id: str,
    *,
    name_filter: str | Callable[[str], bool] | None = None,
    api_root: str,
    api_key: str,
    limit: int | None = None,
) -> Iterator[api.DestinationResponse]:
    """List destinations."""
    return list_resources(
        ResourceTypeEnum.DESTINATION,
        workspace_id=workspace_id,
        name_filter=name_filter,
        limit=limit,
        api_key=api_key,
        api_root=api_root,
        page_size=DEFAULT_PAGE_SIZE,
    )


def list_connections(
    workspace_id: str,
    *,
    name_filter: str | Callable[[str], bool] | None = None,
    api_root: str,
    api_key: str,
    limit: int | None = None,
) -> Iterator[api.ConnectionResponse]:
    """List connections."""
    return list_resources(
        ResourceTypeEnum.CONNECTION,
        workspace_id=workspace_id,
        name_filter=name_filter,
        limit=limit,
        page_size=DEFAULT_PAGE_SIZE,
        api_key=api_key,
        api_root=api_root,
    )


def list_jobs(
    workspace_id: str,
    connection_id: str,
    *,
    api_root: str,
    api_key: str,
    limit: int | None = 30,
) -> Iterator[api.JobResponse]:
    """List jobs."""
    return list_resources(
        ResourceTypeEnum.JOB,
        workspace_id=workspace_id,
        parent_resource_id=connection_id,
        limit=limit,
        page_size=DEFAULT_PAGE_SIZE,
        api_key=api_key,
        api_root=api_root,
    )


# Get resources by name


def fetch_connection_by_name(
    workspace_id: str,
    connection_name: str,
    *,
    api_root: str,
    api_key: str,
) -> models.ConnectionResponse:
    """Get a connection by name.

    Raises `AirbyteMissingResourceError` if the connection is not found or
    `AirbyteMultipleResourcesError` if multiple connections are found.
    """
    return exactly_one_resource(
        list_connections(
            workspace_id=workspace_id,
            name_filter=connection_name,
            api_key=api_key,
            api_root=api_root,
            limit=2,
        )
    )


def fetch_source_by_name(
    workspace_id: str,
    source_name: str,
    *,
    api_root: str,
    api_key: str,
) -> models.SourceResponse:
    """Get a source by name.

    Raises `AirbyteMissingResourceError` if the source is not found or
    `AirbyteMultipleResourcesError` if multiple sources are found.
    """
    return exactly_one_resource(
        list_sources(
            workspace_id=workspace_id,
            name_filter=source_name,
            api_key=api_key,
            api_root=api_root,
            limit=2,
        )
    )


def fetch_destination_by_name(
    workspace_id: str,
    destination_name: str,
    *,
    api_root: str,
    api_key: str,
) -> models.DestinationResponse:
    """Get a destination by name.

    Raises `AirbyteMissingResourceError` if the destination is not found or
    `AirbyteMultipleResourcesError` if multiple destinations are found.
    """
    return exactly_one_resource(
        list_sources(
            workspace_id=workspace_id,
            name_filter=destination_name,
            api_key=api_key,
            api_root=api_root,
            limit=2,
        )
    )


# Run connections


def run_connection(
    workspace_id: str,
    connection_id: str,
    *,
    api_root: str,
    api_key: str,
) -> api.ConnectionResponse:
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


# Create and delete sources


def create_source(
    name: str,
    *,
    workspace_id: str,
    config: dict[str, Any],
    api_root: str,
    api_key: str,
) -> api.SourceResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response: api.CreateSourceResponse = airbyte_instance.sources.create_source(
        models.SourceCreateRequest(
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


# Create and delete destinations


def create_destination(
    name: str,
    *,
    workspace_id: str,
    config: dict[str, Any],
    api_root: str,
    api_key: str,
) -> api.DestinationResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    response: api.CreateDestinationResponse = airbyte_instance.destinations.create_destination(
        models.DestinationCreateRequest(
            name=name,
            workspace_id=workspace_id,
            configuration=config,  # TODO: wrap in a proper configuration object
        ),
    )
    if status_ok(response.status_code) and response.destination_response:
        return response.destination_response

    raise AirbyteError(
        message="Could not create destination.",
        response=response,
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
    selected_stream_names: list[str] | None = None,
) -> models.ConnectionResponse:
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        api_key=api_key,
        api_root=api_root,
    )
    stream_configurations: models.StreamConfigurations | None = None
    if selected_stream_names:
        stream_configuration_list = []
        for stream_name in selected_stream_names:
            stream_configuration = models.StreamConfiguration(
                name=stream_name,
            )
            stream_configuration_list.append(stream_configuration)

        stream_configurations = models.StreamConfigurations(stream_configuration_list)

    response = airbyte_instance.connections.create_connection(
        models.ConnectionCreateRequest(
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


# Legacy API Functions


def _transform_legacy_api_root(api_root: str) -> str:
    """Transform the API root to the legacy API root if needed."""
    if api_root == CLOUD_API_ROOT:
        # We know the user is using Airbyte Cloud, so we can safely return the legacy API root.
        return LEGACY_API_ROOT

    # TODO: Figure out how to translate an OSS/Enterprise API root to the legacy Config API root.
    raise NotImplementedError(
        "Airbyte OSS and Enterprise are not currently supported for this operation."
    )


def check_connector_config(
    connector_id: str,
    connector_type: ConnectorTypeEnum,
    workspace_id: str,
    *,
    api_key: str,
    api_root: str,
) -> None:
    """Check source or destination with its current config.

    Raises `AirbyteConnectorCheckFailedError` if the check fails.

    This calls the Config API because the Airbyte API does not support this operation.

    Equivalent to:

    ```bash
    curl -X POST "https://cloud.airbyte.com/api/v1/sources/check_connection" \
        -H "accept: application/json"\
        -H "content-type: application/json" \
        -d '{"sourceId":"18efe99a-7400-4000-8d95-ca2cb0e7b401"}'
    ```

    API Docs:
        https://airbyte-public-api-docs.s3.us-east-2.amazonaws.com/rapidoc-api-docs.html#post-/v1/sources/check_connection
    """
    legacy_api_root = _transform_legacy_api_root(api_root)

    _ = workspace_id  # Not used
    response: requests.Response = requests.post(
        f"{legacy_api_root}/{connector_type.value}s/check_connection",
        headers={
            "accept": "application/json",
            "content-type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        json={
            f"{connector_type.value}Id": connector_id,
        },
    )
    response.raise_for_status()

    response_json = response.json()
    if not response_json.get("status", None) == "succeeded":
        raise exc.AirbyteConnectorCheckFailedError(
            message=response_json.get("message", None),
            context=response_json,
        )
