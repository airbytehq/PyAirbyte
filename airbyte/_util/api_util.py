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
from airbyte_api.errors import SDKError

from airbyte.constants import CLOUD_API_ROOT, CLOUD_CONFIG_API_ROOT, CLOUD_CONFIG_API_ROOT_ENV_VAR
from airbyte.exceptions import (
    AirbyteConnectionSyncActiveError,
    AirbyteConnectionSyncError,
    AirbyteError,
    AirbyteMissingResourceError,
    AirbyteMultipleResourcesError,
    PyAirbyteInputError,
)
from airbyte.secrets.base import SecretString
from airbyte.secrets.util import try_get_secret


if TYPE_CHECKING:
    from collections.abc import Callable


if TYPE_CHECKING:
    from collections.abc import Callable

    from airbyte_api.models import (
        DestinationConfiguration,
    )


JOB_WAIT_INTERVAL_SECS = 2.0
JOB_WAIT_TIMEOUT_SECS_DEFAULT = 60 * 60  # 1 hour

# Job ordering constants for list_jobs API
JOB_ORDER_BY_CREATED_AT_DESC = "createdAt|DESC"
JOB_ORDER_BY_CREATED_AT_ASC = "createdAt|ASC"


def status_ok(status_code: int) -> bool:
    """Check if a status code is OK."""
    return status_code >= 200 and status_code < 300  # noqa: PLR2004  # allow inline magic numbers


def _get_sdk_error_context(error: SDKError) -> dict[str, Any]:
    """Extract context information from an SDKError for debugging.

    This helper extracts the actual request URL and other useful debugging
    information from the Speakeasy SDK's SDKError exception. The SDK stores
    the raw response object which contains the request details.
    """
    context: dict[str, Any] = {
        "status_code": error.status_code,
        "error_message": error.message,
    }

    if error.raw_response is not None:
        request = error.raw_response.request
        if request is not None:
            context["request_url"] = str(request.url)
            context["request_method"] = request.method
        context["response_content_type"] = error.raw_response.headers.get("content-type")

    return context


def _wrap_sdk_error(error: SDKError, base_context: dict[str, Any] | None = None) -> AirbyteError:
    """Wrap an SDKError with additional context for debugging.

    This function converts a Speakeasy SDK error into an AirbyteError with
    full URL context, making it easier to debug API issues like 404 errors.
    """
    sdk_context = _get_sdk_error_context(error)
    merged_context = {**(base_context or {}), **sdk_context}
    return AirbyteError(
        message=f"API error occurred: {error.message}",
        context=merged_context,
    )


def get_config_api_root(api_root: str) -> str:
    """Get the configuration API root from the main API root.

    Resolution order:
    1. If `AIRBYTE_CLOUD_CONFIG_API_URL` environment variable is set, use that value.
    2. If `api_root` matches the default Cloud API root, return the default Config API root.
    3. Otherwise, raise NotImplementedError (cannot derive Config API from custom API root).

    Args:
        api_root: The main API root URL being used.

    Returns:
        The configuration API root URL.

    Raises:
        NotImplementedError: If the Config API root cannot be determined.
    """
    # First, check if the Config API URL is explicitly set via environment variable
    config_api_override = try_get_secret(CLOUD_CONFIG_API_ROOT_ENV_VAR, default=None)
    if config_api_override:
        return str(config_api_override)

    # Fall back to deriving from the main API root
    # Normalize URLs by stripping trailing slashes to handle common variants
    if api_root.rstrip("/") == CLOUD_API_ROOT.rstrip("/"):
        return CLOUD_CONFIG_API_ROOT

    raise NotImplementedError(
        f"Configuration API root not implemented for api_root='{api_root}'. "
        f"Set the '{CLOUD_CONFIG_API_ROOT_ENV_VAR}' environment variable "
        "to specify the Config API URL."
    )


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
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> airbyte_api.AirbyteAPI:
    """Get an Airbyte API instance.

    Supports two authentication methods (mutually exclusive):
    1. OAuth2 client credentials (client_id + client_secret)
    2. Bearer token authentication

    Args:
        api_root: The API root URL.
        client_id: OAuth2 client ID (required if not using bearer_token).
        client_secret: OAuth2 client secret (required if not using bearer_token).
        bearer_token: Pre-generated bearer token (alternative to client credentials).

    Returns:
        An authenticated AirbyteAPI instance.

    Raises:
        PyAirbyteInputError: If authentication parameters are invalid.
    """
    # Guard: must provide either bearer token OR both client credentials
    if bearer_token is None and (client_id is None or client_secret is None):
        raise PyAirbyteInputError(
            message="No authentication credentials provided.",
            guidance="Provide either client_id and client_secret, or bearer_token.",
        )

    # Guard: cannot provide both auth methods
    if bearer_token is not None and (client_id is not None or client_secret is not None):
        raise PyAirbyteInputError(
            message="Cannot use both client credentials and bearer token authentication.",
            guidance="Provide either client_id and client_secret, or bearer_token, but not both.",
        )

    # Option 1: Bearer token authentication
    if bearer_token is not None:
        return airbyte_api.AirbyteAPI(
            security=models.Security(
                bearer_auth=bearer_token,
            ),
            server_url=api_root,
        )

    # Option 2: Client credentials flow (guaranteed non-None by first guard)

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
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.WorkspaceResponse:
    """Get a workspace object."""
    airbyte_instance = get_airbyte_server_instance(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )
    base_context = {"workspace_id": workspace_id, "api_root": api_root}
    try:
        response = airbyte_instance.workspaces.get_workspace(
            api.GetWorkspaceRequest(
                workspace_id=workspace_id,
            ),
        )
    except SDKError as e:
        raise _wrap_sdk_error(e, base_context) from e

    if status_ok(response.status_code) and response.workspace_response:
        return response.workspace_response

    raise AirbyteMissingResourceError(
        resource_type="workspace",
        context={
            "workspace_id": workspace_id,
            "request_url": response.raw_response.url,
            "status_code": response.status_code,
        },
    )


# List resources


def list_connections(
    workspace_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
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
        bearer_token=bearer_token,
        api_root=api_root,
    )
    result: list[models.ConnectionResponse] = []
    has_more = True
    offset, page_size = 0, 100
    base_context = {"workspace_id": workspace_id, "api_root": api_root}
    while has_more:
        try:
            response = airbyte_instance.connections.list_connections(
                api.ListConnectionsRequest(
                    workspace_ids=[workspace_id],
                    offset=offset,
                    limit=page_size,
                ),
            )
        except SDKError as e:
            raise _wrap_sdk_error(e, base_context) from e

        has_more = bool(response.connections_response and response.connections_response.next)
        offset += page_size

        if not status_ok(response.status_code):
            raise AirbyteError(
                context={
                    "workspace_id": workspace_id,
                    "request_url": response.raw_response.url,
                    "status_code": response.status_code,
                },
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
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
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
        bearer_token=bearer_token,
        api_root=api_root,
    )
    result: list[models.WorkspaceResponse] = []
    has_more = True
    offset, page_size = 0, 100
    base_context = {"workspace_id": workspace_id, "api_root": api_root}
    while has_more:
        try:
            response: api.ListWorkspacesResponse = airbyte_instance.workspaces.list_workspaces(
                api.ListWorkspacesRequest(
                    workspace_ids=[workspace_id], offset=offset, limit=page_size
                ),
            )
        except SDKError as e:
            raise _wrap_sdk_error(e, base_context) from e

        has_more = bool(response.workspaces_response and response.workspaces_response.next)
        offset += page_size

        if not status_ok(response.status_code):
            raise AirbyteError(
                context={
                    "workspace_id": workspace_id,
                    "request_url": response.raw_response.url,
                    "status_code": response.status_code,
                },
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
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
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
        bearer_token=bearer_token,
        api_root=api_root,
    )
    result: list[models.SourceResponse] = []
    has_more = True
    offset, page_size = 0, 100
    base_context = {"workspace_id": workspace_id, "api_root": api_root}
    while has_more:
        try:
            response: api.ListSourcesResponse = airbyte_instance.sources.list_sources(
                api.ListSourcesRequest(
                    workspace_ids=[workspace_id],
                    offset=offset,
                    limit=page_size,
                ),
            )
        except SDKError as e:
            raise _wrap_sdk_error(e, base_context) from e

        has_more = bool(response.sources_response and response.sources_response.next)
        offset += page_size

        if not status_ok(response.status_code):
            raise AirbyteError(
                context={
                    "workspace_id": workspace_id,
                    "request_url": response.raw_response.url,
                    "status_code": response.status_code,
                },
            )
        assert response.sources_response is not None
        result += [source for source in response.sources_response.data if name_filter(source.name)]

    return result


def list_destinations(
    workspace_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
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
        bearer_token=bearer_token,
        api_root=api_root,
    )
    result: list[models.DestinationResponse] = []
    has_more = True
    offset, page_size = 0, 100
    base_context = {"workspace_id": workspace_id, "api_root": api_root}
    while has_more:
        try:
            response = airbyte_instance.destinations.list_destinations(
                api.ListDestinationsRequest(
                    workspace_ids=[workspace_id],
                    offset=offset,
                    limit=page_size,
                ),
            )
        except SDKError as e:
            raise _wrap_sdk_error(e, base_context) from e

        has_more = bool(response.destinations_response and response.destinations_response.next)
        offset += page_size

        if not status_ok(response.status_code):
            raise AirbyteError(
                context={
                    "workspace_id": workspace_id,
                    "request_url": response.raw_response.url,
                    "status_code": response.status_code,
                },
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
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.ConnectionResponse:
    """Get a connection."""
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        api_root=api_root,
    )
    base_context = {
        "workspace_id": workspace_id,
        "connection_id": connection_id,
        "api_root": api_root,
    }
    try:
        response = airbyte_instance.connections.get_connection(
            api.GetConnectionRequest(
                connection_id=connection_id,
            ),
        )
    except SDKError as e:
        raise _wrap_sdk_error(e, base_context) from e

    if status_ok(response.status_code) and response.connection_response:
        return response.connection_response

    raise AirbyteMissingResourceError(
        resource_name_or_id=connection_id,
        resource_type="connection",
        log_text=response.raw_response.text,
        context={
            "request_url": response.raw_response.url,
            "status_code": response.status_code,
        },
    )


def run_connection(
    workspace_id: str,
    connection_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.JobResponse:
    """Get a connection.

    If block is True, this will block until the connection is finished running.

    If raise_on_failure is True, this will raise an exception if the connection fails.
    """
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
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
            "request_url": response.raw_response.url,
            "status_code": response.status_code,
        },
        response=response,
    )


# Get job info (logs)


def get_job_logs(  # noqa: PLR0913  # Too many arguments - needed for auth flexibility
    workspace_id: str,
    connection_id: str,
    limit: int = 100,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    offset: int | None = None,
    order_by: str | None = None,
    job_type: models.JobTypeEnum | None = None,
) -> list[models.JobResponse]:
    """Get a list of jobs for a connection.

    Args:
        workspace_id: The workspace ID.
        connection_id: The connection ID.
        limit: Maximum number of jobs to return. Defaults to 100.
        api_root: The API root URL.
        client_id: The client ID for authentication.
        client_secret: The client secret for authentication.
        bearer_token: Bearer token for authentication (alternative to client credentials).
        offset: Number of jobs to skip from the beginning. Defaults to None (0).
        order_by: Field and direction to order by (e.g., "createdAt|DESC"). Defaults to None.
        job_type: Filter by job type (e.g., JobTypeEnum.SYNC, JobTypeEnum.REFRESH).
            If not specified, defaults to sync and reset jobs only (API default behavior).

    Returns:
        A list of JobResponse objects.
    """
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        api_root=api_root,
    )
    response: api.ListJobsResponse = airbyte_instance.jobs.list_jobs(
        api.ListJobsRequest(
            workspace_ids=[workspace_id],
            connection_id=connection_id,
            limit=limit,
            offset=offset,
            order_by=order_by,
            job_type=job_type,
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
            "request_url": response.raw_response.url,
            "status_code": response.status_code,
        },
    )


def get_job_info(
    job_id: int,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.JobResponse:
    """Get a job."""
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
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
        context={
            "request_url": response.raw_response.url,
            "status_code": response.status_code,
        },
    )


# Create, get, and delete sources


def create_source(
    name: str,
    *,
    workspace_id: str,
    config: models.SourceConfiguration | dict[str, Any],
    definition_id: str | None = None,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.SourceResponse:
    """Create a source connector instance.

    Either `definition_id` or `config[sourceType]` must be provided.
    """
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
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
        context={
            "request_url": response.raw_response.url,
            "status_code": response.status_code,
        },
        response=response,
    )


def get_source(
    source_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.SourceResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
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
        context={
            "request_url": response.raw_response.url,
            "status_code": response.status_code,
        },
    )


def delete_source(
    source_id: str,
    *,
    source_name: str | None = None,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    workspace_id: str | None = None,
    safe_mode: bool = True,
) -> None:
    """Delete a source.

    Args:
        source_id: The source ID to delete
        source_name: Optional source name. If not provided and safe_mode is enabled,
            the source name will be fetched from the API to perform safety checks.
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).
        workspace_id: The workspace ID (not currently used)
        safe_mode: If True, requires the source name to contain "delete-me" or "deleteme"
            (case insensitive) to prevent accidental deletion. Defaults to True.

    Raises:
        PyAirbyteInputError: If safe_mode is True and the source name does not meet
            the safety requirements.
    """
    _ = workspace_id  # Not used (yet)

    if safe_mode:
        if source_name is None:
            source_info = get_source(
                source_id=source_id,
                api_root=api_root,
                client_id=client_id,
                client_secret=client_secret,
                bearer_token=bearer_token,
            )
            source_name = source_info.name

        if not _is_safe_name_to_delete(source_name):
            raise PyAirbyteInputError(
                message=(
                    f"Cannot delete source '{source_name}' with safe_mode enabled. "
                    "To authorize deletion, the source name must contain 'delete-me' or 'deleteme' "
                    "(case insensitive).\n\n"
                    "Please rename the source to meet this requirement before attempting deletion."
                ),
                context={
                    "source_id": source_id,
                    "source_name": source_name,
                    "safe_mode": True,
                },
            )

    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
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
                "request_url": response.raw_response.url,
                "status_code": response.status_code,
            },
        )


def patch_source(
    source_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    name: str | None = None,
    config: models.SourceConfiguration | dict[str, Any] | None = None,
) -> models.SourceResponse:
    """Update/patch a source configuration.

    This is a destructive operation that can break existing connections if the
    configuration is changed incorrectly.

    Args:
        source_id: The ID of the source to update
        api_root: The API root URL
        client_id: Client ID for authentication
        client_secret: Client secret for authentication
        bearer_token: Bearer token for authentication (alternative to client credentials).
        name: Optional new name for the source
        config: Optional new configuration for the source

    Returns:
        Updated SourceResponse object
    """
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        api_root=api_root,
    )
    response = airbyte_instance.sources.patch_source(
        api.PatchSourceRequest(
            source_id=source_id,
            source_patch_request=models.SourcePatchRequest(
                name=name,
                configuration=config,
            ),
        ),
    )
    if status_ok(response.status_code) and response.source_response:
        return response.source_response

    raise AirbyteError(
        message="Could not update source.",
        context={
            "source_id": source_id,
            "request_url": response.raw_response.url,
            "status_code": response.status_code,
        },
        response=response,
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
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.DestinationResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
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
        context={
            "request_url": response.raw_response.url,
            "status_code": response.status_code,
        },
        response=response,
    )


def get_destination(
    destination_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.DestinationResponse:
    """Get a connection."""
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
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
        raw_configuration: dict[str, Any] | None = raw_response.get("configuration")

        destination_type = raw_response.get("destinationType")
        destination_mapping = {
            "snowflake": models.DestinationSnowflake,
            "bigquery": models.DestinationBigquery,
            "postgres": models.DestinationPostgres,
            "duckdb": models.DestinationDuckdb,
        }

        if destination_type in destination_mapping and raw_configuration is not None:
            response.destination_response.configuration = destination_mapping[
                destination_type  # pyrefly: ignore[index-error]
            ](**raw_configuration)
        return response.destination_response

    raise AirbyteMissingResourceError(
        resource_name_or_id=destination_id,
        resource_type="destination",
        log_text=response.raw_response.text,
        context={
            "request_url": response.raw_response.url,
            "status_code": response.status_code,
        },
    )


def delete_destination(
    destination_id: str,
    *,
    destination_name: str | None = None,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    workspace_id: str | None = None,
    safe_mode: bool = True,
) -> None:
    """Delete a destination.

    Args:
        destination_id: The destination ID to delete
        destination_name: Optional destination name. If not provided and safe_mode is enabled,
            the destination name will be fetched from the API to perform safety checks.
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).
        workspace_id: The workspace ID (not currently used)
        safe_mode: If True, requires the destination name to contain "delete-me" or "deleteme"
            (case insensitive) to prevent accidental deletion. Defaults to True.

    Raises:
        PyAirbyteInputError: If safe_mode is True and the destination name does not meet
            the safety requirements.
    """
    _ = workspace_id  # Not used (yet)

    if safe_mode:
        if destination_name is None:
            destination_info = get_destination(
                destination_id=destination_id,
                api_root=api_root,
                client_id=client_id,
                client_secret=client_secret,
                bearer_token=bearer_token,
            )
            destination_name = destination_info.name

        if not _is_safe_name_to_delete(destination_name):
            raise PyAirbyteInputError(
                message=(
                    f"Cannot delete destination '{destination_name}' with safe_mode enabled. "
                    "To authorize deletion, the destination name must contain 'delete-me' or "
                    "'deleteme' (case insensitive).\n\n"
                    "Please rename the destination to meet this requirement "
                    "before attempting deletion."
                ),
                context={
                    "destination_id": destination_id,
                    "destination_name": destination_name,
                    "safe_mode": True,
                },
            )

    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
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
                "request_url": response.raw_response.url,
                "status_code": response.status_code,
            },
        )


def patch_destination(
    destination_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    name: str | None = None,
    config: DestinationConfiguration | dict[str, Any] | None = None,
) -> models.DestinationResponse:
    """Update/patch a destination configuration.

    This is a destructive operation that can break existing connections if the
    configuration is changed incorrectly.

    Args:
        destination_id: The ID of the destination to update
        api_root: The API root URL
        client_id: Client ID for authentication
        client_secret: Client secret for authentication
        bearer_token: Bearer token for authentication (alternative to client credentials).
        name: Optional new name for the destination
        config: Optional new configuration for the destination

    Returns:
        Updated DestinationResponse object
    """
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        api_root=api_root,
    )
    response = airbyte_instance.destinations.patch_destination(
        api.PatchDestinationRequest(
            destination_id=destination_id,
            destination_patch_request=models.DestinationPatchRequest(
                name=name,
                configuration=config,
            ),
        ),
    )
    if status_ok(response.status_code) and response.destination_response:
        return response.destination_response

    raise AirbyteError(
        message="Could not update destination.",
        context={
            "destination_id": destination_id,
            "request_url": response.raw_response.url,
            "status_code": response.status_code,
        },
        response=response,
    )


# Create and delete connections


def build_stream_configurations(
    stream_names: list[str],
) -> models.StreamConfigurations:
    """Build a StreamConfigurations object from a list of stream names.

    This helper creates the proper API model structure for stream configurations.
    Used by both connection creation and updates.

    Args:
        stream_names: List of stream names to include in the configuration

    Returns:
        StreamConfigurations object ready for API submission
    """
    stream_configurations = [
        models.StreamConfiguration(name=stream_name) for stream_name in stream_names
    ]
    return models.StreamConfigurations(streams=stream_configurations)


def create_connection(  # noqa: PLR0913  # Too many arguments
    name: str,
    *,
    source_id: str,
    destination_id: str,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    workspace_id: str | None = None,
    prefix: str,
    selected_stream_names: list[str],
) -> models.ConnectionResponse:
    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        api_root=api_root,
    )
    stream_configurations_obj = build_stream_configurations(selected_stream_names)
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
                "request_url": response.raw_response.url,
                "status_code": response.status_code,
            },
        )

    return response.connection_response


def get_connection_by_name(
    workspace_id: str,
    connection_name: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.ConnectionResponse:
    """Get a connection."""
    connections = list_connections(
        workspace_id=workspace_id,
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
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


def _is_safe_name_to_delete(name: str) -> bool:
    """Check if a name is safe to delete.

    Requires the name to contain either "delete-me" or "deleteme" (case insensitive).
    """
    name_lower = name.lower()
    return any(
        {
            "delete-me" in name_lower,
            "deleteme" in name_lower,
        }
    )


def delete_connection(
    connection_id: str,
    connection_name: str | None = None,
    *,
    api_root: str,
    workspace_id: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    safe_mode: bool = True,
) -> None:
    """Delete a connection.

    Args:
        connection_id: The connection ID to delete
        connection_name: Optional connection name. If not provided and safe_mode is enabled,
            the connection name will be fetched from the API to perform safety checks.
        api_root: The API root URL
        workspace_id: The workspace ID
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).
        safe_mode: If True, requires the connection name to contain "delete-me" or "deleteme"
            (case insensitive) to prevent accidental deletion. Defaults to True.

    Raises:
        PyAirbyteInputError: If safe_mode is True and the connection name does not meet
            the safety requirements.
    """
    if safe_mode:
        if connection_name is None:
            connection_info = get_connection(
                workspace_id=workspace_id,
                connection_id=connection_id,
                api_root=api_root,
                client_id=client_id,
                client_secret=client_secret,
                bearer_token=bearer_token,
            )
            connection_name = connection_info.name

        if not _is_safe_name_to_delete(connection_name):
            raise PyAirbyteInputError(
                message=(
                    f"Cannot delete connection '{connection_name}' with safe_mode enabled. "
                    "To authorize deletion, the connection name must contain 'delete-me' or "
                    "'deleteme' (case insensitive).\n\n"
                    "Please rename the connection to meet this requirement "
                    "before attempting deletion."
                ),
                context={
                    "connection_id": connection_id,
                    "connection_name": connection_name,
                    "safe_mode": True,
                },
            )

    _ = workspace_id  # Not used (yet)
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
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
                "request_url": response.raw_response.url,
                "status_code": response.status_code,
            },
        )


def patch_connection(  # noqa: PLR0913  # Too many arguments
    connection_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    name: str | None = None,
    configurations: models.StreamConfigurationsInput | None = None,
    schedule: models.AirbyteAPIConnectionSchedule | None = None,
    prefix: str | None = None,
    status: models.ConnectionStatusEnum | None = None,
) -> models.ConnectionResponse:
    """Update/patch a connection configuration.

    This is a destructive operation that can break existing connections if the
    configuration is changed incorrectly.

    Args:
        connection_id: The ID of the connection to update
        api_root: The API root URL
        client_id: Client ID for authentication
        client_secret: Client secret for authentication
        bearer_token: Bearer token for authentication (alternative to client credentials).
        name: Optional new name for the connection
        configurations: Optional new stream configurations
        schedule: Optional new sync schedule
        prefix: Optional new table prefix
        status: Optional new connection status

    Returns:
        Updated ConnectionResponse object
    """
    airbyte_instance = get_airbyte_server_instance(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        api_root=api_root,
    )
    response = airbyte_instance.connections.patch_connection(
        api.PatchConnectionRequest(
            connection_id=connection_id,
            connection_patch_request=models.ConnectionPatchRequest(
                name=name,
                configurations=configurations,
                schedule=schedule,
                prefix=prefix,
                status=status,
            ),
        ),
    )
    if status_ok(response.status_code) and response.connection_response:
        return response.connection_response

    raise AirbyteError(
        message="Could not update connection.",
        context={
            "connection_id": connection_id,
            "request_url": response.raw_response.url,
            "status_code": response.status_code,
        },
        response=response,
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
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> dict[str, Any]:
    config_api_root = get_config_api_root(api_root)

    # Use provided bearer token or generate one from client credentials
    if bearer_token is None:
        if client_id is None or client_secret is None:
            raise PyAirbyteInputError(
                message="No authentication credentials provided.",
                guidance="Provide either client_id and client_secret, or bearer_token.",
            )
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
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
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
        bearer_token=bearer_token,
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
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.DeclarativeSourceDefinitionResponse:
    """Create a custom YAML source definition."""
    airbyte_instance = get_airbyte_server_instance(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
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
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> list[models.DeclarativeSourceDefinitionResponse]:
    """List all custom YAML source definitions in a workspace."""
    airbyte_instance = get_airbyte_server_instance(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
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
                "request_url": response.raw_response.url,
                "status_code": response.status_code,
            },
        )
    return response.declarative_source_definitions_response.data


def get_custom_yaml_source_definition(
    workspace_id: str,
    definition_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.DeclarativeSourceDefinitionResponse:
    """Get a specific custom YAML source definition."""
    airbyte_instance = get_airbyte_server_instance(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
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
                "request_url": response.raw_response.url,
                "status_code": response.status_code,
            },
        )
    return response.declarative_source_definition_response


def update_custom_yaml_source_definition(
    workspace_id: str,
    definition_id: str,
    *,
    manifest: dict[str, Any],
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.DeclarativeSourceDefinitionResponse:
    """Update a custom YAML source definition."""
    airbyte_instance = get_airbyte_server_instance(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
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
                "request_url": response.raw_response.url,
                "status_code": response.status_code,
            },
        )
    return response.declarative_source_definition_response


def delete_custom_yaml_source_definition(
    workspace_id: str,
    definition_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    safe_mode: bool = True,
) -> None:
    """Delete a custom YAML source definition.

    Args:
        workspace_id: The workspace ID
        definition_id: The definition ID to delete
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).
        safe_mode: If True, requires the connector name to contain "delete-me" or "deleteme"
            (case insensitive) to prevent accidental deletion. Defaults to True.

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
            bearer_token=bearer_token,
        )
        connector_name = definition_info.name

        if not _is_safe_name_to_delete(definition_info.name):
            raise PyAirbyteInputError(
                message=(
                    f"Cannot delete custom connector definition '{connector_name}' "
                    "with safe_mode enabled. "
                    "To authorize deletion, the connector name must contain 'delete-me' or "
                    "'deleteme' (case insensitive).\n\n"
                    "Please rename the connector to meet this requirement "
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
        bearer_token=bearer_token,
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
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
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
        bearer_token: Bearer token for authentication (alternative to client credentials).

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
        bearer_token=bearer_token,
    )
    return json_result.get("builderProjectId")


def update_connector_builder_project_testing_values(
    *,
    workspace_id: str,
    builder_project_id: str,
    testing_values: dict[str, Any],
    spec: dict[str, Any],
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> dict[str, Any]:
    """Update the testing values for a connector builder project.

    This call replaces the entire testing values object stored for the project.
    Any keys not included in `testing_values` will be removed.

    Uses the Config API endpoint:
    /v1/connector_builder_projects/update_testing_values

    Args:
        workspace_id: The workspace ID
        builder_project_id: The connector builder project ID
        testing_values: The testing values (config blob) to persist. This replaces
            any existing testing values entirely.
        spec: The source definition specification (connector spec)
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).

    Returns:
        The updated testing values from the API response
    """
    return _make_config_api_request(
        path="/connector_builder_projects/update_testing_values",
        json={
            "workspaceId": workspace_id,
            "builderProjectId": builder_project_id,
            "testingValues": testing_values,
            "spec": spec,
        },
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )


# Organization and workspace listing


def list_organizations_for_user(
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> list[models.OrganizationResponse]:
    """List all organizations accessible to the current user.

    Uses the public API endpoint: GET /organizations

    Args:
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).

    Returns:
        List of OrganizationResponse objects containing organization_id, organization_name, email
    """
    airbyte_instance = get_airbyte_server_instance(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )
    response = airbyte_instance.organizations.list_organizations_for_user()

    if status_ok(response.status_code) and response.organizations_response:
        return response.organizations_response.data

    raise AirbyteError(
        message="Failed to list organizations for user.",
        context={
            "request_url": response.raw_response.url,
            "status_code": response.status_code,
        },
    )


def list_workspaces_in_organization(
    organization_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    name_contains: str | None = None,
    max_items_limit: int | None = None,
) -> list[dict[str, Any]]:
    """List workspaces within a specific organization.

    Uses the Config API endpoint: POST /v1/workspaces/list_by_organization_id

    Args:
        organization_id: The organization ID to list workspaces for
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).
        name_contains: Optional substring filter for workspace names (server-side)
        max_items_limit: Optional maximum number of workspaces to return

    Returns:
        List of workspace dictionaries containing workspaceId, organizationId, name, slug, etc.
    """
    result: list[dict[str, Any]] = []
    page_size = 100

    # Build base payload
    payload: dict[str, Any] = {
        "organizationId": organization_id,
        "pagination": {
            "pageSize": page_size,
            "rowOffset": 0,
        },
    }
    if name_contains:
        payload["nameContains"] = name_contains

    # Fetch pages until we have all results or reach the limit
    while True:
        json_result = _make_config_api_request(
            path="/workspaces/list_by_organization_id",
            json=payload,
            api_root=api_root,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
        )

        workspaces = json_result.get("workspaces", [])

        # If no results returned, we've exhausted all pages
        if not workspaces:
            break

        result.extend(workspaces)

        # Check if we've reached the limit
        if max_items_limit is not None and len(result) >= max_items_limit:
            return result[:max_items_limit]

        # If we got fewer results than page_size, this was the last page
        if len(workspaces) < page_size:
            break

        # Bump offset for next iteration
        payload["pagination"]["rowOffset"] += page_size

    return result


def get_workspace_organization_info(
    workspace_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> dict[str, Any]:
    """Get organization info for a workspace.

    Uses the Config API endpoint: POST /v1/workspaces/get_organization_info

    This is an efficient O(1) lookup that directly retrieves the organization
    info for a workspace without needing to iterate through all organizations.

    Args:
        workspace_id: The workspace ID to look up
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).

    Returns:
        Dictionary containing organization info:
        - organizationId: The organization ID
        - organizationName: The organization name
        - sso: Whether SSO is enabled
        - billing: Billing information (optional)
    """
    return _make_config_api_request(
        path="/workspaces/get_organization_info",
        json={"workspaceId": workspace_id},
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )


def get_connection_state(
    connection_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> dict[str, Any]:
    """Get the state for a connection.

    Uses the Config API endpoint: POST /v1/state/get

    Args:
        connection_id: The connection ID to get state for
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).

    Returns:
        Dictionary containing the connection state.
    """
    return _make_config_api_request(
        path="/state/get",
        json={"connectionId": connection_id},
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )


def replace_connection_state(
    connection_id: str,
    connection_state_dict: dict[str, Any],
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> dict[str, Any]:
    """Replace the state for a connection.

    Uses the Config API endpoint: POST /v1/state/create_or_update_safe

    Returns HTTP 423 if a sync is currently running, preventing state
    corruption from concurrent modifications.

    Important: This endpoint replaces the ENTIRE connection state. It does not
    perform per-stream deduplication or merging on the backend. Callers that need
    to update a single stream must first fetch the current state, merge the desired
    stream change into the full state object, and then send the complete state back.
    See ``CloudConnection.set_stream_state()`` for this fetch-modify-push pattern.

    The provided ``connection_id`` is injected into both the outer request
    wrapper and the inner ``connection_state`` payload to ensure consistency.

    Args:
        connection_id: The connection ID to update state for.
        connection_state_dict: The full ConnectionState dict to set. Must include:
            - stateType: "global", "stream", or "legacy"
            - One of: state (legacy), streamState (stream), globalState (global)
            All streams must be included; any stream omitted will have its state dropped.
        api_root: The API root URL.
        client_id: OAuth client ID.
        client_secret: OAuth client secret.
        bearer_token: Bearer token for authentication (alternative to client credentials).

    Returns:
        Dictionary containing the updated ConnectionState object.
    """
    try:
        return _make_config_api_request(
            path="/state/create_or_update_safe",
            json={
                "connectionId": connection_id,
                "connectionState": {
                    **connection_state_dict,
                    "connectionId": connection_id,
                },
            },
            api_root=api_root,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
        )
    except AirbyteError as ex:
        if ex.context and ex.context.get("status_code") == HTTPStatus.LOCKED:
            raise AirbyteConnectionSyncActiveError(
                message="Cannot update connection state while a sync is running.",
                connection_id=connection_id,
                guidance="Wait for the current sync to complete before updating state.",
            ) from ex
        raise


def get_connection_catalog(
    connection_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> dict[str, Any]:
    """Get the configured catalog for a connection.

    Uses the Config API endpoint: POST /v1/web_backend/connections/get

    This returns the full connection info including the syncCatalog field,
    which contains the configured catalog with full stream schemas.

    Args:
        connection_id: The connection ID to get catalog for
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).

    Returns:
        Dictionary containing the connection info with syncCatalog.
    """
    return _make_config_api_request(
        path="/web_backend/connections/get",
        json={"connectionId": connection_id, "withRefreshedCatalog": False},
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )


def replace_connection_catalog(
    connection_id: str,
    configured_catalog_dict: dict[str, Any],
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> dict[str, Any]:
    """Replace the configured catalog for a connection.

    Uses the Config API endpoint: POST /v1/web_backend/connections/update

    This is a patch-style update that replaces the connection's entire syncCatalog
    with the provided catalog. All other connection settings remain unchanged.

    Args:
        connection_id: The connection ID to update catalog for.
        configured_catalog_dict: The configured catalog dict (``{"streams": [...]}``) to set.
        api_root: The API root URL.
        client_id: OAuth client ID.
        client_secret: OAuth client secret.
        bearer_token: Bearer token for authentication (alternative to client credentials).

    Returns:
        Dictionary containing the updated WebBackendConnectionRead response.
    """
    return _make_config_api_request(
        path="/web_backend/connections/update",
        json={
            "connectionId": connection_id,
            "syncCatalog": configured_catalog_dict,
            # Resets are destructive and cause customer-side data outage.
            # If a reset is desired, caller will need to decide & manage.
            "skipReset": True,
        },
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )


def get_organization_info(
    organization_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> dict[str, Any]:
    """Get organization info including billing status.

    Uses the Config API endpoint: POST /v1/organizations/get_organization_info

    Args:
        organization_id: The organization ID to look up
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).

    Returns:
        Dictionary containing organization info:
        - organizationId: The organization ID
        - organizationName: The organization name
        - sso: Whether SSO is enabled
        - billing: Billing information (optional, contains paymentStatus, subscriptionStatus, etc.)
    """
    return _make_config_api_request(
        path="/organizations/get_organization_info",
        json={"organizationId": organization_id},
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )


# Billing status constants (using tuples for safe `in` checks with unhashable types)
LOCKED_PAYMENT_STATUSES: tuple[str, ...] = ("disabled", "locked")
LOCKED_SUBSCRIPTION_STATUSES: tuple[str, ...] = ("unsubscribed",)


def is_account_locked(
    payment_status: str | None,
    subscription_status: str | None,
) -> bool:
    """Determine if an account is locked based on billing status.

    An account is considered locked if either:
    - payment_status is 'disabled' or 'locked'
    - subscription_status is 'unsubscribed'

    Returns False if billing info is unavailable (both statuses are None),
    as we default to assuming the account is not locked unless we have
    affirmative evidence of a locked state.

    Args:
        payment_status: Payment status string (e.g., 'okay', 'disabled', 'locked')
        subscription_status: Subscription status string (e.g., 'subscribed', 'unsubscribed')

    Returns:
        True if the account is locked, False otherwise.
    """
    return (payment_status in LOCKED_PAYMENT_STATUSES) or (
        subscription_status in LOCKED_SUBSCRIPTION_STATUSES
    )
