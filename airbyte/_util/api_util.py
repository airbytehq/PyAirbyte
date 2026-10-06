# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""These internal functions are used to interact with the Airbyte API (module named `airbyte`).

In order to insulate users from breaking changes and to avoid general confusion around naming
and design inconsistencies, we do not expose these functions or other Airbyte API classes within
PyAirbyte. Classes and functions from the Airbyte API external library should always be wrapped in
PyAirbyte classes - unless there's a very compelling reason to surface these models intentionally.

Similarly, modules outside of this file should try to avoid interfacing with
`airbyte_server_models` directly. This will ensure a single source of truth when mapping
between the `airbyte` and `airbyte_server_models` libraries.
"""

from __future__ import annotations

import base64
import json
import logging
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast, overload

import requests
from airbyte_server_models import public_api as models

# The Config API models live in an underscore-prefixed module on purpose: the Config API is
# Airbyte-internal and may change without notice, so the private name is a deliberate signal
# to consumers. PyAirbyte accepts that contract, hence the `PLC2701` (import-private-name)
# suppressions below.
from airbyte_server_models._config_api import (
    AirbyteCatalog,  # noqa: PLC2701
    BuilderProjectForDefinitionRequestBody,  # noqa: PLC2701
    BuilderProjectForDefinitionResponse,  # noqa: PLC2701
    CheckConnectionRead,  # noqa: PLC2701
    ConnectionIdRequestBody,  # noqa: PLC2701
    ConnectionState,  # noqa: PLC2701
    ConnectionStateCreateOrUpdate,  # noqa: PLC2701
    ConnectorBuilderProjectIdWithWorkspaceId,  # noqa: PLC2701
    ConnectorBuilderProjectRead,  # noqa: PLC2701
    ConnectorBuilderProjectTestingValues,  # noqa: PLC2701
    ConnectorBuilderProjectTestingValuesUpdate,  # noqa: PLC2701
    DestinationIdRequestBody,  # noqa: PLC2701
    ListOrganizationsByUserRequestBody,  # noqa: PLC2701
    ListWorkspacesInOrganizationRequestBody,  # noqa: PLC2701
    OrganizationIdRequestBody,  # noqa: PLC2701
    OrganizationInfoRead,  # noqa: PLC2701
    OrganizationRead,
    OrganizationReadList,  # noqa: PLC2701
    Pagination,  # noqa: PLC2701
    PermissionRead,
    PermissionReadList,  # noqa: PLC2701
    SourceDefinitionSpecification,  # noqa: PLC2701
    SourceIdRequestBody,  # noqa: PLC2701
    UserAuthIdRequestBody,  # noqa: PLC2701
    UserIdRequestBody,  # noqa: PLC2701
    UserRead,  # noqa: PLC2701
    WebBackendConnectionRead,  # noqa: PLC2701
    WebBackendConnectionRequestBody,  # noqa: PLC2701
    WebBackendConnectionUpdate,  # noqa: PLC2701
    WorkspaceIdRequestBody,  # noqa: PLC2701
    WorkspaceRead,
    WorkspaceReadList,  # noqa: PLC2701
)
from pydantic import BaseModel, ValidationError

from airbyte._util.meta import AIRBYTE_ANALYTIC_SOURCE_HEADER, get_cloud_api_analytic_source
from airbyte.constants import CLOUD_API_ROOT, CLOUD_CONFIG_API_ROOT, CLOUD_CONFIG_API_ROOT_ENV_VAR
from airbyte.exceptions import (
    AirbyteCloudError,
    AirbyteConnectionSyncActiveError,
    AirbyteConnectionSyncError,
    AirbyteDeferredSetupError,
    AirbyteLibInputError,
    AirbyteMissingResourceError,
    AirbyteMultipleResourcesError,
    AirbyteWorkspaceNotEmptyError,
)
from airbyte.registry import ConnectorType
from airbyte.secrets.base import SecretString
from airbyte.secrets.util import try_get_secret


if TYPE_CHECKING:
    from collections.abc import Callable


logger = logging.getLogger(__name__)


JOB_WAIT_INTERVAL_SECS = 2.0
JOB_WAIT_TIMEOUT_SECS_DEFAULT = 60 * 60  # 1 hour
PAGE_SIZE = 100
JWT_PART_COUNT = 3
_T = TypeVar("_T", bound=BaseModel)
FORBIDDEN_RESOURCE_GUIDANCE = (
    "Airbyte Cloud returns 403 both for IDs that don't exist and for resources outside your "
    "access. Check the ID, and that it belongs to the workspace you passed."
)

# Job ordering constants for list_jobs API
JOB_ORDER_BY_CREATED_AT_DESC = "createdAt|DESC"
JOB_ORDER_BY_CREATED_AT_ASC = "createdAt|ASC"

DEFERRED_CREATE_TIMEOUT_SECS: tuple[float, float] = (5.0, 120.0)
"""Connect and read timeouts for a deferred-credential create on the Config API."""


def status_ok(status_code: int) -> bool:
    """Check if a status code is OK."""
    return status_code >= 200 and status_code < 300  # noqa: PLR2004  # allow inline magic numbers


def _validate_pagination_params(
    *,
    limit: int | None,
) -> None:
    """Validate common pagination parameters."""
    if limit is not None and limit <= 0:
        raise AirbyteLibInputError(message="`limit` must be greater than 0.")


def _get_page_limit(remaining: int | None) -> int:
    """Get the next API page limit from the remaining item count."""
    if remaining is None:
        return PAGE_SIZE
    return min(remaining, PAGE_SIZE)


def _get_api_error_context(response: requests.Response) -> dict[str, Any]:
    """Extract request and response details from a failed API call for debugging.

    Produces the same context keys the Speakeasy SDK error wrapper used to surface.
    """
    context: dict[str, Any] = {
        "status_code": response.status_code,
        "error_message": response.text,
    }
    if response.request is not None:
        context["request_url"] = str(response.request.url)
        context["request_method"] = response.request.method
    context["response_content_type"] = response.headers.get("content-type")
    return context


def _wrap_api_error(
    response: requests.Response, base_context: dict[str, Any] | None = None
) -> AirbyteCloudError:
    """Wrap a failed API response with additional context for debugging.

    Mirrors the former SDK error-wrapping semantics: 403 and 404 become
    `AirbyteMissingResourceError` (403 with extra guidance), everything else
    becomes `AirbyteCloudError`.
    """
    api_context = _get_api_error_context(response)
    merged_context = {**(base_context or {}), **api_context}
    status_code = api_context.get("status_code")
    is_forbidden = status_code == HTTPStatus.FORBIDDEN
    error_type = (
        AirbyteMissingResourceError
        if is_forbidden or status_code == HTTPStatus.NOT_FOUND
        else AirbyteCloudError
    )
    return error_type(
        message=(
            "The requested resource was not found, or these credentials can't access it "
            "(HTTP 403)."
            if is_forbidden
            else f"API error occurred: {response.text}"
        ),
        guidance=FORBIDDEN_RESOURCE_GUIDANCE if is_forbidden else None,
        context=merged_context,
    )


def _infer_config_api_root(api_root: str) -> str | None:
    """Infer the configuration API root from a public API root."""
    normalized_api_root = api_root.rstrip("/")
    public_api_suffix = "/api/public/v1"
    if normalized_api_root.endswith(public_api_suffix):
        return normalized_api_root[: -len(public_api_suffix)] + "/api/v1"

    return None


def get_config_api_root(
    api_root: str,
    *,
    config_api_root: str | None = None,
) -> str:
    """Get the configuration API root from the public API root.

    Resolution order:
    1. If `config_api_root` is provided, use that value.
    2. If `AIRBYTE_CLOUD_CONFIG_API_URL` environment variable is set, use that value.
    3. If `api_root` matches the default Cloud API root, return the default Config API root.
    4. If `api_root` looks like a self-managed public API root, infer the Config API root.
    5. Otherwise, raise NotImplementedError (cannot derive Config API from custom API root).

    Args:
        api_root: The public API root URL being used.
        config_api_root: Optional explicit Config API root URL.

    Returns:
        The configuration API root URL.

    Raises:
        NotImplementedError: If the Config API root cannot be determined.
    """
    if config_api_root:
        return config_api_root.rstrip("/")

    # Next, check if the Config API URL is explicitly set via environment variable
    config_api_override = try_get_secret(CLOUD_CONFIG_API_ROOT_ENV_VAR, default=None)
    if config_api_override:
        return str(config_api_override).rstrip("/")

    # Fall back to deriving from the main API root
    # Normalize URLs by stripping trailing slashes to handle common variants
    if api_root.rstrip("/") == CLOUD_API_ROOT.rstrip("/"):
        return CLOUD_CONFIG_API_ROOT.rstrip("/")

    inferred_config_api_root = _infer_config_api_root(api_root)
    if inferred_config_api_root:
        return inferred_config_api_root

    raise NotImplementedError(
        f"Configuration API root not implemented for api_root='{api_root}'. "
        "Provide the 'config_api_root' argument or set the "
        f"'{CLOUD_CONFIG_API_ROOT_ENV_VAR}' environment variable to specify the Config API URL."
    )


def get_web_url_root(api_root: str) -> str:
    """Get the web URL root from the main API root.

    Self-managed public API roots (`<airbyteUrl>/api/public/v1`) resolve to `<airbyteUrl>`.
    Other custom API roots are returned unchanged.
    """
    normalized_api_root = api_root.rstrip("/")
    if normalized_api_root == CLOUD_API_ROOT.rstrip("/"):
        return "https://cloud.airbyte.com"

    public_api_suffix = "/api/public/v1"
    if normalized_api_root.endswith(public_api_suffix):
        return normalized_api_root[: -len(public_api_suffix)]

    return api_root


def _resolve_bearer_token(
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    timeout: tuple[float, float] | None = None,
) -> SecretString:
    """Resolve the bearer token to use, minting one from client credentials if needed.

    Supports two authentication methods (mutually exclusive):
    1. OAuth2 client credentials (client_id + client_secret)
    2. Bearer token authentication
    """
    # Guard: must provide either bearer token OR both client credentials
    if bearer_token is None and (client_id is None or client_secret is None):
        raise AirbyteLibInputError(
            message="No authentication credentials provided.",
            guidance="Provide either client_id and client_secret, or bearer_token.",
        )

    # Guard: cannot provide both auth methods
    if bearer_token is not None and (client_id is not None or client_secret is not None):
        raise AirbyteLibInputError(
            message="Cannot use both client credentials and bearer token authentication.",
            guidance="Provide either client_id and client_secret, or bearer_token, but not both.",
        )

    if bearer_token is None:
        # Client credentials flow (guaranteed non-None by first guard)
        bearer_token = get_bearer_token(
            client_id=cast(SecretString, client_id),
            client_secret=cast(SecretString, client_secret),
            api_root=api_root,
            timeout=timeout,
        )
    return bearer_token


def _api_headers(
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> dict[str, str]:
    """Build Public API headers, minting a bearer token from client credentials if needed."""
    resolved_token = _resolve_bearer_token(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {resolved_token}",
        "User-Agent": "PyAirbyte Client",
        AIRBYTE_ANALYTIC_SOURCE_HEADER: get_cloud_api_analytic_source(),
    }


@overload
def _make_public_api_request(  # Mirrors the API surface.
    *,
    method: Literal["GET", "POST", "PATCH", "PUT", "DELETE"],
    api_root: str,
    path: str,
    response_model: type[_T],
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    request: BaseModel | None = None,
    params: dict[str, Any] | None = None,
    error_context: dict[str, Any] | None = None,
) -> _T | None: ...


@overload
def _make_public_api_request(  # Mirrors the API surface.
    *,
    method: Literal["GET", "POST", "PATCH", "PUT", "DELETE"],
    api_root: str,
    path: str,
    response_model: None,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    request: BaseModel | None = None,
    params: dict[str, Any] | None = None,
    error_context: dict[str, Any] | None = None,
) -> dict[str, Any] | None: ...


def _make_public_api_request(  # noqa: PLR0913  # Mirrors the API surface.
    *,
    method: Literal["GET", "POST", "PATCH", "PUT", "DELETE"],
    api_root: str,
    path: str,
    response_model: type[_T] | None,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    request: BaseModel | None = None,
    params: dict[str, Any] | None = None,
    error_context: dict[str, Any] | None = None,
) -> _T | None:
    """Make a Public API request and decode the response into `response_model`.

    Raises `AirbyteMissingResourceError` on 403/404 and `AirbyteCloudError` on
    other non-2xx responses, matching the former Speakeasy SDK error wrapping.
    Returns `None` when the response has no content.
    """
    headers = _api_headers(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )
    full_url = api_root.rstrip("/") + path
    response = requests.request(
        method=method,
        url=full_url,
        headers=headers,
        params=params,
        json=(
            request.model_dump(mode="json", by_alias=True, exclude_none=True)
            if request is not None
            else None
        ),
        # No timeout, matching the former SDK behavior.
    )
    if not status_ok(response.status_code):
        raise _wrap_api_error(response, error_context)

    if response.status_code == HTTPStatus.NO_CONTENT or not response.content:
        return None

    try:
        body = response.json()
    except requests.exceptions.JSONDecodeError as ex:
        raise AirbyteCloudError(
            message=f"Public API response for {path} did not match the expected schema.",
            context={
                **(error_context or {}),
                "full_url": full_url,
                "path": path,
                "response": response.text,
            },
        ) from ex

    if response_model is None:
        return body

    try:
        return response_model.model_validate(body)
    except ValidationError as ex:
        raise AirbyteCloudError(
            message=f"Public API response for {path} did not match the expected schema.",
            context={
                **(error_context or {}),
                "full_url": full_url,
                "path": path,
                "response": response.text,
            },
        ) from ex


def _decode_list_items(
    body: Any,  # noqa: ANN401  # Raw JSON payload
    *,
    item_model: type[_T],
    list_path: str,
) -> list[_T]:
    """Decode each item in a list endpoint's `data` payload individually.

    A single malformed item is skipped with a warning instead of failing the
    whole page, so one bad record can't take down a listing.
    """
    items: list[_T] = []
    for item in (body or {}).get("data") or []:
        try:
            items.append(item_model.model_validate(item))
        except ValidationError:
            item_id = item.get("id") if isinstance(item, dict) else getattr(item, "id", None)
            logger.warning(
                "Skipping %s item that failed schema validation on %s (id=%s).",
                getattr(item_model, "__name__", str(item_model)),
                list_path,
                item_id,
            )
    return items


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
    base_context = {"workspace_id": workspace_id, "api_root": api_root}
    workspace = _make_public_api_request(
        method="GET",
        api_root=api_root,
        path=f"/workspaces/{workspace_id}",
        response_model=models.WorkspaceResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context=base_context,
    )

    if workspace:
        return workspace

    raise AirbyteMissingResourceError(
        resource_type="workspace",
        context=base_context,
    )


def create_workspace(
    *,
    name: str,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    organization_id: str | None = None,
    region_id: str | None = None,
) -> models.WorkspaceResponse:
    """Create a workspace."""
    base_context = {
        "name": name,
        "organization_id": organization_id,
        "region_id": region_id,
        "api_root": api_root,
    }
    workspace = _make_public_api_request(
        method="POST",
        api_root=api_root,
        path="/workspaces",
        request=models.WorkspaceCreateRequest(
            name=name,
            organization_id=organization_id,
            region_id=region_id,
        ),
        response_model=models.WorkspaceResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context=base_context,
    )

    if workspace:
        return workspace

    raise AirbyteCloudError(
        message="Could not create workspace.",
        context=base_context,
    )


def rename_workspace(
    workspace_id: str,
    *,
    name: str,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.WorkspaceResponse:
    """Rename a workspace."""
    base_context = {
        "workspace_id": workspace_id,
        "name": name,
        "api_root": api_root,
    }
    workspace = _make_public_api_request(
        method="PATCH",
        api_root=api_root,
        path=f"/workspaces/{workspace_id}",
        request=models.WorkspaceUpdateRequest(name=name),
        response_model=models.WorkspaceResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context=base_context,
    )

    if workspace:
        return workspace

    raise AirbyteCloudError(
        message="Could not rename workspace.",
        context=base_context,
    )


def permanently_delete_workspace(
    workspace_id: str,
    *,
    workspace_name: str | None = None,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    safe_mode: bool = True,
) -> None:
    """Delete an empty workspace.

    Args:
        workspace_id: The workspace ID to delete.
        workspace_name: Optional workspace name. If not provided and safe mode is enabled,
            the workspace name is fetched from the API for safety checks.
        api_root: The API root URL.
        client_id: OAuth client ID.
        client_secret: OAuth client secret.
        bearer_token: Bearer token for authentication.
        safe_mode: If True, the workspace name must contain `delete-me` or `deleteme`
            (case insensitive). Defaults to True.

    Raises:
        AirbyteLibInputError: If safe mode is True and the workspace name does not meet
            the safety requirements.
        AirbyteWorkspaceNotEmptyError: If the workspace contains connections.
    """
    if safe_mode:
        if workspace_name is None:
            workspace_info = get_workspace(
                workspace_id=workspace_id,
                api_root=api_root,
                client_id=client_id,
                client_secret=client_secret,
                bearer_token=bearer_token,
            )
            workspace_name = workspace_info.name

        if not _is_safe_name_to_delete(workspace_name):
            raise AirbyteLibInputError(
                message=(
                    "Cannot delete workspace with safe_mode enabled because the workspace "
                    "name does not contain 'delete-me' or 'deleteme'."
                ),
                context={
                    "workspace_id": workspace_id,
                    "workspace_name": workspace_name,
                    "safe_mode": True,
                },
            )

    connections = list_connections(
        workspace_id=workspace_id,
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        limit=1,
    )
    if connections:
        raise AirbyteWorkspaceNotEmptyError(
            workspace_id=workspace_id,
            connection_ids=[connection.connection_id for connection in connections],
        )

    _make_public_api_request(
        method="DELETE",
        api_root=api_root,
        path=f"/workspaces/{workspace_id}",
        response_model=None,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={
            "workspace_id": workspace_id,
            "api_root": api_root,
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
    limit: int | None = None,
) -> list[models.ConnectionResponse]:
    """List connections."""
    if name is not None and name_filter:
        raise AirbyteLibInputError(message="You can provide name or name_filter, but not both.")
    _validate_pagination_params(limit=limit)
    name_filter = (lambda n: n == name) if name is not None else name_filter or (lambda _: True)

    result: list[models.ConnectionResponse] = []
    current_offset = 0
    remaining = limit
    while remaining is None or remaining > 0:
        body = _make_public_api_request(
            method="GET",
            api_root=api_root,
            path="/connections",
            params={
                "workspaceIds": [workspace_id],
                "offset": current_offset,
                "limit": PAGE_SIZE,
            },
            response_model=None,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            error_context={"workspace_id": workspace_id, "api_root": api_root},
        )
        page_data = _decode_list_items(
            body, item_model=models.ConnectionResponse, list_path="/connections"
        )
        if not page_data:
            break

        matching_connections = [
            connection for connection in page_data if name_filter(connection.name)
        ]
        page_results = (
            matching_connections if remaining is None else matching_connections[:remaining]
        )
        result += page_results
        if remaining is not None:
            remaining -= len(page_results)

        if not (body or {}).get("next"):
            break

        current_offset += len(page_data)
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
    limit: int | None = None,
) -> list[models.WorkspaceResponse]:
    """List workspaces.

    Args:
        workspace_id: Workspace context for the request.
        api_root: The API root URL.
        client_id: OAuth client ID.
        client_secret: OAuth client secret.
        bearer_token: Bearer token for authentication.
        name: Optional exact workspace name to match.
        name_filter: Optional predicate to match workspace names.
        limit: Optional maximum number of matching workspaces to return.
    """
    if name is not None and name_filter:
        raise AirbyteLibInputError(message="You can provide name or name_filter, but not both.")
    _validate_pagination_params(limit=limit)
    has_name_filter = name is not None or name_filter is not None
    name_filter = (lambda n: n == name) if name is not None else name_filter or (lambda _: True)

    result: list[models.WorkspaceResponse] = []
    current_offset = 0
    remaining = limit
    while remaining is None or remaining > 0:
        page_limit = PAGE_SIZE if has_name_filter else _get_page_limit(remaining)
        body = _make_public_api_request(
            method="GET",
            api_root=api_root,
            path="/workspaces",
            params={
                "offset": current_offset,
                "limit": page_limit,
            },
            response_model=None,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            error_context={"workspace_id": workspace_id, "api_root": api_root},
        )
        page_data = _decode_list_items(
            body, item_model=models.WorkspaceResponse, list_path="/workspaces"
        )
        if not page_data:
            break

        matching_workspaces = [workspace for workspace in page_data if name_filter(workspace.name)]
        page_results = matching_workspaces if remaining is None else matching_workspaces[:remaining]
        result += page_results
        if remaining is not None:
            remaining -= len(page_results)

        if not (body or {}).get("next"):
            break

        current_offset += len(page_data)

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
    limit: int | None = None,
) -> list[models.SourceResponse]:
    """List sources."""
    if name is not None and name_filter:
        raise AirbyteLibInputError(message="You can provide name or name_filter, but not both.")
    _validate_pagination_params(limit=limit)
    name_filter = (lambda n: n == name) if name is not None else name_filter or (lambda _: True)

    result: list[models.SourceResponse] = []
    current_offset = 0
    remaining = limit
    while remaining is None or remaining > 0:
        body = _make_public_api_request(
            method="GET",
            api_root=api_root,
            path="/sources",
            params={
                "workspaceIds": [workspace_id],
                "offset": current_offset,
                "limit": PAGE_SIZE,
            },
            response_model=None,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            error_context={"workspace_id": workspace_id, "api_root": api_root},
        )
        page_data = _decode_list_items(body, item_model=models.SourceResponse, list_path="/sources")
        if not page_data:
            break

        matching_sources = [source for source in page_data if name_filter(source.name)]
        page_results = matching_sources if remaining is None else matching_sources[:remaining]
        result += page_results
        if remaining is not None:
            remaining -= len(page_results)

        if not (body or {}).get("next"):
            break

        current_offset += len(page_data)

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
    limit: int | None = None,
) -> list[models.DestinationResponse]:
    """List destinations."""
    if name is not None and name_filter:
        raise AirbyteLibInputError(message="You can provide name or name_filter, but not both.")
    _validate_pagination_params(limit=limit)
    name_filter = (lambda n: n == name) if name is not None else name_filter or (lambda _: True)

    result: list[models.DestinationResponse] = []
    current_offset = 0
    remaining = limit
    while remaining is None or remaining > 0:
        body = _make_public_api_request(
            method="GET",
            api_root=api_root,
            path="/destinations",
            params={
                "workspaceIds": [workspace_id],
                "offset": current_offset,
                "limit": PAGE_SIZE,
            },
            response_model=None,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            error_context={"workspace_id": workspace_id, "api_root": api_root},
        )
        page_data = _decode_list_items(
            body, item_model=models.DestinationResponse, list_path="/destinations"
        )
        if not page_data:
            break

        matching_destinations = [
            destination for destination in page_data if name_filter(destination.name)
        ]
        page_results = (
            matching_destinations if remaining is None else matching_destinations[:remaining]
        )
        result += page_results
        if remaining is not None:
            remaining -= len(page_results)

        if not (body or {}).get("next"):
            break

        current_offset += len(page_data)

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
    base_context = {
        "workspace_id": workspace_id,
        "connection_id": connection_id,
        "api_root": api_root,
    }
    connection = _make_public_api_request(
        method="GET",
        api_root=api_root,
        path=f"/connections/{connection_id}",
        response_model=models.ConnectionResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context=base_context,
    )

    if connection:
        return connection

    raise AirbyteMissingResourceError(
        resource_name_or_id=connection_id,
        resource_type="connection",
        context=base_context,
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
    try:
        job = _make_public_api_request(
            method="POST",
            api_root=api_root,
            path="/jobs",
            request=models.JobCreateRequest(
                connection_id=connection_id,
                job_type=models.JobTypeEnum.SYNC,
            ),
            response_model=models.JobResponse,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            error_context={"workspace_id": workspace_id, "connection_id": connection_id},
        )
    except AirbyteCloudError as e:
        raise AirbyteConnectionSyncError(
            connection_id=connection_id,
            message=e.message,
            context={"workspace_id": workspace_id, **(e.context or {})},
        ) from e

    if job:
        return job

    raise AirbyteConnectionSyncError(
        connection_id=connection_id,
        context={
            "workspace_id": workspace_id,
        },
    )


# Get job info (logs)


def get_job_logs(  # noqa: PLR0913  # Too many arguments - needed for auth flexibility
    workspace_id: str,
    connection_id: str,
    limit: int | None = 100,
    offset: int | None = None,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    order_by: str | None = None,
    job_type: str | models.JobTypeEnum | None = None,
) -> list[models.JobResponse]:
    """Get a list of jobs for a connection.

    Automatically paginates through multiple API pages when the requested
    `limit` exceeds the per-page size.

    Args:
        workspace_id: The workspace ID.
        connection_id: The connection ID.
        limit: Maximum number of jobs to return. Defaults to 100.
        offset: Number of jobs to skip from the beginning. Defaults to None (0).
        api_root: The API root URL.
        client_id: The client ID for authentication.
        client_secret: The client secret for authentication.
        bearer_token: Bearer token for authentication (alternative to client credentials).
        order_by: Field and direction to order by (e.g., "createdAt|DESC"). Defaults to None.
        job_type: Filter by job type (e.g., `sync`, `refresh`).
            If not specified, defaults to sync and reset jobs only (API default behavior).

    Returns:
        A list of JobResponse objects.
    """
    _validate_pagination_params(limit=limit)
    result: list[models.JobResponse] = []
    current_offset = offset or 0
    remaining = limit
    base_context = {
        "workspace_id": workspace_id,
        "connection_id": connection_id,
        "api_root": api_root,
    }
    if isinstance(job_type, str):
        try:
            job_type_value = models.JobTypeEnum(job_type)
        except ValueError:
            valid_job_types = ", ".join(job_type_enum.value for job_type_enum in models.JobTypeEnum)
            raise AirbyteLibInputError(
                message=f"`job_type` must be one of: {valid_job_types}.",
                input_value=job_type,
            ) from None
    else:
        job_type_value = job_type

    while remaining is None or remaining > 0:
        page_limit = _get_page_limit(remaining)
        body = _make_public_api_request(
            method="GET",
            api_root=api_root,
            path="/jobs",
            params={
                "workspaceIds": [workspace_id],
                "connectionId": connection_id,
                "limit": page_limit,
                "offset": current_offset,
                "orderBy": order_by,
                "jobType": job_type_value,
            },
            response_model=None,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            error_context=base_context,
        )

        page_data = _decode_list_items(body, item_model=models.JobResponse, list_path="/jobs")
        if not page_data:
            break

        result += page_data
        if remaining is not None:
            remaining -= len(page_data)
        current_offset += len(page_data)

        if not (body or {}).get("next") or len(page_data) < page_limit:
            break

    return result if limit is None else result[:limit]


def get_job_info(
    job_id: int,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.JobResponse:
    """Get a job."""
    job = _make_public_api_request(
        method="GET",
        api_root=api_root,
        path=f"/jobs/{job_id}",
        response_model=models.JobResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={"job_id": job_id},
    )

    if job:
        return job

    raise AirbyteMissingResourceError(
        resource_name_or_id=str(job_id),
        resource_type="job",
    )


def cancel_job(
    job_id: int,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.JobResponse:
    """Cancel a running job."""
    job = _make_public_api_request(
        method="DELETE",
        api_root=api_root,
        path=f"/jobs/{job_id}",
        response_model=models.JobResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={"job_id": job_id},
    )
    if job:
        return job

    raise AirbyteCloudError(
        message="Job cancellation response payload was empty.",
        context={"job_id": job_id},
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
    source = _make_public_api_request(
        method="POST",
        api_root=api_root,
        path="/sources",
        request=models.SourceCreateRequest(
            name=name,
            workspace_id=workspace_id,
            configuration=config,
            definition_id=definition_id or None,  # Only used for custom sources
            secret_id=None,  # For OAuth, not yet supported
        ),
        response_model=models.SourceResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )
    if source:
        return source

    raise AirbyteCloudError(
        message="Could not create source.",
    )


def get_source(
    source_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.SourceResponse:
    """Get a source with its raw configuration.

    Secrets in the returned configuration are redacted by the API.
    """
    base_context = {"source_id": source_id, "api_root": api_root}
    body = _make_public_api_request(
        method="GET",
        api_root=api_root,
        path=f"/sources/{source_id}",
        response_model=None,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context=base_context,
    )

    if body:
        source = models.SourceResponse.model_validate(body)
        source.configuration = body.get("configuration") or {}  # pyrefly: ignore[bad-assignment]
        return source

    raise AirbyteMissingResourceError(
        resource_name_or_id=source_id,
        resource_type="source",
        context=base_context,
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
        AirbyteLibInputError: If safe_mode is True and the source name does not meet
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
            raise AirbyteLibInputError(
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

    _make_public_api_request(
        method="DELETE",
        api_root=api_root,
        path=f"/sources/{source_id}",
        response_model=None,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={"source_id": source_id},
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
    source = _make_public_api_request(
        method="PATCH",
        api_root=api_root,
        path=f"/sources/{source_id}",
        request=models.SourcePatchRequest(
            name=name,
            configuration=config,
        ),
        response_model=models.SourceResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={"source_id": source_id},
    )
    if source:
        return source

    raise AirbyteCloudError(
        message="Could not update source.",
        context={
            "source_id": source_id,
        },
    )


# Utility function


def _get_destination_type_str(
    destination: dict[str, Any],
) -> str:
    destination_type = destination.get("destinationType")

    if not destination_type or not isinstance(destination_type, str):
        raise AirbyteLibInputError(
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
    config: dict[str, Any],
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.DestinationResponse:
    """Get a connection."""
    definition_id_override: str | None = None
    if _get_destination_type_str(config) == "dev-null":
        # TODO: We have to hard-code the definition ID for dev-null destination.
        #  https://github.com/airbytehq/PyAirbyte/issues/743
        definition_id_override = "a7bcc9d8-13b3-4e49-b80d-d020b90045e3"
    destination = _make_public_api_request(
        method="POST",
        api_root=api_root,
        path="/destinations",
        request=models.DestinationCreateRequest(
            definition_id=definition_id_override,
            name=name,
            workspace_id=workspace_id,
            configuration=config,
        ),
        response_model=models.DestinationResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )
    if destination:
        return destination

    raise AirbyteCloudError(
        message="Could not create destination.",
    )


def get_destination(
    destination_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.DestinationResponse:
    """Get a destination with its configuration as the raw API dictionary.

    Secrets in the returned configuration are redacted by the API.
    """
    base_context = {"destination_id": destination_id, "api_root": api_root}
    body = _make_public_api_request(
        method="GET",
        api_root=api_root,
        path=f"/destinations/{destination_id}",
        response_model=None,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context=base_context,
    )

    if body:
        destination = models.DestinationResponse.model_validate(body)
        destination.configuration = (
            body.get("configuration") or {}  # pyrefly: ignore[bad-assignment]
        )
        return destination

    raise AirbyteMissingResourceError(
        resource_name_or_id=destination_id,
        resource_type="destination",
        context=base_context,
    )


def get_connector(
    connector_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> tuple[ConnectorType, models.SourceResponse | models.DestinationResponse]:
    """Get a connector of unknown kind, returning its kind with the API response.

    Tries the source endpoint first, then the destination endpoint. The source lookup
    falls through on a 404 or a 403, because the API hides a destination's existence
    from the source endpoint behind a 403. Raises `AirbyteMissingResourceError` when
    both lookups 404; otherwise re-raises the source lookup's error when the
    destination lookup also 403s or 404s. Other failures (401, 5xx) raise as-is.
    """
    try:
        return ConnectorType.SOURCE, get_source(
            source_id=connector_id,
            api_root=api_root,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
        )
    except AirbyteCloudError as error:
        if not _is_not_found_or_forbidden(error):
            raise
        source_error = error

    try:
        return ConnectorType.DESTINATION, get_destination(
            destination_id=connector_id,
            api_root=api_root,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
        )
    except AirbyteCloudError as error:
        if not _is_not_found_or_forbidden(error):
            raise
        if _status_code(source_error) == _status_code(error) == HTTPStatus.NOT_FOUND:
            raise AirbyteMissingResourceError(
                resource_name_or_id=connector_id,
                resource_type="connector",
            ) from error
        raise source_error from error


def _status_code(error: AirbyteCloudError) -> object:
    """Return the HTTP status code recorded in `error`'s context.

    An `AirbyteMissingResourceError` without a recorded status counts as a 404.
    """
    status_code = (error.context or {}).get("status_code")
    if status_code is None and isinstance(error, AirbyteMissingResourceError):
        return HTTPStatus.NOT_FOUND
    return status_code


def _is_not_found_or_forbidden(error: AirbyteCloudError) -> bool:
    """Return whether `error` is a 404 or 403, which may just mean the other connector kind.

    Checked by status code: `get_source`/`get_destination` raise
    `AirbyteMissingResourceError` for any non-success response, including 5xx.
    """
    return _status_code(error) in {HTTPStatus.NOT_FOUND, HTTPStatus.FORBIDDEN}


def get_source_definition(
    definition_id: str,
    workspace_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.DefinitionResponse:
    """Get a source connector definition, including its `docker_repository` name."""
    definition = _make_public_api_request(
        method="GET",
        api_root=api_root,
        path=f"/workspaces/{workspace_id}/definitions/sources/{definition_id}",
        response_model=models.DefinitionResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={"definition_id": definition_id, "workspace_id": workspace_id},
    )
    if definition:
        return definition

    raise AirbyteMissingResourceError(
        resource_name_or_id=definition_id,
        resource_type="source definition",
    )


def get_destination_definition(
    definition_id: str,
    workspace_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> models.DefinitionResponse:
    """Get a destination connector definition, including its `docker_repository` name."""
    definition = _make_public_api_request(
        method="GET",
        api_root=api_root,
        path=f"/workspaces/{workspace_id}/definitions/destinations/{definition_id}",
        response_model=models.DefinitionResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={"definition_id": definition_id, "workspace_id": workspace_id},
    )
    if definition:
        return definition

    raise AirbyteMissingResourceError(
        resource_name_or_id=definition_id,
        resource_type="destination definition",
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
        AirbyteLibInputError: If safe_mode is True and the destination name does not meet
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
            raise AirbyteLibInputError(
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

    _make_public_api_request(
        method="DELETE",
        api_root=api_root,
        path=f"/destinations/{destination_id}",
        response_model=None,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={"destination_id": destination_id},
    )


def patch_destination(
    destination_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    name: str | None = None,
    config: dict[str, Any] | None = None,
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
    destination = _make_public_api_request(
        method="PATCH",
        api_root=api_root,
        path=f"/destinations/{destination_id}",
        request=models.DestinationPatchRequest(
            name=name,
            configuration=config,
        ),
        response_model=models.DestinationResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={"destination_id": destination_id},
    )
    if destination:
        return destination

    raise AirbyteCloudError(
        message="Could not update destination.",
        context={
            "destination_id": destination_id,
        },
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


def build_connection_schedule(
    schedule_type: str,
    cron_expression: str | None = None,
) -> models.AirbyteApiConnectionSchedule:
    """Build a connection schedule object."""
    return models.AirbyteApiConnectionSchedule(
        schedule_type=models.ScheduleTypeEnum(schedule_type),
        cron_expression=cron_expression,
    )


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
    stream_configurations_obj = build_stream_configurations(selected_stream_names)
    connection = _make_public_api_request(
        method="POST",
        api_root=api_root,
        path="/connections",
        request=models.ConnectionCreateRequest(
            name=name,
            source_id=source_id,
            destination_id=destination_id,
            configurations=stream_configurations_obj,
            prefix=prefix,
        ),
        response_model=models.ConnectionResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={"source_id": source_id, "destination_id": destination_id},
    )
    if connection is None:
        raise AirbyteCloudError(
            context={
                "source_id": source_id,
                "destination_id": destination_id,
            },
        )

    return connection


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
        AirbyteLibInputError: If safe_mode is True and the connection name does not meet
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
            raise AirbyteLibInputError(
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
    _make_public_api_request(
        method="DELETE",
        api_root=api_root,
        path=f"/connections/{connection_id}",
        response_model=None,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={"connection_id": connection_id},
    )


def patch_connection(  # noqa: PLR0913  # Too many arguments
    connection_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    name: str | None = None,
    configurations: models.StreamConfigurations | None = None,
    schedule: models.AirbyteApiConnectionSchedule | None = None,
    prefix: str | None = None,
    status: str | models.ConnectionStatusEnum | None = None,
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
    if isinstance(status, str):
        valid_statuses = ", ".join(
            connection_status.value for connection_status in models.ConnectionStatusEnum
        )
        if status not in {m.value for m in models.ConnectionStatusEnum}:
            raise AirbyteLibInputError(
                message=f"`status` must be one of: {valid_statuses}.",
                input_value=status,
            )
        status_value = models.ConnectionStatusEnum(status)
    else:
        status_value = status

    connection = _make_public_api_request(
        method="PATCH",
        api_root=api_root,
        path=f"/connections/{connection_id}",
        request=models.ConnectionPatchRequest(
            name=name,
            configurations=configurations,
            schedule=schedule,
            prefix=prefix,
            status=status_value,
        ),
        response_model=models.ConnectionResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={"connection_id": connection_id},
    )

    if connection:
        return connection

    raise AirbyteCloudError(
        message="Could not update connection.",
        context={
            "connection_id": connection_id,
        },
    )


# Functions for leveraging the Airbyte Config API (may not be supported or stable)


def get_bearer_token(
    *,
    client_id: SecretString,
    client_secret: SecretString,
    api_root: str = CLOUD_API_ROOT,
    timeout: tuple[float, float] | None = None,
) -> SecretString:
    """Get a bearer token.

    https://reference.airbyte.com/reference/createaccesstoken

    """
    response = requests.post(
        url=api_root + "/applications/token",
        timeout=timeout,
        headers={
            "content-type": "application/json",
            "accept": "application/json",
            AIRBYTE_ANALYTIC_SOURCE_HEADER: get_cloud_api_analytic_source(),
        },
        json={
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    if not status_ok(response.status_code):
        response.raise_for_status()

    return SecretString(response.json()["access_token"])


@overload
def _make_config_api_request(  # Mirrors the API surface.
    *,
    api_root: str,
    path: str,
    request: BaseModel | dict[str, Any],
    response_model: type[_T],
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    config_api_root: str | None = None,
    timeout: tuple[float, float] | None = None,
) -> _T: ...


@overload
def _make_config_api_request(  # Mirrors the API surface.
    *,
    api_root: str,
    path: str,
    request: BaseModel | dict[str, Any],
    response_model: None = None,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    config_api_root: str | None = None,
    timeout: tuple[float, float] | None = None,
) -> dict[str, Any]: ...


def _make_config_api_request(  # noqa: PLR0913  # Mirrors the API surface.
    *,
    api_root: str,
    path: str,
    request: BaseModel | dict[str, Any],
    response_model: type[_T] | None = None,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    config_api_root: str | None = None,
    timeout: tuple[float, float] | None = None,
) -> _T | dict[str, Any]:
    config_api_root = get_config_api_root(api_root, config_api_root=config_api_root)
    headers = _config_api_headers(
        api_root=api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        timeout=timeout,
    )
    full_url = config_api_root + path
    response = requests.request(
        method="POST",
        url=full_url,
        headers=headers,
        json=(
            request.model_dump(mode="json", exclude_none=True)
            if isinstance(request, BaseModel)
            else request
        ),
        timeout=timeout,
    )
    if not status_ok(response.status_code):
        try:
            response.raise_for_status()
        except requests.HTTPError as ex:
            error_message = f"API request failed with status {response.status_code}"
            error_context = {
                "full_url": full_url,
                "config_api_root": config_api_root,
                "path": path,
                "status_code": response.status_code,
                "url": response.request.url,
                "body": response.request.body,
                "response": response.__dict__,
            }
            if response.status_code == HTTPStatus.FORBIDDEN:
                raise AirbyteMissingResourceError(
                    message=(
                        "The requested resource was not found, or these credentials can't "
                        "access it (HTTP 403)."
                    ),
                    guidance=FORBIDDEN_RESOURCE_GUIDANCE,
                    context=error_context,
                ) from ex
            raise AirbyteCloudError(
                message=error_message,
                context=error_context,
            ) from ex

    if response.status_code == HTTPStatus.NO_CONTENT:
        return {}

    try:
        body = response.json()
    except requests.exceptions.JSONDecodeError as ex:
        if response_model is None:
            raise
        raise AirbyteCloudError(
            message=f"Config API response for {path} did not match the expected schema.",
            context={
                "full_url": full_url,
                "path": path,
                "response": response.text,
            },
        ) from ex

    if response_model is None:
        return body

    try:
        return response_model.model_validate(body)
    except ValidationError as ex:
        raise AirbyteCloudError(
            message=f"Config API response for {path} did not match the expected schema.",
            context={
                "full_url": full_url,
                "path": path,
                "response": response.text,
            },
        ) from ex


def _config_api_headers(
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    timeout: tuple[float, float] | None = None,
) -> dict[str, str]:
    """Build Config API headers, minting a bearer token from client credentials if needed."""
    if bearer_token is None:
        if client_id is None or client_secret is None:
            raise AirbyteLibInputError(
                message="No authentication credentials provided.",
                guidance="Provide either client_id and client_secret, or bearer_token.",
            )
        bearer_token = get_bearer_token(
            client_id=client_id,
            client_secret=client_secret,
            api_root=api_root,
            timeout=timeout,
        )
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "PyAirbyte Client",
        AIRBYTE_ANALYTIC_SOURCE_HEADER: get_cloud_api_analytic_source(),
    }


def create_connector_deferred(  # noqa: PLR0913  # Mirrors the API surface.
    *,
    connector_type: Literal["source", "destination"],
    name: str,
    workspace_id: str,
    definition_id: str,
    config: dict[str, Any],
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    config_api_root: str | None = None,
) -> str:
    """Create a draft connector on the Config API and return its ID.

    Missing configuration stays absent until a person completes it in Airbyte Cloud.
    The response must acknowledge `isDraft: true`; a successful connection check
    promotes the saved draft so it can be used in connections.

    Redirects are not followed, since `requests` would replay the POST and could create the
    connector twice, and both the create and any token request use bounded timeouts.
    """
    config_api_root = get_config_api_root(api_root, config_api_root=config_api_root)
    path = f"/{connector_type}s/create"
    full_url = config_api_root + path
    response = requests.post(
        full_url,
        headers=_config_api_headers(
            api_root=api_root,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            timeout=DEFERRED_CREATE_TIMEOUT_SECS,
        ),
        json={
            "name": name,
            "workspaceId": workspace_id,
            f"{connector_type}DefinitionId": definition_id,
            "connectionConfiguration": config,
            "createAsDraft": True,
        },
        timeout=DEFERRED_CREATE_TIMEOUT_SECS,
        allow_redirects=False,
    )
    if not status_ok(response.status_code):
        raise AirbyteCloudError(
            message=f"API request failed with status {response.status_code}",
            context={
                "full_url": full_url,
                "path": path,
                "status_code": response.status_code,
            },
        )

    try:
        body = response.json()
    except requests.exceptions.JSONDecodeError:
        raise AirbyteCloudError(
            message="Cloud returned an invalid draft-create response."
        ) from None
    actor_id = body.get(f"{connector_type}Id") if isinstance(body, dict) else None
    if not isinstance(actor_id, str) or not actor_id:
        raise AirbyteCloudError(
            message="Cloud did not return the created connector ID.",
            context={"full_url": full_url, "path": path},
        )
    if body.get("isDraft") is not True:
        raise AirbyteDeferredSetupError(
            message="Cloud created the connector without acknowledging draft mode.",
            guidance=(
                "Inspect the created connector before retrying. The platform must support drafts."
            ),
            actor_id=actor_id,
        )
    return actor_id


def check_connector(
    *,
    actor_id: str,
    connector_type: ConnectorType,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    workspace_id: str | None = None,
    api_root: str = CLOUD_API_ROOT,
    config_api_root: str | None = None,
) -> tuple[bool, str | None]:
    """Check a source.

    Raises an exception if the check fails. Uses one of these endpoints:

    - /v1/sources/check_connection: https://github.com/airbytehq/airbyte-platform-internal/blob/10bb92e1745a282e785eedfcbed1ba72654c4e4e/oss/airbyte-api/server-api/src/main/openapi/config.yaml#L1409
    - /v1/destinations/check_connection: https://github.com/airbytehq/airbyte-platform-internal/blob/10bb92e1745a282e785eedfcbed1ba72654c4e4e/oss/airbyte-api/server-api/src/main/openapi/config.yaml#L1995
    """
    _ = workspace_id  # Not used (yet)

    request: BaseModel
    if connector_type == "source":
        request = SourceIdRequestBody(sourceId=actor_id)
    else:
        request = DestinationIdRequestBody(destinationId=actor_id)

    try:
        json_result = _make_config_api_request(
            path=f"/{connector_type}s/check_connection",
            request=request,
            response_model=CheckConnectionRead,
            api_root=api_root,
            config_api_root=config_api_root,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
        )
    except AirbyteCloudError as ex:
        # A draft connector with incomplete configuration returns HTTP 422; report it as
        # a failed check rather than an operational error so a person can finish setup.
        if (ex.context or {}).get("status_code") == HTTPStatus.UNPROCESSABLE_ENTITY:
            return (
                False,
                "Connector configuration is incomplete or invalid; "
                "finish setup in Airbyte Cloud.",
            )
        raise
    result, message = (
        json_result.status.value if json_result.status is not None else None,
        json_result.message,
    )

    if result == "succeeded":
        return True, None

    if result == "failed":
        return False, message

    raise AirbyteCloudError(
        context={
            "actor_id": actor_id,
            "connector_type": str(connector_type),
            "response": json_result.model_dump(mode="json", by_alias=True, exclude_none=True),
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
            raise AirbyteLibInputError(message=error, context={"manifest": manifest})
        return False, error

    required_fields = ["version", "type"]
    missing = [f for f in required_fields if f not in manifest]
    if missing:
        error = f"Manifest missing required fields: {', '.join(missing)}"
        if raise_on_error:
            raise AirbyteLibInputError(message=error, context={"manifest": manifest})
        return False, error

    if manifest.get("type") != "DeclarativeSource":
        error = f"Manifest type must be 'DeclarativeSource', got '{manifest.get('type')}'"
        if raise_on_error:
            raise AirbyteLibInputError(message=error, context={"manifest": manifest})
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
    definition = _make_public_api_request(
        method="POST",
        api_root=api_root,
        path=f"/workspaces/{workspace_id}/definitions/declarative_sources",
        request=models.CreateDeclarativeSourceDefinitionRequest(
            name=name,
            manifest=manifest,
        ),
        response_model=models.DeclarativeSourceDefinitionResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={"name": name, "workspace_id": workspace_id},
    )
    if definition is None:
        raise AirbyteCloudError(
            message="Failed to create custom YAML source definition",
            context={"name": name, "workspace_id": workspace_id},
        )
    return definition


def list_custom_yaml_source_definitions(
    workspace_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> list[models.DeclarativeSourceDefinitionResponse]:
    """List all custom YAML source definitions in a workspace."""
    path = f"/workspaces/{workspace_id}/definitions/declarative_sources"
    body = _make_public_api_request(
        method="GET",
        api_root=api_root,
        path=path,
        response_model=None,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={"workspace_id": workspace_id},
    )
    if body is None:
        raise AirbyteCloudError(
            message="Failed to list custom YAML source definitions",
            context={
                "workspace_id": workspace_id,
            },
        )
    return _decode_list_items(
        body, item_model=models.DeclarativeSourceDefinitionResponse, list_path=path
    )


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
    definition = _make_public_api_request(
        method="GET",
        api_root=api_root,
        path=f"/workspaces/{workspace_id}/definitions/declarative_sources/{definition_id}",
        response_model=models.DeclarativeSourceDefinitionResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={
            "workspace_id": workspace_id,
            "definition_id": definition_id,
        },
    )
    if definition is None:
        raise AirbyteCloudError(
            message="Failed to get custom YAML source definition",
            context={
                "workspace_id": workspace_id,
                "definition_id": definition_id,
            },
        )
    return definition


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
    definition = _make_public_api_request(
        method="PUT",
        api_root=api_root,
        path=f"/workspaces/{workspace_id}/definitions/declarative_sources/{definition_id}",
        request=models.UpdateDeclarativeSourceDefinitionRequest(
            manifest=manifest,
        ),
        response_model=models.DeclarativeSourceDefinitionResponse,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={
            "workspace_id": workspace_id,
            "definition_id": definition_id,
        },
    )
    if definition is None:
        raise AirbyteCloudError(
            message="Failed to update custom YAML source definition",
            context={
                "workspace_id": workspace_id,
                "definition_id": definition_id,
            },
        )
    return definition


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
        AirbyteLibInputError: If safe_mode is True and the connector name does not meet
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
            raise AirbyteLibInputError(
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

    _make_public_api_request(
        method="DELETE",
        api_root=api_root,
        path=f"/workspaces/{workspace_id}/definitions/declarative_sources/{definition_id}",
        response_model=None,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        error_context={
            "workspace_id": workspace_id,
            "definition_id": definition_id,
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
    config_api_root: str | None = None,
) -> BuilderProjectForDefinitionResponse:
    """Get the connector builder project info for a declarative source definition.

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
        config_api_root: Optional explicit Config API root URL.

    Returns:
        A response containing 'builderProjectId' and 'workspaceId' (the workspace that
        owns the builder project, which may differ from the caller's workspace).
    """
    return _make_config_api_request(
        path="/connector_builder_projects/get_for_definition_id",
        request=BuilderProjectForDefinitionRequestBody(
            actorDefinitionId=definition_id,
            workspaceId=workspace_id,
        ),
        response_model=BuilderProjectForDefinitionResponse,
        api_root=api_root,
        config_api_root=config_api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )


def list_connector_builder_projects(
    workspace_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    config_api_root: str | None = None,
) -> list[dict[str, Any]]:
    """List connector builder projects for a workspace.

    Calls `POST /v1/connector_builder_projects/list`.
    """
    response = _make_config_api_request(
        path="/connector_builder_projects/list",
        request={"workspaceId": workspace_id},
        api_root=api_root,
        config_api_root=config_api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )
    return response["projects"]


def get_connector_builder_project(
    *,
    workspace_id: str,
    builder_project_id: str,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    config_api_root: str | None = None,
) -> ConnectorBuilderProjectRead:
    """Get a connector builder project, including the draft manifest if one exists.

    Uses the Config API endpoint:
    /v1/connector_builder_projects/get_with_manifest

    See: https://github.com/airbytehq/airbyte-platform-internal/blob/master/oss/airbyte-api/server-api/src/main/openapi/config.yaml#L1253

    Args:
        workspace_id: The workspace ID
        builder_project_id: The connector builder project ID
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).
        config_api_root: Optional explicit Config API root URL.

    Returns:
        A generated response containing the builder project details. Key fields include:
        - builderProject: The project metadata (name, hasDraft, etc.)
        - declarativeManifest: The draft manifest data (if hasDraft is True),
          which contains a 'manifest' field with the actual YAML manifest dict.
    """
    return _make_config_api_request(
        path="/connector_builder_projects/get_with_manifest",
        request=ConnectorBuilderProjectIdWithWorkspaceId(
            workspaceId=workspace_id,
            builderProjectId=builder_project_id,
        ),
        response_model=ConnectorBuilderProjectRead,
        api_root=api_root,
        config_api_root=config_api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )


def update_connector_builder_project(  # noqa: PLR0913
    *,
    workspace_id: str,
    builder_project_id: str,
    name: str,
    draft_manifest: dict[str, Any] | None,
    components_file_content: str | None,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    config_api_root: str | None = None,
) -> None:
    """Update a connector builder project name and preserve its optional draft content.

    The Config API replaces the draft manifest and components file with the values in
    `builderProject`, so these fields are included only when not None to avoid wiping
    existing content.
    """
    builder_project: dict[str, Any] = {"name": name}
    if draft_manifest is not None:
        builder_project["draftManifest"] = draft_manifest
    if components_file_content is not None:
        builder_project["componentsFileContent"] = components_file_content

    _make_config_api_request(
        path="/connector_builder_projects/update",
        request={
            "workspaceId": workspace_id,
            "builderProjectId": builder_project_id,
            "builderProject": builder_project,
        },
        api_root=api_root,
        config_api_root=config_api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )


def update_connector_builder_project_testing_values(  # noqa: PLR0913
    *,
    workspace_id: str,
    builder_project_id: str,
    testing_values: dict[str, Any],
    spec: dict[str, Any],
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    config_api_root: str | None = None,
) -> ConnectorBuilderProjectTestingValues:
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
        config_api_root: Optional explicit Config API root URL.

    Returns:
        The updated testing values from the API response
    """
    return _make_config_api_request(
        path="/connector_builder_projects/update_testing_values",
        request=ConnectorBuilderProjectTestingValuesUpdate(
            workspaceId=workspace_id,
            builderProjectId=builder_project_id,
            testingValues=ConnectorBuilderProjectTestingValues.model_validate(testing_values),
            spec=SourceDefinitionSpecification.model_validate(spec),
        ),
        response_model=ConnectorBuilderProjectTestingValues,
        api_root=api_root,
        config_api_root=config_api_root,
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
    body = _make_public_api_request(
        method="GET",
        api_root=api_root,
        path="/organizations",
        response_model=None,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )

    if body is not None:
        return _decode_list_items(
            body, item_model=models.OrganizationResponse, list_path="/organizations"
        )

    raise AirbyteCloudError(
        message="Failed to list organizations for user.",
    )


def list_organizations_for_user_id(
    user_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    config_api_root: str | None = None,
    name_contains: str | None = None,
    limit: int | None = None,
) -> list[OrganizationRead]:
    """List organizations the given user is a member of.

    Uses the Config API endpoint: POST /v1/organizations/list_by_user_id

    Unlike the public `GET /organizations` endpoint, this endpoint supports
    server-side name filtering and pagination.

    Args:
        user_id: The Airbyte user ID to list organizations for
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).
        config_api_root: Optional explicit Config API root URL.
        name_contains: Optional substring filter for organization names (server-side)
        limit: Optional maximum number of organizations to return

    Returns:
        List of organization models containing organizationId, organizationName, email, etc.
    """
    _validate_pagination_params(limit=limit)
    result: list[OrganizationRead] = []
    page_size = PAGE_SIZE

    row_offset = 0

    while True:
        json_result = _make_config_api_request(
            path="/organizations/list_by_user_id",
            request=ListOrganizationsByUserRequestBody(
                userId=user_id,
                pagination=Pagination(pageSize=page_size, rowOffset=row_offset),
                nameContains=name_contains,
            ),
            response_model=OrganizationReadList,
            api_root=api_root,
            config_api_root=config_api_root,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
        )

        organizations = json_result.organizations

        if not organizations:
            break

        result.extend(organizations)

        if limit is not None and len(result) >= limit:
            return result[:limit]

        if len(organizations) < page_size:
            break

        row_offset += page_size

    return result


def list_workspaces_in_organization(
    organization_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    config_api_root: str | None = None,
    name_contains: str | None = None,
    limit: int | None = None,
) -> list[WorkspaceRead]:
    """List workspaces within a specific organization.

    Uses the Config API endpoint: POST /v1/workspaces/list_by_organization_id

    Args:
        organization_id: The organization ID to list workspaces for
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).
        config_api_root: Optional explicit Config API root URL.
        name_contains: Optional substring filter for workspace names (server-side)
        limit: Optional maximum number of workspaces to return

    Returns:
        List of workspace models containing workspaceId, organizationId, name, slug, etc.
    """
    _validate_pagination_params(limit=limit)
    result: list[WorkspaceRead] = []
    page_size = 100

    row_offset = 0

    # Fetch pages until we have all results or reach the limit
    while True:
        json_result = _make_config_api_request(
            path="/workspaces/list_by_organization_id",
            request=ListWorkspacesInOrganizationRequestBody(
                organizationId=organization_id,
                pagination=Pagination(pageSize=page_size, rowOffset=row_offset),
                nameContains=name_contains,
            ),
            response_model=WorkspaceReadList,
            api_root=api_root,
            config_api_root=config_api_root,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
        )

        workspaces = json_result.workspaces

        # If no results returned, we've exhausted all pages
        if not workspaces:
            break

        result.extend(workspaces)

        # Check if we've reached the limit
        if limit is not None and len(result) >= limit:
            return result[:limit]

        # If we got fewer results than page_size, this was the last page
        if len(workspaces) < page_size:
            break

        # Bump offset for next iteration
        row_offset += page_size

    return result


def list_workspaces_by_user(  # noqa: PLR0913  # Mirrors list_workspaces_in_organization.
    user_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    config_api_root: str | None = None,
    name_contains: str | None = None,
    name_filter: Callable[[str], bool] | None = None,
    limit: int | None = None,
    page_size: int = 100,
) -> list[dict[str, Any]]:
    """List workspaces visible to a user.

    Uses the Config API endpoint: POST /v1/workspaces/list_by_user_id

    Args:
        user_id: The Airbyte user ID to list workspaces for
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).
        config_api_root: Optional explicit Config API root URL.
        name_contains: Optional substring filter for workspace names (server-side)
        name_filter: Optional predicate to filter workspace names (client-side)
        limit: Optional maximum number of workspaces to return
        page_size: Number of workspaces to request per page

    Returns:
        List of workspace dictionaries containing workspaceId, organizationId, name, etc.
    """
    _validate_pagination_params(limit=limit)
    result: list[dict[str, Any]] = []

    payload: dict[str, Any] = {
        "userId": user_id,
        "pagination": {
            "pageSize": page_size,
            "rowOffset": 0,
        },
    }
    if name_contains is not None:
        payload["nameContains"] = name_contains

    while True:
        json_result = _make_config_api_request(
            path="/workspaces/list_by_user_id",
            request={
                **payload,
                "pagination": payload["pagination"].copy(),
            },
            api_root=api_root,
            config_api_root=config_api_root,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
        )

        workspaces = json_result.get("workspaces", [])

        if not workspaces:
            break

        matches = [
            workspace
            for workspace in workspaces
            if name_filter is None or name_filter(workspace.get("name", ""))
        ]
        result.extend(matches)

        if limit is not None and len(result) >= limit:
            return result[:limit]

        if len(workspaces) < page_size:
            break

        payload["pagination"]["rowOffset"] += page_size

    return result


def get_workspace_organization_info(
    workspace_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    config_api_root: str | None = None,
    timeout: tuple[float, float] | None = None,
) -> OrganizationInfoRead:
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
        config_api_root: Optional explicit Config API root URL.
        timeout: Optional connect and read timeout for the request.

    Returns:
        Generated organization info response:
        - organizationId: The organization ID
        - organizationName: The organization name
        - sso: Whether SSO is enabled
        - billing: Billing information (optional)
    """
    return _make_config_api_request(
        path="/workspaces/get_organization_info",
        request=WorkspaceIdRequestBody(workspaceId=workspace_id),
        response_model=OrganizationInfoRead,
        api_root=api_root,
        config_api_root=config_api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        timeout=timeout,
    )


def get_connection_state(
    connection_id: str,
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    config_api_root: str | None = None,
) -> dict[str, Any]:
    """Get the state for a connection.

    Uses the Config API endpoint: POST /v1/state/get

    Args:
        connection_id: The connection ID to get state for
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).
        config_api_root: Optional explicit Config API root URL.

    Returns:
        Dictionary containing the connection state.
    """
    response = _make_config_api_request(
        path="/state/get",
        request=ConnectionIdRequestBody(connectionId=connection_id),
        response_model=ConnectionState,
        api_root=api_root,
        config_api_root=config_api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )
    return response.model_dump(mode="json", by_alias=True, exclude_none=True)


def replace_connection_state(
    connection_id: str,
    connection_state_dict: dict[str, Any],
    *,
    api_root: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    config_api_root: str | None = None,
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
        config_api_root: Optional explicit Config API root URL.

    Returns:
        Dictionary containing the updated ConnectionState object.
    """
    try:
        response = _make_config_api_request(
            path="/state/create_or_update_safe",
            request=ConnectionStateCreateOrUpdate(
                connectionId=connection_id,
                connectionState=ConnectionState.model_validate(
                    {
                        **connection_state_dict,
                        "connectionId": connection_id,
                    }
                ),
            ),
            response_model=ConnectionState,
            api_root=api_root,
            config_api_root=config_api_root,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
        )
        return response.model_dump(mode="json", by_alias=True, exclude_none=True)
    except AirbyteCloudError as ex:
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
    config_api_root: str | None = None,
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
        config_api_root: Optional explicit Config API root URL.

    Returns:
        Dictionary containing the connection info with syncCatalog.
    """
    response = _make_config_api_request(
        path="/web_backend/connections/get",
        request=WebBackendConnectionRequestBody(
            connectionId=connection_id,
            withRefreshedCatalog=False,
        ),
        response_model=WebBackendConnectionRead,
        api_root=api_root,
        config_api_root=config_api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )
    return response.model_dump(mode="json", by_alias=True, exclude_none=True)


def replace_connection_catalog(
    connection_id: str,
    configured_catalog_dict: dict[str, Any],
    *,
    api_root: str,
    config_api_root: str | None = None,
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
        config_api_root: Optional explicit Config API root URL.

    Returns:
        Dictionary containing the updated WebBackendConnectionRead response.
    """
    response = _make_config_api_request(
        path="/web_backend/connections/update",
        request=WebBackendConnectionUpdate(
            connectionId=connection_id,
            syncCatalog=AirbyteCatalog.model_validate(configured_catalog_dict),
            # Resets are destructive and cause customer-side data outage.
            # If a reset is desired, caller will need to decide & manage.
            skipReset=True,
        ),
        response_model=WebBackendConnectionRead,
        api_root=api_root,
        config_api_root=config_api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )
    return response.model_dump(mode="json", by_alias=True, exclude_none=True)


def get_organization_info(
    organization_id: str,
    *,
    api_root: str,
    config_api_root: str | None = None,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> OrganizationInfoRead:
    """Get organization info including billing status.

    Uses the Config API endpoint: POST /v1/organizations/get_organization_info

    Args:
        organization_id: The organization ID to look up
        api_root: The API root URL
        client_id: OAuth client ID
        client_secret: OAuth client secret
        bearer_token: Bearer token for authentication (alternative to client credentials).
        config_api_root: Optional explicit Config API root URL.

    Returns:
        Generated organization info response:
        - organizationId: The organization ID
        - organizationName: The organization name
        - sso: Whether SSO is enabled
        - billing: Billing information (optional, contains paymentStatus, subscriptionStatus, etc.)
    """
    return _make_config_api_request(
        path="/organizations/get_organization_info",
        request=OrganizationIdRequestBody(organizationId=organization_id),
        response_model=OrganizationInfoRead,
        api_root=api_root,
        config_api_root=config_api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )


def get_user_id_from_bearer_token(bearer_token: SecretString) -> str:
    """Extract the authentication user ID from a bearer token."""
    token_parts = str(bearer_token).split(".")
    if len(token_parts) != JWT_PART_COUNT:
        raise AirbyteLibInputError(
            message="The bearer token is not a valid JWT.",
            guidance="Provide a valid bearer token.",
        )

    try:
        payload = json.loads(
            base64.urlsafe_b64decode(
                token_parts[1] + "=" * (-len(token_parts[1]) % 4),
            ).decode("utf-8")
        )
    except (UnicodeDecodeError, ValueError) as error:
        raise AirbyteLibInputError(
            message="The bearer token payload could not be decoded.",
            guidance="Provide a valid bearer token.",
        ) from error

    user_id = payload.get("user_id") if isinstance(payload, dict) else None
    if not isinstance(user_id, str) or not user_id:
        user_id = payload.get("sub") if isinstance(payload, dict) else None
    if not isinstance(user_id, str) or not user_id:
        raise AirbyteLibInputError(
            message="The bearer token does not contain a user_id or sub claim.",
            guidance="Provide a bearer token issued for an Airbyte user.",
        )
    return user_id


def get_user_by_auth_id(
    auth_user_id: str,
    *,
    api_root: str,
    config_api_root: str | None = None,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    timeout: tuple[float, float] | None = None,
) -> UserRead:
    """Get an Airbyte user by the authentication provider user ID."""
    return _make_config_api_request(
        path="/users/get_by_auth_id",
        request=UserAuthIdRequestBody(authUserId=auth_user_id),
        response_model=UserRead,
        api_root=api_root,
        config_api_root=config_api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        timeout=timeout,
    )


def update_user_default_workspace(
    user_id: str,
    workspace_id: str,
    *,
    api_root: str,
    config_api_root: str | None = None,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> dict[str, Any]:
    """Update an Airbyte user's stored default workspace.

    Uses the Config API endpoint: POST /v1/users/update
    """
    result = _make_config_api_request(
        path="/users/update",
        request={
            "userId": user_id,
            "defaultWorkspaceId": workspace_id,
        },
        api_root=api_root,
        config_api_root=config_api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )
    if isinstance(result, dict):
        return result

    raise AirbyteCloudError(
        message="The user API returned an unexpected response.",
        context={"response": result},
    )


def get_workspace_config_api(
    workspace_id: str,
    *,
    api_root: str,
    config_api_root: str | None = None,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> dict[str, Any]:
    """Get a workspace record via the Config API, including tombstoned rows.

    Uses the Config API endpoint: POST /v1/workspaces/get

    Tombstoned rows are requested explicitly so callers can distinguish a
    deleted workspace from one that was never found.
    """
    result = _make_config_api_request(
        path="/workspaces/get",
        request={
            "workspaceId": workspace_id,
            "includeTombstone": True,
        },
        api_root=api_root,
        config_api_root=config_api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )
    if isinstance(result, dict):
        return result

    raise AirbyteCloudError(
        message="The workspace API returned an unexpected response.",
        context={"response": result},
    )


def list_permissions_for_user(
    user_id: str,
    *,
    api_root: str,
    config_api_root: str | None = None,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
) -> list[PermissionRead]:
    """List permissions granted to an Airbyte user."""
    result = _make_config_api_request(
        path="/permissions/list_by_user",
        request=UserIdRequestBody(userId=user_id),
        response_model=PermissionReadList,
        api_root=api_root,
        config_api_root=config_api_root,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )
    return result.permissions


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
