# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Internal HTTP plumbing for the Airbyte Agents API.

The Agents API is a distinct API surface from both the Public API and the Config API, and
it is not covered by the `airbyte-api` SDK, so this module holds its raw HTTP calls.

Airbyte Cloud credentials authenticate against the Agents API, so the public classes in
this package reuse the same credentials (and the same `AIRBYTE_CLOUD_*` environment
variables) used elsewhere in `airbyte.cloud`.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Literal

import requests

from airbyte._util.api_util import get_bearer_token, status_ok
from airbyte.constants import CLOUD_API_ROOT, CLOUD_CONFIG_API_ROOT
from airbyte.exceptions import AirbyteError, PyAirbyteInputError


if TYPE_CHECKING:
    from airbyte.cloud._credentials import _AirbyteCredentials


_AGENTS_API_ROOT = "https://api.airbyte.ai/api/v1"
"""The Airbyte Agents API root URL.

This is deliberately private and not configurable: the Agents API is a hosted Airbyte
service with a single root, so there is nothing for callers to override.
"""

_REQUEST_TIMEOUT_SECONDS = 300
"""Timeout for a single Agents API request.

Generous, because a connector action runs a live third-party API call behind the Agents
API, but finite, so a stalled request cannot hang the caller forever.
"""

_MULTIPLE_ORGANIZATIONS_HINT = "specify target organization"
"""Fragment of the Agents API error returned when credentials span several organizations."""


def check_public_cloud_api_roots(credentials: _AirbyteCredentials) -> None:
    """Raise `PyAirbyteInputError` unless the credentials use the public Cloud API roots.

    The Agents API has a single hosted root, so an Agents object carries no API root of its
    own. Converting from a Cloud object that points somewhere other than public Airbyte
    Cloud would therefore silently discard those roots, so the conversion is refused.
    """
    overridden = {
        name: value
        for name, value, default in (
            ("api_root", credentials.public_api_root, CLOUD_API_ROOT),
            ("config_api_root", credentials.config_api_root, CLOUD_CONFIG_API_ROOT),
        )
        if value is not None and value.rstrip("/") != default
    }
    if overridden:
        raise PyAirbyteInputError(
            message="The Airbyte Agents API is only available on Airbyte Cloud.",
            guidance=(
                "Agents objects always use the hosted Agents API, so a custom Cloud API "
                "root cannot be honored. Convert from a Cloud object using the public "
                "Airbyte Cloud API roots instead."
            ),
            context=overridden,
        )


def _resolve_bearer_token(credentials: _AirbyteCredentials) -> str:
    """Return a bearer token, exchanging client credentials for one if needed."""
    if credentials.bearer_token is not None:
        return str(credentials.bearer_token)

    if credentials.client_id is None or credentials.client_secret is None:
        raise PyAirbyteInputError(
            message="No authentication credentials provided.",
            guidance="Provide either `client_id` and `client_secret`, or `bearer_token`.",
        )
    return str(
        get_bearer_token(
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            api_root=credentials.public_api_root or CLOUD_API_ROOT,
        )
    )


def make_agents_api_request(
    *,
    method: Literal["GET", "POST"],
    path: str,
    credentials: _AirbyteCredentials,
    organization_id: str | None = None,
    params: dict[str, Any] | None = None,
    json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Send a request to the Airbyte Agents API and return the parsed JSON response.

    The `organization_id` is sent as the `X-Organization-Id` header, which the Agents API
    requires when the caller's credentials map to more than one organization.
    """
    headers: dict[str, str] = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {_resolve_bearer_token(credentials)}",
        "User-Agent": "PyAirbyte Client",
    }
    if organization_id:
        headers["X-Organization-Id"] = organization_id

    full_url = _AGENTS_API_ROOT + path
    response = requests.request(
        method=method,
        url=full_url,
        headers=headers,
        params=params,
        json=json,
        timeout=_REQUEST_TIMEOUT_SECONDS,
    )
    if not status_ok(response.status_code):
        raise AirbyteError(
            message=_error_message(response=response, full_url=full_url),
            guidance=_error_guidance(response=response),
            context={
                "full_url": full_url,
                "path": path,
                "status_code": response.status_code,
                "response_text": response.text,
            },
        )

    content_type = response.headers.get("Content-Type", "")
    if "json" not in content_type:
        raise AirbyteError(
            message="The Airbyte Agents API returned a non-JSON response.",
            guidance=(
                "PyAirbyte does not yet support streaming responses, which some actions "
                "return for binary payloads."
            ),
            context={"full_url": full_url, "content_type": content_type},
        )

    try:
        parsed: Any = response.json()
    except requests.exceptions.JSONDecodeError as ex:
        raise AirbyteError(
            message="The Airbyte Agents API returned malformed JSON.",
            context={"full_url": full_url},
        ) from ex

    if not isinstance(parsed, dict):
        raise AirbyteError(
            message="Unexpected response payload from the Airbyte Agents API.",
            context={"full_url": full_url, "payload_type": type(parsed).__name__},
        )
    return parsed


def _error_message(*, response: requests.Response, full_url: str) -> str:
    """Build an error message for a failed Agents API request."""
    message = f"Airbyte Agents API request failed with status {response.status_code}"
    if response.status_code == HTTPStatus.UNAUTHORIZED:
        return f"{message} (Unauthorized) when accessing: {full_url}."
    if response.status_code == HTTPStatus.FORBIDDEN:
        return f"{message} (Forbidden) when accessing: {full_url}."
    return f"{message} when accessing: {full_url}."


def _error_guidance(*, response: requests.Response) -> str | None:
    """Return actionable guidance for a failed Agents API request, if any applies."""
    if response.status_code == HTTPStatus.UNAUTHORIZED:
        return "Check that the Airbyte Cloud credentials are valid."
    if response.status_code == HTTPStatus.FORBIDDEN:
        return (
            "Authentication succeeded but access was denied. The organization may not have "
            "an Airbyte Agents subscription."
        )
    if (
        response.status_code == HTTPStatus.BAD_REQUEST
        and _MULTIPLE_ORGANIZATIONS_HINT in response.text
    ):
        return (
            "These credentials belong to more than one organization, so the Agents API "
            "cannot infer which one to use. Pass `organization_id`, or set the "
            "`AIRBYTE_CLOUD_ORGANIZATION_ID` environment variable."
        )
    return None


def list_agent_workspaces(
    *,
    credentials: _AirbyteCredentials,
    organization_id: str | None = None,
) -> list[dict[str, Any]]:
    """List the workspaces visible to the caller in the Airbyte Agents API."""
    response = make_agents_api_request(
        method="GET",
        path="/workspaces",
        credentials=credentials,
        organization_id=organization_id,
    )
    return _records_from_response(response=response, path="/workspaces")


def get_agent_workspace(
    *,
    workspace_id: str,
    credentials: _AirbyteCredentials,
    organization_id: str | None = None,
) -> dict[str, Any]:
    """Fetch a single workspace from the Airbyte Agents API.

    A successful response is authoritative proof that the workspace is reachable through
    the Agents API with these credentials.
    """
    return make_agents_api_request(
        method="GET",
        path=f"/workspaces/{workspace_id}",
        credentials=credentials,
        organization_id=organization_id,
    )


def list_agent_connectors(
    *,
    workspace_id: str,
    credentials: _AirbyteCredentials,
    organization_id: str | None = None,
) -> list[dict[str, Any]]:
    """List the connectors configured in an Airbyte Agents workspace."""
    response = make_agents_api_request(
        method="GET",
        path="/integrations/connectors",
        params={"workspace_id": workspace_id},
        credentials=credentials,
        organization_id=organization_id,
    )
    return _records_from_response(response=response, path="/integrations/connectors")


def inspect_agent_connector(
    *,
    connector_id: str,
    credentials: _AirbyteCredentials,
    organization_id: str | None = None,
) -> dict[str, Any]:
    """Return metadata for an Airbyte Agents connector."""
    return make_agents_api_request(
        method="GET",
        path=f"/integrations/connectors/{connector_id}/inspect",
        credentials=credentials,
        organization_id=organization_id,
    )


def execute_agent_connector_action(
    *,
    connector_id: str,
    request_body: dict[str, Any],
    credentials: _AirbyteCredentials,
    organization_id: str | None = None,
) -> dict[str, Any]:
    """Execute a single connector action and return the raw response payload."""
    return make_agents_api_request(
        method="POST",
        path=f"/integrations/connectors/{connector_id}/execute",
        json=request_body,
        credentials=credentials,
        organization_id=organization_id,
    )


def _records_from_response(*, response: dict[str, Any], path: str) -> list[dict[str, Any]]:
    """Return the `data` array from a list response, validating its shape."""
    records: Any = response.get("data")
    if not isinstance(records, list) or any(not isinstance(record, dict) for record in records):
        raise AirbyteError(
            message="Unexpected list payload from the Airbyte Agents API.",
            context={"path": path},
        )
    return records
