# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Internal helpers for calling the Airbyte Agents API.

The Agents API (`https://api.airbyte.ai/api/v1`) is a separate API surface from the Public
API and the Config API, and it is not covered by the `airbyte-api` SDK. This module holds
the raw HTTP plumbing for it.

Airbyte Cloud credentials (client credentials or a bearer token) authenticate against the
Agents API, so callers reuse the same credentials they use elsewhere in the Cloud module.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Literal

import requests

from airbyte._util.api_util import get_bearer_token, status_ok
from airbyte.constants import AGENTS_API_ROOT, CLOUD_API_ROOT
from airbyte.exceptions import AirbyteError, PyAirbyteInputError


if TYPE_CHECKING:
    from airbyte.secrets.base import SecretString


def _resolve_bearer_token(
    *,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    public_api_root: str,
) -> SecretString:
    """Return a bearer token, exchanging client credentials for one if needed."""
    if bearer_token is not None:
        return bearer_token

    if client_id is None or client_secret is None:
        raise PyAirbyteInputError(
            message="No authentication credentials provided.",
            guidance="Provide either client_id and client_secret, or bearer_token.",
        )
    return get_bearer_token(
        client_id=client_id,
        client_secret=client_secret,
        api_root=public_api_root,
    )


def make_agents_api_request(  # noqa: PLR0913  # Credentials and routing are all explicit.
    *,
    method: Literal["GET", "POST"],
    path: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    organization_id: str | None = None,
    params: dict[str, Any] | None = None,
    json: dict[str, Any] | None = None,
    agents_api_root: str | None = None,
    public_api_root: str = CLOUD_API_ROOT,
) -> dict[str, Any]:
    """Send a request to the Airbyte Agents API and return the parsed JSON response.

    The `organization_id` is sent as the `X-Organization-Id` header, which the Agents API
    requires when the caller's credentials map to more than one organization.
    """
    agents_api_root = agents_api_root or AGENTS_API_ROOT
    resolved_token = _resolve_bearer_token(
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        public_api_root=public_api_root,
    )
    headers: dict[str, str] = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {resolved_token}",
        "User-Agent": "PyAirbyte Client",
    }
    if organization_id:
        headers["X-Organization-Id"] = organization_id

    full_url = agents_api_root.rstrip("/") + path
    response = requests.request(
        method=method,
        url=full_url,
        headers=headers,
        params=params,
        json=json,
    )
    if not status_ok(response.status_code):
        raise AirbyteError(
            message=_error_message(response=response, full_url=full_url),
            context={
                "full_url": full_url,
                "agents_api_root": agents_api_root,
                "path": path,
                "status_code": response.status_code,
                "response_text": response.text,
            },
        )

    parsed: Any = response.json()
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
        return (
            f"{message} (Unauthorized) when accessing: {full_url}. "
            "Check that the Airbyte credentials are valid."
        )
    if response.status_code == HTTPStatus.FORBIDDEN:
        return (
            f"{message} (Forbidden) when accessing: {full_url}. "
            "The organization may not have an Airbyte Agents subscription."
        )
    return f"{message} when accessing: {full_url}."


def list_agent_connectors(
    *,
    workspace_id: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    organization_id: str | None = None,
    agents_api_root: str | None = None,
    public_api_root: str = CLOUD_API_ROOT,
) -> list[dict[str, Any]]:
    """List the connectors configured in an Airbyte Agents workspace."""
    response = make_agents_api_request(
        method="GET",
        path="/integrations/connectors",
        params={"workspace_id": workspace_id},
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        organization_id=organization_id,
        agents_api_root=agents_api_root,
        public_api_root=public_api_root,
    )
    records: Any = response.get("data", [])
    if not isinstance(records, list):
        raise AirbyteError(
            message="Unexpected connector list payload from the Airbyte Agents API.",
            context={"workspace_id": workspace_id},
        )
    return [record for record in records if isinstance(record, dict)]


def inspect_agent_connector(
    *,
    connector_id: str,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    organization_id: str | None = None,
    agents_api_root: str | None = None,
    public_api_root: str = CLOUD_API_ROOT,
) -> dict[str, Any]:
    """Return metadata and available entities for an Airbyte Agents connector."""
    return make_agents_api_request(
        method="GET",
        path=f"/integrations/connectors/{connector_id}/inspect",
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        organization_id=organization_id,
        agents_api_root=agents_api_root,
        public_api_root=public_api_root,
    )


def execute_agent_connector_action(  # noqa: PLR0913  # Mirrors the Agents API request body.
    *,
    connector_id: str,
    entity: str,
    action: str,
    params: dict[str, Any] | None = None,
    select_fields: list[str] | None = None,
    exclude_fields: list[str] | None = None,
    skip_truncation: bool = True,
    intent: str | None = None,
    client_id: SecretString | None,
    client_secret: SecretString | None,
    bearer_token: SecretString | None,
    organization_id: str | None = None,
    agents_api_root: str | None = None,
    public_api_root: str = CLOUD_API_ROOT,
) -> dict[str, Any]:
    """Execute a single connector action and return the raw response payload."""
    request_body: dict[str, Any] = {
        "entity": entity,
        "action": action,
        "params": params or {},
        "skip_truncation": skip_truncation,
    }
    if select_fields is not None:
        request_body["select_fields"] = select_fields
    if exclude_fields is not None:
        request_body["exclude_fields"] = exclude_fields
    if intent is not None:
        request_body["intent"] = intent

    return make_agents_api_request(
        method="POST",
        path=f"/integrations/connectors/{connector_id}/execute",
        json=request_body,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
        organization_id=organization_id,
        agents_api_root=agents_api_root,
        public_api_root=public_api_root,
    )
