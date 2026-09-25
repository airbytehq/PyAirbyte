# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""HTTP helpers for the direct-access and agent-context features hosted by Airbyte Cloud.

These helpers call the Context layer endpoints exposed by the Cloud Config API —
connector `execute` routes and workspace skill-docs routes — using Airbyte Cloud
credentials. Cloud derives the caller's organization from the workspace on each request,
so no organization scoping is needed client-side. Callers should check
`deployment.is_agents_api_available()` before using non-default API roots.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Literal, NamedTuple

import requests

from airbyte._util import deployment
from airbyte._util.api_util import (
    AIRBYTE_ANALYTIC_SOURCE_HEADER,
    CLOUD_API_ROOT,
    get_bearer_token,
    get_cloud_api_analytic_source,
    get_config_api_root,
    status_ok,
)
from airbyte.exceptions import (
    AirbyteAgentsUnavailableError,
    AirbyteError,
    PyAirbyteInputError,
)
from airbyte.registry import ConnectorType


if TYPE_CHECKING:
    from airbyte.cloud._credentials import _AirbyteCredentials


_REQUEST_TIMEOUT_SECONDS = 300
"""Timeout for a single Cloud Config API request.

Generous, because a connector action runs a live third-party API call behind the
Context layer, but finite, so a stalled request cannot hang the caller forever.
"""


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


def _error_message(*, response: requests.Response, full_url: str) -> str:
    """Build an error message for a failed Cloud API request."""
    message = f"Airbyte Cloud API request failed with status {response.status_code}"
    if response.status_code == HTTPStatus.UNAUTHORIZED:
        return f"{message} (Unauthorized) when accessing: {full_url}."
    if response.status_code == HTTPStatus.FORBIDDEN:
        return f"{message} (Forbidden) when accessing: {full_url}."
    return f"{message} when accessing: {full_url}."


def _error_guidance(*, response: requests.Response) -> str | None:
    """Return actionable guidance for a failed Cloud API request, if any applies."""
    if response.status_code == HTTPStatus.UNAUTHORIZED:
        return "Check that the Airbyte Cloud credentials are valid."
    if response.status_code == HTTPStatus.FORBIDDEN:
        return (
            "Authentication succeeded but access was denied; the workspace or connector "
            "may not be enabled for agent access in Airbyte Cloud."
        )
    return None


def is_not_enabled_error(error: AirbyteError) -> bool:
    """Return whether `error` reports the connector is not enabled for agent access.

    Only 403 and 404 responses mean that: a 404 says no docs skill exists for the
    connector, and a 403 says the workspace or connector lacks Context layer access.
    Auth, server, and malformed-response failures carry other statuses and must not be
    read as "not enabled".
    """
    return (error.context or {}).get("status_code") in {
        HTTPStatus.FORBIDDEN,
        HTTPStatus.NOT_FOUND,
    }


def make_cloud_agent_request(
    *,
    method: Literal["GET", "POST"],
    path: str,
    credentials: _AirbyteCredentials,
    params: dict[str, Any] | None = None,
    json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Make a request to the Cloud Config API and return the parsed response.

    The request URL is the deployment's Config API root plus `path`. Authentication is a
    `Bearer` token resolved from the credentials; Cloud derives the organization from the
    workspace, so no organization header is sent.

    Raises `AirbyteAgentsUnavailableError` when the credentials' API roots have no
    Context layer API, and `AirbyteError` with the status code and response text on
    non-2xx responses, or when the response is not a JSON object.
    """
    if not deployment.is_agents_api_available(
        public_api_root=credentials.public_api_root,
        config_api_root=credentials.config_api_root,
    ):
        raise AirbyteAgentsUnavailableError(
            context={
                "api_root": credentials.public_api_root,
                "config_api_root": credentials.config_api_root,
            }
        )

    api_root = get_config_api_root(
        credentials.public_api_root or CLOUD_API_ROOT,
        config_api_root=credentials.config_api_root,
    )
    full_url = f"{api_root}{path}"
    headers: dict[str, str] = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {_resolve_bearer_token(credentials)}",
        "User-Agent": "PyAirbyte Client",
        AIRBYTE_ANALYTIC_SOURCE_HEADER: get_cloud_api_analytic_source(),
    }

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
            message="The Airbyte Cloud API returned a non-JSON response.",
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
            message="The Airbyte Cloud API returned malformed JSON.",
            context={"full_url": full_url},
        ) from ex

    if not isinstance(parsed, dict):
        raise AirbyteError(
            message="Unexpected response payload from the Airbyte Cloud API.",
            context={"full_url": full_url, "payload_type": type(parsed).__name__},
        )
    return parsed


def execute_cloud_connector_action(
    *,
    connector_id: str,
    connector_type: ConnectorType,
    request_body: dict[str, Any],
    credentials: _AirbyteCredentials,
) -> dict[str, Any]:
    """Execute an action on a deployed Cloud connector via the Cloud Config API.

    The connector kind selects the route: `/sources/{id}/execute` for sources and
    `/destinations/{id}/execute` for destinations. The request body is forwarded as-is.
    Cloud `data` and optional `meta` are normalized to the public execution result
    fields without changing the payload or extracting nested pagination metadata.
    """
    path = (
        f"/sources/{connector_id}/execute"
        if connector_type == ConnectorType.SOURCE
        else f"/destinations/{connector_id}/execute"
    )
    response = make_cloud_agent_request(
        method="POST",
        path=path,
        credentials=credentials,
        json=request_body,
    )

    if "data" not in response:
        raise AirbyteError(
            message="Malformed Airbyte Cloud execute response: missing required `data` field.",
            context={"path": path},
        )
    if "meta" in response and not isinstance(response["meta"], dict):
        raise AirbyteError(
            message="Malformed Airbyte Cloud execute response: `meta` must be an object.",
            context={"path": path, "meta_type": type(response["meta"]).__name__},
        )
    return {
        "status": "success",
        "result": response["data"],
        "connector_metadata": response.get("meta", {}),
    }


def read_cloud_skill_docs(
    *,
    workspace_id: str,
    skill_id: str,
    credentials: _AirbyteCredentials,
    section: str | None = None,
) -> dict[str, Any]:
    """Read skill docs for a workspace via the Cloud Config API.

    The `skill_id` is sent as the `id` query parameter, with `section` added only when
    provided.
    """
    params: dict[str, Any] = {"id": skill_id}
    if section is not None:
        params["section"] = section
    return make_cloud_agent_request(
        method="GET",
        path=f"/workspaces/{workspace_id}/skills/docs",
        credentials=credentials,
        params=params,
    )


class _ConnectorLookup(NamedTuple):
    """What to look a connector up by, once the lookup arguments have been validated.

    Both fields are set when the caller passed a positional value that could be either an
    ID or a name, in which case an ID match takes precedence over a name match.
    """

    connector_id: str | None
    name: str | None


def _resolve_connector_lookup(
    id_or_name: str | None,
    /,
    *,
    id: str | None,  # noqa: A002  # Mirrors the public `id` alias it validates.
    connector_id: str | None,
    name: str | None,
) -> _ConnectorLookup:
    """Validate connector lookup arguments and return what to look the connector up by.

    `id` and `connector_id` are synonyms, so exactly one of them, `name`, or the positional
    `id_or_name` is required. Conflicting synonym values are rejected, as is a blank value,
    which would otherwise be treated as an omitted argument.
    """
    all_args = {
        "id_or_name": id_or_name,
        "id": id,
        "connector_id": connector_id,
        "name": name,
    }

    blank_args = sorted(
        key for key, value in all_args.items() if value is not None and not value.strip()
    )
    if blank_args:
        raise PyAirbyteInputError(
            message="Connector lookup arguments cannot be blank.",
            guidance="Omit the argument entirely, or pass a non-blank value.",
            context={"blank_args": blank_args},
        )

    if id_or_name:
        keyword_args = sorted(
            key for key, value in all_args.items() if value and key != "id_or_name"
        )
        if keyword_args:
            raise PyAirbyteInputError(
                message="A positional connector lookup cannot be combined with keyword arguments.",
                guidance="Pass the value positionally, or pass `id`, `connector_id`, or `name`.",
                context={"keyword_args": keyword_args},
            )
        return _ConnectorLookup(connector_id=id_or_name, name=id_or_name)

    provided = {
        key: value for key, value in {"id": id, "connector_id": connector_id}.items() if value
    }
    if len(set(provided.values())) > 1:
        raise PyAirbyteInputError(
            message="`id` and `connector_id` were given conflicting values.",
            guidance="These arguments are synonyms, so pass only one of them.",
            context={"provided": sorted(provided)},
        )

    if bool(provided) == bool(name):
        raise PyAirbyteInputError(
            message="Exactly one connector lookup argument is required.",
            guidance=(
                "Pass a connector ID or name positionally, or as `id`, `connector_id`, "
                "or `name`."
            ),
        )

    return _ConnectorLookup(connector_id=next(iter(provided.values()), None), name=name)
