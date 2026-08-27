# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Airbyte Agents connectors, and the single-action `execute` interface."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airbyte.agents import _api_util
from airbyte.agents.models import AgentConnectorDetails, AgentExecuteResult
from airbyte.exceptions import PyAirbyteInputError


if TYPE_CHECKING:
    from airbyte.cloud._credentials import _AirbyteCredentials


UNSUPPORTED_ACTIONS: set[str] = {"download"}
"""Actions PyAirbyte rejects before sending them to the Agents API.

`download` returns a binary stream rather than JSON, and PyAirbyte does not yet support
streaming responses, so it is rejected with actionable guidance instead of failing later
inside the transport layer.
"""

_PAGINATION_ARGS: tuple[str, ...] = ("limit", "cursor")
"""Convenience arguments that PyAirbyte merges into the connector's `params`."""


class AgentConnector:
    """A connector in an Airbyte Agents workspace.

    Get one from `AgentWorkspace.get_connector()` rather than constructing it directly.

    ```python
    from airbyte import agents

    workspace = agents.AgentWorkspace.from_env()
    connector = workspace.get_connector(name="GitHub")
    result = connector.list_entities("issues", api_args={"repository": "airbytehq/PyAirbyte"})
    for record in result.records:
        print(record["title"])
    ```
    """

    def __init__(
        self,
        connector_id: str,
        *,
        credentials: _AirbyteCredentials,
        name: str | None = None,
    ) -> None:
        """Initialize an `AgentConnector`. Prefer `AgentWorkspace.get_connector()`."""
        self.connector_id = connector_id
        """The connector ID."""

        self._credentials = credentials
        self._name = name
        self._details: AgentConnectorDetails | None = None

    @property
    def name(self) -> str | None:
        """The connector name, fetched from the Agents API if not already known."""
        if self._name is None:
            self._name = self.describe().name
        return self._name

    def describe(self, *, force_refresh: bool = False) -> AgentConnectorDetails:
        """Return connector metadata from the Agents API `inspect` endpoint.

        The result is cached; pass `force_refresh=True` to fetch it again.
        """
        if self._details is None or force_refresh:
            self._details = AgentConnectorDetails.model_validate(
                _api_util.inspect_agent_connector(
                    connector_id=self.connector_id,
                    credentials=self._credentials,
                    organization_id=self._credentials.organization_id,
                )
            )
            self._name = self._details.name or self._name
        return self._details

    def execute(  # noqa: PLR0913  # Explicit args are the point of this public API.
        self,
        entity: str,
        action: str,
        api_args: dict[str, Any] | None = None,
        *,
        select_fields: list[str] | None = None,
        exclude_fields: list[str] | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        skip_truncation: bool = True,
        intent: str | None = None,
    ) -> AgentExecuteResult:
        """Execute a single action against an entity on this connector.

        `entity` and `action` are connector-specific, for example `issues` and `list`. Use
        `describe()` to see what a connector supports.

        `api_args` holds connector-specific arguments passed through to the connector, for
        example `{"repository": "airbytehq/PyAirbyte"}`. All other arguments are interpreted
        by PyAirbyte or by the Agents API itself:

        - `select_fields` and `exclude_fields` prune fields from returned records.
        - `limit` and `cursor` are merged into `api_args` as pagination arguments. Pass the
          `end_cursor` of a previous result as `cursor` to fetch the next page.
        - `skip_truncation` disables the Agents API's default truncation of large payloads.
        - `intent` is a free-text description of why the action is being run, which some
          connectors use to refine results.

        The `download` action is rejected, because it returns a binary stream and PyAirbyte
        does not yet support streaming responses.
        """
        if action in UNSUPPORTED_ACTIONS:
            raise PyAirbyteInputError(
                message=f"The {action!r} action is not supported by PyAirbyte.",
                guidance=(
                    "This action returns a binary stream instead of JSON, and PyAirbyte does "
                    "not yet support streaming responses."
                ),
                context={"entity": entity, "action": action},
            )

        request_body: dict[str, Any] = {
            "entity": entity,
            "action": action,
            "params": _build_params(api_args=api_args, limit=limit, cursor=cursor),
            "skip_truncation": skip_truncation,
        }
        if select_fields is not None:
            request_body["select_fields"] = select_fields
        if exclude_fields is not None:
            request_body["exclude_fields"] = exclude_fields
        if intent is not None:
            request_body["intent"] = intent

        return AgentExecuteResult.model_validate(
            _api_util.execute_agent_connector_action(
                connector_id=self.connector_id,
                request_body=request_body,
                credentials=self._credentials,
                organization_id=self._credentials.organization_id,
            )
        )

    def list_entities(
        self,
        entity: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `list` action, which returns a page of records for `entity`."""
        return self.execute(entity, "list", api_args, **kwargs)

    def search_entities(
        self,
        entity: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `search` action, which returns matching records for `entity`."""
        return self.execute(entity, "search", api_args, **kwargs)

    def get_entity(
        self,
        entity: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `get` action, which returns a single record of `entity`."""
        return self.execute(entity, "get", api_args, **kwargs)

    def create_entity(
        self,
        entity: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `create` action, which creates a record of `entity`."""
        return self.execute(entity, "create", api_args, **kwargs)

    def update_entity(
        self,
        entity: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `update` action, which updates a record of `entity`."""
        return self.execute(entity, "update", api_args, **kwargs)

    def delete_entity(
        self,
        entity: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `delete` action, which deletes a record of `entity`."""
        return self.execute(entity, "delete", api_args, **kwargs)


def _build_params(
    *,
    api_args: dict[str, Any] | None,
    limit: int | None,
    cursor: str | None,
) -> dict[str, Any]:
    """Merge the pagination conveniences into the connector-specific `api_args`."""
    params: dict[str, Any] = dict(api_args or {})
    pagination: dict[str, Any] = {"limit": limit, "cursor": cursor}

    conflicts = sorted(
        name for name in _PAGINATION_ARGS if pagination[name] is not None and name in params
    )
    if conflicts:
        raise PyAirbyteInputError(
            message="Pagination arguments were provided twice.",
            guidance=(
                "Pass each of `limit` and `cursor` either as a keyword argument or within "
                "`api_args`, but not both."
            ),
            context={"duplicated_args": conflicts},
        )

    params.update(
        {name: pagination[name] for name in _PAGINATION_ARGS if pagination[name] is not None}
    )
    return params
