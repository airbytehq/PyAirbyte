# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Airbyte Agents connectors, and the single-action `execute` interface.

> ## ⚠️ Experimental Interface
>
> **The Airbyte Agents Python interfaces are experimental.** Class names, method signatures,
> and result models may change or be removed without notice between minor versions of
> PyAirbyte. Pin an exact PyAirbyte version if you depend on them.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any

from airbyte._direct_connectors import api_util as _api_util
from airbyte._direct_connectors.actions import (
    UNSUPPORTED_ACTIONS,
    AgentAction,
    AgentReadAction,
    AgentWriteAction,
    _build_params,
)
from airbyte._direct_connectors.models import AgentConnectorDetails, AgentExecuteResult
from airbyte.exceptions import PyAirbyteInputError


if TYPE_CHECKING:
    from collections.abc import Iterator

    from airbyte.cloud._credentials import _AirbyteCredentials


class AgentConnector:
    """A connector in an Airbyte Agents workspace.

    Get one from `AgentWorkspace.get_connector()` rather than constructing it directly.

    ```python
    from airbyte import agents

    workspace = agents.AgentWorkspace.from_env()
    connector = workspace.get_connector("GitHub")  # by ID or name (case insensitive)
    result = connector.list_entities("issues", api_args={"repository": "airbytehq/PyAirbyte"})
    for entity in result.entities:
        print(entity["title"])
    ```
    """

    def __init__(
        self,
        connector_id: str,
        *,
        credentials: _AirbyteCredentials,
        name: str | None = None,
        workspace_id: str | None = None,
    ) -> None:
        """Initialize an `AgentConnector`. Prefer `AgentWorkspace.get_connector()`."""
        self.connector_id = connector_id
        """The connector ID."""

        self.workspace_id = workspace_id
        """The workspace ID."""

        self._credentials = credentials
        self._name = name
        self._details: AgentConnectorDetails | None = None

    @property
    def name(self) -> str | None:
        """The connector name, fetched from the Agents API if not already known."""
        if self._name is None:
            self._name = self.inspect().name
        return self._name

    def inspect(self, *, force_refresh: bool = False) -> AgentConnectorDetails:
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
        entity_type: str,
        action: AgentAction | str,
        api_args: dict[str, Any] | None = None,
        *,
        select_fields: list[str] | None = None,
        exclude_fields: list[str] | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
        workspace_id: str | None = None,
        skip_truncation: bool = True,
        intent: str | None = None,
    ) -> AgentExecuteResult:
        """Execute a single action against one entity type on this connector.

        `entity_type` and `action` are connector-specific, for example `issues` and `list`.
        Use `inspect()` to see what a connector supports.

        `api_args` holds connector-specific arguments passed through to the connector, for
        example `{"repository": "airbytehq/PyAirbyte"}`. All other arguments are interpreted
        by PyAirbyte or by the Agents API itself:

        - `select_fields` and `exclude_fields` prune fields from returned entities.
        - `page_size` and `cursor` are merged into `api_args` as pagination arguments. Context
          Store `search` uses `limit` and `cursor`, `sql_select` uses the top-level `cursor`, and
          direct connector actions use their own pagination arguments in `api_args`. Pass the
          result cursor according to the action type.
        - `workspace_id` selects the workspace an action runs against. It defaults to the
          connector's workspace and is currently sent for `sql_select`, the only action the
          Agents API scopes by workspace; direct connector actions are scoped by the connector.
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
                context={"entity_type": entity_type, "action": action},
            )

        if action not in {*AgentReadAction, *AgentWriteAction}:
            action_names = ", ".join(
                member.value for member in (*AgentReadAction, *AgentWriteAction)
            )
            raise PyAirbyteInputError(
                message=f"The {action!r} action is not a valid action name for `execute`.",
                guidance=f"Use one of: {action_names}.",
                context={"entity_type": entity_type, "action": action},
            )

        action_value = action.value if isinstance(action, Enum) else action
        params = _build_params(api_args=api_args, page_size=page_size, cursor=cursor)
        if action_value == AgentReadAction.SQL_SELECT.value:
            resolved_workspace_id = workspace_id or self.workspace_id
            if (
                resolved_workspace_id is not None
                and params.get("workspace_id") is None
                and params.get("workspace_name") is None
            ):
                params.pop("workspace_name", None)
                params["workspace_id"] = resolved_workspace_id
        request_body: dict[str, Any] = {
            "entity": entity_type,
            "action": action_value,
            "params": params,
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
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `list` action, which returns a page of entities of `entity_type`."""
        return self.execute(entity_type, "list", api_args, **kwargs)

    def iter_entities(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        *,
        limit: int | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `list_entities()`.
    ) -> Iterator[dict[str, Any]]:
        """Yield entities of `entity_type`, following the connector's pagination cursor.

        This is the pagination-free way to read entities: each page is fetched lazily as
        the caller iterates, so no cursor bookkeeping is needed.

        ```python
        for issue in connector.iter_entities("issues", {"repository": "airbytehq/PyAirbyte"}):
            print(issue["title"])
        ```

        `limit` caps how many entities are yielded in total, which matters for entity types
        with no natural end. Pass `page_size` to control how many are fetched per request.

        Iteration stops early if the connector reports another page without advancing its
        cursor, rather than requesting the same page forever.

        Use `list_entities()` instead when a single page is enough, or when the result's
        `status`, `warning`, or `execution_metadata` are needed.
        """
        cursor: str | None = kwargs.pop("cursor", None)
        yield from _api_util.iter_paged_entities(
            lambda page_cursor: self.list_entities(
                entity_type,
                api_args,
                cursor=page_cursor,
                **kwargs,
            ),
            limit=limit,
            cursor=cursor,
        )

    def search_entities(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `search` action, which returns matching entities of `entity_type`."""
        return self.execute(entity_type, "search", api_args, **kwargs)

    def get_entity(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `get` action, which returns a single entity of `entity_type`."""
        return self.execute(entity_type, "get", api_args, **kwargs)

    def create_entity(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `create` action, which creates an entity of `entity_type`."""
        return self.execute(entity_type, "create", api_args, **kwargs)

    def update_entity(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `update` action, which updates an entity of `entity_type`."""
        return self.execute(entity_type, "update", api_args, **kwargs)

    def delete_entity(
        self,
        entity_type: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `delete` action, which deletes an entity of `entity_type`."""
        return self.execute(entity_type, "delete", api_args, **kwargs)
