# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Airbyte Agents connectors, and the single-action `execute` interface."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, NamedTuple

from airbyte.agents import _api_util
from airbyte.agents.models import AgentConnectorDetails, AgentExecuteResult
from airbyte.exceptions import PyAirbyteInputError


if TYPE_CHECKING:
    from collections.abc import Iterator

    from airbyte.cloud._credentials import _AirbyteCredentials


UNSUPPORTED_ACTIONS: set[str] = {"download"}
"""Actions PyAirbyte rejects before sending them to the Agents API.

`download` returns a binary stream rather than JSON, and PyAirbyte does not yet support
streaming responses, so it is rejected with actionable guidance instead of failing later
inside the transport layer.
"""

_PAGINATION_ARGS: dict[str, str] = {"page_size": "limit", "cursor": "cursor"}
"""Pagination conveniences PyAirbyte merges into the connector's `params`.

Maps the PyAirbyte argument name to the connector's own `params` key: the Agents API calls
page size `limit`, which PyAirbyte does not expose under that name because `limit` reads as
a cap on the whole result set rather than on one page.
"""


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
        entity_type: str,
        action: str,
        api_args: dict[str, Any] | None = None,
        *,
        select_fields: list[str] | None = None,
        exclude_fields: list[str] | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
        skip_truncation: bool = True,
        intent: str | None = None,
    ) -> AgentExecuteResult:
        """Execute a single action against one entity type on this connector.

        `entity_type` and `action` are connector-specific, for example `issues` and `list`.
        Use `describe()` to see what a connector supports.

        `api_args` holds connector-specific arguments passed through to the connector, for
        example `{"repository": "airbytehq/PyAirbyte"}`. All other arguments are interpreted
        by PyAirbyte or by the Agents API itself:

        - `select_fields` and `exclude_fields` prune fields from returned entities.
        - `page_size` and `cursor` are merged into `api_args` as pagination arguments, where
          the connector receives `page_size` as its own `limit`. Pass the `end_cursor` of a
          previous result as `cursor` to fetch the next page.
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

        request_body: dict[str, Any] = {
            "entity": entity_type,
            "action": action,
            "params": _build_params(api_args=api_args, page_size=page_size, cursor=cursor),
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
        seen_cursors: set[str] = set()
        yielded = 0

        while True:
            result = self.list_entities(entity_type, api_args, cursor=cursor, **kwargs)
            for agent_entity in result.entities:
                yield agent_entity
                yielded += 1
                if limit is not None and yielded >= limit:
                    return

            cursor = result.end_cursor
            if not result.has_next_page or cursor is None or cursor in seen_cursors:
                return
            seen_cursors.add(cursor)

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


def _build_params(
    *,
    api_args: dict[str, Any] | None,
    page_size: int | None,
    cursor: str | None,
) -> dict[str, Any]:
    """Merge the pagination conveniences into the connector-specific `api_args`."""
    params: dict[str, Any] = dict(api_args or {})
    pagination: dict[str, Any] = {"page_size": page_size, "cursor": cursor}

    conflicts = sorted(
        name
        for name, param_key in _PAGINATION_ARGS.items()
        if pagination[name] is not None and param_key in params
    )
    if conflicts:
        raise PyAirbyteInputError(
            message="Pagination arguments were provided twice.",
            guidance=(
                "Pass each of `page_size` and `cursor` either as a keyword argument or "
                "within `api_args`, but not both. Note that `page_size` is sent to the "
                "connector as `limit`."
            ),
            context={"duplicated_args": conflicts},
        )

    params.update(
        {
            param_key: pagination[name]
            for name, param_key in _PAGINATION_ARGS.items()
            if pagination[name] is not None
        }
    )
    return params
