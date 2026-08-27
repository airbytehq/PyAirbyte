# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Airbyte Agents connectors, and the single-action `execute` interface."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, NamedTuple

from airbyte.agents import _api_util
from airbyte.agents.models import AgentConnectorDetails, AgentExecuteResult
from airbyte.cloud._credentials import _AirbyteCredentials
from airbyte.exceptions import PyAirbyteInputError


if TYPE_CHECKING:
    from airbyte.secrets.base import SecretString


UNSUPPORTED_ACTIONS: set[str] = {"download"}
"""Actions PyAirbyte rejects before sending them to the Agents API.

`download` returns a binary stream rather than JSON, and PyAirbyte does not yet support
streaming responses, so it is rejected with actionable guidance instead of failing later
inside the transport layer.
"""

_PAGINATION_ARGS: tuple[str, ...] = ("limit", "cursor")
"""Convenience arguments that PyAirbyte merges into the connector's `params`."""


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
    connector = workspace.get_connector("GitHub")  # by ID or name
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

    @classmethod
    def from_auth(
        cls,
        connector_id: str,
        *,
        organization_id: str | None = None,
        client_id: str | SecretString | None = None,
        client_secret: str | SecretString | None = None,
        bearer_token: str | SecretString | None = None,
    ) -> AgentConnector:
        """Create an `AgentConnector` from credentials, without a workspace lookup.

        The Agents API addresses a connector by ID alone, so no workspace is needed.
        Credentials fall back to the `AIRBYTE_CLOUD_*` environment variables when they are
        not passed explicitly.
        """
        return cls(
            connector_id,
            credentials=_AirbyteCredentials.from_auth(
                organization_id=organization_id,
                client_id=client_id,
                client_secret=client_secret,
                bearer_token=bearer_token,
                # Mirrors `CloudWorkspace.__init__`: any explicit credential disables env
                # fallback, since an env bearer token plus explicit client creds is rejected
                # as mutually exclusive auth.
                env_vars=not (client_id or client_secret or bearer_token),
            ),
        )

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

        - `select_fields` and `exclude_fields` prune fields from returned entities.
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
        """Run the `list` action, which returns a page of entities for `entity`."""
        return self.execute(entity, "list", api_args, **kwargs)

    def search_entities(
        self,
        entity: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `search` action, which returns matching entities for `entity`."""
        return self.execute(entity, "search", api_args, **kwargs)

    def get_entity(
        self,
        entity: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `get` action, which returns a single entity of type `entity`."""
        return self.execute(entity, "get", api_args, **kwargs)

    def create_entity(
        self,
        entity: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `create` action, which creates an entity of type `entity`."""
        return self.execute(entity, "create", api_args, **kwargs)

    def update_entity(
        self,
        entity: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `update` action, which updates an entity of type `entity`."""
        return self.execute(entity, "update", api_args, **kwargs)

    def delete_entity(
        self,
        entity: str,
        api_args: dict[str, Any] | None = None,
        **kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `execute()`.
    ) -> AgentExecuteResult:
        """Run the `delete` action, which deletes an entity of type `entity`."""
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
