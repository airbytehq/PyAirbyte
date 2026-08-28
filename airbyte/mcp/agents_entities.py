# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Airbyte Agents entity MCP operations.

.. include:: ../../docs/mcp-generated/agents_entities.md
"""

# No public Python API — MCP primitives are registered via decorators and
# documented via the generated Markdown include above. Setting `__all__` to an
# empty list tells pdoc (and other doc tools) not to surface the individual
# tool / helper definitions as a redundant "API Documentation" list.
__all__: list[str] = []

from typing import Annotated, Any

from fastmcp import Context, FastMCP
from fastmcp_extensions import mcp_tool, register_mcp_tools
from pydantic import BaseModel, Field

from airbyte.exceptions import PyAirbyteInputError
from airbyte.mcp._tool_utils import AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET
from airbyte.mcp.agents import (
    AGENTS_AUTH_TIP_TEXT,
    WORKSPACE_ID_TIP_TEXT,
    AgentExecuteToolResult,
    _execute,
)
from airbyte.mcp.cloud import _add_defaults_for_exclude_args


CONNECTOR_ID_TIP_TEXT = "The ID of the Airbyte Agents connector."
ENTITY_TYPE_TIP_TEXT = (
    "The type of entity to act on, for example 'issues'. Call `describe_agent_connector` "
    "to see the entity types a connector supports."
)
API_ARGS_TIP_TEXT = (
    "Connector-specific arguments for the action, as an object or a JSON object string. "
    "For example {'repository': 'airbytehq/PyAirbyte'}."
)


class AgentEntityPageResult(BaseModel):
    """One page of entities returned by an Airbyte Agents connector."""

    status: str
    """The execution status reported by the Agents API, for example `success`."""

    entities: list[dict[str, Any]]
    """The entities in this page."""

    has_next_page: bool = False
    """Whether the connector reported more entities after this page."""

    end_cursor: str | None = None
    """The cursor to pass as `cursor` to fetch the next page, when one is available."""

    warning: dict[str, Any] | None = None
    """A warning reported alongside an otherwise successful result."""


class AgentEntityResult(BaseModel):
    """A single entity returned by an Airbyte Agents connector."""

    status: str
    """The execution status reported by the Agents API, for example `success`."""

    entity: dict[str, Any] | None = None
    """The entity, or `null` when the connector found no match."""

    warning: dict[str, Any] | None = None
    """A warning reported alongside an otherwise successful result."""


class AgentWriteResult(BaseModel):
    """The outcome of a create, update, or delete action on an Airbyte Agents connector."""

    status: str
    """The execution status reported by the Agents API, for example `success`."""

    entity: dict[str, Any] | None = None
    """The written entity, when the connector echoes it back."""

    result: Any = None
    """The raw action payload, for connectors whose write response is not a single entity."""

    warning: dict[str, Any] | None = None
    """A warning reported alongside an otherwise successful result."""


def _as_entity_list(result: AgentExecuteToolResult) -> list[dict[str, Any]]:
    """Coerce an action payload into a list of entities."""
    if result.result is None:
        return []
    if isinstance(result.result, dict):
        return [result.result]
    if isinstance(result.result, list) and all(
        isinstance(entity, dict) for entity in result.result
    ):
        return result.result

    raise PyAirbyteInputError(
        message="This action did not return entities.",
        guidance=(
            "Use `execute_agent_connector_ro` to read the raw payload of actions that do "
            "not return entities."
        ),
        context={"result_type": type(result.result).__name__},
    )


def _as_single_entity(result: AgentExecuteToolResult) -> dict[str, Any] | None:
    """Coerce an action payload into at most one entity."""
    entities = _as_entity_list(result)
    if not entities:
        return None
    if len(entities) > 1:
        raise PyAirbyteInputError(
            message="This action returned more than one entity.",
            guidance=(
                "Narrow `api_args` to identify a single entity, or use "
                "`list_agent_entities` to read a page of entities."
            ),
            context={"entity_count": len(entities)},
        )
    return entities[0]


def _read_page(  # noqa: PLR0913  # Mirrors the tool signatures it serves.
    ctx: Context,
    *,
    connector_id: str,
    workspace_id: str | None,
    entity_type: str,
    action: str,
    api_args: dict[str, Any] | str | None,
    select_fields: list[str] | str | None,
    exclude_fields: list[str] | str | None,
    page_size: int | None,
    cursor: str | None,
    intent: str | None,
) -> AgentEntityPageResult:
    """Run a page-returning read action and shape it into an `AgentEntityPageResult`."""
    result = _execute(
        ctx,
        connector_id=connector_id,
        workspace_id=workspace_id,
        entity_type=entity_type,
        action=action,
        api_args=api_args,
        select_fields=select_fields,
        exclude_fields=exclude_fields,
        page_size=page_size,
        cursor=cursor,
        intent=intent,
        read_only=True,
    )
    return AgentEntityPageResult(
        status=result.status,
        entities=_as_entity_list(result),
        has_next_page=result.has_next_page,
        end_cursor=result.end_cursor,
        warning=result.warning,
    )


def _write(
    ctx: Context,
    *,
    connector_id: str,
    workspace_id: str | None,
    entity_type: str,
    action: str,
    api_args: dict[str, Any] | str,
    intent: str | None,
) -> AgentWriteResult:
    """Run a write action and shape it into an `AgentWriteResult`."""
    result = _execute(
        ctx,
        connector_id=connector_id,
        workspace_id=workspace_id,
        entity_type=entity_type,
        action=action,
        api_args=api_args,
        select_fields=None,
        exclude_fields=None,
        page_size=None,
        cursor=None,
        intent=intent,
    )
    return AgentWriteResult(
        status=result.status,
        entity=result.result if isinstance(result.result, dict) else None,
        result=result.result,
        warning=result.warning,
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def list_agent_entities(  # noqa: PLR0913  # Explicit args are the point of this tool.
    ctx: Context,
    connector_id: Annotated[str, Field(description=CONNECTOR_ID_TIP_TEXT)],
    entity_type: Annotated[str, Field(description=ENTITY_TYPE_TIP_TEXT)],
    api_args: Annotated[
        dict[str, Any] | str | None,
        Field(description=API_ARGS_TIP_TEXT, default=None),
    ],
    *,
    select_fields: Annotated[
        list[str] | str | None,
        Field(
            description="Fields to keep in the returned entities, as a list or a CSV string.",
            default=None,
        ),
    ],
    exclude_fields: Annotated[
        list[str] | str | None,
        Field(
            description="Fields to drop from the returned entities, as a list or a CSV string.",
            default=None,
        ),
    ],
    page_size: Annotated[
        int | None,
        Field(description="Maximum number of entities to return in this page.", default=None),
    ],
    cursor: Annotated[
        str | None,
        Field(
            description="Pagination cursor, taken from `end_cursor` of a previous result.",
            default=None,
        ),
    ],
    intent: Annotated[
        str | None,
        Field(description="A short description of why the action is being run.", default=None),
    ],
    workspace_id: Annotated[
        str | None,
        Field(description=WORKSPACE_ID_TIP_TEXT, default=None),
    ],
) -> AgentEntityPageResult:
    """List a page of entities of one type from an Airbyte Agents connector.

    Entity types are connector-specific, so call `describe_agent_connector` first. Follow
    `end_cursor` to read the next page while `has_next_page` is true. The connector must
    belong to the given workspace.
    """
    return _read_page(
        ctx,
        connector_id=connector_id,
        workspace_id=workspace_id,
        entity_type=entity_type,
        action="list",
        api_args=api_args,
        select_fields=select_fields,
        exclude_fields=exclude_fields,
        page_size=page_size,
        cursor=cursor,
        intent=intent,
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def search_agent_entities(  # noqa: PLR0913  # Explicit args are the point of this tool.
    ctx: Context,
    connector_id: Annotated[str, Field(description=CONNECTOR_ID_TIP_TEXT)],
    entity_type: Annotated[str, Field(description=ENTITY_TYPE_TIP_TEXT)],
    api_args: Annotated[
        dict[str, Any] | str | None,
        Field(
            description=(
                f"{API_ARGS_TIP_TEXT} Search terms are connector-specific and belong here, "
                "for example {'query': 'flaky test'}."
            ),
            default=None,
        ),
    ],
    *,
    use_api_search: Annotated[
        bool,
        Field(
            description=(
                "Set to `true` to run the connector's `api_search` action, which queries the "
                "upstream API directly instead of the Context Store."
            ),
            default=False,
        ),
    ],
    select_fields: Annotated[
        list[str] | str | None,
        Field(
            description="Fields to keep in the returned entities, as a list or a CSV string.",
            default=None,
        ),
    ],
    exclude_fields: Annotated[
        list[str] | str | None,
        Field(
            description="Fields to drop from the returned entities, as a list or a CSV string.",
            default=None,
        ),
    ],
    page_size: Annotated[
        int | None,
        Field(description="Maximum number of entities to return in this page.", default=None),
    ],
    cursor: Annotated[
        str | None,
        Field(
            description="Pagination cursor, taken from `end_cursor` of a previous result.",
            default=None,
        ),
    ],
    intent: Annotated[
        str | None,
        Field(description="A short description of why the action is being run.", default=None),
    ],
    workspace_id: Annotated[
        str | None,
        Field(description=WORKSPACE_ID_TIP_TEXT, default=None),
    ],
) -> AgentEntityPageResult:
    """Search entities of one type on an Airbyte Agents connector.

    Not every connector supports `search` for every entity type. Use `list_agent_entities`
    when no search terms are needed. The connector must belong to the given workspace.
    """
    return _read_page(
        ctx,
        connector_id=connector_id,
        workspace_id=workspace_id,
        entity_type=entity_type,
        action="api_search" if use_api_search else "search",
        api_args=api_args,
        select_fields=select_fields,
        exclude_fields=exclude_fields,
        page_size=page_size,
        cursor=cursor,
        intent=intent,
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def get_agent_entity(
    ctx: Context,
    connector_id: Annotated[str, Field(description=CONNECTOR_ID_TIP_TEXT)],
    entity_type: Annotated[str, Field(description=ENTITY_TYPE_TIP_TEXT)],
    api_args: Annotated[
        dict[str, Any] | str,
        Field(
            description=(
                f"{API_ARGS_TIP_TEXT} The entity's identifying arguments belong here, for "
                "example {'repository': 'airbytehq/PyAirbyte', 'number': 1127}."
            ),
        ),
    ],
    *,
    select_fields: Annotated[
        list[str] | str | None,
        Field(
            description="Fields to keep in the returned entity, as a list or a CSV string.",
            default=None,
        ),
    ],
    exclude_fields: Annotated[
        list[str] | str | None,
        Field(
            description="Fields to drop from the returned entity, as a list or a CSV string.",
            default=None,
        ),
    ],
    intent: Annotated[
        str | None,
        Field(description="A short description of why the action is being run.", default=None),
    ],
    workspace_id: Annotated[
        str | None,
        Field(description=WORKSPACE_ID_TIP_TEXT, default=None),
    ],
) -> AgentEntityResult:
    """Get a single entity of one type from an Airbyte Agents connector.

    Which arguments identify an entity is connector-specific, so call
    `describe_agent_connector` first. An action that returns more than one entity is
    rejected: use `list_agent_entities` for that. The connector must belong to the given
    workspace.
    """
    result = _execute(
        ctx,
        connector_id=connector_id,
        workspace_id=workspace_id,
        entity_type=entity_type,
        action="get",
        api_args=api_args,
        select_fields=select_fields,
        exclude_fields=exclude_fields,
        page_size=None,
        cursor=None,
        intent=intent,
        read_only=True,
    )
    return AgentEntityResult(
        status=result.status,
        entity=_as_single_entity(result),
        warning=result.warning,
    )


@mcp_tool(
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def create_agent_entity(
    ctx: Context,
    connector_id: Annotated[str, Field(description=CONNECTOR_ID_TIP_TEXT)],
    entity_type: Annotated[str, Field(description=ENTITY_TYPE_TIP_TEXT)],
    api_args: Annotated[
        dict[str, Any] | str,
        Field(
            description=(
                f"{API_ARGS_TIP_TEXT} The new entity's fields belong here, for example "
                "{'repository': 'airbytehq/PyAirbyte', 'title': 'Bug report'}."
            ),
        ),
    ],
    *,
    intent: Annotated[
        str | None,
        Field(description="A short description of why the entity is being created.", default=None),
    ],
    workspace_id: Annotated[
        str | None,
        Field(description=WORKSPACE_ID_TIP_TEXT, default=None),
    ],
) -> AgentWriteResult:
    """Create an entity of one type on an Airbyte Agents connector.

    This writes to the upstream system the connector is configured against. Which fields
    are required is connector-specific, so call `describe_agent_connector` first. The
    connector must belong to the given workspace.
    """
    return _write(
        ctx,
        connector_id=connector_id,
        workspace_id=workspace_id,
        entity_type=entity_type,
        action="create",
        api_args=api_args,
        intent=intent,
    )


@mcp_tool(
    idempotent=True,
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def update_agent_entity(
    ctx: Context,
    connector_id: Annotated[str, Field(description=CONNECTOR_ID_TIP_TEXT)],
    entity_type: Annotated[str, Field(description=ENTITY_TYPE_TIP_TEXT)],
    api_args: Annotated[
        dict[str, Any] | str,
        Field(
            description=(
                f"{API_ARGS_TIP_TEXT} Both the entity's identifying arguments and the fields "
                "to change belong here, for example {'number': 1127, 'state': 'closed'}."
            ),
        ),
    ],
    *,
    intent: Annotated[
        str | None,
        Field(description="A short description of why the entity is being updated.", default=None),
    ],
    workspace_id: Annotated[
        str | None,
        Field(description=WORKSPACE_ID_TIP_TEXT, default=None),
    ],
) -> AgentWriteResult:
    """Update an entity of one type on an Airbyte Agents connector.

    This writes to the upstream system the connector is configured against. Which fields
    can be changed is connector-specific, so call `describe_agent_connector` first. The
    connector must belong to the given workspace.
    """
    return _write(
        ctx,
        connector_id=connector_id,
        workspace_id=workspace_id,
        entity_type=entity_type,
        action="update",
        api_args=api_args,
        intent=intent,
    )


@mcp_tool(
    destructive=True,
    idempotent=True,
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def delete_agent_entity(
    ctx: Context,
    connector_id: Annotated[str, Field(description=CONNECTOR_ID_TIP_TEXT)],
    entity_type: Annotated[str, Field(description=ENTITY_TYPE_TIP_TEXT)],
    api_args: Annotated[
        dict[str, Any] | str,
        Field(
            description=(
                f"{API_ARGS_TIP_TEXT} The entity's identifying arguments belong here, for "
                "example {'repository': 'airbytehq/PyAirbyte', 'number': 1127}."
            ),
        ),
    ],
    *,
    intent: Annotated[
        str | None,
        Field(description="A short description of why the entity is being deleted.", default=None),
    ],
    workspace_id: Annotated[
        str | None,
        Field(description=WORKSPACE_ID_TIP_TEXT, default=None),
    ],
) -> AgentWriteResult:
    """Delete an entity of one type on an Airbyte Agents connector.

    This deletes from the upstream system the connector is configured against and cannot be
    undone by PyAirbyte. Which arguments identify an entity is connector-specific, so call
    `describe_agent_connector` first. The connector must belong to the given workspace.
    """
    return _write(
        ctx,
        connector_id=connector_id,
        workspace_id=workspace_id,
        entity_type=entity_type,
        action="delete",
        api_args=api_args,
        intent=intent,
    )


def register_agent_entity_tools(app: FastMCP) -> None:
    """Register the Airbyte Agents entity tools with the FastMCP app."""
    exclude_args = ["workspace_id"] if AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET else None
    if exclude_args:
        _add_defaults_for_exclude_args(exclude_args)
    register_mcp_tools(
        app,
        mcp_module=__name__,
        exclude_args=exclude_args,
    )
