# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Airbyte Agents MCP operations.

.. include:: ../../docs/mcp-generated/agents.md
"""

# No public Python API — MCP primitives are registered via decorators and
# documented via the generated Markdown include above. Setting `__all__` to an
# empty list tells pdoc (and other doc tools) not to surface the individual
# tool / helper definitions as a redundant "API Documentation" list.
__all__: list[str] = []

import json
from typing import Annotated, Any, Literal, get_args

from fastmcp import Context, FastMCP
from fastmcp_extensions import get_mcp_config, mcp_tool, register_mcp_tools
from pydantic import BaseModel, Field

from airbyte.agents.connectors import AgentConnector
from airbyte.agents.organizations import AgentOrganization
from airbyte.agents.workspaces import AgentWorkspace
from airbyte.constants import (
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CLIENT_SECRET_ENV_VAR,
    CLOUD_WORKSPACE_ID_ENV_VAR,
    MCP_BEARER_TOKEN_HEADER,
    MCP_CONFIG_BEARER_TOKEN,
    MCP_CONFIG_CLIENT_ID,
    MCP_CONFIG_CLIENT_SECRET,
    MCP_CONFIG_WORKSPACE_ID,
    MCP_WORKSPACE_ID_HEADER,
)
from airbyte.exceptions import PyAirbyteInputError
from airbyte.mcp._arg_resolvers import resolve_list_of_strings
from airbyte.mcp._tool_utils import AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET
from airbyte.mcp.cloud import _add_defaults_for_exclude_args


AgentReadAction = Literal["list", "get", "search", "api_search"]
"""The connector actions that only read data.

The `download` action is deliberately absent even though it reads: it returns a binary
stream rather than JSON, which PyAirbyte does not yet support.
"""

AgentAction = Literal["list", "get", "search", "api_search", "create", "update", "delete"]
"""Every connector action callable through the MCP layer, including writes."""

AGENTS_AUTH_TIP_TEXT = (
    f"The Airbyte Agents API authenticates with Airbyte Cloud credentials. When connecting "
    f"to a hosted MCP server, provide a bearer token via the `{MCP_BEARER_TOKEN_HEADER}` "
    f"header, or client credentials via the transport `Client-Id` and `Client-Secret` "
    f"headers. For local or stdio connections, set the `{CLOUD_BEARER_TOKEN_ENV_VAR}` "
    f"environment variable, or both `{CLOUD_CLIENT_ID_ENV_VAR}` and "
    f"`{CLOUD_CLIENT_SECRET_ENV_VAR}`. Call `list_agent_connectors` to discover connector "
    f"IDs, then `describe_agent_connector` to learn which entities a connector supports, "
    f"before calling `execute_agent_connector`."
)
WORKSPACE_ID_TIP_TEXT = (
    f"Workspace ID. Hosted MCP connections pass it via the `{MCP_WORKSPACE_ID_HEADER}` "
    f"header; local or stdio connections use the `{CLOUD_WORKSPACE_ID_ENV_VAR}` "
    f"environment variable."
)


class AgentWorkspaceResult(BaseModel):
    """Information about a workspace on the Airbyte Agents platform."""

    workspace_id: str
    """The workspace ID."""

    workspace_name: str | None = None
    """Display name of the workspace."""

    organization_id: str | None = None
    """The organization that owns the workspace, when reported."""


class AgentWorkspaceListResult(BaseModel):
    """Result of listing workspaces on the Airbyte Agents platform."""

    workspaces: list[AgentWorkspaceResult]
    """Workspaces reachable through the Agents API with these credentials."""


class AgentConnectorResult(BaseModel):
    """Information about a connector configured on the Airbyte Agents platform."""

    connector_id: str
    """The connector ID, used as `connector_id` in the other Agents tools."""

    connector_name: str | None = None
    """Display name of the connector."""


class AgentConnectorListResult(BaseModel):
    """Result of listing connectors in an Airbyte Agents workspace."""

    connectors: list[AgentConnectorResult]
    """Connectors configured in the workspace."""


class AgentConnectorDetailsResult(BaseModel):
    """Details about a single Airbyte Agents connector."""

    connector_id: str
    """The connector ID."""

    connector_name: str | None = None
    """Display name of the connector."""

    workspace_id: str | None = None
    """The workspace that owns the connector."""

    source_definition_name: str | None = None
    """The name of the underlying source definition, for example `GitHub`."""

    context_store_entities: list[str]
    """Entities this connector can cache in the Context Store.

    This is not an exhaustive list of executable entities: an entity may be executable via
    `execute_agent_connector` without appearing here.
    """

    warnings: list[str]
    """Warnings the Agents API reported about this connector."""


class AgentExecuteToolResult(BaseModel):
    """Result of executing a single action against an Airbyte Agents connector."""

    status: str
    """The execution status reported by the Agents API, for example `success`."""

    result: Any = None
    """The action's payload. Entity-returning actions put a list of entities here."""

    has_next_page: bool = False
    """Whether the connector reported more entities after this page."""

    end_cursor: str | None = None
    """The cursor to pass as `cursor` to fetch the next page, when one is available."""

    execution_time_ms: int | None = None
    """How long the connector took to execute the action, when reported."""

    warning: dict[str, Any] | None = None
    """A warning reported alongside an otherwise successful result."""


def _resolve_api_args(api_args: dict[str, Any] | str | None) -> dict[str, Any] | None:
    """Resolve `api_args` from a dictionary or a JSON object string."""
    if api_args is None or isinstance(api_args, dict):
        return api_args

    try:
        parsed: Any = json.loads(api_args)
    except json.JSONDecodeError as ex:
        raise PyAirbyteInputError(
            message="The `api_args` string is not valid JSON.",
            guidance="Pass `api_args` as an object, or as a JSON object string.",
        ) from ex

    if not isinstance(parsed, dict):
        raise PyAirbyteInputError(
            message="The `api_args` string is not a JSON object.",
            guidance="Pass `api_args` as an object, or as a JSON object string.",
            context={"parsed_type": type(parsed).__name__},
        )
    return parsed


def _get_agent_organization(ctx: Context, organization_id: str | None) -> AgentOrganization:
    """Build an `AgentOrganization` from MCP config."""
    return AgentOrganization(
        organization_id=organization_id,
        client_id=get_mcp_config(ctx, MCP_CONFIG_CLIENT_ID),
        client_secret=get_mcp_config(ctx, MCP_CONFIG_CLIENT_SECRET),
        bearer_token=get_mcp_config(ctx, MCP_CONFIG_BEARER_TOKEN),
    )


def _get_agent_workspace(ctx: Context, workspace_id: str | None) -> AgentWorkspace:
    """Build an `AgentWorkspace` from MCP config."""
    return AgentWorkspace(
        workspace_id=workspace_id or get_mcp_config(ctx, MCP_CONFIG_WORKSPACE_ID),
        client_id=get_mcp_config(ctx, MCP_CONFIG_CLIENT_ID),
        client_secret=get_mcp_config(ctx, MCP_CONFIG_CLIENT_SECRET),
        bearer_token=get_mcp_config(ctx, MCP_CONFIG_BEARER_TOKEN),
    )


def _get_agent_connector(
    ctx: Context,
    connector_id: str,
    workspace_id: str | None = None,
) -> AgentConnector:
    """Get an `AgentConnector` from its workspace, using MCP config.

    The Agents API addresses a connector by ID alone, but the connector is fetched through
    its workspace anyway, so a connector ID belonging to another workspace raises before
    any action runs.
    """
    return _get_agent_workspace(ctx, workspace_id).get_connector(connector_id)


def _execute(  # noqa: PLR0913  # Mirrors the tool signatures it serves.
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
    read_only: bool | None = None,
) -> AgentExecuteToolResult:
    """Execute one connector action and shape it into an `AgentExecuteToolResult`.

    When `read_only` is `True`, write actions are rejected before any request is sent.
    """
    if read_only and action not in get_args(AgentReadAction):
        raise PyAirbyteInputError(
            message="This action writes data and cannot run in read-only mode.",
            guidance=f"Read-only actions are: {', '.join(get_args(AgentReadAction))}.",
            context={"action": action},
        )

    result = _get_agent_connector(ctx, connector_id, workspace_id).execute(
        entity_type,
        action,
        _resolve_api_args(api_args),
        select_fields=resolve_list_of_strings(select_fields),
        exclude_fields=resolve_list_of_strings(exclude_fields),
        page_size=page_size,
        cursor=cursor,
        intent=intent,
    )
    return AgentExecuteToolResult(
        status=result.status,
        result=result.result,
        has_next_page=result.has_next_page,
        end_cursor=result.end_cursor,
        execution_time_ms=result.execution_metadata.execution_time_ms,
        warning=result.warning,
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def list_agent_workspaces(
    ctx: Context,
    *,
    organization_id: Annotated[
        str | None,
        Field(
            description=(
                "Organization ID to scope the listing to. Omit it when the credentials "
                "belong to exactly one organization."
            ),
            default=None,
        ),
    ],
) -> AgentWorkspaceListResult:
    """List the workspaces reachable through the Airbyte Agents API."""
    organization = _get_agent_organization(ctx, organization_id)
    return AgentWorkspaceListResult(
        workspaces=[
            AgentWorkspaceResult(
                workspace_id=workspace.workspace_id,
                workspace_name=workspace.name,
                organization_id=workspace.organization_id,
            )
            for workspace in organization.list_workspaces()
        ]
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def list_agent_connectors(
    ctx: Context,
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> AgentConnectorListResult:
    """List the connectors configured in an Airbyte Agents workspace."""
    workspace = _get_agent_workspace(ctx, workspace_id)
    return AgentConnectorListResult(
        connectors=[
            AgentConnectorResult(
                connector_id=connector.connector_id,
                connector_name=connector.name,
            )
            for connector in workspace.list_connectors()
        ]
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def describe_agent_connector(
    ctx: Context,
    connector_id: Annotated[
        str,
        Field(description="The ID of the Airbyte Agents connector."),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> AgentConnectorDetailsResult:
    """Describe an Airbyte Agents connector, including its Context Store entities.

    Call this before `execute_agent_connector` to learn what the connector exposes. The
    connector must belong to the given workspace.
    """
    details = _get_agent_connector(ctx, connector_id, workspace_id).describe()
    return AgentConnectorDetailsResult(
        connector_id=details.connector_id,
        connector_name=details.name,
        workspace_id=details.workspace_id,
        source_definition_name=details.source_definition_name,
        context_store_entities=details.context_store_entities,
        warnings=[str(warning) for warning in details.warnings],
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def execute_agent_connector_ro(  # noqa: PLR0913  # Explicit args are the point of this tool.
    ctx: Context,
    connector_id: Annotated[
        str,
        Field(description="The ID of the Airbyte Agents connector."),
    ],
    entity_type: Annotated[
        str,
        Field(
            description=(
                "The type of entity to act on, for example 'issues'. Call "
                "`describe_agent_connector` to see the entity types a connector supports."
            ),
        ),
    ],
    action: Annotated[
        AgentReadAction,
        Field(description="The read action to run against the entity type."),
    ],
    api_args: Annotated[
        dict[str, Any] | str | None,
        Field(
            description=(
                "Connector-specific arguments for the action, as an object or a JSON "
                "object string. For example {'repository': 'airbytehq/PyAirbyte'}."
            ),
            default=None,
        ),
    ],
    *,
    select_fields: Annotated[
        list[str] | str | None,
        Field(
            description="Fields to keep in the response, as a list or a CSV string.",
            default=None,
        ),
    ],
    exclude_fields: Annotated[
        list[str] | str | None,
        Field(
            description="Fields to drop from the response, as a list or a CSV string.",
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
        Field(
            description="A short description of why the action is being run.",
            default=None,
        ),
    ],
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> AgentExecuteToolResult:
    """Read data from an Airbyte Agents connector, without modifying anything.

    This tool only accepts read actions, so it stays available in read-only mode. Use
    `execute_agent_connector` for actions that create, update, or delete data. Entity types
    are connector-specific, so call `describe_agent_connector` first. The connector must
    belong to the given workspace.
    """
    return _execute(
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


@mcp_tool(
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def execute_agent_connector(  # noqa: PLR0913  # Explicit args are the point of this tool.
    ctx: Context,
    connector_id: Annotated[
        str,
        Field(description="The ID of the Airbyte Agents connector."),
    ],
    entity_type: Annotated[
        str,
        Field(
            description=(
                "The type of entity to act on, for example 'issues'. Call "
                "`describe_agent_connector` to see the entity types a connector supports."
            ),
        ),
    ],
    action: Annotated[
        AgentAction,
        Field(description="The action to run against the entity type."),
    ],
    api_args: Annotated[
        dict[str, Any] | str | None,
        Field(
            description=(
                "Connector-specific arguments for the action, as an object or a JSON "
                "object string. For example {'repository': 'airbytehq/PyAirbyte'}."
            ),
            default=None,
        ),
    ],
    *,
    select_fields: Annotated[
        list[str] | str | None,
        Field(
            description="Fields to keep in the response, as a list or a CSV string.",
            default=None,
        ),
    ],
    exclude_fields: Annotated[
        list[str] | str | None,
        Field(
            description="Fields to drop from the response, as a list or a CSV string.",
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
        Field(
            description="A short description of why the action is being run.",
            default=None,
        ),
    ],
    read_only: Annotated[
        bool | None,
        Field(
            description=(
                "Set to `true` to reject write actions before any request is sent, when the "
                "caller wants a read guarantee from this tool."
            ),
            default=None,
        ),
    ],
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> AgentExecuteToolResult:
    """Execute a single action against an Airbyte Agents connector, including writes.

    Prefer `execute_agent_connector_ro` when only reading, since it is available in
    read-only mode. Entity types and actions are connector-specific, so call
    `describe_agent_connector` first. The connector must belong to the given workspace.
    """
    return _execute(
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
        read_only=read_only,
    )


def register_agents_tools(app: FastMCP) -> None:
    """Register the Airbyte Agents tools with the FastMCP app."""
    exclude_args = ["workspace_id"] if AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET else None
    if exclude_args:
        _add_defaults_for_exclude_args(exclude_args)
    register_mcp_tools(
        app,
        mcp_module=__name__,
        exclude_args=exclude_args,
    )
