# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Airbyte Agents MCP operations.

> ## ⚠️ Experimental Tools — Insiders Only
>
> **The Airbyte Agents MCP tools are experimental and hidden by default.** They are advertised
> only when insiders mode is enabled (`AIRBYTE_MCP_INSIDERS` for stdio servers, `X-MCP-Insiders`
> for hosted servers) or when the include-modules setting explicitly names `agents`. Tool names,
> arguments, and result shapes may change or be removed without notice between minor versions of
> PyAirbyte. Pin an exact PyAirbyte version if you depend on them.
>
> These tools are also Cloud-only: they are hidden whenever
> `AIRBYTE_CLOUD_API_URL` / `AIRBYTE_CLOUD_CONFIG_API_URL` are overridden, unless
> `AIRBYTE_AGENTS_API_URL` is set.

.. include:: ../../docs/mcp-generated/agents.md
"""

# No public Python API — MCP primitives are registered via decorators and
# documented via the generated Markdown include above. Setting `__all__` to an
# empty list tells pdoc (and other doc tools) not to surface the individual
# tool / helper definitions as a redundant "API Documentation" list.
__all__: list[str] = []

import json
from http import HTTPStatus
from typing import Annotated, Any, Literal

from fastmcp import Context, FastMCP
from fastmcp_extensions import get_mcp_config, mcp_tool, register_mcp_tools
from pydantic import BaseModel, Field

from airbyte.agents._destination_docs import (
    SQL_PASSTHROUGH_DESTINATION_DEFINITION_IDS,
    SQL_PASSTHROUGH_DESTINATION_DIALECTS,
    build_destination_connector_details,
    build_destination_skill_docs,
    connector_id_from_skill_id,
)
from airbyte.agents.connectors import AgentAction, AgentConnector, AgentReadAction
from airbyte.agents.models import AgentSkillInfo
from airbyte.agents.organizations import AgentOrganization
from airbyte.agents.workspaces import AgentWorkspace
from airbyte.cloud.connectors import CloudDestination, CloudSource
from airbyte.constants import (
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CLIENT_SECRET_ENV_VAR,
    CLOUD_ORGANIZATION_ID_ENV_VAR,
    CLOUD_WORKSPACE_ID_ENV_VAR,
    MCP_BEARER_TOKEN_HEADER,
    MCP_CONFIG_API_URL,
    MCP_CONFIG_BEARER_TOKEN,
    MCP_CONFIG_CLIENT_ID,
    MCP_CONFIG_CLIENT_SECRET,
    MCP_CONFIG_CONFIG_API_URL,
    MCP_CONFIG_ORGANIZATION_ID,
    MCP_CONFIG_WORKSPACE_ID,
    MCP_ORGANIZATION_ID_HEADER,
    MCP_WORKSPACE_ID_HEADER,
)
from airbyte.exceptions import AirbyteError, PyAirbyteInputError
from airbyte.mcp._arg_resolvers import resolve_list_of_strings
from airbyte.mcp._tool_utils import AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET
from airbyte.mcp.cloud import (
    _add_defaults_for_exclude_args,
    _get_cloud_client,
    _get_cloud_workspace,
)


AGENTS_AUTH_TIP_TEXT = (
    f"The Airbyte Agents API authenticates with Airbyte Cloud credentials. When connecting "
    f"to a hosted MCP server, provide a bearer token via the `{MCP_BEARER_TOKEN_HEADER}` "
    f"header, or client credentials via the transport `Client-Id` and `Client-Secret` "
    f"headers. For local or stdio connections, set the `{CLOUD_BEARER_TOKEN_ENV_VAR}` "
    f"environment variable, or both `{CLOUD_CLIENT_ID_ENV_VAR}` and "
    f"`{CLOUD_CLIENT_SECRET_ENV_VAR}`. Call `list_agent_connectors` to discover connector "
    f"IDs, then `inspect_agent_connector` to learn which entities a connector supports, "
    f"before calling `execute_agent_connector`. Use `list_agent_skills` or "
    f"`search_agent_skills` to discover skills, and pass a `docs_skill_id` reported by "
    f"`inspect_agent_connector` to `read_agent_skill_docs` for connector usage docs."
)
WORKSPACE_ID_TIP_TEXT = (
    f"Workspace ID. Hosted MCP connections pass it via the `{MCP_WORKSPACE_ID_HEADER}` "
    f"header; local or stdio connections use the `{CLOUD_WORKSPACE_ID_ENV_VAR}` "
    f"environment variable."
)
ORGANIZATION_ID_TIP_TEXT = (
    f"Organization ID. Omit it when the credentials belong to exactly one organization, or "
    f"when it is already configured via the "
    f"`{MCP_ORGANIZATION_ID_HEADER}` header or the `{CLOUD_ORGANIZATION_ID_ENV_VAR}` "
    f"environment variable. To discover organization IDs, call `list_cloud_organizations` "
    f"to search organizations by name, or `list_agent_workspaces`, which reports the owning "
    f"organization of each workspace. Workspace-scoped tools derive it from the workspace's "
    f"parent organization when omitted."
)
LIST_WORKSPACES_ORGANIZATION_ID_TIP_TEXT = (
    f"Organization ID. Required when the credentials belong to more than one organization; "
    f"the Agents API rejects the call with HTTP 400 otherwise. Omit it when the credentials "
    f"belong to exactly one organization or when it is already configured via the "
    f"`{MCP_ORGANIZATION_ID_HEADER}` header or the `{CLOUD_ORGANIZATION_ID_ENV_VAR}` "
    f"environment variable. Discover organization IDs with `list_cloud_organizations`."
)

AGENTS_ACCESS_DENIED_STATUS = "access_denied"
"""The `status` reported when the Agents API refused the request."""

AGENTS_UNAUTHORIZED_MESSAGE = (
    "The Airbyte Agents API rejected these credentials. Verify the Airbyte Cloud "
    "credentials, or ask the user for valid ones."
)
AGENTS_FORBIDDEN_MESSAGE = (
    "The Airbyte Agents API authenticated these credentials but denied access. Either the "
    "organization does not have an Airbyte Agents subscription, or these credentials lack "
    "access to this workspace. Ask the user to confirm which applies rather than retrying."
)
"""Fallback explanation for a 403 whose response body carries no `message`/`detail`."""

AGENTS_ACTOR_NOT_ENABLED_DETAIL = "Actor is not enabled for Agents access."
"""The Agents API `detail` when a source or destination exists but is not toggled on."""

AGENTS_ENABLE_ACTOR_GUIDANCE = (
    "The connector or destination exists in the Airbyte Cloud workspace but has not been "
    "enabled for Agents access. An organization admin must enable it in Airbyte Cloud under "
    "Settings -> Context layer, by selecting the workspace and turning on the toggle for that "
    "source or destination. Do not retry until the user confirms it has been enabled."
)
AGENTS_DESTINATION_ACCESS_NOTE = (
    'Query with `execute_agent_connector_ro` and `action="sql_select"` only; `inspect` returns '
    "built-in docs and `SHOW TABLES` / `DESCRIBE TABLE` discover tables and columns. If "
    "`sql_select` returns `access_denied`, an organization admin must enable the destination "
    "in Airbyte Cloud under Settings -> Context layer (workspace -> Destinations toggle)."
)
"""Attached to destinations in `list_agent_connectors`, whose enabled state is not exposed."""


CONNECTOR_NOT_FOUND_MESSAGE = "No connector found with the given ID or name."
"""The `AgentWorkspace.get_connector` message when the Agents API does not list the ID."""

CONTEXT_LAYER_ENABLE_GUIDANCE = (
    "Agents access cannot be enabled from this tool. Ask the user to have an Airbyte Cloud "
    "organization admin enable it in the Airbyte Cloud webapp under Organization settings -> "
    "Context layer (or from the connector's own settings page), then retry."
)
"""How the human, not the agent, turns on Agents access for an organization or connector."""

AGENTS_NO_CONNECTORS_ENABLED_MESSAGE = (
    "No connectors in this workspace are enabled for Agents access, so there is nothing to "
    f"list, inspect, or execute. {CONTEXT_LAYER_ENABLE_GUIDANCE}"
)
"""For a workspace whose Cloud sources exist but none are enabled for Agents access."""

AGENTS_WORKSPACE_HAS_NO_SOURCES_MESSAGE = (
    "This Airbyte Cloud workspace has no source connectors, so there is nothing to enable for "
    "Agents access. Ask the user to create a source in the Airbyte Cloud webapp first, then "
    "have an organization admin enable it for Agents access under Organization settings -> "
    "Context layer."
)
"""For a workspace with no Cloud sources at all; enablement guidance alone cannot help."""


class _ConnectorNotEnabledError(AirbyteError):
    """A connector exists in the Cloud workspace but is not enabled for Agents access."""


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

    message: str | None = None
    """Why the listing is empty, when the Agents API denied the request."""


class AgentConnectorResult(BaseModel):
    """Information about a connector configured on the Airbyte Agents platform."""

    connector_id: str
    """The connector ID, used as `connector_id` in the other Agents tools."""

    connector_name: str | None = None
    """Display name of the connector."""

    connector_kind: Literal["source", "destination"] = "source"
    """`source` for Agents API connectors; `destination` for SQL passthrough destinations."""

    supported_actions: list[str] | None = None
    """Actions the connector supports, when limited. Destinations support only `sql_select`."""

    sql_dialect: str | None = None
    """The `sql_dialect` to pass in `api_args` for `sql_select`. Destinations only."""

    note: str | None = None
    """How to use this connector, when it differs from the standard Agents flow."""


class AgentConnectorListResult(BaseModel):
    """Result of listing connectors in an Airbyte Agents workspace."""

    connectors: list[AgentConnectorResult]
    """Connectors configured in the workspace."""

    message: str | None = None
    """Why the listing is empty, when the Agents API denied the request."""


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

    docs_skill_id: str | None = None
    """Skill ID for this connector's usage docs, when reported by the Agents API."""

    context_store_entities: list[str]
    """Entities this connector can cache in the Context Store.

    This is not an exhaustive list of executable entities: an entity may be executable via
    `execute_agent_connector` without appearing here.
    """

    warnings: list[str]
    """Warnings the Agents API reported about this connector."""

    message: str | None = None
    """Why the details are empty, when the Agents API denied the request."""


class AgentSkillResult(BaseModel):
    """A skill discoverable on the Airbyte Agents platform."""

    skill_id: str
    """The skill ID, used as `skill_id` in `read_agent_skill_docs`."""

    kind: str | None = None
    """The skill category, for example `static` or `connector_source`."""

    title: str | None = None
    """The human-readable skill title."""

    summary: str | None = None
    """A short summary of what the skill documents."""

    tags: list[str]
    """Search and categorization tags for the skill."""


class AgentSkillListResult(BaseModel):
    """Result of listing or searching skills on the Airbyte Agents platform."""

    skills: list[AgentSkillResult]
    """Skills matching the listing or search, across all pages."""

    message: str | None = None
    """Why the listing is empty, when the Agents API denied the request."""


class AgentSkillSectionResult(BaseModel):
    """A section of a skill's docs, as listed in the docs outline."""

    section_id: str
    """The section ID, passed as `section` to `read_agent_skill_docs`."""

    title: str | None = None
    """The human-readable section title."""

    summary: str | None = None
    """A short summary of the section content."""

    available: bool = True
    """Whether this section can currently be read."""


class AgentSkillDocsResult(BaseModel):
    """Documentation for a single skill on the Airbyte Agents platform."""

    skill_id: str
    """The skill ID that was read."""

    title: str | None = None
    """The human-readable skill title."""

    section_id: str | None = None
    """The section that was read, or `None` for the default docs response."""

    outline: list[AgentSkillSectionResult]
    """The sections available for this skill."""

    content: list[dict[str, Any]]
    """Rendered docs content blocks, such as headings, paragraphs, and code blocks."""

    warnings: list[str]
    """Non-fatal issues reported while building or reading the docs."""

    message: str | None = None
    """Why the docs are empty, when the Agents API denied the request."""


class AgentExecuteToolResult(BaseModel):
    """Result of executing a single action against an Airbyte Agents connector."""

    status: str
    """The execution status reported by the Agents API, for example `success`."""

    result: Any = None
    """The action's payload. Entity-returning actions put a list of entities here."""

    has_next_page: bool = False
    """Whether the connector reported more entities after this page."""

    end_cursor: str | None = None
    """The cursor for the next page, when one is available. Pass it as `cursor` for Context Store
    `search`, or as the connector's own cursor argument in `api_args` for direct connector
    actions."""

    execution_time_ms: int | None = None
    """How long the connector took to execute the action, when reported."""

    warning: dict[str, Any] | None = None
    """A warning reported alongside an otherwise successful result."""

    message: str | None = None
    """Why the action did not run, when the Agents API denied the request."""


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


def _agents_access_message(error: AirbyteError) -> str | None:
    """Return a concise explanation of an Agents API authorization failure.

    Returns `None` when the failure is not an authorization failure, so the caller can
    re-raise it with a bare `raise` and keep the original traceback.
    """
    if isinstance(error, _ConnectorNotEnabledError):
        return error.get_message()
    context = error.context or {}
    status_code = context.get("status_code")
    if status_code == HTTPStatus.UNAUTHORIZED:
        return AGENTS_UNAUTHORIZED_MESSAGE
    if status_code == HTTPStatus.FORBIDDEN:
        detail = _agents_error_detail(context.get("response_text"))
        if detail is None:
            return AGENTS_FORBIDDEN_MESSAGE
        if detail == AGENTS_ACTOR_NOT_ENABLED_DETAIL:
            return f"{detail} {AGENTS_ENABLE_ACTOR_GUIDANCE}"
        return f"The Airbyte Agents API denied access: {detail}"
    return None


def _agents_error_detail(response_text: object) -> str | None:
    """Extract the human-readable reason from an Agents API error body.

    The API wraps `HTTPException.detail` as `{"message": ..., "errors": [{"message": ...}]}`;
    plain FastAPI bodies use `{"detail": ...}`. Returns `None` for anything else, including
    non-JSON bodies, so the caller falls back to the generic explanation.
    """
    if not isinstance(response_text, str) or not response_text.strip():
        return None
    try:
        body = json.loads(response_text)
    except json.JSONDecodeError:
        return None
    if not isinstance(body, dict):
        return None
    for key in ("message", "detail"):
        value = body.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    errors = body.get("errors")
    if isinstance(errors, list) and errors and isinstance(errors[0], dict):
        value = errors[0].get("message")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _is_not_found(error: AirbyteError) -> bool:
    """Return whether the Agents API reported the connector or skill as not found."""
    return (error.context or {}).get("status_code") == HTTPStatus.NOT_FOUND


def _resolve_cloud_destination(
    ctx: Context,
    connector_id: str,
    workspace_id: str | None = None,
    organization_id: str | None = None,
) -> CloudDestination | None:
    """Look a connector ID up in the Cloud workspace's destinations.

    Destinations are targets of `sql_select` and are not listed by the Agents API. Returns
    `None` when the ID does not match any destination in the workspace; `list_destinations`
    is used so a bogus ID does not raise on lazy fetch.
    """
    workspace = _get_agent_workspace(ctx, workspace_id, organization_id)
    for destination in _get_cloud_workspace(ctx, workspace.workspace_id).list_destinations():
        if destination.connector_id == connector_id:
            return destination
    return None


def _resolve_cloud_source(
    ctx: Context,
    connector_id: str,
    workspace_id: str,
) -> CloudSource | None:
    """Look a connector ID up in the Cloud workspace's sources.

    Returns `None` when the ID does not match any source in the workspace.
    """
    for source in _get_cloud_workspace(ctx, workspace_id).list_sources():
        if source.connector_id == connector_id:
            return source
    return None


def _source_not_enabled_message(source: CloudSource, workspace_id: str) -> str:
    """Explain that a Cloud source exists but is not enabled for Agents access."""
    return (
        f"Source '{source.name}' ({source.connector_id}) exists in Airbyte Cloud "
        f"workspace {workspace_id} but is not enabled for Agents access. "
        f"{CONTEXT_LAYER_ENABLE_GUIDANCE}"
    )


def _connector_unavailable_error(
    ctx: Context,
    connector_id: str,
    workspace_id: str,
) -> AirbyteError:
    """Build the error for a connector ID the Agents API does not list.

    A `_ConnectorNotEnabledError` when the ID is a source in the Cloud workspace, so the
    caller can report the disabled state instead of a bare miss; otherwise a not-found
    error pointing at `list_agent_connectors`.
    """
    source = _resolve_cloud_source(ctx, connector_id, workspace_id)
    if source is not None:
        return _ConnectorNotEnabledError(
            message=_source_not_enabled_message(source, workspace_id),
            context={"connector_id": connector_id, "workspace_id": workspace_id},
        )
    return AirbyteError(
        message=CONNECTOR_NOT_FOUND_MESSAGE,
        guidance=(
            "Use `list_agent_connectors` to see the connectors enabled for Agents access in "
            "this workspace."
        ),
        context={"connector_id": connector_id, "workspace_id": workspace_id},
    )


def _inspect_destination_fallback(
    ctx: Context,
    connector_id: str,
    workspace_id: str | None = None,
    organization_id: str | None = None,
) -> AgentConnectorDetailsResult:
    """Build an inspect result for a connector ID the Agents API returned 404 for.

    SQL passthrough destinations get built-in details; anything else gets a message
    instead of an error.
    """
    resolved_workspace_id = _get_agent_workspace(ctx, workspace_id, organization_id).workspace_id
    destination = _resolve_cloud_destination(ctx, connector_id, workspace_id, organization_id)
    if (
        destination is not None
        and destination.definition_id in SQL_PASSTHROUGH_DESTINATION_DEFINITION_IDS
    ):
        details = build_destination_connector_details(destination)
        return AgentConnectorDetailsResult(
            connector_id=details.connector_id,
            connector_name=details.name,
            workspace_id=details.workspace_id,
            docs_skill_id=details.docs_skill_id,
            context_store_entities=[],
            warnings=[],
        )
    if destination is not None:
        message = (
            f"Destination '{destination.name}' (definition {destination.definition_id}) is not "
            "a SQL passthrough destination; only Snowflake and BigQuery destinations support "
            "`sql_select`."
        )
    elif (source := _resolve_cloud_source(ctx, connector_id, resolved_workspace_id)) is not None:
        message = _source_not_enabled_message(source, resolved_workspace_id)
    else:
        message = (
            f"Connector {connector_id} was not found in the Agents API and is not a source or "
            f"destination in workspace {resolved_workspace_id}. Use `list_agent_connectors` to "
            "see the connectors enabled for Agents access, or "
            f"`read_agent_skill_docs(skill_id='connector-source:{connector_id}')`."
        )
    return AgentConnectorDetailsResult(
        connector_id=connector_id,
        context_store_entities=[],
        warnings=[],
        message=message,
    )


def _destination_skill_docs_fallback(
    ctx: Context,
    skill_id: str,
    section: str | None,
    workspace_id: str | None = None,
) -> AgentSkillDocsResult:
    """Build a skill docs result for a skill ID the Agents API returned 404 for."""
    connector_id = connector_id_from_skill_id(skill_id)
    resolved_workspace_id = _get_agent_workspace(ctx, workspace_id).workspace_id
    destination = _resolve_cloud_destination(ctx, connector_id, workspace_id)
    if (
        destination is not None
        and destination.definition_id in SQL_PASSTHROUGH_DESTINATION_DEFINITION_IDS
    ):
        docs = build_destination_skill_docs(destination, section=section)
        return AgentSkillDocsResult(
            skill_id=docs.metadata.id,
            title=docs.metadata.title,
            section_id=docs.section_id,
            outline=[
                AgentSkillSectionResult(
                    section_id=docs_section.id,
                    title=docs_section.title,
                    summary=docs_section.summary,
                    available=docs_section.available,
                )
                for docs_section in docs.outline
            ],
            content=docs.content,
            warnings=[str(warning) for warning in docs.metadata.warnings],
        )
    if destination is not None:
        message = (
            f"Destination '{destination.name}' (definition {destination.definition_id}) is not "
            "a SQL passthrough destination; only Snowflake and BigQuery destinations support "
            "`sql_select`."
        )
    else:
        message = (
            f"Skill {skill_id} was not found in the Agents API and connector {connector_id} "
            f"is not a destination in workspace {resolved_workspace_id}."
        )
    return AgentSkillDocsResult(
        skill_id=skill_id,
        section_id=section,
        outline=[],
        content=[],
        warnings=[],
        message=message,
    )


def _get_agent_organization(ctx: Context, organization_id: str | None) -> AgentOrganization:
    """Build an `AgentOrganization` from MCP config."""
    return AgentOrganization(
        organization_id=organization_id or get_mcp_config(ctx, MCP_CONFIG_ORGANIZATION_ID),
        client_id=get_mcp_config(ctx, MCP_CONFIG_CLIENT_ID),
        client_secret=get_mcp_config(ctx, MCP_CONFIG_CLIENT_SECRET),
        bearer_token=get_mcp_config(ctx, MCP_CONFIG_BEARER_TOKEN),
        public_api_root=get_mcp_config(ctx, MCP_CONFIG_API_URL),
        config_api_root=get_mcp_config(ctx, MCP_CONFIG_CONFIG_API_URL),
    )


def _get_agent_workspace(
    ctx: Context,
    workspace_id: str | None,
    organization_id: str | None = None,
) -> AgentWorkspace:
    """Build an `AgentWorkspace`, deriving an absent organization from its workspace."""
    resolved_workspace_id = workspace_id or get_mcp_config(ctx, MCP_CONFIG_WORKSPACE_ID)
    resolved_organization_id = organization_id or get_mcp_config(ctx, MCP_CONFIG_ORGANIZATION_ID)
    if not resolved_workspace_id or not resolved_organization_id:
        client = _get_cloud_client(ctx)
        if not resolved_workspace_id:
            resolved_workspace_id = client.resolve_default_workspace_id()
        if resolved_workspace_id and not resolved_organization_id:
            resolved_organization_id = client.get_workspace_parent_organization_id(
                resolved_workspace_id
            )
    return AgentWorkspace(
        workspace_id=resolved_workspace_id,
        organization_id=resolved_organization_id,
        client_id=get_mcp_config(ctx, MCP_CONFIG_CLIENT_ID),
        client_secret=get_mcp_config(ctx, MCP_CONFIG_CLIENT_SECRET),
        bearer_token=get_mcp_config(ctx, MCP_CONFIG_BEARER_TOKEN),
        public_api_root=get_mcp_config(ctx, MCP_CONFIG_API_URL),
        config_api_root=get_mcp_config(ctx, MCP_CONFIG_CONFIG_API_URL),
    )


def _get_agent_connector(
    ctx: Context,
    connector_id: str,
    workspace_id: str | None = None,
    organization_id: str | None = None,
) -> AgentConnector:
    """Get an `AgentConnector` from its workspace, using MCP config.

    The Agents API addresses a connector by ID alone, but the connector is fetched through
    its workspace anyway, so a connector ID belonging to another workspace raises before
    any action runs.

    Destination connectors (targets of `sql_select`) are not listed by the Agents API, so IDs it
    does not know are verified against the Cloud workspace's destinations instead. An ID that
    is a Cloud source the Agents API does not list is reported as not enabled for Agents
    access, rather than as missing.
    """
    workspace = _get_agent_workspace(ctx, workspace_id, organization_id)
    try:
        return workspace.get_connector(connector_id)
    except AirbyteError as error:
        if error.get_message() != CONNECTOR_NOT_FOUND_MESSAGE:
            raise
        cloud_destination_ids = {
            destination.connector_id
            for destination in _get_cloud_workspace(ctx, workspace.workspace_id).list_destinations()
        }
        if connector_id not in cloud_destination_ids:
            raise _connector_unavailable_error(ctx, connector_id, workspace.workspace_id) from error
        return workspace.get_connector(connector_id=connector_id)


def _execute(  # noqa: PLR0913  # Mirrors the tool signatures it serves.
    ctx: Context,
    *,
    connector_id: str,
    workspace_id: str | None,
    organization_id: str | None,
    entity_type: str,
    action: AgentAction,
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
    if read_only and action not in set(AgentReadAction):
        raise PyAirbyteInputError(
            message="This action writes data and cannot run in read-only mode.",
            guidance=(
                "Read-only actions are: "
                f"{', '.join(member.value for member in AgentReadAction)}."
            ),
            context={"action": action},
        )

    try:
        result = _get_agent_connector(
            ctx=ctx,
            connector_id=connector_id,
            workspace_id=workspace_id,
            organization_id=organization_id,
        ).execute(
            entity_type=entity_type,
            action=action,
            api_args=_resolve_api_args(api_args),
            select_fields=resolve_list_of_strings(select_fields),
            exclude_fields=resolve_list_of_strings(exclude_fields),
            page_size=page_size,
            cursor=cursor,
            workspace_id=workspace_id,
            intent=intent,
        )
    except AirbyteError as error:
        message = _agents_access_message(error)
        if message is None:
            raise
        return AgentExecuteToolResult(
            status=AGENTS_ACCESS_DENIED_STATUS,
            message=message,
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
            description=LIST_WORKSPACES_ORGANIZATION_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> AgentWorkspaceListResult:
    """List the workspaces reachable through the Airbyte Agents API.

    An organization ID is required when the credentials belong to more than one organization.
    """
    organization = _get_agent_organization(ctx, organization_id)
    try:
        workspaces = organization.list_workspaces()
    except AirbyteError as error:
        message = _agents_access_message(error)
        if message is None:
            raise
        return AgentWorkspaceListResult(workspaces=[], message=message)

    return AgentWorkspaceListResult(
        workspaces=[
            AgentWorkspaceResult(
                workspace_id=workspace.workspace_id,
                workspace_name=workspace.name,
                organization_id=workspace.organization_id,
            )
            for workspace in workspaces
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
    organization_id: Annotated[
        str | None,
        Field(
            description=ORGANIZATION_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> AgentConnectorListResult:
    """List the connectors configured in an Airbyte Agents workspace.

    Sources come from the Agents API. SQL passthrough destinations (Snowflake, BigQuery) in
    the Cloud workspace are appended with `connector_kind="destination"`; they support only
    the `sql_select` action of `execute_agent_connector_ro`.
    """
    workspace = _get_agent_workspace(ctx, workspace_id, organization_id)
    try:
        connectors = workspace.list_connectors()
    except AirbyteError as error:
        message = _agents_access_message(error)
        if message is None:
            raise
        return AgentConnectorListResult(connectors=[], message=message)

    results = [
        AgentConnectorResult(
            connector_id=connector.connector_id,
            connector_name=connector.name,
        )
        for connector in connectors
    ]
    results.extend(
        _destination_connector_result(destination)
        for destination in _list_sql_passthrough_destinations(ctx, workspace.workspace_id)
    )
    if not results:
        return AgentConnectorListResult(
            connectors=[],
            message=_empty_connector_list_message(ctx, workspace.workspace_id),
        )
    return AgentConnectorListResult(connectors=results)


def _empty_connector_list_message(ctx: Context, workspace_id: str) -> str:
    """Explain an empty Agents connector list.

    Enablement guidance only helps when the Cloud workspace actually has sources; an empty
    Cloud workspace needs a source created first.
    """
    if _get_cloud_workspace(ctx, workspace_id).list_sources():
        return AGENTS_NO_CONNECTORS_ENABLED_MESSAGE
    return AGENTS_WORKSPACE_HAS_NO_SOURCES_MESSAGE


def _list_sql_passthrough_destinations(
    ctx: Context,
    workspace_id: str,
) -> list[CloudDestination]:
    """Return the Cloud workspace's destinations that `sql_select` can query."""
    return [
        destination
        for destination in _get_cloud_workspace(ctx, workspace_id).list_destinations()
        if destination.definition_id in SQL_PASSTHROUGH_DESTINATION_DEFINITION_IDS
    ]


def _destination_connector_result(destination: CloudDestination) -> AgentConnectorResult:
    """Describe a SQL passthrough destination as a `sql_select`-only connector."""
    return AgentConnectorResult(
        connector_id=destination.connector_id,
        connector_name=destination.name,
        connector_kind="destination",
        supported_actions=["sql_select"],
        sql_dialect=SQL_PASSTHROUGH_DESTINATION_DIALECTS[destination.definition_id],
        note=AGENTS_DESTINATION_ACCESS_NOTE,
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def inspect_agent_connector(
    ctx: Context,
    connector_id: Annotated[
        str,
        Field(
            description=(
                "The ID of a supported Airbyte source or destination with agent features enabled."
            ),
        ),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
    organization_id: Annotated[
        str | None,
        Field(
            description=ORGANIZATION_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> AgentConnectorDetailsResult:
    """Inspect an Airbyte Agents connector: metadata, readiness, warnings, and `docs_skill_id`.

    Call this before `execute_agent_connector` to learn what the connector exposes.
    Airbyte Cloud destinations in the workspace are also accepted and resolve to
    built-in docs under `connector-destination:<id>`. The reported `docs_skill_id`
    can be passed to `read_agent_skill_docs` to read the connector's usage docs.
    """
    try:
        workspace = _get_agent_workspace(ctx, workspace_id, organization_id)
        details = workspace.get_connector(connector_id).inspect()
    except AirbyteError as error:
        if _is_not_found(error) or error.get_message() == CONNECTOR_NOT_FOUND_MESSAGE:
            return _inspect_destination_fallback(ctx, connector_id, workspace_id, organization_id)
        message = _agents_access_message(error)
        if message is None:
            raise
        return AgentConnectorDetailsResult(
            connector_id=connector_id,
            context_store_entities=[],
            warnings=[],
            message=message,
        )

    return AgentConnectorDetailsResult(
        connector_id=details.connector_id,
        connector_name=details.name,
        workspace_id=details.workspace_id,
        source_definition_name=details.source_definition_name,
        docs_skill_id=details.docs_skill_id,
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
        Field(
            description=(
                "The ID of the Airbyte Agents connector, from `list_agent_connectors`. For "
                "`sql_select`, pass the `connector_id` of a destination entry "
                '(`connector_kind="destination"`) from that listing.'
            ),
        ),
    ],
    entity_type: Annotated[
        str,
        Field(
            description=(
                "The type of entity to act on, for example 'issues'. Call "
                "`inspect_agent_connector` to see the entity types a connector supports."
            ),
        ),
    ],
    action: Annotated[
        AgentReadAction,
        Field(
            description=(
                "The read action to run against the entity type. "
                "The `search` action is the connector's native API search, parallel to `get` "
                "and `list`. "
                "For `sql_select`, pass `sql` and `sql_dialect` (snowflake or bigquery) in "
                "`api_args` and any value for `entity_type`; the `connector_id` is a "
                "destination listed by `list_agent_connectors`, and `SHOW TABLES` / `DESCRIBE "
                "TABLE <name>` discover its tables and columns. The `download` action "
                "is deliberately absent because it returns a binary stream rather than JSON."
            ),
        ),
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
        Field(
            description=(
                "Maximum number of entities to return in this page. Honored by Context Store "
                "`search` actions (sent as `limit`). Direct connector actions take their own "
                "page-size argument, if any, in `api_args`."
            ),
            default=None,
        ),
    ],
    cursor: Annotated[
        str | None,
        Field(
            description=(
                "Pagination cursor for Context Store `search` actions and `sql_select`, taken "
                "from `end_cursor` of a previous result. Direct connector "
                "actions such as `list` do not read this; pass their own cursor argument in "
                "`api_args` instead (for example GitHub's `after`), as named in the skill docs."
            ),
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
    organization_id: Annotated[
        str | None,
        Field(
            description=ORGANIZATION_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> AgentExecuteToolResult:
    """Read data from an Airbyte Agents connector, without modifying anything.

    This tool only accepts read actions, so it stays available in read-only mode. Use
    `execute_agent_connector` for actions that create, update, or delete data. Entity types
    are connector-specific, so call `inspect_agent_connector` first. The connector must
    belong to the given workspace.

    To query a destination, use `action="sql_select"` with the destination's `connector_id`
    and `sql_dialect` as reported by `list_agent_connectors`. Start with `SHOW TABLES` and
    `DESCRIBE TABLE <name>` to discover tables and columns before selecting data.
    """
    return _execute(
        ctx,
        connector_id=connector_id,
        workspace_id=workspace_id,
        organization_id=organization_id,
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
                "`inspect_agent_connector` to see the entity types a connector supports."
            ),
        ),
    ],
    action: Annotated[
        AgentAction,
        Field(
            description=(
                "The action to run against the entity type. "
                "The `search` action is the connector's native API search, parallel to `get` "
                "and `list`. "
                "For `sql_select`, pass `sql` and `sql_dialect` (snowflake or bigquery) in "
                "`api_args` and any value for `entity_type`. The `download` action "
                "is deliberately absent because it returns a binary stream rather than JSON."
            ),
        ),
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
        Field(
            description=(
                "Maximum number of entities to return in this page. Honored by Context Store "
                "`search` actions (sent as `limit`). Direct connector actions take their own "
                "page-size argument, if any, in `api_args`."
            ),
            default=None,
        ),
    ],
    cursor: Annotated[
        str | None,
        Field(
            description=(
                "Pagination cursor for Context Store `search` actions and `sql_select`, taken "
                "from `end_cursor` of a previous result. Direct connector "
                "actions such as `list` do not read this; pass their own cursor argument in "
                "`api_args` instead (for example GitHub's `after`), as named in the skill docs."
            ),
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
    organization_id: Annotated[
        str | None,
        Field(
            description=ORGANIZATION_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> AgentExecuteToolResult:
    """Execute a single action against an Airbyte Agents connector, including writes.

    Prefer `execute_agent_connector_ro` when only reading, since it is available in
    read-only mode. Entity types and actions are connector-specific, so call
    `inspect_agent_connector` first. The connector must belong to the given workspace.
    """
    return _execute(
        ctx,
        connector_id=connector_id,
        workspace_id=workspace_id,
        organization_id=organization_id,
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


def _agent_skill_result(skill: AgentSkillInfo) -> AgentSkillResult:
    """Shape an `AgentSkillInfo` into an `AgentSkillResult`."""
    return AgentSkillResult(
        skill_id=skill.id,
        kind=skill.kind,
        title=skill.title,
        summary=skill.summary,
        tags=skill.tags,
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def list_agent_skills(
    ctx: Context,
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> AgentSkillListResult:
    """List all skills available to an Airbyte Agents workspace.

    Skills are reusable documentation the Agents API serves, for example connector usage
    docs. All pages are fetched, so no pagination arguments are needed. Pass a listed
    skill's `skill_id` to `read_agent_skill_docs` to read it.
    """
    workspace = _get_agent_workspace(ctx, workspace_id)
    try:
        skills = workspace.list_skills()
    except AirbyteError as error:
        message = _agents_access_message(error)
        if message is None:
            raise
        return AgentSkillListResult(skills=[], message=message)

    return AgentSkillListResult(
        skills=[_agent_skill_result(skill.info) for skill in skills],
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def search_agent_skills(
    ctx: Context,
    query: Annotated[
        str,
        Field(
            description=("Keyword query to match against skill titles, summaries, and tags."),
        ),
    ],
    *,
    workspace_id: Annotated[
        str | None,
        Field(
            description=WORKSPACE_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> AgentSkillListResult:
    """Search skills by keyword in an Airbyte Agents workspace.

    All pages are fetched, so no pagination arguments are needed. Pass a matching skill's
    `skill_id` to `read_agent_skill_docs` to read it.
    """
    workspace = _get_agent_workspace(ctx, workspace_id)
    try:
        skills = workspace.search_skills(query)
    except AirbyteError as error:
        message = _agents_access_message(error)
        if message is None:
            raise
        return AgentSkillListResult(skills=[], message=message)

    return AgentSkillListResult(
        skills=[_agent_skill_result(skill.info) for skill in skills],
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def read_agent_skill_docs(
    ctx: Context,
    skill_id: Annotated[
        str,
        Field(
            description=(
                "Skill ID, e.g. the `docs_skill_id` reported by `inspect_agent_connector`, "
                "or a `skill_id` from `list_agent_skills`. SQL passthrough destinations use "
                "`connector-destination:<destination_id>`."
            ),
        ),
    ],
    *,
    section: Annotated[
        str | None,
        Field(
            description=(
                "Omit to get metadata, guidance, and the outline of available sections. "
                "Pass an exact section `id` from the outline to read that section."
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
) -> AgentSkillDocsResult:
    """Read a skill's docs in an Airbyte Agents workspace.

    Without `section`, this returns the skill's metadata, guidance, and the outline of
    sections, which is the cheapest way to orient before reading a specific section.
    """
    workspace = _get_agent_workspace(ctx, workspace_id)
    try:
        docs = workspace.read_skill_docs(skill_id, section=section)
    except AirbyteError as error:
        if _is_not_found(error):
            return _destination_skill_docs_fallback(ctx, skill_id, section, workspace_id)
        message = _agents_access_message(error)
        if message is None:
            raise
        return AgentSkillDocsResult(
            skill_id=skill_id,
            section_id=section,
            outline=[],
            content=[],
            warnings=[],
            message=message,
        )

    return AgentSkillDocsResult(
        skill_id=docs.metadata.id,
        title=docs.metadata.title,
        section_id=docs.section_id,
        outline=[
            AgentSkillSectionResult(
                section_id=docs_section.id,
                title=docs_section.title,
                summary=docs_section.summary,
                available=docs_section.available,
            )
            for docs_section in docs.outline
        ],
        content=docs.content,
        warnings=[str(warning) for warning in docs.metadata.warnings],
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
