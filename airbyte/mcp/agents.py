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
> These tools use Cloud APIs and honor Cloud API root overrides. Execution requires
> a deployment with the execution and connector-docs routes enabled. Listing an actor
> does not establish execution availability. Writes and skill catalog search are unsupported.

.. include:: ../../docs/mcp-generated/agents.md
"""

# No public Python API — MCP primitives are registered via decorators and
# documented via the generated Markdown include above. Setting `__all__` to an
# empty list tells pdoc (and other doc tools) not to surface the individual
# tool / helper definitions as a redundant "API Documentation" list.
__all__: list[str] = []

import json
from enum import Enum
from http import HTTPStatus
from typing import Annotated, Any
from uuid import UUID

from fastmcp import Context, FastMCP
from fastmcp_extensions import get_mcp_config, mcp_tool, register_mcp_tools
from pydantic import BaseModel, Field
from requests.exceptions import RequestException

from airbyte._util import api_util
from airbyte.cloud.client import CloudClient
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.constants import (
    CLOUD_API_ROOT,
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
from airbyte.exceptions import AirbyteError, PyAirbyteError, PyAirbyteInputError
from airbyte.mcp._arg_resolvers import resolve_list_of_strings
from airbyte.mcp._cloud_execution import CloudExecutionClient, CloudExecutionError
from airbyte.mcp._tool_utils import AIRBYTE_CLOUD_WORKSPACE_ID_IS_SET
from airbyte.mcp.cloud import (
    _add_defaults_for_exclude_args,
)


_MAX_INTENT_LENGTH = 512
_MAX_SQL_BYTES = 32768


class AgentReadAction(str, Enum):
    """Read actions supported by the Cloud execution routes."""

    GET = "get"
    LIST = "list"
    SEARCH = "search"
    SQL_SELECT = "sql_select"


AGENTS_AUTH_TIP_TEXT = (
    f"Use Airbyte Cloud credentials: hosted `{MCP_BEARER_TOKEN_HEADER}` or Client-Id and "
    f"Client-Secret headers; stdio `{CLOUD_BEARER_TOKEN_ENV_VAR}` or "
    f"`{CLOUD_CLIENT_ID_ENV_VAR}` and `{CLOUD_CLIENT_SECRET_ENV_VAR}`. "
    "Discover Cloud source IDs with `list_agent_connectors`, or destination IDs with "
    "the Cloud destination listing. Inspect the actor, then pass its `docs_skill_id` "
    "to `read_agent_skill_docs`. Listing and docs do not establish execution enablement. "
    "Only source get/list/search and restricted Snowflake reads are supported. "
    "Skill catalog list/search and static skill docs are unavailable."
)
WORKSPACE_ID_TIP_TEXT = (
    f"Workspace ID. Hosted MCP connections pass it via the `{MCP_WORKSPACE_ID_HEADER}` "
    f"header; local or stdio connections use the `{CLOUD_WORKSPACE_ID_ENV_VAR}` "
    f"environment variable."
)
ORGANIZATION_ID_TIP_TEXT = (
    f"Organization ID. Omit it when the credentials belong to exactly one "
    f"organization, or when it is already configured via the "
    f"`{MCP_ORGANIZATION_ID_HEADER}` header or the `{CLOUD_ORGANIZATION_ID_ENV_VAR}` "
    f"environment variable. To discover organization IDs, call `list_agent_workspaces`, "
    f"which reports the owning organization of each workspace, or "
    f"`list_cloud_organizations` to search organizations by name."
)

AGENTS_ACCESS_DENIED_STATUS = "access_denied"
"""The `status` reported when the Cloud API refused the request."""

AGENTS_UNAUTHORIZED_MESSAGE = (
    "The Airbyte Cloud API rejected these credentials. Verify the Airbyte Cloud "
    "credentials, or ask the user for valid ones."
)
AGENTS_FORBIDDEN_MESSAGE = (
    "The Airbyte Cloud API authenticated these credentials but denied access. "
    "These credentials lack "
    "workspace access or execution is not enabled. Ask the user to verify access rather "
    "than retrying."
)


class AgentWorkspaceResult(BaseModel):
    """Information about a workspace on Airbyte Cloud."""

    workspace_id: str
    """The workspace ID."""

    workspace_name: str | None = None
    """Display name of the workspace."""

    organization_id: str | None = None
    """The organization that owns the workspace, when reported."""


class AgentWorkspaceListResult(BaseModel):
    """Result of listing workspaces on Airbyte Cloud."""

    workspaces: list[AgentWorkspaceResult]
    """Workspaces reachable through the Cloud API with these credentials."""

    message: str | None = None
    """Why the listing is empty, when the Cloud API denied the request."""


class AgentConnectorResult(BaseModel):
    """Information about a connector configured on Airbyte Cloud."""

    connector_id: str
    """The connector ID, used as `connector_id` in the other Agents tools."""

    connector_name: str | None = None
    """Display name of the connector."""


class AgentConnectorListResult(BaseModel):
    """Result of listing connectors in an Airbyte Cloud workspace."""

    connectors: list[AgentConnectorResult]
    """Connectors configured in the workspace."""

    message: str | None = None
    """Why the listing is empty, when the Cloud API denied the request."""


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
    """Cloud docs ID for this actor; availability is established by reading the docs."""

    definition_id: str | None = None
    """The Cloud connector definition ID."""

    docs_outline: list[dict[str, Any]] = Field(default_factory=list)
    """Authorized documentation sections; docs do not establish execution readiness."""

    warnings: list[str]
    """Warnings the Cloud API reported about this connector."""

    message: str | None = None
    """Why the details are empty, when the Cloud API denied the request."""


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
    """Documentation for a single skill on Airbyte Cloud."""

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
    """Why the docs are empty, when the Cloud API denied the request."""


class AgentExecuteToolResult(BaseModel):
    """Result of executing a single action against an Airbyte Agents connector."""

    status: str
    """The execution status reported by the Cloud API, for example `success`."""

    result: Any = None
    """The action's payload. Entity-returning actions put a list of entities here."""

    meta: dict[str, Any] | None = None
    """Full native metadata, including connector-specific pagination and truncation."""

    has_next_page: bool | None = None
    """Unknown when no verified mapping exists; consult native meta for pagination."""

    end_cursor: str | None = None
    """The cursor to pass as `cursor` to fetch the next page, when one is available."""

    execution_time_ms: int | None = None
    """How long the connector took to execute the action, when reported."""

    warning: dict[str, Any] | None = None
    """A warning reported alongside an otherwise successful result."""

    message: str | None = None
    """Why the action did not run, when the Cloud API denied the request."""


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


def _agents_access_message(error: PyAirbyteError) -> str | None:
    """Return a concise explanation of a Cloud API authorization failure.

    Returns `None` when the failure is not an authorization failure, so the caller can
    re-raise it with a bare `raise` and keep the original traceback.
    """
    status_code = (error.context or {}).get("status_code")
    if status_code == HTTPStatus.UNAUTHORIZED:
        return AGENTS_UNAUTHORIZED_MESSAGE
    if status_code == HTTPStatus.FORBIDDEN:
        return AGENTS_FORBIDDEN_MESSAGE
    return None


def _cloud_clients(
    ctx: Context,
    organization_id: str | None = None,
) -> tuple[CloudClient, CloudExecutionClient]:
    """Share one bounded token exchange across discovery and execution."""
    transport = CloudExecutionClient(
        api_root=get_mcp_config(ctx, MCP_CONFIG_API_URL) or CLOUD_API_ROOT,
        config_api_root=get_mcp_config(ctx, MCP_CONFIG_CONFIG_API_URL),
        bearer_token=get_mcp_config(ctx, MCP_CONFIG_BEARER_TOKEN),
        client_id=get_mcp_config(ctx, MCP_CONFIG_CLIENT_ID),
        client_secret=get_mcp_config(ctx, MCP_CONFIG_CLIENT_SECRET),
    )
    return CloudClient(
        bearer_token=transport.bearer_token,
        public_api_root=get_mcp_config(ctx, MCP_CONFIG_API_URL) or CLOUD_API_ROOT,
        config_api_root=get_mcp_config(ctx, MCP_CONFIG_CONFIG_API_URL),
        workspace_id=get_mcp_config(ctx, MCP_CONFIG_WORKSPACE_ID),
        organization_id=organization_id or get_mcp_config(ctx, MCP_CONFIG_ORGANIZATION_ID),
    ), transport


def _cloud_workspace(client: CloudClient, workspace_id: str | None) -> CloudWorkspace:
    resolved_id = workspace_id or client.resolve_default_workspace_id()
    if not resolved_id:
        raise PyAirbyteInputError(message="Provide an unambiguous Cloud workspace ID.")
    try:
        UUID(resolved_id)
    except ValueError:
        raise PyAirbyteInputError(message="Cloud workspace ID must be a UUID.") from None
    if client.organization_id:
        parent = client.get_workspace_parent_organization_id(resolved_id)
        if parent != client.organization_id:
            raise PyAirbyteInputError(
                message="Cloud workspace organization could not be verified or does not match."
            )
    return client.get_workspace(resolved_id)


def _cloud_inventory(
    workspace: CloudWorkspace,
    *,
    destination: bool,
) -> list[AgentConnectorDetailsResult]:
    # CloudWorkspace's actor wrappers discard the response workspace ID.
    # Validate ownership before projecting the inventory into safe metadata.
    actors = (api_util.list_destinations if destination else api_util.list_sources)(
        workspace_id=workspace.workspace_id,
        api_root=workspace.api_root,
        client_id=None,
        client_secret=None,
        bearer_token=workspace.bearer_token,
    )
    results = []
    for actor in actors:
        if actor.workspace_id != workspace.workspace_id:
            raise PyAirbyteInputError(
                message="Cloud inventory contains an actor in another workspace."
            )
        actor_id = actor.destination_id if destination else actor.source_id
        results.append(
            AgentConnectorDetailsResult(
                connector_id=actor_id,
                connector_name=actor.name,
                workspace_id=actor.workspace_id,
                definition_id=actor.definition_id,
                docs_skill_id=f"connector-source:{actor_id}",
                warnings=[],
            )
        )
    return results


def _cloud_actor(
    workspace: CloudWorkspace,
    connector_id: str,
    *,
    destination: bool | None,
) -> AgentConnectorDetailsResult:
    try:
        UUID(connector_id)
    except ValueError:
        raise PyAirbyteInputError(message="Cloud connector ID must be a UUID.") from None
    actors: list[AgentConnectorDetailsResult] = []
    if destination is not True:
        actors.extend(_cloud_inventory(workspace, destination=False))
    if destination is not False:
        actors.extend(_cloud_inventory(workspace, destination=True))
    matches = [actor for actor in actors if actor.connector_id == connector_id]
    if len(matches) != 1:
        raise PyAirbyteInputError(
            message="Connector was not uniquely found in the selected Cloud workspace and kind."
        )
    return matches[0]


def _cloud_failure(
    error: PyAirbyteError | RequestException | ValueError | KeyError | TypeError,
) -> str:
    """Keep discovery errors from exposing SDK request/response internals."""
    if isinstance(error, PyAirbyteError):
        message = _agents_access_message(error)
        if message:
            return message
    if isinstance(error, CloudExecutionError):
        raise error from None
    raise AirbyteError(
        message="Cloud request failed; verify workspace, inputs and deployment."
    ) from None


def _execute(  # noqa: PLR0913  # Mirrors the tool signatures it serves.
    ctx: Context,
    *,
    connector_id: str,
    workspace_id: str | None,
    organization_id: str | None,
    entity_type: str,
    action: AgentReadAction,
    api_args: dict[str, Any] | str | None,
    select_fields: list[str] | str | None,
    exclude_fields: list[str] | str | None,
    page_size: int | None,
    cursor: str | None,
    intent: str | None,
) -> AgentExecuteToolResult:
    """Dispatch one read; interruptions may leave its remote outcome unknown."""
    if action not in {member.value for member in AgentReadAction}:
        raise PyAirbyteInputError(
            message="Cloud MCP supports only get, list, search and sql_select."
        )
    params = dict(_resolve_api_args(api_args) or {})
    selected = resolve_list_of_strings(select_fields)
    excluded = resolve_list_of_strings(exclude_fields)
    if not entity_type or (intent is not None and len(intent) > _MAX_INTENT_LENGTH):
        raise PyAirbyteInputError(
            message="Entity is required and intent must be at most 512 characters."
        )
    destination = action == "sql_select"
    if destination:
        sql = params.get("sql")
        if not isinstance(sql, str) or not sql.strip() or len(sql.encode()) > _MAX_SQL_BYTES:
            raise PyAirbyteInputError(message="sql must be nonempty and at most 32768 UTF-8 bytes.")
        if selected or excluded or page_size is not None or cursor is not None:
            raise PyAirbyteInputError(
                message="Snowflake reads do not support pagination or projections."
            )
        if (
            set(params) - {"sql", "sql_dialect", "dry_run"}
            or params.get("sql_dialect") != "snowflake"
            or ("dry_run" in params and params["dry_run"] is not False)
        ):
            raise PyAirbyteInputError(
                message="sql_select requires sql_dialect='snowflake' and dry_run=false or absent."
            )
        body: dict[str, Any] = {
            "entity": "record",
            "action": "list",
            "params": {"statement": params["sql"]},
        }
    else:
        if page_size is not None:
            if page_size <= 0 or "limit" in params:
                raise PyAirbyteInputError(
                    message="page_size must be positive and cannot duplicate params.limit."
                )
            params["limit"] = page_size
        if cursor is not None:
            if "cursor" in params:
                raise PyAirbyteInputError(message="cursor cannot duplicate params.cursor.")
            params["cursor"] = cursor
        body = {"entity": entity_type, "action": action, "params": params, "skip_truncation": True}
        if selected is not None:
            body["select_fields"] = selected
        if excluded is not None:
            body["exclude_fields"] = excluded
    if intent is not None:
        body["intent"] = intent
    try:
        client, transport = _cloud_clients(ctx, organization_id)
        workspace = _cloud_workspace(client, workspace_id)
        _cloud_actor(workspace, connector_id, destination=destination)
        payload = (
            transport.execute_destination(connector_id, body)
            if destination
            else transport.execute_source(connector_id, body)
        )
    except (PyAirbyteError, RequestException, ValueError, KeyError, TypeError) as error:
        return AgentExecuteToolResult(
            status=AGENTS_ACCESS_DENIED_STATUS, message=_cloud_failure(error)
        )
    return AgentExecuteToolResult(
        status="success", result=payload["data"], meta=payload.get("meta")
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
            description=ORGANIZATION_ID_TIP_TEXT,
            default=None,
        ),
    ],
) -> AgentWorkspaceListResult:
    """List Cloud workspaces visible to these credentials."""
    try:
        client, _ = _cloud_clients(ctx, organization_id)
        workspaces = client.list_workspaces(organization_id=client.organization_id)
    except (PyAirbyteError, RequestException, ValueError, KeyError, TypeError) as error:
        message = _cloud_failure(error)
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
    """List Cloud sources; presence does not imply execution enablement.

    Discover destinations through the existing Cloud destination listing.
    """
    try:
        client, _ = _cloud_clients(ctx, organization_id)
        workspace = _cloud_workspace(client, workspace_id)
        connectors = _cloud_inventory(workspace, destination=False)
    except (PyAirbyteError, RequestException, ValueError, KeyError, TypeError) as error:
        message = _cloud_failure(error)
        return AgentConnectorListResult(connectors=[], message=message)

    return AgentConnectorListResult(
        connectors=[
            AgentConnectorResult(
                connector_id=connector.connector_id,
                connector_name=connector.connector_name,
            )
            for connector in connectors
        ]
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
        Field(description="The Cloud source or destination UUID."),
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
    """Inspect Cloud actor metadata and docs without claiming execution readiness.

    Pass the docs_skill_id to read_agent_skill_docs for exact section content.
    Source and destination membership are checked in the selected workspace.
    """
    try:
        client, transport = _cloud_clients(ctx, organization_id)
        workspace = _cloud_workspace(client, workspace_id)
        result = _cloud_actor(workspace, connector_id, destination=None)
    except (PyAirbyteError, RequestException, ValueError, KeyError, TypeError) as error:
        return AgentConnectorDetailsResult(
            connector_id=connector_id,
            warnings=[],
            message=_cloud_failure(error),
        )
    try:
        docs = transport.read_docs(workspace.workspace_id, f"connector-source:{connector_id}")
    except (PyAirbyteError, RequestException, ValueError, KeyError, TypeError):
        result.message = (
            "Cloud actor metadata is available; documentation is unavailable "
            "or unauthorized. Execution readiness is unknown."
        )
    else:
        result.docs_outline = docs["outline"]
    return result


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
        Field(description="The Cloud source or destination UUID."),
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
                "For `sql_select`, pass `sql` and `sql_dialect= snowflake` in `api_args`; "
                "only restricted Snowflake reads are supported. The `download` action "
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
        Field(description="Maximum number of entities to return in this page.", default=None),
    ],
    cursor: Annotated[
        str | None,
        Field(
            description="Pagination cursor; consult native meta and connector docs.",
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

    This tool accepts only source get/list/search or restricted Snowflake sql_select.
    Writes are unsupported by both execution tools. Native pagination and truncation
    are preserved in meta; legacy cursor fields remain unknown. Timeouts do not prove
    cancellation: decide whether to resubmit explicitly. Entity types
    are connector-specific, so call `inspect_agent_connector` first. The connector must
    belong to the given workspace.
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
    )


@mcp_tool(
    read_only=True,
    idempotent=True,
    open_world=True,
    extra_help_text=AGENTS_AUTH_TIP_TEXT,
)
def execute_agent_connector(  # noqa: PLR0913  # Explicit args are the point of this tool.
    ctx: Context,
    connector_id: Annotated[
        str,
        Field(description="The Cloud source or destination UUID."),
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
                "The action to run against the entity type. "
                "The `search` action is the connector's native API search, parallel to `get` "
                "and `list`. "
                "For `sql_select`, pass `sql` and `sql_dialect= snowflake` in `api_args`; "
                "only restricted Snowflake reads are supported. The `download` action "
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
        Field(description="Maximum number of entities to return in this page.", default=None),
    ],
    cursor: Annotated[
        str | None,
        Field(
            description="Pagination cursor; consult native meta and connector docs.",
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
    read_only: Annotated[  # noqa: ARG001  # Retained tool argument; cannot permit writes.
        bool | None,
        Field(
            description=(
                "Retained for compatibility; all actions are read-only regardless of this value."
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
    """Read-only alias for execute_agent_connector_ro.

    Writes are no longer supported. Native data and opaque meta are preserved,
    including Snowflake positional rows, duplicate column labels and truncation.
    No automatic retries or pagination are performed; timeout does not prove cancellation.
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
                "using connector-source:<Cloud actor UUID>. Static skills are unsupported."
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
    """Read a skill's docs in an Airbyte Cloud workspace.

    Without `section`, this returns the skill's metadata, guidance, and the outline of
    sections, which is the cheapest way to orient before reading a specific section.
    """
    if not skill_id.startswith("connector-source:"):
        raise PyAirbyteInputError(
            message="Only connector-source:<Cloud actor UUID> docs are supported."
        )
    connector_id = skill_id.removeprefix("connector-source:")
    try:
        client, transport = _cloud_clients(ctx)
        workspace = _cloud_workspace(client, workspace_id)
        _cloud_actor(workspace, connector_id, destination=None)
        docs = transport.read_docs(workspace.workspace_id, skill_id, section=section)
    except (PyAirbyteError, RequestException, ValueError, KeyError, TypeError) as error:
        return AgentSkillDocsResult(
            skill_id=skill_id,
            section_id=section,
            outline=[],
            content=[],
            warnings=[],
            message=_cloud_failure(error),
        )
    return AgentSkillDocsResult(
        skill_id=docs["metadata"]["id"],
        title=docs["metadata"]["title"],
        section_id=docs.get("section_id"),
        outline=[
            AgentSkillSectionResult(
                section_id=item["id"],
                title=item["title"],
                summary=item.get("summary"),
                available=item.get("available", True),
            )
            for item in docs["outline"]
        ],
        content=docs.get("content", []),
        warnings=docs["metadata"].get("warnings", []),
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
