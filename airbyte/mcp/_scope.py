# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Per-call workspace and organization scope for MCP tool-call telemetry.

Each tool call records the workspace and organization it acted on, and where each ID
came from:

- `arg`: the tool's `workspace_id` / `organization_id` argument.
- `header`: the `X-Airbyte-Workspace-Id` / `X-Airbyte-Organization-Id` header, or the
  matching environment variable on stdio.
- `default`: the caller's default workspace, resolved by the tool itself.
- `user_default`: for calls that name no workspace or organization, by ID or by name, the
  organization of the authenticated user's default workspace. `workspace_id` stays null.

Only UUID-shaped values are recorded. Tracing enriches a known workspace with its
organization using the shared bounded lookup/cache, before analytics reads the scope.
If lookup fails, the organization remains absent; unrelated defaults are never used.
"""

from __future__ import annotations

import logging
import re
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from fastmcp.server.middleware import Middleware
from fastmcp_extensions import get_mcp_config

from airbyte.constants import MCP_CONFIG_ORGANIZATION_ID, MCP_CONFIG_WORKSPACE_ID
from airbyte.mcp._user_identity import (
    resolve_call_workspace_organization_id,
    resolve_user_default_organization_id,
)


if TYPE_CHECKING:
    from fastmcp.server.context import Context
    from fastmcp.server.middleware import CallNext, MiddlewareContext
    from fastmcp.tools import ToolResult
    from mcp.types import CallToolRequestParams


logger = logging.getLogger(__name__)

ScopeSource = Literal["arg", "header", "default", "user_default"]

_NAME_SELECTOR_ARGS = ("workspace_name", "organization_name")
"""Tool arguments that select a workspace or organization by name rather than ID."""

_UUID_RE = re.compile(
    r"\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z", re.IGNORECASE
)


@dataclass
class CallScope:
    """The workspace and organization a tool call acted on."""

    workspace_id: str | None = None
    workspace_source: ScopeSource | None = None
    organization_id: str | None = None
    organization_source: ScopeSource | None = None
    user_default_organization_id: str | None = None

    def resolved(self) -> CallScope:
        """Return this scope, or the user's default organization if it names neither ID."""
        if self.workspace_id or self.organization_id or not self.user_default_organization_id:
            return self
        return CallScope(
            organization_id=self.user_default_organization_id,
            organization_source="user_default",
        )

    @property
    def scope_source(self) -> ScopeSource | None:
        """Where `workspace_id` came from, else where `organization_id` came from."""
        return self.workspace_source or self.organization_source

    def to_properties(self) -> dict[str, str | None]:
        """Return the telemetry properties for this scope."""
        return {
            "workspace_id": self.workspace_id,
            "organization_id": self.organization_id,
            "scope_source": self.scope_source,
        }


_CALL_SCOPE: ContextVar[CallScope | None] = ContextVar("airbyte_mcp_call_scope", default=None)


def _as_uuid(value: object) -> str | None:
    return value.lower() if isinstance(value, str) and _UUID_RE.fullmatch(value) else None


def _resolve(
    context: MiddlewareContext[CallToolRequestParams],
    *,
    arg_name: str,
    config_name: str,
) -> tuple[str | None, ScopeSource | None]:
    arguments = context.message.arguments or {}
    if arguments.get(arg_name) is not None:
        value = _as_uuid(arguments[arg_name])
        return value, "arg" if value else None
    if context.fastmcp_context is None:
        return None, None
    try:
        value = _as_uuid(get_mcp_config(context.fastmcp_context, config_name))
    except Exception:
        return None, None
    return value, "header" if value else None


def scope_from_request(context: MiddlewareContext[CallToolRequestParams]) -> CallScope:
    """Build the scope known before a tool runs: its arguments, then config headers."""
    try:
        workspace_id, workspace_source = _resolve(
            context, arg_name="workspace_id", config_name=MCP_CONFIG_WORKSPACE_ID
        )
        organization_id, organization_source = _resolve(
            context, arg_name="organization_id", config_name=MCP_CONFIG_ORGANIZATION_ID
        )
    except Exception:
        logger.debug("MCP call scope unavailable", exc_info=True)
        return CallScope()
    return CallScope(
        workspace_id=workspace_id,
        workspace_source=workspace_source,
        organization_id=organization_id,
        organization_source=organization_source,
    )


def current_call_scope() -> CallScope | None:
    """Return the scope of the tool call in progress, if any."""
    return _CALL_SCOPE.get()


def record_default_workspace(workspace_id: str | None) -> None:
    """Record the default workspace a tool resolved when the call named none."""
    scope = _CALL_SCOPE.get()
    value = _as_uuid(workspace_id)
    if scope is not None and scope.workspace_source is None and value is not None:
        scope.workspace_id = value
        scope.workspace_source = "default"


async def enrich_call_scope(ctx: Context | None) -> None:
    """Fill in the effective workspace's organization before trace/analytics finalization."""
    scope = current_call_scope()
    if scope is None or scope.workspace_id is None or scope.organization_id is not None:
        return
    try:
        organization_id = _as_uuid(
            await resolve_call_workspace_organization_id(scope.workspace_id, ctx)
        )
        if organization_id is not None:
            scope.organization_id = organization_id
            scope.organization_source = scope.workspace_source
    except Exception:
        logger.debug("MCP workspace organization unavailable", exc_info=True)


def call_scope_properties() -> dict[str, str | None]:
    """Return `workspace_id`, `organization_id` and `scope_source` for the current call."""
    return (_CALL_SCOPE.get() or CallScope()).resolved().to_properties()


class CallScopeMiddleware(Middleware):
    """Hold a fresh `CallScope` for the duration of each tool call.

    Must wrap the tool-call telemetry middleware, which reads the scope after the tool
    returns, and be wrapped by `AirbyteUserMiddleware`, which resolves the caller.
    """

    async def on_call_tool(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        """Resolve the known scope, then let the tool fill in its default workspace."""
        scope = scope_from_request(context)
        arguments = context.message.arguments or {}
        names_a_scope = any(arguments.get(name) is not None for name in _NAME_SELECTOR_ARGS)
        if scope.workspace_id is None and scope.organization_id is None and not names_a_scope:
            try:
                scope.user_default_organization_id = await resolve_user_default_organization_id(
                    context.fastmcp_context
                )
            except Exception:
                logger.debug("MCP user default organization unavailable", exc_info=True)
        token = _CALL_SCOPE.set(scope)
        try:
            return await call_next(context)
        finally:
            _CALL_SCOPE.reset(token)
