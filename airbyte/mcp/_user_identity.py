# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Resolve the canonical Airbyte user behind each MCP tool call for telemetry.

The verified access token only carries the auth provider's user ID (`sub`), which
is not the Airbyte user UUID used by the rest of Airbyte's analytics. The
middleware here maps it to the Airbyte user and default workspace via
`/users/get_by_auth_id`, caches successful lookups per process, and exposes the
identity to telemetry for the duration of a call. Resolution never blocks a tool
call on failure: an unresolved user is reported as `None`.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from fastmcp.server.dependencies import get_access_token
from fastmcp.server.middleware import Middleware
from fastmcp_extensions import get_mcp_config

from airbyte import exceptions as exc
from airbyte._util import api_util
from airbyte.constants import CLOUD_API_ROOT, MCP_CONFIG_API_URL, MCP_CONFIG_CONFIG_API_URL
from airbyte.secrets.base import SecretString


if TYPE_CHECKING:
    from collections.abc import Iterator

    from fastmcp.server.context import Context
    from fastmcp.server.middleware import CallNext, MiddlewareContext
    from fastmcp.tools import ToolResult
    from mcp.types import CallToolRequestParams


logger = logging.getLogger(__name__)

_ValueT = TypeVar("_ValueT")

USER_ID_CACHE_MAX_ENTRIES = 4096
"""Upper bound on cached auth users per process; the least recently used are evicted."""

USER_ID_LOOKUP_TIMEOUT_SECONDS = 30.0
"""Longest an MCP request waits on identity or scope lookup before proceeding."""

DEFAULT_WORKSPACE_CACHE_TTL_SECONDS = 300.0
"""Refresh cached default workspace data after this interval."""

_current_airbyte_user_id: ContextVar[str | None] = ContextVar(
    "airbyte_mcp_airbyte_user_id",
    default=None,
)


@dataclass(frozen=True)
class AirbyteUser:
    """Canonical Airbyte user identity and its default workspace, when available."""

    user_id: str
    default_workspace_id: str | None


@dataclass(frozen=True)
class _CachedAirbyteUser:
    user: AirbyteUser
    fetched_at: float


class _LruCache(Generic[_ValueT]):
    """Thread-safe, size-bounded least-recently-used cache."""

    def __init__(self, *, max_entries: int) -> None:
        self._max_entries = max_entries
        self._entries: OrderedDict[str, _ValueT] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> _ValueT | None:
        with self._lock:
            value = self._entries.get(key)
            if value is not None:
                self._entries.move_to_end(key)
            return value

    def set(self, key: str, value: _ValueT) -> None:
        with self._lock:
            self._entries[key] = value
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


_user_id_cache = _LruCache[_CachedAirbyteUser](max_entries=USER_ID_CACHE_MAX_ENTRIES)
_workspace_organization_id_cache = _LruCache[str](max_entries=USER_ID_CACHE_MAX_ENTRIES)


def current_airbyte_user_id() -> str | None:
    """Return the Airbyte user ID resolved for the tool call in progress, if any."""
    return _current_airbyte_user_id.get()


def airbyte_user_properties() -> dict[str, Any]:
    """Return the `airbyte_user_id` telemetry property for the current tool call."""
    return {"airbyte_user_id": current_airbyte_user_id()}


def _lookup_airbyte_user(
    auth_user_id: str,
    *,
    bearer_token: SecretString,
    api_root: str,
    config_api_root: str | None,
) -> AirbyteUser | None:
    user = api_util.get_user_by_auth_id(
        auth_user_id,
        api_root=api_root,
        config_api_root=config_api_root,
        client_id=None,
        client_secret=None,
        bearer_token=bearer_token,
        timeout=(USER_ID_LOOKUP_TIMEOUT_SECONDS, USER_ID_LOOKUP_TIMEOUT_SECONDS),
    )
    user_id = user.get("userId")
    if not isinstance(user_id, str) or not user_id:
        return None
    default_workspace_id = user.get("defaultWorkspaceId")
    return AirbyteUser(
        user_id=user_id,
        default_workspace_id=(
            default_workspace_id
            if isinstance(default_workspace_id, str) and default_workspace_id
            else None
        ),
    )


async def resolve_airbyte_user_for_token(
    bearer_token: SecretString,
    *,
    api_root: str,
    config_api_root: str | None,
    max_age_seconds: float | None = None,
) -> AirbyteUser | None:
    """Resolve the canonical Airbyte user and default workspace for a bearer token."""
    try:
        auth_user_id = api_util.get_user_id_from_bearer_token(bearer_token)
    except exc.PyAirbyteInputError:
        return None

    cached_user = _user_id_cache.get(auth_user_id)
    if cached_user is not None and (
        max_age_seconds is None or time.monotonic() - cached_user.fetched_at <= max_age_seconds
    ):
        return cached_user.user

    user: AirbyteUser | None = None
    try:
        user = await asyncio.wait_for(
            asyncio.to_thread(
                _lookup_airbyte_user,
                auth_user_id,
                bearer_token=bearer_token,
                api_root=api_root,
                config_api_root=config_api_root,
            ),
            timeout=USER_ID_LOOKUP_TIMEOUT_SECONDS,
        )
    except Exception:
        logger.debug("Airbyte user lookup for MCP telemetry failed", exc_info=True)

    if user is not None:
        _user_id_cache.set(
            auth_user_id,
            _CachedAirbyteUser(user=user, fetched_at=time.monotonic()),
        )
        return user
    return cached_user.user if cached_user is not None else None


async def resolve_airbyte_user(ctx: Context | None) -> AirbyteUser | None:
    """Resolve the caller's Airbyte user for its verified access token.

    Returns `None` when the request has no verified token (stdio, or a server
    without a transport auth provider), when the token has no user claim, or
    when the lookup fails or times out.
    """
    access_token = get_access_token()
    if access_token is None or not access_token.token:
        return None

    bearer_token = SecretString(access_token.token)
    api_root = CLOUD_API_ROOT
    config_api_root: str | None = None
    if ctx is not None:
        api_root = get_mcp_config(ctx, MCP_CONFIG_API_URL) or CLOUD_API_ROOT
        config_api_root = get_mcp_config(ctx, MCP_CONFIG_CONFIG_API_URL) or None

    return await resolve_airbyte_user_for_token(
        bearer_token,
        api_root=api_root,
        config_api_root=config_api_root,
    )


async def resolve_airbyte_user_id(ctx: Context | None) -> str | None:
    """Resolve the canonical Airbyte user ID for the request's access token."""
    user = await resolve_airbyte_user(ctx)
    return user.user_id if user is not None else None


def _lookup_workspace_organization_id(
    workspace_id: str,
    *,
    bearer_token: SecretString,
    api_root: str,
    config_api_root: str | None,
) -> str | None:
    organization = api_util.get_workspace_organization_info(
        workspace_id=workspace_id,
        api_root=api_root,
        config_api_root=config_api_root,
        client_id=None,
        client_secret=None,
        bearer_token=bearer_token,
        timeout=(USER_ID_LOOKUP_TIMEOUT_SECONDS, USER_ID_LOOKUP_TIMEOUT_SECONDS),
    )
    organization_id = organization.get("organizationId")
    return organization_id if isinstance(organization_id, str) and organization_id else None


async def resolve_workspace_organization_id(
    workspace_id: str,
    *,
    bearer_token: SecretString,
    api_root: str,
    config_api_root: str | None,
) -> str | None:
    """Resolve and cache the organization containing a workspace."""
    cached_organization_id = _workspace_organization_id_cache.get(workspace_id)
    if cached_organization_id is not None:
        return cached_organization_id

    organization_id: str | None = None
    try:
        organization_id = await asyncio.wait_for(
            asyncio.to_thread(
                _lookup_workspace_organization_id,
                workspace_id,
                bearer_token=bearer_token,
                api_root=api_root,
                config_api_root=config_api_root,
            ),
            timeout=USER_ID_LOOKUP_TIMEOUT_SECONDS,
        )
    except Exception:
        logger.debug("MCP workspace organization lookup failed", exc_info=True)

    if organization_id is not None:
        _workspace_organization_id_cache.set(workspace_id, organization_id)
    return organization_id


@contextmanager
def airbyte_user_context(user_id: str | None) -> Iterator[None]:
    """Set the Airbyte user identity for telemetry emitted in this context."""
    token = _current_airbyte_user_id.set(user_id)
    try:
        yield
    finally:
        _current_airbyte_user_id.reset(token)


class AirbyteUserMiddleware(Middleware):
    """Expose the caller's Airbyte user ID to telemetry for each tool call.

    Must wrap `ToolCallTelemetryMiddleware` so the ID is still set when the
    telemetry event is emitted after the tool returns.
    """

    async def on_call_tool(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        """Resolve the caller's Airbyte user ID, then run the call with it in context."""
        try:
            user_id = await resolve_airbyte_user_id(context.fastmcp_context)
        except Exception:
            logger.debug("Airbyte user resolution for MCP telemetry failed", exc_info=True)
            user_id = None

        with airbyte_user_context(user_id):
            return await call_next(context)
