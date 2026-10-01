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

USER_DEFAULT_ORGANIZATION_WAIT_SECONDS = 1.0
"""Longest a tool call waits on its user's default organization before dispatching.

A slower lookup keeps running in the background and caches its result for later calls.
"""

USER_DEFAULT_ORGANIZATION_RETRY_SECONDS = 300.0
"""How long a failed or slow default-organization lookup is not retried."""

_current_airbyte_user_id: ContextVar[str | None] = ContextVar(
    "airbyte_mcp_airbyte_user_id",
    default=None,
)


@dataclass(frozen=True)
class AirbyteUser:
    """Canonical Airbyte user identity and its default workspace, when available.

    Cached without expiry (evicted when `set_default_cloud_workspace` changes it), so
    `default_workspace_id` may be stale. It is only used to derive the organization
    recorded in telemetry; tools resolve the default workspace live.
    """

    user_id: str
    default_workspace_id: str | None


_current_airbyte_user: ContextVar[AirbyteUser | None] = ContextVar(
    "airbyte_mcp_airbyte_user",
    default=None,
)


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

    def pop(self, key: str) -> None:
        with self._lock:
            self._entries.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


_user_id_cache = _LruCache[AirbyteUser](max_entries=USER_ID_CACHE_MAX_ENTRIES)
_workspace_organization_id_cache = _LruCache[str](max_entries=USER_ID_CACHE_MAX_ENTRIES)
_default_organization_lookup_failed_at = _LruCache[float](max_entries=USER_ID_CACHE_MAX_ENTRIES)
_pending_default_organization_lookups: dict[str, asyncio.Task[str | None]] = {}


def forget_cached_airbyte_user() -> None:
    """Drop the cached Airbyte user for the current request's verified access token."""
    try:
        access_token = get_access_token()
    except Exception:
        logger.debug(
            "MCP access token unavailable while evicting cached Airbyte user",
            exc_info=True,
        )
        return
    if access_token is None or not access_token.token:
        return
    try:
        auth_user_id = api_util.get_user_id_from_bearer_token(SecretString(access_token.token))
    except exc.PyAirbyteInputError:
        return
    _user_id_cache.pop(auth_user_id)


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
) -> AirbyteUser | None:
    """Resolve the canonical Airbyte user and default workspace for a bearer token."""
    try:
        auth_user_id = api_util.get_user_id_from_bearer_token(bearer_token)
    except exc.PyAirbyteInputError:
        return None

    cached_user = _user_id_cache.get(auth_user_id)
    if cached_user is not None:
        return cached_user

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
        _user_id_cache.set(auth_user_id, user)
    return user


def _request_credentials(ctx: Context | None) -> tuple[SecretString, str, str | None] | None:
    """Return the verified bearer token and API roots for the current request."""
    access_token = get_access_token()
    if access_token is None or not access_token.token:
        return None

    api_root = CLOUD_API_ROOT
    config_api_root: str | None = None
    if ctx is not None:
        api_root = get_mcp_config(ctx, MCP_CONFIG_API_URL) or CLOUD_API_ROOT
        config_api_root = get_mcp_config(ctx, MCP_CONFIG_CONFIG_API_URL) or None
    return SecretString(access_token.token), api_root, config_api_root


async def resolve_airbyte_user(ctx: Context | None) -> AirbyteUser | None:
    """Resolve the caller's Airbyte user for its verified access token.

    Returns `None` when the request has no verified token (stdio, or a server
    without a transport auth provider), when the token has no user claim, or
    when the lookup fails or times out.
    """
    credentials = _request_credentials(ctx)
    if credentials is None:
        return None

    bearer_token, api_root, config_api_root = credentials
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


async def resolve_user_default_organization_id(ctx: Context | None) -> str | None:
    """Resolve the organization of the current tool call's user's default workspace.

    Requires `AirbyteUserMiddleware` to have resolved the user for this call. Waits at
    most `USER_DEFAULT_ORGANIZATION_WAIT_SECONDS`, sharing any pending lookup.
    Failed or timed-out lookups are not restarted within
    `USER_DEFAULT_ORGANIZATION_RETRY_SECONDS`.
    """
    user = _current_airbyte_user.get()
    if user is None or user.default_workspace_id is None:
        return None
    return await resolve_call_workspace_organization_id(user.default_workspace_id, ctx)


async def resolve_call_workspace_organization_id(
    workspace_id: str, ctx: Context | None
) -> str | None:
    """Resolve a call's workspace organization with the existing bounded wait and cache."""
    cached_organization_id = _workspace_organization_id_cache.get(workspace_id)
    if cached_organization_id is not None:
        return cached_organization_id
    lookup = _pending_default_organization_lookups.get(workspace_id)
    if lookup is None:
        # Suppression prevents new work, not another bounded wait for a lookup
        # that survived an earlier caller's timeout.
        failed_at = _default_organization_lookup_failed_at.get(workspace_id)
        if (
            failed_at is not None
            and time.monotonic() - failed_at < USER_DEFAULT_ORGANIZATION_RETRY_SECONDS
        ):
            return None
        credentials = _request_credentials(ctx)
        if credentials is None:
            return None
        bearer_token, api_root, config_api_root = credentials
        lookup = asyncio.create_task(
            resolve_workspace_organization_id(
                workspace_id,
                bearer_token=bearer_token,
                api_root=api_root,
                config_api_root=config_api_root,
            )
        )
        _pending_default_organization_lookups[workspace_id] = lookup
        lookup.add_done_callback(lambda _: _pending_default_organization_lookups.pop(workspace_id))
    organization_id: str | None = None
    try:
        organization_id = await asyncio.wait_for(
            asyncio.shield(lookup), timeout=USER_DEFAULT_ORGANIZATION_WAIT_SECONDS
        )
    except TimeoutError:
        logger.debug("MCP user default organization lookup still pending")
    if organization_id is None:
        _default_organization_lookup_failed_at.set(workspace_id, time.monotonic())
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
    """Expose the caller's Airbyte user to telemetry for each tool call.

    Must wrap `CallScopeMiddleware` and `ToolCallTelemetryMiddleware`, so the user is
    known when the call scope is resolved and when the telemetry event is emitted.
    """

    async def on_call_tool(
        self,
        context: MiddlewareContext[CallToolRequestParams],
        call_next: CallNext[CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        """Resolve the caller's Airbyte user, then run the call with it in context."""
        try:
            user = await resolve_airbyte_user(context.fastmcp_context)
        except Exception:
            logger.debug("Airbyte user resolution for MCP telemetry failed", exc_info=True)
            user = None

        user_token = _current_airbyte_user.set(user)
        try:
            with airbyte_user_context(user.user_id if user is not None else None):
                return await call_next(context)
        finally:
            _current_airbyte_user.reset(user_token)
