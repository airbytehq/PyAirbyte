# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Resolve the canonical Airbyte user behind each MCP tool call for telemetry.

The verified access token only carries the auth provider's user ID (`sub`), which
is not the Airbyte user UUID used by the rest of Airbyte's analytics. The
middleware here maps it to the Airbyte user via `/users/get_by_auth_id` once per
auth user per process, and exposes the result to telemetry for the duration of
the call. Resolution never blocks a tool call on failure: an unresolved user is
reported as `None`.
"""

from __future__ import annotations

import asyncio
import logging
import math
import threading
import time
from collections import OrderedDict
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from fastmcp.server.dependencies import get_access_token
from fastmcp.server.middleware import Middleware
from fastmcp_extensions import get_mcp_config

from airbyte import exceptions as exc
from airbyte._util import api_util
from airbyte.constants import CLOUD_API_ROOT, MCP_CONFIG_API_URL, MCP_CONFIG_CONFIG_API_URL
from airbyte.secrets.base import SecretString


if TYPE_CHECKING:
    from fastmcp.server.context import Context
    from fastmcp.server.middleware import CallNext, MiddlewareContext
    from fastmcp.tools import ToolResult
    from mcp.types import CallToolRequestParams


logger = logging.getLogger(__name__)

USER_ID_FAILURE_TTL_SECONDS = 60.0
"""How long a failed lookup is remembered, so an outage doesn't add a call per request."""

USER_ID_CACHE_MAX_ENTRIES = 4096
"""Upper bound on cached auth users per process; the least recently used are evicted."""

USER_ID_LOOKUP_TIMEOUT_SECONDS = 5.0
"""Longest a tool call waits on the user lookup before proceeding without a user ID."""

_current_airbyte_user_id: ContextVar[str | None] = ContextVar(
    "airbyte_mcp_airbyte_user_id",
    default=None,
)


class _UserIdCache:
    """Thread-safe, size-bounded cache of auth user ID to Airbyte user ID."""

    def __init__(self, *, max_entries: int) -> None:
        self._max_entries = max_entries
        self._entries: OrderedDict[str, tuple[str | None, float]] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, auth_user_id: str) -> tuple[bool, str | None]:
        """Return `(hit, user_id)`, where a hit may still carry an unresolved `None`."""
        with self._lock:
            entry = self._entries.get(auth_user_id)
            if entry is None:
                return False, None
            user_id, expires_at = entry
            if expires_at <= time.monotonic():
                del self._entries[auth_user_id]
                return False, None
            self._entries.move_to_end(auth_user_id)
            return True, user_id

    def set(self, auth_user_id: str, user_id: str | None, *, ttl_seconds: float = math.inf) -> None:
        with self._lock:
            self._entries[auth_user_id] = (user_id, time.monotonic() + ttl_seconds)
            self._entries.move_to_end(auth_user_id)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


_user_id_cache = _UserIdCache(max_entries=USER_ID_CACHE_MAX_ENTRIES)


def current_airbyte_user_id() -> str | None:
    """Return the Airbyte user ID resolved for the tool call in progress, if any."""
    return _current_airbyte_user_id.get()


def airbyte_user_properties() -> dict[str, Any]:
    """Return the `airbyte_user_id` telemetry property for the current tool call."""
    return {"airbyte_user_id": current_airbyte_user_id()}


def _lookup_airbyte_user_id(
    auth_user_id: str,
    *,
    bearer_token: SecretString,
    api_root: str,
    config_api_root: str | None,
) -> str | None:
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
    return user_id if isinstance(user_id, str) and user_id else None


async def resolve_airbyte_user_id(ctx: Context | None) -> str | None:
    """Resolve the Airbyte user ID for the request's verified access token.

    Returns `None` when the request has no verified token (stdio, or a server
    without a transport auth provider), when the token has no user claim, or
    when the lookup fails or times out.
    """
    access_token = get_access_token()
    if access_token is None or not access_token.token:
        return None

    bearer_token = SecretString(access_token.token)
    try:
        auth_user_id = api_util.get_user_id_from_bearer_token(bearer_token)
    except exc.PyAirbyteInputError:
        return None

    hit, cached_user_id = _user_id_cache.get(auth_user_id)
    if hit:
        return cached_user_id

    api_root = CLOUD_API_ROOT
    config_api_root: str | None = None
    if ctx is not None:
        api_root = get_mcp_config(ctx, MCP_CONFIG_API_URL) or CLOUD_API_ROOT
        config_api_root = get_mcp_config(ctx, MCP_CONFIG_CONFIG_API_URL) or None

    user_id: str | None = None
    try:
        user_id = await asyncio.wait_for(
            asyncio.to_thread(
                _lookup_airbyte_user_id,
                auth_user_id,
                bearer_token=bearer_token,
                api_root=api_root,
                config_api_root=config_api_root,
            ),
            timeout=USER_ID_LOOKUP_TIMEOUT_SECONDS,
        )
    except Exception:
        logger.debug("Airbyte user lookup for MCP telemetry failed", exc_info=True)

    if user_id is None:
        _user_id_cache.set(auth_user_id, None, ttl_seconds=USER_ID_FAILURE_TTL_SECONDS)
    else:
        _user_id_cache.set(auth_user_id, user_id)
    return user_id


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

        token = _current_airbyte_user_id.set(user_id)
        try:
            return await call_next(context)
        finally:
            _current_airbyte_user_id.reset(token)
