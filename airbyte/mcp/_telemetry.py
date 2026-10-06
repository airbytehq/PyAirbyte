# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Shared context properties and session lifecycle events for MCP telemetry.

Every MCP Segment event (tool calls, `ServerConnected`, `AuthFailed`) carries the same
context keys, so events can be grouped by session, client, organization, and workspace
without a warehouse join. Hosted HTTP is stateless: the client name and version travel
in the `Mcp-Session-Id` token that `fastmcp_extensions` mints on `initialize`, which
`McpRequestTelemetryMiddleware` decodes before the header is replaced with a digest.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal, TypeVar

from fastmcp.server.dependencies import get_access_token, get_context, get_http_request
from fastmcp.server.middleware import Middleware
from fastmcp_extensions import TelemetryRecord, get_mcp_config
from fastmcp_extensions.capability_tokens import (
    SessionToken,
    decode_session_token,
    minted_session_token,
)
from starlette.datastructures import Headers

from airbyte.constants import (
    CLOUD_API_ROOT,
    CLOUD_API_ROOT_ENV_VAR,
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_ORGANIZATION_ID_ENV_VAR,
    CLOUD_WORKSPACE_ID_ENV_VAR,
    MCP_CONFIG_API_URL,
    MCP_CONFIG_CONFIG_API_URL,
    MCP_CONFIG_ORGANIZATION_ID,
    MCP_CONFIG_WORKSPACE_ID,
    MCP_ORGANIZATION_ID_HEADER,
    MCP_WORKSPACE_ID_HEADER,
    is_hosted_mcp_mode,
)
from airbyte.mcp._user_identity import (
    airbyte_user_context,
    resolve_airbyte_user_for_token,
    resolve_workspace_organization_id,
)
from airbyte.secrets.base import SecretString


if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from fastmcp.server.context import Context
    from fastmcp.server.middleware import CallNext, MiddlewareContext
    from fastmcp_extensions import TelemetrySinks
    from mcp import types as mcp_types
    from starlette.types import ASGIApp, Message, Receive, Scope, Send


logger = logging.getLogger(__name__)

_RequestT = TypeVar("_RequestT")
_ResultT = TypeVar("_ResultT")

SERVER_CONNECTED_EVENT = "Airbyte.MCP.ServerConnected"
AUTH_FAILED_EVENT = "Airbyte.MCP.AuthFailed"

MCP_SESSION_ID_HEADER = "mcp-session-id"
OAUTH_CALLBACK_PATH = "/auth/callback"

_SESSION_TOKEN_STATE_KEY = "airbyte_mcp_session_token"
_SESSION_ID_STATE_KEY = "airbyte_mcp_session_id"
_AUTH_METHOD_STATE_KEY = "airbyte_mcp_auth_method"
_PENDING_SERVER_CONNECTED_STATE_KEY = "airbyte_mcp_pending_server_connected"

_STDIO_SESSION_ID = uuid.uuid4().hex
"""A stdio process serves exactly one client session."""

AuthMethod = Literal["bearer", "client_credentials", "none"]
Transport = Literal["streamable-http", "stdio"]


@dataclass(frozen=True)
class _PendingServerConnected:
    """Handshake record and identity lookup context deferred until the HTTP response."""

    record: TelemetryRecord
    bearer_token: SecretString | None
    api_root: str
    config_api_root: str | None


def session_id_digest(raw_session_id: str) -> str:
    """Return the digest that replaces the unsigned `Mcp-Session-Id` value in telemetry."""
    return hashlib.sha256(raw_session_id.encode("latin-1")).hexdigest()


def _edition() -> str:
    api_root = os.getenv(CLOUD_API_ROOT_ENV_VAR, "").strip() or CLOUD_API_ROOT
    return "cloud" if api_root.rstrip("/") == CLOUD_API_ROOT.rstrip("/") else "oss"


def _transport() -> Transport:
    return "streamable-http" if is_hosted_mcp_mode() else "stdio"


def _auth_method_from_headers(headers: Headers) -> AuthMethod:
    if headers.get("client-id"):
        return "client_credentials"
    scheme = headers.get("authorization", "").strip().split(" ", maxsplit=1)[0].lower()
    if scheme == "basic":
        return "client_credentials"
    if scheme == "bearer":
        return "bearer"
    return "none"


def _stdio_auth_method() -> AuthMethod:
    if os.getenv(CLOUD_BEARER_TOKEN_ENV_VAR):
        return "bearer"
    if os.getenv(CLOUD_CLIENT_ID_ENV_VAR):
        return "client_credentials"
    return "none"


def context_properties(
    *,
    auth_method: AuthMethod,
    session_id: str | None,
    client_name: str | None,
    client_version: str | None,
    protocol_version: str | None,
    organization_id: str | None,
    workspace_id: str | None,
) -> dict[str, object]:
    """Build the context properties shared by every MCP telemetry event."""
    return {
        "is_hosted_mcp": is_hosted_mcp_mode(),
        "edition": _edition(),
        "transport": _transport(),
        "auth_method": auth_method,
        "session_id": session_id,
        "mcp_client_name": client_name,
        "mcp_client_version": client_version,
        "mcp_protocol_version": protocol_version,
        "organization_id": organization_id or None,
        "workspace_id": workspace_id or None,
    }


def _mutable_request_state() -> dict[str, object] | None:
    try:
        request = get_http_request()
    except RuntimeError:
        return None
    state = request.scope.get("state")
    return state if isinstance(state, dict) else None


def _request_state() -> Mapping[str, object]:
    return _mutable_request_state() or {}


def _pending_server_connected_context(
    ctx: Context | None,
) -> tuple[SecretString | None, str, str | None]:
    bearer_token: SecretString | None = None
    try:
        access_token = get_access_token()
        if access_token is not None and access_token.token:
            bearer_token = SecretString(access_token.token)
    except Exception:
        logger.debug("MCP access token unavailable for ServerConnected", exc_info=True)

    api_root = CLOUD_API_ROOT
    config_api_root: str | None = None
    if ctx is not None:
        try:
            api_root = get_mcp_config(ctx, MCP_CONFIG_API_URL) or CLOUD_API_ROOT
            config_api_root = get_mcp_config(ctx, MCP_CONFIG_CONFIG_API_URL) or None
        except (AttributeError, KeyError, RuntimeError, ValueError):
            logger.debug("MCP API roots unavailable for ServerConnected", exc_info=True)
    return bearer_token, api_root, config_api_root


def _config_value(name: str) -> str | None:
    try:
        return get_mcp_config(get_context(), name) or None
    except (AttributeError, KeyError, RuntimeError, ValueError):
        return None


def _session_client_info() -> mcp_types.Implementation | None:
    try:
        client_params = get_context().session.client_params
    except RuntimeError:
        return None
    return client_params.client_info if client_params is not None else None


def _session_protocol_version() -> str | None:
    try:
        client_params = get_context().session.client_params
    except RuntimeError:
        return None
    if client_params is None:
        return None
    return str(client_params.protocol_version) or None


def request_properties(
    client_info: mcp_types.Implementation | None = None,
    protocol_version: str | None = None,
) -> dict[str, object]:
    """Resolve context properties for the MCP request in flight.

    Explicit `client_info` and `protocol_version` take precedence; otherwise they come
    from the live session, then from the session token decoded by
    `McpRequestTelemetryMiddleware`.
    """
    state = _request_state()
    token = state.get(_SESSION_TOKEN_STATE_KEY)
    session_token = token if isinstance(token, SessionToken) else None
    client_info = client_info or _session_client_info()
    protocol_version = protocol_version or _session_protocol_version()

    if is_hosted_mcp_mode():
        auth_method = state.get(_AUTH_METHOD_STATE_KEY)
        session_id = state.get(_SESSION_ID_STATE_KEY)
    else:
        auth_method = _stdio_auth_method()
        session_id = _STDIO_SESSION_ID

    # A manually hosted HTTP app may not set the hosted-mode flag. Prefer its
    # actual request scheme; middleware state preserves it across token exchange.
    with suppress(RuntimeError):
        auth_method = state.get(_AUTH_METHOD_STATE_KEY) or _auth_method_from_headers(
            get_http_request().headers
        )

    return context_properties(
        auth_method=auth_method if auth_method in {"bearer", "client_credentials"} else "none",
        session_id=session_id if isinstance(session_id, str) else None,
        client_name=client_info.name
        if client_info
        else (session_token.client_name if session_token else None),
        client_version=client_info.version
        if client_info
        else (session_token.client_version if session_token else None),
        protocol_version=protocol_version
        or (session_token.protocol_version if session_token else None),
        organization_id=_config_value(MCP_CONFIG_ORGANIZATION_ID),
        workspace_id=_config_value(MCP_CONFIG_WORKSPACE_ID),
    )


def _record(
    sinks: TelemetrySinks,
    *,
    event: str,
    name: str,
    started: float,
    success: bool,
    error_type: str | None,
    properties: Mapping[str, object],
) -> TelemetryRecord:
    return TelemetryRecord(
        invocation_type=event,
        name=name,
        timestamp=datetime.now(UTC).isoformat(),
        duration_ms=round((time.perf_counter() - started) * 1000, 2),
        success=success,
        error_type=error_type,
        package_version=sinks.package_version,
        extra=properties,
    )


class ServerConnectedTelemetryMiddleware(Middleware):
    """Emit `Airbyte.MCP.ServerConnected` for each MCP handshake.

    Legacy clients connect with `initialize`; 2026-07-28 clients may connect with
    `server/discover` instead. The `name` property records which one was used.
    """

    def __init__(
        self,
        sinks: TelemetrySinks,
        properties: Callable[..., Mapping[str, object]] = request_properties,
    ) -> None:
        """Emit through `sinks` using `properties` for the shared event context."""
        self._sinks = sinks
        self._properties = properties

    async def on_initialize(
        self,
        context: MiddlewareContext[mcp_types.InitializeRequest],
        call_next: CallNext[mcp_types.InitializeRequest, mcp_types.InitializeResult | None],
    ) -> mcp_types.InitializeResult | None:
        """Track the legacy `initialize` handshake."""
        params = context.message.params
        return await self._track(
            "initialize",
            context,
            call_next,
            client_info=params.client_info,
            protocol_version=str(params.protocol_version),
        )

    async def on_discover(
        self,
        context: MiddlewareContext[mcp_types.DiscoverRequest],
        call_next: CallNext[
            mcp_types.DiscoverRequest, mcp_types.DiscoverResult | dict[str, object]
        ],
    ) -> mcp_types.DiscoverResult | dict[str, object]:
        """Track the stateless `server/discover` handshake."""
        return await self._track("server/discover", context, call_next)

    async def _track(
        self,
        name: str,
        context: MiddlewareContext[_RequestT],
        call_next: CallNext[_RequestT, _ResultT],
        *,
        client_info: mcp_types.Implementation | None = None,
        protocol_version: str | None = None,
    ) -> _ResultT:
        started = time.perf_counter()
        error_type: str | None = None
        try:
            return await call_next(context)
        except Exception as ex:
            error_type = type(ex).__name__
            raise
        finally:
            try:
                properties = self._properties(client_info, protocol_version)
                record = _record(
                    self._sinks,
                    event=SERVER_CONNECTED_EVENT,
                    name=name,
                    started=started,
                    success=error_type is None,
                    error_type=error_type,
                    properties=properties,
                )
                state = _mutable_request_state() if is_hosted_mcp_mode() else None
                if state is None:
                    self._sinks.emit(
                        replace(
                            record,
                            extra={
                                **record.extra,
                                "airbyte_user_id": None,
                                "scope_source": None,
                            },
                        )
                    )
                else:
                    bearer_token, api_root, config_api_root = _pending_server_connected_context(
                        context.fastmcp_context
                    )
                    state[_PENDING_SERVER_CONNECTED_STATE_KEY] = _PendingServerConnected(
                        record=record,
                        bearer_token=bearer_token,
                        api_root=api_root,
                        config_api_root=config_api_root,
                    )
            except Exception:
                logger.debug("ServerConnected telemetry failed", exc_info=True)


def _auth_failure_reason(
    *,
    path: str,
    status: int,
    auth_method: AuthMethod,
    www_authenticate: str,
) -> str | None:
    """Classify a hosted HTTP response as an auth failure, or `None` if it is not one.

    A `401` without credentials is the first step of OAuth discovery, not a failure.
    """
    if path.rstrip("/").endswith(OAUTH_CALLBACK_PATH):
        return "oauth_callback_error" if status >= 400 else None  # noqa: PLR2004
    if status == 401:  # noqa: PLR2004
        if auth_method == "none":
            return None
        if auth_method == "client_credentials":
            return "invalid_client_credentials"
        return "invalid_token"
    if status == 403:  # noqa: PLR2004
        return "insufficient_scope" if "insufficient_scope" in www_authenticate else "forbidden"
    return None


class McpRequestTelemetryMiddleware:
    """Finalize `ServerConnected` events and emit `Airbyte.MCP.AuthFailed`.

    Sits outside the client-credentials exchange and FastMCP's auth so it sees the raw
    credential scheme, the raw `Mcp-Session-Id`, and every `401`/`403`. The decoded
    session token, the session ID digest, and the auth method are stored in the ASGI
    scope state for the in-app telemetry to read. Hosted `ServerConnected` events are
    emitted after the HTTP response exposes the minted session ID.
    """

    def __init__(self, app: ASGIApp, *, sinks: TelemetrySinks, mcp_path: str) -> None:
        """Wrap `app`, tracking auth failures on `mcp_path` and the OAuth callback."""
        self.app = app
        self._sinks = sinks
        self._mcp_path = mcp_path.rstrip("/") or "/"

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Record request context, then watch the response status."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        started = time.perf_counter()
        headers = Headers(scope=scope)
        auth_method = _auth_method_from_headers(headers)
        raw_session_id = minted_session_token(scope) or headers.get(MCP_SESSION_ID_HEADER)
        session_token = decode_session_token(raw_session_id) if raw_session_id else None
        session_id = session_id_digest(raw_session_id) if raw_session_id else None
        state = scope.setdefault("state", {})
        state[_AUTH_METHOD_STATE_KEY] = auth_method
        state[_SESSION_ID_STATE_KEY] = session_id
        state[_SESSION_TOKEN_STATE_KEY] = session_token

        response: dict[str, object] = {}

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                response["status"] = message["status"]
                response["headers"] = Headers(raw=message.get("headers", []))
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            state = scope.get("state")
            pending = (
                state.pop(_PENDING_SERVER_CONNECTED_STATE_KEY, None)
                if isinstance(state, dict)
                else None
            )
            if isinstance(pending, _PendingServerConnected):
                try:
                    response_headers = response.get("headers")
                    await self._emit_server_connected(
                        pending,
                        response_headers if isinstance(response_headers, Headers) else None,
                    )
                except Exception:
                    logger.debug("ServerConnected telemetry failed", exc_info=True)

        status = response.get("status")
        response_headers = response.get("headers")
        path: str = scope.get("path", "")
        if not isinstance(status, int) or not self._is_tracked_path(path):
            return
        reason = _auth_failure_reason(
            path=path,
            status=status,
            auth_method=auth_method,
            www_authenticate=response_headers.get("www-authenticate", "")
            if isinstance(response_headers, Headers)
            else "",
        )
        if reason is None:
            return
        properties = context_properties(
            auth_method=auth_method,
            session_id=session_id,
            client_name=session_token.client_name if session_token else None,
            client_version=session_token.client_version if session_token else None,
            protocol_version=session_token.protocol_version if session_token else None,
            organization_id=headers.get(MCP_ORGANIZATION_ID_HEADER)
            or os.getenv(CLOUD_ORGANIZATION_ID_ENV_VAR),
            workspace_id=headers.get(MCP_WORKSPACE_ID_HEADER)
            or os.getenv(CLOUD_WORKSPACE_ID_ENV_VAR),
        ) | {"http_status": status, "reason": reason}
        try:
            self._sinks.emit(
                _record(
                    self._sinks,
                    event=AUTH_FAILED_EVENT,
                    name=reason,
                    started=started,
                    success=False,
                    error_type=reason,
                    properties=properties,
                )
            )
        except Exception:
            logger.debug("AuthFailed telemetry failed", exc_info=True)

    async def _emit_server_connected(
        self,
        pending: _PendingServerConnected,
        response_headers: Headers | None,
    ) -> None:
        record = pending.record
        extra = dict(record.extra)
        if extra.get("session_id") is None and response_headers is not None:
            response_session_id = response_headers.get(MCP_SESSION_ID_HEADER)
            if response_session_id:
                extra["session_id"] = session_id_digest(response_session_id)

        airbyte_user_id: str | None = None
        scope_source: str | None = None
        workspace_id = extra.get("workspace_id")
        has_workspace_id = isinstance(workspace_id, str) and bool(workspace_id)
        organization_id = extra.get("organization_id")
        has_organization_id = isinstance(organization_id, str) and bool(organization_id)
        if has_workspace_id or has_organization_id:
            scope_source = "header"

        if pending.bearer_token is not None:
            user = await resolve_airbyte_user_for_token(
                pending.bearer_token,
                api_root=pending.api_root,
                config_api_root=pending.config_api_root,
            )
            if user is not None:
                airbyte_user_id = user.user_id

            organization_workspace_id = (
                workspace_id if has_workspace_id else user.default_workspace_id if user else None
            )
            uses_default_workspace = not has_workspace_id and organization_workspace_id is not None

            if (
                not has_organization_id
                and isinstance(organization_workspace_id, str)
                and organization_workspace_id
            ):
                organization_id = await resolve_workspace_organization_id(
                    organization_workspace_id,
                    bearer_token=pending.bearer_token,
                    api_root=pending.api_root,
                    config_api_root=pending.config_api_root,
                )
                if organization_id is not None:
                    extra["organization_id"] = organization_id
                    if uses_default_workspace:
                        scope_source = "default"

        extra["airbyte_user_id"] = airbyte_user_id
        extra["scope_source"] = scope_source
        enriched_record = replace(record, extra=extra)
        with airbyte_user_context(airbyte_user_id):
            self._sinks.emit(enriched_record)

    def _is_tracked_path(self, path: str) -> bool:
        normalized = path.rstrip("/") or "/"
        return normalized == self._mcp_path or normalized.endswith(OAUTH_CALLBACK_PATH)
