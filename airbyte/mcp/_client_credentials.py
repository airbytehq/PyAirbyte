# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Opt-in HTTP Basic client-credentials transport auth for the MCP server.

The headless bearer path verifies an already-minted, short-lived (~15 min) JWT.
That works for MCP clients that run the OAuth flow and refresh tokens
automatically, but not for a truly headless agent that can only set a *static*
`Authorization` header value and cannot re-mint on a timer.

This module bridges that gap, behind an opt-in flag. When enabled, the server
accepts the long-lived `client_id` / `client_secret` presented via standard HTTP
Basic auth (`Authorization: Basic base64(client_id:client_secret)`, the
`client_secret_basic` token-endpoint auth method), exchanges them for a
short-lived access token at the Airbyte token endpoint, and rewrites the request
to `Authorization: Bearer <token>` so the existing `JWTVerifier` validates it
unchanged. The agent thus presents a durable credential once; the server owns
the short-lived-token churn.

The exchange runs as the outermost ASGI layer (ahead of FastMCP's auth
middleware) so the rewritten bearer header is what the verifier sees. Minted
tokens are cached per credential until shortly before expiry to avoid minting on
every request. A `Bearer` request, or any request when the flag is unset, passes
through untouched.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import logging
import os
import time
from typing import TYPE_CHECKING

import httpx


if TYPE_CHECKING:
    from starlette.types import ASGIApp, Receive, Scope, Send


logger = logging.getLogger(__name__)

# Opt-in flag. Off by default: accepting long-lived credentials at the transport
# is a deliberate escalation, so a deployment must explicitly turn it on.
ALLOW_CLIENT_CREDENTIALS_ENV = "AIRBYTE_MCP_AUTH_ALLOW_CLIENT_CREDENTIALS"

# Airbyte token endpoint that mints an application access token from a
# `client_id` / `client_secret`. Defaults to Airbyte Cloud; overridable for
# self-hosted deployments pointing at their own Airbyte instance.
AIRBYTE_CLOUD_TOKEN_URL = "https://api.airbyte.com/v1/applications/token"
TOKEN_URL_ENV = "AIRBYTE_MCP_AUTH_CLIENT_CREDENTIALS_TOKEN_URL"

# Re-mint this many seconds before the cached token actually expires, so a token
# never lapses mid-request due to clock skew or in-flight latency.
_EXPIRY_SAFETY_MARGIN_SECONDS = 60

_TRUTHY = frozenset({"1", "true", "t", "yes", "y", "on"})


def client_credentials_enabled(env: os._Environ[str] | None = None) -> bool:
    """Return whether the opt-in HTTP Basic client-credentials grant is enabled."""
    source = env if env is not None else os.environ
    return source.get(ALLOW_CLIENT_CREDENTIALS_ENV, "").strip().lower() in _TRUTHY


def _token_url() -> str:
    """Return the token endpoint, defaulting to Airbyte Cloud."""
    return os.getenv(TOKEN_URL_ENV, AIRBYTE_CLOUD_TOKEN_URL)


class ClientCredentialsExchangeMiddleware:
    """ASGI middleware that exchanges HTTP Basic client credentials for a bearer.

    Runs as the outermost layer so the rewritten `Authorization: Bearer` header
    reaches FastMCP's auth verifier. Only `Basic` requests are touched; `Bearer`
    and unauthenticated requests pass through unchanged (the latter are then
    rejected by the verifier, preserving fail-closed behavior).
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        token_url: str,
        expiry_margin_seconds: int = _EXPIRY_SAFETY_MARGIN_SECONDS,
    ) -> None:
        self._app = app
        self._token_url = token_url
        self._expiry_margin_seconds = expiry_margin_seconds
        # Maps a per-credential cache key to `(access_token, expiry_epoch)`.
        self._token_cache: dict[str, tuple[str, float]] = {}
        # A lock per credential so a slow/unreachable token endpoint stalls only
        # the affected credential, not all Basic-auth traffic. `_locks_guard`
        # serializes creation of the per-credential locks themselves.
        self._locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    async def _lock_for(self, cache_key: str) -> asyncio.Lock:
        """Return the lock dedicated to `cache_key`, creating it on first use."""
        async with self._locks_guard:
            lock = self._locks.get(cache_key)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[cache_key] = lock
            return lock

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Rewrite a Basic-auth HTTP request to Bearer, then delegate downstream."""
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        credentials = _parse_basic_credentials(scope)
        if credentials is None:
            await self._app(scope, receive, send)
            return

        token = await self._token_for(credentials)
        if token is None:
            # Exchange failed (e.g. bad credentials): pass the request through
            # unmodified so the verifier rejects it with a 401, rather than
            # masking the failure here.
            await self._app(scope, receive, send)
            return

        await self._app(_with_bearer(scope, token), receive, send)

    async def _token_for(self, credentials: tuple[str, str]) -> str | None:
        """Return a valid access token for the credentials, minting if needed."""
        client_id, client_secret = credentials
        cache_key = _cache_key(client_id, client_secret)
        now = time.monotonic()

        lock = await self._lock_for(cache_key)
        async with lock:
            cached = self._token_cache.get(cache_key)
            if cached is not None and cached[1] > now:
                return cached[0]

            minted = await self._mint_token(client_id, client_secret)
            if minted is None:
                return None

            token, expires_in = minted
            expiry = now + max(expires_in - self._expiry_margin_seconds, 0)
            self._token_cache[cache_key] = (token, expiry)
            return token

    async def _mint_token(self, client_id: str, client_secret: str) -> tuple[str, float] | None:
        """Exchange client credentials for `(access_token, expires_in)` or `None`.

        Returns `None` when the token endpoint rejects the credentials or omits
        an access token; never logs the credentials or the minted token.
        """
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    self._token_url,
                    json={"client_id": client_id, "client_secret": client_secret},
                    headers={"Content-Type": "application/json"},
                )
        except httpx.HTTPError as exc:
            # Unreachable endpoint, timeout, or transport error: fail closed by
            # passing the request through for the verifier to reject with a 401
            # rather than surfacing a 500. The exception type is safe to log; the
            # credentials are not.
            logger.warning(
                "Client-credentials token exchange request failed (%s); passing "
                "request through for the verifier to reject.",
                type(exc).__name__,
            )
            return None

        if response.status_code != httpx.codes.OK:
            logger.warning(
                "Client-credentials token exchange failed (HTTP %d); passing "
                "request through for the verifier to reject.",
                response.status_code,
            )
            return None

        try:
            payload = response.json()
        except ValueError:
            logger.warning(
                "Client-credentials token endpoint returned a non-JSON body; "
                "passing request through for the verifier to reject."
            )
            return None
        access_token = payload.get("access_token")
        if not access_token:
            logger.warning(
                "Client-credentials token exchange returned no access token; "
                "passing request through for the verifier to reject."
            )
            return None

        # `expires_in` is advisory; fall back to a conservative default so a
        # missing field can't produce a non-expiring cache entry.
        expires_in = payload.get("expires_in")
        return access_token, float(expires_in) if expires_in else 0.0


def _parse_basic_credentials(scope: Scope) -> tuple[str, str] | None:
    """Return `(client_id, client_secret)` from a Basic `Authorization` header.

    Returns `None` when the header is absent, uses a non-Basic scheme, or is
    malformed, so the request passes through for the verifier to handle.
    """
    for name, value in scope.get("headers", []):
        if name == b"authorization":
            return _decode_basic(value)
    return None


def _decode_basic(header_value: bytes) -> tuple[str, str] | None:
    """Decode a `Basic <base64(client_id:client_secret)>` header value."""
    scheme, _, encoded = header_value.partition(b" ")
    if scheme.lower() != b"basic" or not encoded:
        return None
    try:
        decoded = base64.b64decode(encoded, validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError):
        return None
    client_id, sep, client_secret = decoded.partition(":")
    if not sep:
        return None
    return client_id, client_secret


def _cache_key(client_id: str, client_secret: str) -> str:
    """Return a stable, non-reversible cache key for a credential pair.

    Hashes the secret so plaintext credentials never sit in the cache dict.
    """
    return hashlib.sha256(f"{client_id}:{client_secret}".encode()).hexdigest()


def _with_bearer(scope: Scope, token: str) -> Scope:
    """Return a shallow copy of `scope` with `Authorization` set to `Bearer`."""
    headers = [(name, value) for name, value in scope["headers"] if name != b"authorization"]
    headers.append((b"authorization", b"Bearer " + token.encode("ascii")))
    new_scope = dict(scope)
    new_scope["headers"] = headers
    return new_scope


def wrap_if_enabled(app: ASGIApp) -> ASGIApp:
    """Wrap `app` with the client-credentials exchange when the flag is set.

    Returns `app` unchanged when the opt-in flag is unset, so the standard
    bearer/OIDC transport auth is the only path. When enabled, returns `app`
    wrapped as the outermost ASGI layer so the Basic-to-Bearer rewrite happens
    before FastMCP's auth verifier runs.
    """
    if not client_credentials_enabled():
        return app
    logger.info(
        "HTTP Basic client-credentials transport auth is enabled (%s); the "
        "server will exchange presented client credentials for bearer tokens.",
        ALLOW_CLIENT_CREDENTIALS_ENV,
    )
    return ClientCredentialsExchangeMiddleware(app, token_url=_token_url())
