# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

"""Exchange verified forwarded user tokens for Cloud API access tokens."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import httpx
from fastmcp.server.auth import AccessToken, TokenVerifier


if TYPE_CHECKING:
    from collections.abc import Callable


logger = logging.getLogger(__name__)

_TOKEN_EXCHANGE_GRANT = "urn:ietf:params:oauth:grant-type:token-exchange"
_ACCESS_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token"
_MAX_CACHED_TOKENS = 1024
_CLOCK_SKEW_SECONDS = 30
_HTTP_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True)
class _CachedToken:
    token: str
    expires_at: float


class UserTokenExchangeClient:
    """Exchange verified user tokens and cache exchanges by inbound-token digest."""

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        scopes: str,
        default_issuer: str,
        client_factory: Callable[[], httpx.AsyncClient] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._scopes = scopes
        self._default_issuer = default_issuer
        self._client_factory = client_factory or (
            lambda: httpx.AsyncClient(timeout=_HTTP_TIMEOUT_SECONDS)
        )
        self._clock = clock
        self._default_token_endpoint: str | None = None
        self._discovery_lock = asyncio.Lock()
        self._cache: OrderedDict[str, _CachedToken] = OrderedDict()
        self._locks: dict[str, asyncio.Lock] = {}

    async def exchange_token(
        self,
        token: str,
        access_token: AccessToken,
        *,
        token_endpoint: str | None = None,
    ) -> str | None:
        """Return the exchanged token, or `None` when exchange fails."""
        digest = hashlib.sha256(token.encode()).hexdigest()
        cached = self._cached(digest)
        if cached is not None:
            return cached.token

        lock = self._locks.setdefault(digest, asyncio.Lock())
        try:
            async with lock:
                cached = self._cached(digest)
                if cached is not None:
                    return cached.token

                exchanged, expires_in = await self._request_exchange(
                    token,
                    token_endpoint=token_endpoint,
                )
                if exchanged is None:
                    return None

                self._cache_exchange(digest, exchanged, access_token, expires_in)
                return exchanged
        finally:
            if not lock.locked():
                self._locks.pop(digest, None)

    def _cached(self, digest: str) -> _CachedToken | None:
        cached = self._cache.get(digest)
        if cached is None:
            return None
        if cached.expires_at <= self._clock():
            del self._cache[digest]
            return None
        self._cache.move_to_end(digest)
        return cached

    def _cache_exchange(
        self,
        digest: str,
        token: str,
        access_token: AccessToken,
        expires_in: float | None,
    ) -> None:
        inbound_exp = access_token.claims.get("exp")
        if (
            isinstance(inbound_exp, bool)
            or not isinstance(inbound_exp, (int, float))
            or isinstance(expires_in, bool)
            or not isinstance(expires_in, (int, float))
        ):
            return
        expires_at = min(float(inbound_exp), self._clock() + expires_in) - _CLOCK_SKEW_SECONDS
        if expires_at <= self._clock():
            return
        self._cache[digest] = _CachedToken(token=token, expires_at=expires_at)
        self._cache.move_to_end(digest)
        while len(self._cache) > _MAX_CACHED_TOKENS:
            self._cache.popitem(last=False)

    async def _request_exchange(
        self,
        token: str,
        *,
        token_endpoint: str | None,
    ) -> tuple[str | None, float | None]:
        try:
            endpoint = token_endpoint or await self._resolve_default_token_endpoint()
            async with self._client_factory() as client:
                response = await client.post(
                    endpoint,
                    data={
                        "grant_type": _TOKEN_EXCHANGE_GRANT,
                        "subject_token": token,
                        "subject_token_type": _ACCESS_TOKEN_TYPE,
                        "requested_token_type": _ACCESS_TOKEN_TYPE,
                        "scope": self._scopes,
                    },
                    auth=(self._client_id, self._client_secret),
                )
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            logger.warning("User token exchange failed: %s", type(exc).__name__)
            return None, None

        if response.status_code != HTTPStatus.OK:
            logger.warning("User token exchange returned HTTP %s", response.status_code)
            return None, None
        try:
            document = response.json()
        except ValueError:
            logger.warning("User token exchange returned a non-JSON response")
            return None, None
        if not isinstance(document, dict):
            logger.warning("User token exchange returned a non-object response")
            return None, None
        exchanged = document.get("access_token")
        if not isinstance(exchanged, str) or not exchanged:
            logger.warning("User token exchange response omitted access_token")
            return None, None
        expires_in = document.get("expires_in")
        return exchanged, (
            float(expires_in)
            if isinstance(expires_in, (int, float)) and not isinstance(expires_in, bool)
            else None
        )

    async def _resolve_default_token_endpoint(self) -> str:
        if self._default_token_endpoint is not None:
            return self._default_token_endpoint
        async with self._discovery_lock:
            if self._default_token_endpoint is not None:
                return self._default_token_endpoint
            discovery_url = f"{self._default_issuer.rstrip('/')}/.well-known/openid-configuration"
            async with self._client_factory() as client:
                response = await client.get(discovery_url)
            if response.status_code != HTTPStatus.OK:
                raise ValueError(f"Default realm discovery returned HTTP {response.status_code}")
            document = response.json()
            if not isinstance(document, dict) or document.get("issuer") != self._default_issuer:
                raise ValueError("Default realm discovery issuer did not match")
            endpoint = document.get("token_endpoint")
            if not isinstance(endpoint, str):
                raise TypeError("Default realm discovery omitted token_endpoint")
            parsed_endpoint = urlsplit(endpoint)
            if parsed_endpoint.scheme != "https" or not parsed_endpoint.netloc:
                raise ValueError("Default realm token endpoint must use HTTPS")
            self._default_token_endpoint = endpoint
            return endpoint


class UserTokenExchangeVerifier(TokenVerifier):
    """Wrap a user-token verifier and exchange its successfully verified token."""

    def __init__(
        self,
        verifier: TokenVerifier,
        *,
        exchange_client: UserTokenExchangeClient,
        token_endpoint: str | None = None,
    ) -> None:
        super().__init__()
        self._verifier = verifier
        self._exchange_client = exchange_client
        self._token_endpoint = token_endpoint

    async def verify_token(self, token: str) -> AccessToken | None:
        access_token = await self._verifier.verify_token(token)
        if access_token is None:
            return None
        exchanged = await self._exchange_client.exchange_token(
            token,
            access_token,
            token_endpoint=self._token_endpoint,
        )
        if exchanged is None:
            return None
        return AccessToken(
            token=exchanged,
            client_id=access_token.client_id,
            scopes=access_token.scopes,
            expires_at=access_token.expires_at,
            resource=access_token.resource,
            subject=access_token.subject,
            claims=access_token.claims,
        )
