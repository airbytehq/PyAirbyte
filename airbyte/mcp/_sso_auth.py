# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""SSO (multi-realm) login for the hosted MCP server's interactive OIDC path.

Airbyte Cloud SSO customers each get a dedicated Keycloak realm named after their
company identifier, and the Cloud API accepts tokens from any realm on that Keycloak
instance. A stock `OIDCProxy` is pinned to one realm: its authorize URL, token
endpoint, and JWKS all come from the single discovery document it fetches at
startup. This module makes those per-login instead, mirroring the Cloud webapp's
`/sso` page:

1. `AirbyteSsoOidcProxy.authorize` sends the browser to a small identifier-entry page
   served by this proxy (`SsoRealmConfig.login_path`) instead of straight to the
   default realm.
2. The page offers "Continue with Airbyte Cloud" (the default realm, unchanged) or
   "Sign in with SSO" plus a company identifier field.
3. On submit the identifier is validated, substituted into
   `SsoRealmConfig.discovery_url_template`, and that realm's discovery document is
   fetched (and cached). The realm choice is stored against the OAuth transaction
   and the browser is redirected to that realm's authorization endpoint with the
   same OIDC client id: the platform clones the deployment's client into every SSO
   realm, so client id and secret never change.
4. The IdP callback, transparent refresh, refresh-token exchange, and revocation
   resolve the realm again (from the stored choice or from the token's `iss`) and
   run the stock `OIDCProxy` logic with that realm's endpoints made active through
   a `ContextVar`.
5. Upstream tokens are verified by `MultiRealmTokenVerifier`, which routes on the
   token's `iss` to a per-realm `JWTVerifier` whose JWKS URI comes only from the
   template-fetched discovery document, never from the token itself.

This works by subclassing `OIDCProxy` and overriding a handful of its methods and
private attributes (listed in `_OVERRIDDEN_FASTMCP_METHODS`). Those are FastMCP
internals with no compatibility promise, checked against fastmcp 3.2.0, so
`check_fastmcp_compatibility()` runs at startup and fails with a clear error if a
FastMCP upgrade removed one.

Prerequisite on the platform side: the deployment's OIDC client must exist, with the
same client id, secret, and callback URL, in every SSO realm.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import functools
import json
import logging
import re
import secrets
import time
from collections import OrderedDict
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx
from fastmcp.server.auth import TokenVerifier
from fastmcp.server.auth.oauth_proxy.models import OAuthTransaction
from fastmcp.server.auth.oauth_proxy.ui import create_error_html
from fastmcp.server.auth.oidc_proxy import OIDCConfiguration, OIDCProxy
from fastmcp.server.auth.providers.jwt import JWTVerifier
from fastmcp.utilities.ui import create_secure_html_response
from mcp.server.auth.provider import TokenError
from pydantic import BaseModel
from starlette.responses import RedirectResponse
from starlette.routing import Route

from airbyte.mcp._sso_login_page import CHOICE_DEFAULT, CHOICE_SSO, render_login_page


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterator

    from fastmcp.server.auth import AccessToken
    from fastmcp.server.auth.oauth_proxy.models import UpstreamTokenSet
    from key_value.aio.protocols.key_value import AsyncKeyValue
    from mcp.server.auth.provider import AccessToken as SdkAccessToken
    from mcp.server.auth.provider import AuthorizationParams, RefreshToken
    from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
    from starlette.requests import Request
    from starlette.responses import HTMLResponse, Response

    DiscoveryFetch = Callable[[str], Awaitable[httpx.Response]]
    """Fetches a discovery URL. Injectable so tests never touch the network."""


logger = logging.getLogger(__name__)

SSO_LOGIN_PATH = "/auth/login"
"""Route (relative to the proxy's base URL) that serves the identifier-entry page."""

REALM_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$")
"""Company identifiers that can name a realm.

The leading character excludes `_`-prefixed internal realms, and the character class
excludes `/`, `.`, `%`, and `@`, so an identifier can only ever land inside the one
path segment the template reserves for it.
"""

DISCOVERY_SUFFIX = "/.well-known/openid-configuration"
REALM_PLACEHOLDER = "{realm}"

LOGIN_STATE_COOKIE = "MCP_LOGIN_STATE"
"""Signed list of outstanding login-page CSRF tokens (double-submit half)."""

LAST_REALM_COOKIE = "MCP_SSO_LAST_REALM"
"""Signed single-item list remembering the last company identifier for prefill."""

SSO_CHOICE_COLLECTION = "airbyte-mcp-sso-realm-choices"
"""`client_storage` collection holding `SsoRealmChoice` rows keyed by transaction id."""

LOGIN_TTL_SECONDS = 15 * 60
LAST_REALM_COOKIE_MAX_AGE_SECONDS = 365 * 24 * 3600
_EMPTY_LIST_COOKIE_MAX_AGE_SECONDS = 60
_JWT_SEGMENT_COUNT = 3
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


class InvalidRealmIdentifierError(ValueError):
    """The submitted company identifier cannot name an SSO realm.

    The message is shown to the user inline on the login page.
    """


class RealmDiscoveryUnavailableError(RuntimeError):
    """A realm's discovery document could not be fetched or was not trustworthy.

    Distinct from "realm does not exist" (a 404, reported as `None`): this is a
    transient or configuration failure and must not be cached as a miss.
    """


def _same_issuer(left: str, right: str) -> bool:
    return left.rstrip("/") == right.rstrip("/")


def _is_loopback_url(url: str) -> bool:
    return (urlsplit(url).hostname or "") in _LOOPBACK_HOSTS


def _validate_discovery_url_template(template: str) -> None:
    if template.count(REALM_PLACEHOLDER) != 1:
        msg = f"expected exactly one {REALM_PLACEHOLDER} placeholder"
        raise ValueError(msg)
    parts = urlsplit(template)
    if not parts.hostname:
        msg = "must include a host"
        raise ValueError(msg)
    if parts.query or parts.fragment:
        msg = "must not contain a query string or fragment"
        raise ValueError(msg)
    if REALM_PLACEHOLDER in parts.netloc:
        msg = f"{REALM_PLACEHOLDER} must be a path segment, not part of the host"
        raise ValueError(msg)
    if not template.endswith(DISCOVERY_SUFFIX):
        msg = f"must end with {DISCOVERY_SUFFIX}"
        raise ValueError(msg)
    if parts.scheme != "https" and not (parts.scheme == "http" and _is_loopback_url(template)):
        msg = "must use https (http is accepted only for loopback hosts)"
        raise ValueError(msg)
    if parts.path.split("/").count(REALM_PLACEHOLDER) != 1:
        msg = f"{REALM_PLACEHOLDER} must occupy a whole path segment"
        raise ValueError(msg)


@dataclass(frozen=True, kw_only=True)
class SsoRealmConfig:
    """Deployment settings for SSO realm login.

    `discovery_url_template` carries a single `{realm}` placeholder occupying one
    whole path segment and ends with `/.well-known/openid-configuration`; the realm's
    issuer is the template with that suffix removed. `idp_hint`, when set, is sent
    as Keycloak's `kc_idp_hint` so the realm hands off to the customer IdP without
    showing Keycloak's own login form. `reserved_realms` are names that must never
    be treated as customer realms even though they match the identifier pattern.
    """

    discovery_url_template: str
    idp_hint: str | None = None
    reserved_realms: frozenset[str] = frozenset()
    login_path: str = SSO_LOGIN_PATH
    discovery_timeout_seconds: float = 10.0
    discovery_cache_ttl_seconds: float = 3600.0
    negative_cache_ttl_seconds: float = 60.0
    max_cached_realms: int = 512

    def __post_init__(self) -> None:
        _validate_discovery_url_template(self.discovery_url_template)
        if not self.login_path.startswith("/"):
            msg = "login_path must start with '/'"
            raise ValueError(msg)

    @property
    def issuer_prefix(self) -> str:
        """Everything before the realm name in a realm's issuer URL."""
        return self._issuer_parts()[0]

    @property
    def issuer_suffix(self) -> str:
        """Everything after the realm name in a realm's issuer URL (usually empty)."""
        return self._issuer_parts()[1]

    def _issuer_parts(self) -> tuple[str, str]:
        issuer_template = self.discovery_url_template[: -len(DISCOVERY_SUFFIX)]
        prefix, _, suffix = issuer_template.partition(REALM_PLACEHOLDER)
        return prefix, suffix

    def issuer_for(self, realm: str) -> str:
        """Return the issuer URL the template implies for `realm`."""
        return f"{self.issuer_prefix}{realm}{self.issuer_suffix}"

    def discovery_url_for(self, realm: str) -> str:
        """Return the discovery URL for `realm`."""
        return self.discovery_url_template.replace(REALM_PLACEHOLDER, realm)

    def is_reserved(self, realm: str) -> bool:
        """Whether `realm` is a reserved (non-customer) realm name, case-insensitively."""
        folded = realm.casefold()
        return any(folded == reserved.casefold() for reserved in self.reserved_realms)

    @property
    def allows_http_endpoints(self) -> bool:
        """True only for a local `http://` template; realms may then use `http://localhost` URLs."""
        return urlsplit(self.discovery_url_template).scheme == "http"


@dataclass(frozen=True, kw_only=True)
class RealmEndpoints:
    """The per-realm values a login needs, taken from that realm's discovery document."""

    realm: str
    issuer: str
    authorization_endpoint: str
    token_endpoint: str
    jwks_uri: str
    revocation_endpoint: str | None = None


_UPSTREAM_UNAVAILABLE = RealmEndpoints(
    realm="", issuer="", authorization_endpoint="", token_endpoint="", jwks_uri=""
)
"""Active realm meaning "a known SSO realm whose discovery is down right now".

It has no endpoints at all, so FastMCP code running under it cannot reach the default
realm by mistake; revocation uses it to keep local cleanup while skipping the upstream call.
"""


def validate_realm_identifier(raw: str, *, config: SsoRealmConfig, default_issuer: str) -> str:
    """Return the cleaned company identifier, or raise `InvalidRealmIdentifierError`.

    Rejects anything outside `REALM_IDENTIFIER_PATTERN`, reserved realm names, and
    the default realm (which has its own button). The value is not case-folded:
    Keycloak realm names are case-sensitive.
    """
    realm = raw.strip()
    if not realm:
        raise InvalidRealmIdentifierError("Enter your company identifier to sign in with SSO.")
    if not REALM_IDENTIFIER_PATTERN.fullmatch(realm):
        raise InvalidRealmIdentifierError(
            "Company identifiers contain only letters, numbers, '-' and '_', start with a "
            "letter or number, and are at most 63 characters long."
        )
    if config.is_reserved(realm) or _same_issuer(config.issuer_for(realm), default_issuer):
        raise InvalidRealmIdentifierError("That identifier is not an SSO company identifier.")
    return realm


def peek_jwt_claims(token: str) -> dict[str, Any] | None:
    """Decode a JWT payload without verifying it.

    For routing only (which realm's keys to verify against); never trust the result.
    """
    parts = token.split(".")
    if len(parts) != _JWT_SEGMENT_COUNT:
        return None
    payload = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        decoded = json.loads(base64.urlsafe_b64decode(payload))
    except (ValueError, binascii.Error):
        return None
    return decoded if isinstance(decoded, dict) else None


def _peek_issuer(token: str) -> str | None:
    claims = peek_jwt_claims(token) or {}
    issuer = claims.get("iss")
    return issuer if isinstance(issuer, str) and issuer else None


@dataclass
class _CacheEntry:
    endpoints: RealmEndpoints | None
    expires_at: float


class SsoRealmRegistry:
    """Resolves company identifiers to realm endpoints via the discovery template.

    Successful lookups are cached for `discovery_cache_ttl_seconds`. A 404 (no such
    realm) is cached for `negative_cache_ttl_seconds`, so guessing identifiers does
    not turn into one upstream request per guess. Timeouts, 5xx responses, malformed
    documents, and issuer mismatches raise `RealmDiscoveryUnavailableError` and are
    not cached. The cache is LRU-bounded by `max_cached_realms`.
    """

    def __init__(
        self,
        config: SsoRealmConfig,
        *,
        fetch: DiscoveryFetch | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create a registry; `fetch` and `clock` are injectable for tests."""
        self._config = config
        self._fetch: DiscoveryFetch = fetch or self._default_fetch
        self._clock = clock
        self._cache: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._locks: dict[str, asyncio.Lock] = {}

    @property
    def config(self) -> SsoRealmConfig:
        """The realm configuration this registry resolves against."""
        return self._config

    def issuer_for(self, realm: str) -> str:
        """Return the issuer URL the template implies for `realm`."""
        return self._config.issuer_for(realm)

    def discovery_url_for(self, realm: str) -> str:
        """Return the discovery URL for `realm`."""
        return self._config.discovery_url_for(realm)

    def realm_from_issuer(self, issuer: str) -> str | None:
        """Map a token's `iss` back to a realm name, or `None` if it is not one of ours.

        The issuer must match the template's prefix and suffix exactly, the extracted
        realm must satisfy `REALM_IDENTIFIER_PATTERN`, must not be reserved, and must
        round-trip through `issuer_for`, so foreign hosts, traversal, and reserved
        realms all map to `None`.
        """
        prefix, suffix = self._config.issuer_prefix, self._config.issuer_suffix
        if len(issuer) <= len(prefix) + len(suffix):
            return None
        if not (issuer.startswith(prefix) and issuer.endswith(suffix)):
            return None
        realm = issuer[len(prefix) : len(issuer) - len(suffix)]
        if not REALM_IDENTIFIER_PATTERN.fullmatch(realm) or self._config.is_reserved(realm):
            return None
        if self.issuer_for(realm) != issuer:
            return None
        return realm

    async def get_endpoints(self, realm: str) -> RealmEndpoints | None:
        """Return `realm`'s endpoints, `None` if the realm does not exist.

        Raises `RealmDiscoveryUnavailableError` when discovery cannot be completed
        or the document fails validation.
        """
        cached = self._cached(realm)
        if cached is not None:
            return cached.endpoints
        # One lock per realm, so a slow or failing realm never blocks lookups for
        # other realms. The lock is dropped once nobody holds it; a task that was
        # still waiting keeps its reference, so at worst two fetches overlap.
        lock = self._locks.setdefault(realm, asyncio.Lock())
        try:
            async with lock:
                cached = self._cached(realm)
                if cached is not None:
                    return cached.endpoints
                endpoints = await self._discover(realm)
                ttl = (
                    self._config.discovery_cache_ttl_seconds
                    if endpoints is not None
                    else self._config.negative_cache_ttl_seconds
                )
                self._store(realm, endpoints, ttl)
                return endpoints
        finally:
            if not lock.locked():
                self._locks.pop(realm, None)

    def _cached(self, realm: str) -> _CacheEntry | None:
        entry = self._cache.get(realm)
        if entry is None:
            return None
        if entry.expires_at <= self._clock():
            del self._cache[realm]
            return None
        self._cache.move_to_end(realm)
        return entry

    def _store(self, realm: str, endpoints: RealmEndpoints | None, ttl: float) -> None:
        self._cache[realm] = _CacheEntry(endpoints=endpoints, expires_at=self._clock() + ttl)
        self._cache.move_to_end(realm)
        while len(self._cache) > self._config.max_cached_realms:
            self._cache.popitem(last=False)

    async def _discover(self, realm: str) -> RealmEndpoints | None:
        url = self.discovery_url_for(realm)
        try:
            response = await self._fetch(url)
        except httpx.HTTPError as exc:
            msg = f"Discovery request for SSO realm {realm!r} failed: {exc}"
            raise RealmDiscoveryUnavailableError(msg) from exc
        if response.status_code == HTTPStatus.NOT_FOUND:
            logger.info("SSO realm %r does not exist (discovery returned 404)", realm)
            return None
        if response.status_code != HTTPStatus.OK:
            msg = f"Discovery for SSO realm {realm!r} returned HTTP {response.status_code}"
            raise RealmDiscoveryUnavailableError(msg)
        try:
            document = response.json()
        except ValueError as exc:
            msg = f"Discovery for SSO realm {realm!r} returned a non-JSON body"
            raise RealmDiscoveryUnavailableError(msg) from exc
        return self._parse_document(realm, document)

    def _parse_document(self, realm: str, document: object) -> RealmEndpoints:
        if not isinstance(document, dict):
            msg = f"Discovery for SSO realm {realm!r} returned a non-object body"
            raise RealmDiscoveryUnavailableError(msg)
        try:
            oidc = OIDCConfiguration.model_validate({**document, "strict": True})
        except ValueError as exc:
            msg = f"Discovery document for SSO realm {realm!r} is invalid: {exc}"
            raise RealmDiscoveryUnavailableError(msg) from exc
        issuer = str(oidc.issuer)
        if not _same_issuer(issuer, self.issuer_for(realm)):
            msg = (
                f"Discovery document for SSO realm {realm!r} claims issuer {issuer!r}, "
                f"expected {self.issuer_for(realm)!r}"
            )
            raise RealmDiscoveryUnavailableError(msg)
        return RealmEndpoints(
            realm=realm,
            issuer=issuer,
            authorization_endpoint=self._secure_url(realm, str(oidc.authorization_endpoint)),
            token_endpoint=self._secure_url(realm, str(oidc.token_endpoint)),
            jwks_uri=self._secure_url(realm, str(oidc.jwks_uri)),
            revocation_endpoint=(
                self._secure_url(realm, str(oidc.revocation_endpoint))
                if oidc.revocation_endpoint
                else None
            ),
        )

    def _secure_url(self, realm: str, url: str) -> str:
        scheme = urlsplit(url).scheme
        if scheme == "https":
            return url
        if scheme == "http" and self._config.allows_http_endpoints and _is_loopback_url(url):
            return url
        msg = f"Discovery document for SSO realm {realm!r} lists a non-https endpoint: {url}"
        raise RealmDiscoveryUnavailableError(msg)

    async def _default_fetch(self, url: str) -> httpx.Response:
        async with httpx.AsyncClient(timeout=self._config.discovery_timeout_seconds) as client:
            return await client.get(url)


class MultiRealmTokenVerifier(TokenVerifier):
    """Verifies upstream tokens from the default realm or any known SSO realm.

    The token's unverified `iss` only picks which verifier runs; that verifier then
    checks signature, issuer, audience, expiry, and scopes in full. Each realm's
    `JWTVerifier` is built from the realm's discovery document, so the JWKS URI never
    comes from the token. Tokens from any other issuer are rejected.
    """

    def __init__(
        self,
        default_verifier: TokenVerifier,
        *,
        default_issuer: str,
        registry: SsoRealmRegistry,
        algorithm: str | None = None,
        audience: str | None = None,
        required_scopes: list[str] | None = None,
        verifier_factory: Callable[[RealmEndpoints], TokenVerifier] | None = None,
    ) -> None:
        """Wrap `default_verifier`; `verifier_factory` is injectable for tests."""
        super().__init__(required_scopes=required_scopes)
        self._default_verifier = default_verifier
        self._default_issuer = default_issuer
        self._registry = registry
        self._verifier_factory = verifier_factory or functools.partial(
            self._build_jwt_verifier,
            algorithm=algorithm,
            audience=audience,
            required_scopes=required_scopes,
        )
        self._realm_verifiers: OrderedDict[RealmEndpoints, TokenVerifier] = OrderedDict()

    @staticmethod
    def _build_jwt_verifier(
        endpoints: RealmEndpoints,
        *,
        algorithm: str | None,
        audience: str | None,
        required_scopes: list[str] | None,
    ) -> TokenVerifier:
        return JWTVerifier(
            jwks_uri=endpoints.jwks_uri,
            issuer=endpoints.issuer,
            algorithm=algorithm,
            audience=audience,
            required_scopes=required_scopes,
        )

    async def verify_token(self, token: str) -> AccessToken | None:
        """Verify `token` against the realm its `iss` names, or the default realm."""
        issuer = _peek_issuer(token)
        if issuer is None:
            logger.debug("Rejecting upstream token without a readable issuer")
            return None
        if _same_issuer(issuer, self._default_issuer):
            return await self._default_verifier.verify_token(token)
        realm = self._registry.realm_from_issuer(issuer)
        if realm is None:
            logger.debug("Rejecting upstream token from unrecognized issuer %s", issuer)
            return None
        try:
            endpoints = await self._registry.get_endpoints(realm)
        except RealmDiscoveryUnavailableError as exc:
            logger.warning("Cannot verify token for SSO realm %r: %s", realm, exc)
            return None
        if endpoints is None:
            logger.debug("Rejecting upstream token for unknown SSO realm %r", realm)
            return None
        return await self._verifier_for(endpoints).verify_token(token)

    def _verifier_for(self, endpoints: RealmEndpoints) -> TokenVerifier:
        verifier = self._realm_verifiers.get(endpoints)
        if verifier is None:
            verifier = self._verifier_factory(endpoints)
            self._realm_verifiers[endpoints] = verifier
            while len(self._realm_verifiers) > self._registry.config.max_cached_realms:
                self._realm_verifiers.popitem(last=False)
        self._realm_verifiers.move_to_end(endpoints)
        return verifier


class SsoRealmChoice(BaseModel):
    """The realm a user picked on the login page, keyed by OAuth transaction id."""

    txn_id: str
    realm: str
    chosen_at: float


class _SsoChoiceStore:
    """`SsoRealmChoice` rows in the proxy's `client_storage`, keyed by transaction id.

    Uses the `AsyncKeyValue` protocol directly, so the choice lives in the same
    (encrypted, shared) store as the rest of the OAuth state without importing the
    `key_value` package at runtime.
    """

    def __init__(self, storage: AsyncKeyValue) -> None:
        self._storage = storage

    async def get(self, *, key: str) -> SsoRealmChoice | None:
        raw = await self._storage.get(key, collection=SSO_CHOICE_COLLECTION)
        return SsoRealmChoice.model_validate(raw) if raw is not None else None

    async def put(self, *, key: str, value: SsoRealmChoice, ttl: float) -> None:
        await self._storage.put(key, value.model_dump(), collection=SSO_CHOICE_COLLECTION, ttl=ttl)

    async def delete(self, *, key: str) -> None:
        await self._storage.delete(key, collection=SSO_CHOICE_COLLECTION)


_active_realm: ContextVar[RealmEndpoints | None] = ContextVar(
    "airbyte_mcp_active_sso_realm", default=None
)


@contextmanager
def realm_context(endpoints: RealmEndpoints | None) -> Iterator[None]:
    """Make `endpoints` the active realm for the duration of the block.

    `None` selects the default realm. The proxy's upstream-endpoint properties read
    this, so stock `OIDCProxy` code run inside the block talks to the chosen realm.
    """
    token = _active_realm.set(endpoints)
    try:
        yield
    finally:
        _active_realm.reset(token)


def active_realm() -> RealmEndpoints | None:
    """Return the realm made active by `realm_context`, or `None` for the default."""
    return _active_realm.get()


class _LoginFlowError(Exception):
    """A login-page or callback failure that ends the flow with a styled error page."""

    def __init__(self, status_code: int, message: str, *, title: str = "Sign-in error") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.title = title

    def response(self) -> HTMLResponse:
        return create_secure_html_response(
            create_error_html(error_title=self.title, error_message=self.message),
            status_code=self.status_code,
        )


_OVERRIDDEN_FASTMCP_METHODS = (
    "authorize",
    "get_routes",
    "get_client",
    "get_token_verifier",
    "_handle_idp_callback",
    "_try_transparent_refresh",
    "exchange_refresh_token",
    "revoke_token",
    "_build_upstream_authorize_url",
    "_set_consent_binding_cookie",
    "_verify_consent_binding_cookie",
    "_decode_list_cookie",
    "_encode_list_cookie",
    "_set_list_cookie",
)
_REQUIRED_TRANSACTION_FIELDS = (
    "txn_id",
    "client_id",
    "client_redirect_uri",
    "csrf_token",
    "csrf_expires_at",
    "consent_token",
)


def check_fastmcp_compatibility() -> None:
    """Fail at startup if FastMCP no longer has a method or field this module relies on.

    Everything overridden here is FastMCP-internal. A FastMCP upgrade that renames
    one would otherwise change behavior silently; this turns it into a clear error.
    """
    missing = [name for name in _OVERRIDDEN_FASTMCP_METHODS if not hasattr(OIDCProxy, name)]
    missing.extend(
        f"OAuthTransaction.{field}"
        for field in _REQUIRED_TRANSACTION_FIELDS
        if field not in OAuthTransaction.model_fields
    )
    if missing:
        msg = (
            "The installed fastmcp is missing methods or fields that SSO login depends "
            f"on: {', '.join(missing)}. Pin fastmcp to a version that has them or update "
            "airbyte.mcp._sso_auth."
        )
        raise RuntimeError(msg)


class AirbyteSsoOidcProxy(OIDCProxy):
    """`OIDCProxy` whose upstream realm is chosen per login on an identifier-entry page.

    Constructed like `OIDCProxy` plus `sso_config`. The default realm (the one from
    `config_url`) behaves exactly as before; SSO realms are reached through
    `SsoRealmConfig.discovery_url_template` with the same client id and secret.
    """

    def __init__(
        self,
        *,
        sso_config: SsoRealmConfig,
        discovery_fetch: DiscoveryFetch | None = None,
        **oidc_kwargs: Any,  # noqa: ANN401  # Forwarded verbatim to `OIDCProxy.__init__`.
    ) -> None:
        """Build the proxy; every keyword except `sso_config` goes to `OIDCProxy`."""
        if oidc_kwargs.get("token_verifier") is not None:
            msg = (
                "AirbyteSsoOidcProxy builds its own multi-realm token verifier; "
                "do not pass token_verifier."
            )
            raise ValueError(msg)
        check_fastmcp_compatibility()
        self._sso = sso_config
        self._registry = SsoRealmRegistry(sso_config, fetch=discovery_fetch)
        self._default_issuer = ""
        # `OAuthProxy.__init__` assigns these names; the property setters below
        # capture the default realm's values here.
        self._default_upstream_authorization_endpoint = ""
        self._default_upstream_token_endpoint = ""
        self._default_upstream_revocation_endpoint: str | None = None
        self._default_extra_authorize_params: dict[str, str] = {}
        super().__init__(**oidc_kwargs)
        self._default_issuer = str(self.oidc_config.issuer)
        if (
            not self._default_upstream_authorization_endpoint
            or not self._default_upstream_token_endpoint
        ):
            msg = (
                "OIDCProxy no longer assigns _upstream_authorization_endpoint / "
                "_upstream_token_endpoint during construction; airbyte.mcp._sso_auth "
                "must be updated for this fastmcp version."
            )
            raise RuntimeError(msg)
        self._sso_choice_store = _SsoChoiceStore(self._client_storage)

    # -- Realm-aware views of the upstream configuration ---------------------------------
    # `OAuthProxy.__init__` sets these four as plain attributes. Replacing them with
    # properties (with setters, so that assignment still works) makes FastMCP's own
    # code use the realm chosen for the current request without any other changes.

    @property
    # pyrefly: ignore[bad-override]  # Deliberate: property shadows a base instance attribute.
    def _upstream_authorization_endpoint(self) -> str:
        active = _active_realm.get()
        return (
            active.authorization_endpoint
            if active
            else self._default_upstream_authorization_endpoint
        )

    @_upstream_authorization_endpoint.setter
    def _upstream_authorization_endpoint(self, value: str) -> None:
        self._default_upstream_authorization_endpoint = value

    @property
    # pyrefly: ignore[bad-override]  # Deliberate: property shadows a base instance attribute.
    def _upstream_token_endpoint(self) -> str:
        active = _active_realm.get()
        return active.token_endpoint if active else self._default_upstream_token_endpoint

    @_upstream_token_endpoint.setter
    def _upstream_token_endpoint(self, value: str) -> None:
        self._default_upstream_token_endpoint = value

    @property
    # pyrefly: ignore[bad-override]  # Deliberate: property shadows a base instance attribute.
    def _upstream_revocation_endpoint(self) -> str | None:
        active = _active_realm.get()
        return active.revocation_endpoint if active else self._default_upstream_revocation_endpoint

    @_upstream_revocation_endpoint.setter
    def _upstream_revocation_endpoint(self, value: str | None) -> None:
        self._default_upstream_revocation_endpoint = value

    @property
    # pyrefly: ignore[bad-override]  # Deliberate: property shadows a base instance attribute.
    def _extra_authorize_params(self) -> dict[str, str]:
        # The SSO branch sends only the IdP hint: the deployment's default-realm
        # extras (e.g. `prompt=consent`) target Keycloak's own login page, which
        # the hint skips, and some brokered IdPs reject `prompt` values they do
        # not know.
        if _active_realm.get() is None:
            return self._default_extra_authorize_params
        return {"kc_idp_hint": self._sso.idp_hint} if self._sso.idp_hint else {}

    @_extra_authorize_params.setter
    def _extra_authorize_params(self, value: dict[str, str]) -> None:
        self._default_extra_authorize_params = value

    # -- Verification ---------------------------------------------------------------------

    def get_token_verifier(
        self,
        *,
        algorithm: str | None = None,
        audience: str | None = None,
        required_scopes: list[str] | None = None,
        timeout_seconds: int | None = None,
    ) -> TokenVerifier:
        """Wrap the default realm's `JWTVerifier` in a `MultiRealmTokenVerifier`."""
        default_verifier = super().get_token_verifier(
            algorithm=algorithm,
            audience=audience,
            required_scopes=required_scopes,
            timeout_seconds=timeout_seconds,
        )
        self._default_issuer = str(self.oidc_config.issuer)
        return MultiRealmTokenVerifier(
            default_verifier,
            default_issuer=self._default_issuer,
            registry=self._registry,
            algorithm=algorithm,
            audience=audience,
            required_scopes=required_scopes,
        )

    # -- Authorization: detour through the identifier-entry page ------------------------

    async def authorize(
        self, client: OAuthClientInformationFull, params: AuthorizationParams
    ) -> str:
        """Start the transaction as usual, then send the browser to the login page."""
        upstream_url = await super().authorize(client, params)
        query = parse_qs(urlsplit(upstream_url).query)
        txn_id = (query.get("state") or query.get("txn_id") or [""])[0]
        if not txn_id:
            logger.warning(
                "Upstream authorize URL carried no transaction id; skipping the SSO login page"
            )
            return upstream_url
        base = str(self.base_url).rstrip("/")
        return f"{base}{self._sso.login_path}?{urlencode({'txn_id': txn_id})}"

    def get_routes(self, mcp_path: str | None = None) -> list[Route]:
        """Add the login page route beside FastMCP's OAuth routes."""
        routes = super().get_routes(mcp_path)
        routes.append(
            Route(path=self._sso.login_path, endpoint=self._handle_login, methods=["GET", "POST"])
        )
        return routes

    async def _handle_login(self, request: Request) -> Response:
        if request.method == "POST":
            return await self._submit_login(request)
        return await self._show_login_page(request)

    async def _load_login_transaction(self, txn_id: str) -> OAuthTransaction:
        txn = await self._transaction_store.get(key=txn_id) if txn_id else None
        if txn is None:
            raise _LoginFlowError(
                HTTPStatus.BAD_REQUEST,
                "This sign-in link is invalid or has expired. "
                "Please start again from your MCP client.",
            )
        return txn

    def _remembered_identifier(self, request: Request) -> str:
        values = self._decode_list_cookie(request, LAST_REALM_COOKIE)
        if not values:
            return ""
        try:
            return validate_realm_identifier(
                values[0], config=self._sso, default_issuer=self._default_issuer
            )
        except InvalidRealmIdentifierError:
            return ""

    async def _render_login(
        self,
        txn: OAuthTransaction,
        *,
        csrf_token: str,
        company_identifier: str,
        error_message: str | None,
        status_code: int,
    ) -> HTMLResponse:
        client = await self.get_client(txn.client_id)
        client_name = getattr(client, "client_name", None) or txn.client_id
        page = render_login_page(
            txn_id=txn.txn_id,
            csrf_token=csrf_token,
            client_name=client_name,
            client_redirect_uri=txn.client_redirect_uri,
            company_identifier=company_identifier,
            error_message=error_message,
        )
        return create_secure_html_response(page, status_code=status_code)

    async def _show_login_page(self, request: Request) -> Response:
        try:
            txn = await self._load_login_transaction(request.query_params.get("txn_id", ""))
        except _LoginFlowError as err:
            return err.response()
        csrf_token = secrets.token_urlsafe(32)
        txn.csrf_token = csrf_token
        txn.csrf_expires_at = time.time() + LOGIN_TTL_SECONDS
        await self._transaction_store.put(key=txn.txn_id, value=txn, ttl=LOGIN_TTL_SECONDS)
        response = await self._render_login(
            txn,
            csrf_token=csrf_token,
            company_identifier=self._remembered_identifier(request),
            error_message=None,
            status_code=HTTPStatus.OK,
        )
        outstanding = self._decode_list_cookie(request, LOGIN_STATE_COOKIE)
        outstanding.append(csrf_token)
        self._set_list_cookie(
            response,
            LOGIN_STATE_COOKIE,
            self._encode_list_cookie(outstanding),
            max_age=LOGIN_TTL_SECONDS,
        )
        return response

    def _check_login_csrf(self, request: Request, txn: OAuthTransaction, csrf_token: str) -> None:
        expected = txn.csrf_token
        expires_at = txn.csrf_expires_at or 0.0
        if (
            not expected
            or not csrf_token
            or not secrets.compare_digest(csrf_token, expected)
            or time.time() > expires_at
        ):
            raise _LoginFlowError(
                HTTPStatus.BAD_REQUEST,
                "This sign-in form has expired. Please start again from your MCP client.",
            )
        if csrf_token not in self._decode_list_cookie(request, LOGIN_STATE_COOKIE):
            logger.warning(
                "Login CSRF double-submit check failed for transaction %s "
                "(possible cross-site login forgery)",
                txn.txn_id,
            )
            raise _LoginFlowError(
                HTTPStatus.FORBIDDEN,
                "Sign-in session mismatch. Please try signing in again.",
            )

    async def _resolve_sso_choice(self, company_identifier: str) -> tuple[str, RealmEndpoints]:
        realm = validate_realm_identifier(
            company_identifier, config=self._sso, default_issuer=self._default_issuer
        )
        endpoints = await self._registry.get_endpoints(realm)
        if endpoints is None:
            raise InvalidRealmIdentifierError(
                "No SSO configuration was found for that company identifier. "
                "Check the spelling or ask your Airbyte administrator."
            )
        return realm, endpoints

    async def _submit_login(self, request: Request) -> Response:
        form = await request.form()
        txn_id = str(form.get("txn_id", ""))
        choice = str(form.get("choice", ""))
        csrf_token = str(form.get("csrf_token", ""))
        company_identifier = str(form.get("company_identifier", ""))
        try:
            txn = await self._load_login_transaction(txn_id)
            self._check_login_csrf(request, txn, csrf_token)
        except _LoginFlowError as err:
            return err.response()

        realm: str | None = None
        endpoints: RealmEndpoints | None = None
        if choice == CHOICE_SSO:
            try:
                realm, endpoints = await self._resolve_sso_choice(company_identifier)
            except InvalidRealmIdentifierError as err:
                return await self._render_login(
                    txn,
                    csrf_token=csrf_token,
                    company_identifier=company_identifier,
                    error_message=str(err),
                    status_code=HTTPStatus.BAD_REQUEST,
                )
            except RealmDiscoveryUnavailableError as err:
                logger.warning("SSO login blocked by discovery failure: %s", err)
                return _LoginFlowError(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    "Single sign-on is temporarily unavailable. Please try again in a moment.",
                ).response()
        elif choice != CHOICE_DEFAULT:
            return _LoginFlowError(HTTPStatus.BAD_REQUEST, "Invalid sign-in choice.").response()

        return await self._redirect_upstream(
            request, txn, realm=realm, endpoints=endpoints, used_csrf_token=csrf_token
        )

    async def _redirect_upstream(
        self,
        request: Request,
        txn: OAuthTransaction,
        *,
        realm: str | None,
        endpoints: RealmEndpoints | None,
        used_csrf_token: str,
    ) -> RedirectResponse:
        if realm is not None:
            await self._sso_choice_store.put(
                key=txn.txn_id,
                value=SsoRealmChoice(txn_id=txn.txn_id, realm=realm, chosen_at=time.time()),
                ttl=LOGIN_TTL_SECONDS,
            )
        # Bind this browser to the transaction exactly as FastMCP's consent page
        # does, so the IdP callback can reject a different browser (confused deputy).
        consent_token = secrets.token_urlsafe(32)
        txn.consent_token = consent_token
        txn.csrf_token = None
        txn.csrf_expires_at = None
        await self._transaction_store.put(key=txn.txn_id, value=txn, ttl=LOGIN_TTL_SECONDS)

        with realm_context(endpoints):
            upstream_url = self._build_upstream_authorize_url(txn.txn_id, txn.model_dump())
        response = RedirectResponse(url=upstream_url, status_code=HTTPStatus.FOUND)
        self._set_consent_binding_cookie(request, response, txn.txn_id, consent_token)

        remaining = [
            token
            for token in self._decode_list_cookie(request, LOGIN_STATE_COOKIE)
            if token != used_csrf_token
        ]
        self._set_list_cookie(
            response,
            LOGIN_STATE_COOKIE,
            self._encode_list_cookie(remaining),
            max_age=LOGIN_TTL_SECONDS if remaining else _EMPTY_LIST_COOKIE_MAX_AGE_SECONDS,
        )
        if realm is not None:
            self._set_list_cookie(
                response,
                LAST_REALM_COOKIE,
                self._encode_list_cookie([realm]),
                max_age=LAST_REALM_COOKIE_MAX_AGE_SECONDS,
            )
        logger.info(
            "Login page sent transaction %s to %s realm",
            txn.txn_id,
            f"SSO realm {realm!r}" if realm else "the default",
        )
        return response

    # -- Callback and token lifecycle: re-resolve the realm, then defer to FastMCP ------

    def _check_consent_binding(self, request: Request, txn: OAuthTransaction) -> None:
        if txn.consent_token and self._verify_consent_binding_cookie(
            request, txn.txn_id, txn.consent_token
        ):
            return
        logger.warning(
            "Login binding cookie missing or invalid for transaction %s "
            "(possible confused deputy attack)",
            txn.txn_id,
        )
        raise _LoginFlowError(
            HTTPStatus.FORBIDDEN,
            "Authorization session mismatch. This can happen if you followed a link from "
            "another person or your session expired. Please try signing in again.",
            title="Authorization error",
        )

    async def _endpoints_for_choice(self, choice: SsoRealmChoice | None) -> RealmEndpoints | None:
        if choice is None:
            return None
        try:
            endpoints = await self._registry.get_endpoints(choice.realm)
        except RealmDiscoveryUnavailableError as exc:
            logger.warning("SSO callback blocked by discovery failure: %s", exc)
            endpoints = None
        if endpoints is None:
            raise _LoginFlowError(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "Single sign-on is temporarily unavailable. Please try signing in again.",
            )
        return endpoints

    async def _handle_idp_callback(self, request: Request) -> HTMLResponse | RedirectResponse:
        txn_id = request.query_params.get("state")
        if not txn_id or not request.query_params.get("code") or request.query_params.get("error"):
            # FastMCP renders the error pages for these cases.
            return await super()._handle_idp_callback(request)
        txn = await self._transaction_store.get(key=txn_id)
        if txn is None:
            return await super()._handle_idp_callback(request)
        try:
            self._check_consent_binding(request, txn)
            choice = await self._sso_choice_store.get(key=txn_id)
            endpoints = await self._endpoints_for_choice(choice)
        except _LoginFlowError as err:
            return err.response()
        with realm_context(endpoints):
            response = await super()._handle_idp_callback(request)
        if choice is not None:
            await self._sso_choice_store.delete(key=txn_id)
        return response

    async def _endpoints_for_issuer(self, issuer: str | None) -> RealmEndpoints | None:
        """Endpoints for the realm named by `issuer`, or `None` for the default realm.

        Issuers that are not one of our realms also return `None`; the default realm
        then rejects them on its own. For a known SSO realm, discovery trouble raises
        `RealmDiscoveryUnavailableError` so nothing is ever sent to the wrong realm.
        """
        if issuer is None or _same_issuer(issuer, self._default_issuer):
            return None
        realm = self._registry.realm_from_issuer(issuer)
        if realm is None:
            return None
        endpoints = await self._registry.get_endpoints(realm)
        if endpoints is None:
            msg = f"SSO realm {realm!r} no longer exists"
            raise RealmDiscoveryUnavailableError(msg)
        return endpoints

    async def _endpoints_for_upstream_token(
        self, upstream_token_set: UpstreamTokenSet
    ) -> RealmEndpoints | None:
        return await self._endpoints_for_issuer(_peek_issuer(upstream_token_set.access_token))

    async def _endpoints_for_proxy_token(
        self, token: str, *, token_use: str
    ) -> RealmEndpoints | None:
        """Resolve the realm behind a FastMCP-issued token via its JTI mapping."""
        try:
            jti = self.jwt_issuer.verify_token(token, expected_token_use=token_use)["jti"]
            mapping = await self._jti_mapping_store.get(key=jti)
            upstream = (
                await self._upstream_token_store.get(key=mapping.upstream_token_id)
                if mapping
                else None
            )
        except Exception as exc:
            logger.debug("Could not resolve the SSO realm for a %s token: %s", token_use, exc)
            return None
        if upstream is None:
            return None
        return await self._endpoints_for_upstream_token(upstream)

    async def _try_transparent_refresh(
        self, upstream_token_set: UpstreamTokenSet
    ) -> UpstreamTokenSet:
        # A discovery failure raises here. `load_access_token` treats that like any
        # other failed refresh and rejects the token, so the client signs in again.
        endpoints = await self._endpoints_for_upstream_token(upstream_token_set)
        with realm_context(endpoints):
            return await super()._try_transparent_refresh(upstream_token_set)

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        """Refresh against the realm that issued the upstream token.

        If that realm's discovery is down the refresh is refused (the same
        `invalid_grant` FastMCP uses for a failed upstream refresh) rather than
        attempted against the default realm.
        """
        try:
            endpoints = await self._endpoints_for_proxy_token(
                refresh_token.token, token_use="refresh"
            )
        except RealmDiscoveryUnavailableError as exc:
            logger.warning("Refusing token refresh: %s", exc)
            raise TokenError("invalid_grant", f"Upstream refresh failed: {exc}") from exc
        with realm_context(endpoints):
            return await super().exchange_refresh_token(client, refresh_token, scopes)

    async def revoke_token(self, token: SdkAccessToken | RefreshToken) -> None:
        """Revoke against the realm that issued the upstream token.

        An access token here is the upstream JWT, so its `iss` names the realm. A
        refresh token is FastMCP's own reference JWT, resolved through its JTI
        mapping instead; the first lookup simply finds no realm for it.
        """
        try:
            endpoints = await self._endpoints_for_issuer(_peek_issuer(token.token))
            if endpoints is None:
                endpoints = await self._endpoints_for_proxy_token(token.token, token_use="refresh")
        except RealmDiscoveryUnavailableError as exc:
            # Keep FastMCP's local cleanup, skip the upstream call, and never aim
            # it at the default realm.
            logger.warning("Skipping upstream revocation: %s", exc)
            endpoints = _UPSTREAM_UNAVAILABLE
        with realm_context(endpoints):
            await super().revoke_token(token)


def make_sso_proxy_factory(config: SsoRealmConfig) -> Callable[..., OIDCProxy]:
    """Return the `OIDCAuthConfig.proxy_factory` that builds an `AirbyteSsoOidcProxy`.

    `build_mcp_auth` calls the factory with the standard `OIDCProxy` keyword
    arguments; the partial binds `sso_config`.
    """
    return functools.partial(AirbyteSsoOidcProxy, sso_config=config)
