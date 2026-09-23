# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for SSO realm login on the hosted MCP server (`airbyte.mcp._sso_auth`).

Nothing here touches the network: the default realm's discovery document is
monkeypatched onto `OIDCConfiguration`, SSO realm discovery goes through an
injected fetch stub, the upstream token exchange uses a fake OAuth client, and
OAuth state lives in an in-memory `MemoryStore`. Browser-facing behavior (login
page, CSRF, cookies, callback) is exercised through Starlette's `TestClient`,
with async setup run on the client's portal so everything shares one event loop.
"""

from __future__ import annotations

import asyncio
import base64
import functools
import hashlib
import json
import re
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from fastmcp.server.auth import TokenVerifier
from fastmcp.server.auth.auth import AccessToken
from fastmcp.server.auth.oauth_proxy import proxy as fastmcp_proxy_module
from fastmcp.server.auth.oauth_proxy.models import JTIMapping, UpstreamTokenSet
from fastmcp.server.auth.oidc_proxy import OIDCConfiguration
from fastmcp_extensions import OIDCAuthConfig, build_mcp_auth
from key_value.aio.stores.memory import MemoryStore
from mcp.server.auth.provider import AuthorizationParams, RefreshToken, TokenError
from mcp.shared.auth import OAuthClientInformationFull
from pydantic import AnyUrl
from starlette.applications import Starlette
from starlette.testclient import TestClient

from airbyte.mcp import _sso_auth as sso
from airbyte.mcp import _sso_login_page as login_page
from airbyte.mcp._transport_security import HostOriginGuardMiddleware


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterator

    from pytest import MonkeyPatch


BASE_URL = "https://mcp.example"
KEYCLOAK = "https://kc.example/auth/realms"
DEFAULT_REALM = "_airbyte-cloud-users"
DEFAULT_ISSUER = f"{KEYCLOAK}/{DEFAULT_REALM}"
DEFAULT_CONFIG_URL = f"{DEFAULT_ISSUER}/.well-known/openid-configuration"
TEMPLATE = f"{KEYCLOAK}/{{realm}}/.well-known/openid-configuration"
RESERVED = frozenset({"master"})
CLIENT_ID = "cloud-mcp"
CLIENT_SECRET = "s3cret"
SCOPES = ["openid", "email", "profile"]
MCP_CLIENT_ID = "client-1"
MCP_REDIRECT_URI = "http://localhost:1234/cb"


def _issuer(realm: str) -> str:
    return f"{KEYCLOAK}/{realm}"


def _discovery_doc(issuer: str, *, revocation: bool = True) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "issuer": issuer,
        "authorization_endpoint": f"{issuer}/protocol/openid-connect/auth",
        "token_endpoint": f"{issuer}/protocol/openid-connect/token",
        "jwks_uri": f"{issuer}/protocol/openid-connect/certs",
        "response_types_supported": ["code"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
    }
    if revocation:
        doc["revocation_endpoint"] = f"{issuer}/protocol/openid-connect/revoke"
    return doc


def _realm_ok(realm: str) -> httpx.Response:
    return httpx.Response(200, json=_discovery_doc(_issuer(realm)))


def _config(**overrides: Any) -> sso.SsoRealmConfig:
    kwargs: dict[str, Any] = {
        "discovery_url_template": TEMPLATE,
        "idp_hint": "default",
        "reserved_realms": RESERVED,
    }
    kwargs.update(overrides)
    return sso.SsoRealmConfig(**kwargs)


def _unsigned_jwt(claims: dict[str, Any]) -> str:
    def segment(obj: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")

    return f"{segment({'alg': 'none'})}.{segment(claims)}.signature"


class _FakeFetch:
    """Discovery fetch stub keyed by realm name; unknown realms 404 like Keycloak."""

    def __init__(
        self, responses: dict[str, httpx.Response | Exception] | None = None
    ) -> None:
        self.responses = responses or {}
        self.calls: list[str] = []

    async def __call__(self, url: str) -> httpx.Response:
        self.calls.append(url)
        realm = url.split("/realms/", 1)[1].split("/", 1)[0]
        result = self.responses.get(
            realm, httpx.Response(404, json={"error": "Realm does not exist"})
        )
        if isinstance(result, Exception):
            raise result
        return result


class _RecordingVerifier(TokenVerifier):
    def __init__(self, name: str, *, accept: bool = True) -> None:
        super().__init__()
        self.name = name
        self.accept = accept
        self.tokens: list[str] = []

    async def verify_token(self, token: str) -> AccessToken | None:
        self.tokens.append(token)
        if not self.accept:
            return None
        return AccessToken(token=token, client_id=self.name, scopes=[])


def _upstream_tokens(access_token: str) -> dict[str, Any]:
    return {
        "access_token": access_token,
        "refresh_token": "upstream-refresh",
        "expires_in": 300,
        "token_type": "Bearer",
        "scope": " ".join(SCOPES),
    }


class _FakeUpstreamOAuthClient:
    """Stands in for authlib's `AsyncOAuth2Client`; records the endpoint it was sent to."""

    def __init__(self, access_token: str) -> None:
        self.client_secret = CLIENT_SECRET
        self.access_token = access_token
        self.fetch_calls: list[dict[str, Any]] = []
        self.refresh_calls: list[dict[str, Any]] = []

    async def fetch_token(self, **kwargs: Any) -> dict[str, Any]:
        self.fetch_calls.append(kwargs)
        return _upstream_tokens(self.access_token)

    async def refresh_token(self, **kwargs: Any) -> dict[str, Any]:
        self.refresh_calls.append(kwargs)
        return _upstream_tokens(self.access_token)


# ---------------------------------------------------------------------------
# Config and identifier validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "template, reason",
    [
        pytest.param(
            f"{KEYCLOAK}/acme/.well-known/openid-configuration",
            "one",
            id="no-placeholder",
        ),
        pytest.param(
            f"{KEYCLOAK}/{{realm}}/{{realm}}/.well-known/openid-configuration",
            "one",
            id="two-placeholders",
        ),
        pytest.param(
            f"{KEYCLOAK}/tenant-{{realm}}/.well-known/openid-configuration",
            "whole path segment",
            id="partial-segment",
        ),
        pytest.param(
            "https://{realm}.example/.well-known/openid-configuration",
            "not part of the host",
            id="host-placeholder",
        ),
        pytest.param(f"{KEYCLOAK}/{{realm}}", "well-known", id="missing-suffix"),
        pytest.param(
            "http://kc.example/auth/realms/{realm}/.well-known/openid-configuration",
            "https",
            id="plain-http",
        ),
        pytest.param(
            f"{KEYCLOAK}/{{realm}}/.well-known/openid-configuration?x=1",
            "query",
            id="query-string",
        ),
        pytest.param(
            "https:///auth/realms/{realm}/.well-known/openid-configuration",
            "host",
            id="missing-host",
        ),
    ],
)
def test_config_rejects_bad_templates(template: str, reason: str) -> None:
    with pytest.raises(ValueError, match=reason):
        _config(discovery_url_template=template)


def test_config_allows_http_for_loopback() -> None:
    config = _config(
        discovery_url_template=(
            "http://localhost:8180/realms/{realm}/.well-known/openid-configuration"
        )
    )
    assert config.issuer_for("acme") == "http://localhost:8180/realms/acme"


def test_config_derives_issuer_and_discovery_urls() -> None:
    config = _config()
    assert config.issuer_prefix == f"{KEYCLOAK}/"
    assert config.issuer_suffix == ""
    assert config.issuer_for("acme") == _issuer("acme")
    assert (
        config.discovery_url_for("acme")
        == f"{_issuer('acme')}/.well-known/openid-configuration"
    )


@pytest.mark.parametrize("identifier", ["acme", "Acme-1_x", "airbyte", "a", "x" * 63])
def test_validate_realm_identifier_accepts(identifier: str) -> None:
    result = sso.validate_realm_identifier(
        f"  {identifier} ", config=_config(), default_issuer=DEFAULT_ISSUER
    )
    assert result == identifier


@pytest.mark.parametrize(
    "identifier",
    [
        pytest.param("", id="empty"),
        pytest.param("   ", id="blank"),
        pytest.param("_internal", id="leading-underscore"),
        pytest.param("-acme", id="leading-dash"),
        pytest.param("acme/../master", id="traversal"),
        pytest.param("acme.example", id="dot"),
        pytest.param("acme%2f", id="percent"),
        pytest.param("user@acme", id="at"),
        pytest.param("x" * 64, id="too-long"),
        pytest.param("MASTER", id="reserved-case-insensitive"),
        pytest.param("_Airbyte-Cloud-Users", id="default-realm"),
    ],
)
def test_validate_realm_identifier_rejects(identifier: str) -> None:
    with pytest.raises(sso.InvalidRealmIdentifierError):
        sso.validate_realm_identifier(
            identifier, config=_config(), default_issuer=DEFAULT_ISSUER
        )


def test_validate_realm_identifier_rejects_the_default_realm_by_issuer() -> None:
    """A default realm that passes the pattern is still refused: it has its own button."""
    with pytest.raises(
        sso.InvalidRealmIdentifierError, match="not an SSO company identifier"
    ):
        sso.validate_realm_identifier(
            "airbyte-cloud", config=_config(), default_issuer=_issuer("airbyte-cloud")
        )


def test_airbyte_is_an_ordinary_realm() -> None:
    """`airbyte` is a real customer realm, not a reserved name."""
    config = _config()
    assert not config.is_reserved("airbyte")
    assert (
        sso.SsoRealmRegistry(config).realm_from_issuer(_issuer("airbyte")) == "airbyte"
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "issuer, expected",
    [
        pytest.param(_issuer("acme"), "acme", id="round-trip"),
        pytest.param(_issuer("Acme-1_x"), "Acme-1_x", id="mixed-case"),
        pytest.param("https://evil.example/auth/realms/acme", None, id="foreign-host"),
        pytest.param(f"{KEYCLOAK}/acme/../master", None, id="traversal"),
        pytest.param(f"{KEYCLOAK}/acme/extra", None, id="extra-segment"),
        pytest.param(_issuer("master"), None, id="reserved"),
        pytest.param(_issuer("_airbyte-internal"), None, id="internal"),
        pytest.param(f"{KEYCLOAK}/", None, id="empty-realm"),
        pytest.param(_issuer("acme") + "/", None, id="trailing-slash"),
    ],
)
def test_realm_from_issuer(issuer: str, expected: str | None) -> None:
    assert sso.SsoRealmRegistry(_config()).realm_from_issuer(issuer) == expected


def test_registry_caches_successful_discovery() -> None:
    fetch = _FakeFetch({"acme": _realm_ok("acme")})
    registry = sso.SsoRealmRegistry(_config(), fetch=fetch)

    async def scenario() -> tuple[sso.RealmEndpoints | None, sso.RealmEndpoints | None]:
        return await registry.get_endpoints("acme"), await registry.get_endpoints(
            "acme"
        )

    first, second = asyncio.run(scenario())
    assert first is not None
    assert first == second
    assert first.issuer == _issuer("acme")
    assert (
        first.authorization_endpoint
        == f"{_issuer('acme')}/protocol/openid-connect/auth"
    )
    assert first.token_endpoint == f"{_issuer('acme')}/protocol/openid-connect/token"
    assert first.jwks_uri == f"{_issuer('acme')}/protocol/openid-connect/certs"
    assert (
        first.revocation_endpoint == f"{_issuer('acme')}/protocol/openid-connect/revoke"
    )
    assert fetch.calls == [f"{_issuer('acme')}/.well-known/openid-configuration"]


def test_registry_negative_caches_unknown_realm_until_ttl() -> None:
    fetch = _FakeFetch()
    now = [1000.0]
    registry = sso.SsoRealmRegistry(
        _config(negative_cache_ttl_seconds=60), fetch=fetch, clock=lambda: now[0]
    )

    async def scenario() -> list[sso.RealmEndpoints | None]:
        results = [
            await registry.get_endpoints("nope"),
            await registry.get_endpoints("nope"),
        ]
        now[0] += 61
        results.append(await registry.get_endpoints("nope"))
        return results

    assert asyncio.run(scenario()) == [None, None, None]
    assert len(fetch.calls) == 2


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(httpx.Response(500, text="boom"), id="server-error"),
        pytest.param(httpx.ConnectTimeout("timed out"), id="timeout"),
        pytest.param(httpx.Response(200, text="not json"), id="bad-json"),
        pytest.param(
            httpx.Response(200, json={"issuer": _issuer("acme")}), id="incomplete"
        ),
        pytest.param(
            httpx.Response(
                200, json=_discovery_doc("https://evil.example/realms/acme")
            ),
            id="issuer-mismatch",
        ),
        pytest.param(
            httpx.Response(
                200,
                json={
                    **_discovery_doc(_issuer("acme")),
                    "token_endpoint": "http://kc.example/insecure/token",
                },
            ),
            id="insecure-endpoint",
        ),
    ],
)
def test_registry_failures_raise_and_are_not_cached(
    failure: httpx.Response | Exception,
) -> None:
    fetch = _FakeFetch({"acme": failure})
    registry = sso.SsoRealmRegistry(_config(), fetch=fetch)

    async def scenario() -> None:
        for _ in range(2):
            with pytest.raises(sso.RealmDiscoveryUnavailableError):
                await registry.get_endpoints("acme")

    asyncio.run(scenario())
    assert len(fetch.calls) == 2


def test_registry_rejects_http_endpoints_for_an_https_template() -> None:
    """A production template never accepts `http://localhost` endpoints from a realm."""
    doc = {**_discovery_doc(_issuer("acme")), "jwks_uri": "http://localhost:8180/certs"}
    registry = sso.SsoRealmRegistry(
        _config(), fetch=_FakeFetch({"acme": httpx.Response(200, json=doc)})
    )
    with pytest.raises(sso.RealmDiscoveryUnavailableError, match="non-https"):
        asyncio.run(registry.get_endpoints("acme"))


def test_registry_accepts_http_endpoints_for_a_local_http_template() -> None:
    local = "http://localhost:8180/realms"
    config = _config(
        discovery_url_template=f"{local}/{{realm}}/.well-known/openid-configuration"
    )
    doc = _discovery_doc(f"{local}/acme")
    registry = sso.SsoRealmRegistry(
        config, fetch=_FakeFetch({"acme": httpx.Response(200, json=doc)})
    )
    endpoints = asyncio.run(registry.get_endpoints("acme"))
    assert endpoints is not None
    assert endpoints.token_endpoint == f"{local}/acme/protocol/openid-connect/token"


def test_registry_serializes_only_same_realm_lookups() -> None:
    """A slow realm must not block lookups for other realms."""
    order: list[str] = []
    gate: asyncio.Event | None = None

    class _GatedFetch(_FakeFetch):
        async def __call__(self, url: str) -> httpx.Response:
            assert gate is not None
            if "/realms/slow/" in url:
                await gate.wait()
            order.append(url.split("/realms/")[1].split("/")[0])
            return await super().__call__(url)

    fetch = _GatedFetch({"slow": _realm_ok("slow"), "fast": _realm_ok("fast")})
    registry = sso.SsoRealmRegistry(_config(), fetch=fetch)

    async def scenario() -> None:
        nonlocal gate
        gate = asyncio.Event()
        slow = asyncio.create_task(registry.get_endpoints("slow"))
        await asyncio.sleep(0.01)
        fast = await registry.get_endpoints("fast")
        assert fast is not None
        gate.set()
        assert await slow is not None

    asyncio.run(scenario())
    assert order == ["fast", "slow"]


def test_registry_is_lru_bounded() -> None:
    fetch = _FakeFetch({r: _realm_ok(r) for r in ("acme", "globex", "initech")})
    registry = sso.SsoRealmRegistry(_config(max_cached_realms=2), fetch=fetch)

    async def scenario() -> None:
        await registry.get_endpoints("acme")
        await registry.get_endpoints("globex")
        await registry.get_endpoints("initech")  # evicts acme
        await registry.get_endpoints("globex")  # still cached
        await registry.get_endpoints("acme")  # refetched

    asyncio.run(scenario())
    realms = [url.split("/realms/")[1].split("/")[0] for url in fetch.calls]
    assert realms == ["acme", "globex", "initech", "acme"]


# ---------------------------------------------------------------------------
# Multi-realm verifier
# ---------------------------------------------------------------------------


def _verifier_under_test(
    fetch: _FakeFetch | None = None,
) -> tuple[
    sso.MultiRealmTokenVerifier, _RecordingVerifier, dict[str, _RecordingVerifier]
]:
    default = _RecordingVerifier("default")
    realm_verifiers: dict[str, _RecordingVerifier] = {}

    def factory(endpoints: sso.RealmEndpoints) -> TokenVerifier:
        verifier = _RecordingVerifier(endpoints.realm)
        realm_verifiers[endpoints.realm] = verifier
        return verifier

    registry = sso.SsoRealmRegistry(
        _config(), fetch=fetch or _FakeFetch({"acme": _realm_ok("acme")})
    )
    verifier = sso.MultiRealmTokenVerifier(
        default,
        default_issuer=DEFAULT_ISSUER,
        registry=registry,
        required_scopes=SCOPES,
        verifier_factory=factory,
    )
    return verifier, default, realm_verifiers


def test_multi_realm_verifier_exposes_required_scopes() -> None:
    verifier, _, _ = _verifier_under_test()
    assert verifier.required_scopes == SCOPES


def test_multi_realm_verifier_routes_default_issuer_to_default_verifier() -> None:
    verifier, default, realm_verifiers = _verifier_under_test()
    token = _unsigned_jwt({"iss": DEFAULT_ISSUER})
    result = asyncio.run(verifier.verify_token(token))
    assert result is not None
    assert result.client_id == "default"
    assert default.tokens == [token]
    assert realm_verifiers == {}


def test_multi_realm_verifier_routes_sso_issuer_to_realm_verifier() -> None:
    verifier, default, realm_verifiers = _verifier_under_test()
    token = _unsigned_jwt({"iss": _issuer("acme")})

    async def scenario() -> list[AccessToken | None]:
        return [await verifier.verify_token(token), await verifier.verify_token(token)]

    results = asyncio.run(scenario())
    assert [r.client_id for r in results if r] == ["acme", "acme"]
    assert default.tokens == []
    assert list(realm_verifiers) == ["acme"]
    assert realm_verifiers["acme"].tokens == [token, token]


@pytest.mark.parametrize(
    "token",
    [
        pytest.param(
            _unsigned_jwt({"iss": "https://evil.example/auth/realms/acme"}),
            id="foreign",
        ),
        pytest.param(_unsigned_jwt({"iss": _issuer("master")}), id="reserved"),
        pytest.param(_unsigned_jwt({"iss": _issuer("unknown")}), id="unknown-realm"),
        pytest.param(_unsigned_jwt({"sub": "no-iss"}), id="no-issuer"),
        pytest.param("not-a-jwt", id="opaque"),
        pytest.param("a.!!!.c", id="garbage-payload"),
    ],
)
def test_multi_realm_verifier_rejects_unroutable_tokens(token: str) -> None:
    verifier, default, realm_verifiers = _verifier_under_test()
    assert asyncio.run(verifier.verify_token(token)) is None
    assert default.tokens == []
    assert realm_verifiers == {}


def test_multi_realm_verifier_fails_closed_when_discovery_is_down() -> None:
    verifier, _, realm_verifiers = _verifier_under_test(
        fetch=_FakeFetch({"acme": httpx.ConnectTimeout("down")})
    )
    token = _unsigned_jwt({"iss": _issuer("acme")})
    assert asyncio.run(verifier.verify_token(token)) is None
    assert realm_verifiers == {}


# ---------------------------------------------------------------------------
# Login page template
# ---------------------------------------------------------------------------


def test_login_template_files_ship_inside_the_package() -> None:
    """The HTML and CSS are package data, loaded via importlib, not relative paths."""
    from importlib import resources

    templates = resources.files(login_page.TEMPLATE_PACKAGE) / login_page.TEMPLATE_DIR
    assert (templates / login_page.LOGIN_TEMPLATE).is_file()
    assert (templates / login_page.LOGIN_STYLESHEET).is_file()


def test_login_page_inlines_stylesheet_and_escapes_values() -> None:
    html = login_page.render_login_page(
        txn_id='t"1',
        csrf_token="c<1>",
        client_name="<script>alert(1)</script>",
        client_redirect_uri="http://localhost:1234/cb?x=<y>&z=1",
        company_identifier='"><img src=x>',
        error_message="<b>bad</b>",
    )
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
    assert 'value="t&#34;1"' in html
    assert 'value="c&lt;1&gt;"' in html
    assert 'value="&#34;&gt;&lt;img src=x&gt;"' in html
    assert "&lt;b&gt;bad&lt;/b&gt;" in html
    assert "<b>bad</b>" not in html
    assert "--accent: #615eff" in html  # stylesheet inlined verbatim
    # Autoescape turns the policy's single quotes into `&#39;`, which browsers decode.
    assert f'content="{login_page.CSP_POLICY.replace(chr(39), "&#39;")}"' in html
    assert 'name="choice" value="default"' in html
    assert 'name="choice" value="sso"' in html


def test_login_page_omits_error_block_when_there_is_no_error() -> None:
    html = login_page.render_login_page(
        txn_id="t",
        csrf_token="c",
        client_name="Client",
        client_redirect_uri="http://l/cb",
    )
    assert 'role="alert"' not in html
    assert 'value=""' in html  # empty identifier prefill


# ---------------------------------------------------------------------------
# Proxy harness
# ---------------------------------------------------------------------------


def _patch_default_discovery(monkeypatch: MonkeyPatch) -> None:
    def fake(
        _cls: type[OIDCConfiguration], _config_url: object, **_kwargs: object
    ) -> OIDCConfiguration:
        return OIDCConfiguration.model_validate(_discovery_doc(DEFAULT_ISSUER))

    monkeypatch.setattr(OIDCConfiguration, "get_oidc_configuration", classmethod(fake))


def _proxy_kwargs(**overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "config_url": DEFAULT_CONFIG_URL,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "base_url": BASE_URL,
        "required_scopes": SCOPES,
        "client_storage": MemoryStore(),
        "require_authorization_consent": "external",
        "extra_authorize_params": {"prompt": "consent"},
        "enable_cimd": False,
        "forward_resource": False,
    }
    kwargs.update(overrides)
    return kwargs


def _make_proxy(
    monkeypatch: MonkeyPatch,
    *,
    fetch: _FakeFetch | None = None,
    config: sso.SsoRealmConfig | None = None,
    **overrides: Any,
) -> sso.AirbyteSsoOidcProxy:
    _patch_default_discovery(monkeypatch)
    return sso.AirbyteSsoOidcProxy(
        sso_config=config or _config(),
        discovery_fetch=fetch or _FakeFetch({"acme": _realm_ok("acme")}),
        **_proxy_kwargs(**overrides),
    )


class _Harness:
    """A proxy mounted in Starlette plus a `TestClient` whose portal runs async setup."""

    def __init__(self, proxy: sso.AirbyteSsoOidcProxy, client: TestClient) -> None:
        self.proxy = proxy
        self.client = client

    def run(self, fn: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        portal = self.client.portal
        assert portal is not None, "use the harness inside the TestClient context"
        return portal.call(functools.partial(fn, *args, **kwargs))

    def start_login(self) -> str:
        """Register an MCP client and start a transaction; return its `txn_id`."""

        async def go() -> str:
            await self.proxy.register_client(
                OAuthClientInformationFull(
                    client_id=MCP_CLIENT_ID,
                    client_name="Test Client",
                    redirect_uris=[AnyUrl(MCP_REDIRECT_URI)],
                )
            )
            client = await self.proxy.get_client(MCP_CLIENT_ID)
            assert client is not None
            url = await self.proxy.authorize(
                client,
                AuthorizationParams(
                    state="client-state",
                    scopes=SCOPES,
                    code_challenge="client-challenge",
                    redirect_uri=AnyUrl(MCP_REDIRECT_URI),
                    redirect_uri_provided_explicitly=True,
                    resource=None,
                ),
            )
            assert url.startswith(f"{BASE_URL}{sso.SSO_LOGIN_PATH}?txn_id=")
            return parse_qs(urlsplit(url).query)["txn_id"][0]

        return self.run(go)

    def get_login_page(self, txn_id: str) -> tuple[httpx.Response, str]:
        response = self.client.get(f"{sso.SSO_LOGIN_PATH}?txn_id={txn_id}")
        match = re.search(r'name="csrf_token" value="([^"]+)"', response.text)
        return response, (match.group(1) if match else "")

    def post_login(self, form: dict[str, str], **kwargs: Any) -> httpx.Response:
        return self.client.post(
            sso.SSO_LOGIN_PATH, data=form, follow_redirects=False, **kwargs
        )

    def transaction(self, txn_id: str) -> Any:  # noqa: ANN401
        return self.run(self.proxy._transaction_store.get, key=txn_id)  # noqa: SLF001

    def choice(self, txn_id: str) -> sso.SsoRealmChoice | None:
        return self.run(self.proxy._sso_choice_store.get, key=txn_id)  # noqa: SLF001


@pytest.fixture
def harness(monkeypatch: MonkeyPatch) -> Iterator[_Harness]:
    proxy = _make_proxy(monkeypatch)
    app = Starlette(routes=proxy.get_routes("/mcp"))
    with TestClient(app, base_url=BASE_URL) as client:
        yield _Harness(proxy, client)


def _login_default(h: _Harness) -> tuple[str, httpx.Response]:
    txn_id = h.start_login()
    _, csrf = h.get_login_page(txn_id)
    return txn_id, h.post_login({
        "txn_id": txn_id,
        "csrf_token": csrf,
        "choice": "default",
    })


def _login_sso(h: _Harness, identifier: str = "acme") -> tuple[str, httpx.Response]:
    txn_id = h.start_login()
    _, csrf = h.get_login_page(txn_id)
    form = {
        "txn_id": txn_id,
        "csrf_token": csrf,
        "choice": "sso",
        "company_identifier": identifier,
    }
    return txn_id, h.post_login(form)


# ---------------------------------------------------------------------------
# Proxy construction and wiring
# ---------------------------------------------------------------------------


def test_proxy_wires_multi_realm_verifier(monkeypatch: MonkeyPatch) -> None:
    proxy = _make_proxy(monkeypatch)
    assert isinstance(proxy._token_validator, sso.MultiRealmTokenVerifier)  # noqa: SLF001
    assert proxy.required_scopes == SCOPES


def test_proxy_rejects_custom_token_verifier(monkeypatch: MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="token_verifier"):
        _make_proxy(monkeypatch, token_verifier=_RecordingVerifier("x"))


def test_proxy_factory_plugs_into_build_mcp_auth(monkeypatch: MonkeyPatch) -> None:
    _patch_default_discovery(monkeypatch)
    factory = sso.make_sso_proxy_factory(_config())
    assert isinstance(factory, functools.partial)
    assert factory.func is sso.AirbyteSsoOidcProxy
    auth = build_mcp_auth(
        oidc=OIDCAuthConfig(
            config_url=DEFAULT_CONFIG_URL,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            base_url=BASE_URL,
            required_scopes=SCOPES,
            client_storage=MemoryStore(),
            require_authorization_consent="external",
            extra_authorize_params={"prompt": "consent"},
            proxy_factory=factory,
        )
    )
    assert isinstance(auth, sso.AirbyteSsoOidcProxy)


def test_upstream_properties_follow_the_active_realm(monkeypatch: MonkeyPatch) -> None:
    proxy = _make_proxy(monkeypatch)
    acme = asyncio.run(proxy._registry.get_endpoints("acme"))  # noqa: SLF001
    assert acme is not None

    default_auth = proxy._upstream_authorization_endpoint  # noqa: SLF001
    assert default_auth == f"{DEFAULT_ISSUER}/protocol/openid-connect/auth"
    assert (
        proxy._upstream_token_endpoint
        == f"{DEFAULT_ISSUER}/protocol/openid-connect/token"
    )  # noqa: SLF001
    assert proxy._extra_authorize_params == {"prompt": "consent"}  # noqa: SLF001
    assert sso.active_realm() is None

    with sso.realm_context(acme):
        assert sso.active_realm() is acme
        assert proxy._upstream_authorization_endpoint == acme.authorization_endpoint  # noqa: SLF001
        assert proxy._upstream_token_endpoint == acme.token_endpoint  # noqa: SLF001
        assert proxy._upstream_revocation_endpoint == acme.revocation_endpoint  # noqa: SLF001
        assert proxy._extra_authorize_params == {"kc_idp_hint": "default"}  # noqa: SLF001
        # Same client in every realm: the platform clones it with the same secret.
        assert proxy._upstream_client_id == CLIENT_ID  # noqa: SLF001
        assert proxy._upstream_client_secret is not None  # noqa: SLF001
        assert proxy._upstream_client_secret.get_secret_value() == CLIENT_SECRET  # noqa: SLF001

    assert proxy._upstream_authorization_endpoint == default_auth  # noqa: SLF001
    assert sso.active_realm() is None


def test_sso_branch_sends_no_idp_hint_when_unconfigured(
    monkeypatch: MonkeyPatch,
) -> None:
    proxy = _make_proxy(monkeypatch, config=_config(idp_hint=None))
    acme = asyncio.run(proxy._registry.get_endpoints("acme"))  # noqa: SLF001
    with sso.realm_context(acme):
        assert proxy._extra_authorize_params == {}  # noqa: SLF001


def test_compatibility_check_names_missing_fastmcp_members(
    monkeypatch: MonkeyPatch,
) -> None:
    sso.check_fastmcp_compatibility()
    monkeypatch.setattr(
        sso, "_OVERRIDDEN_FASTMCP_METHODS", (*sso._OVERRIDDEN_FASTMCP_METHODS, "_gone")
    )  # noqa: SLF001
    monkeypatch.setattr(
        sso,
        "_REQUIRED_TRANSACTION_FIELDS",
        (*sso._REQUIRED_TRANSACTION_FIELDS, "vanished"),  # noqa: SLF001
    )
    with pytest.raises(RuntimeError, match=r"_gone.*OAuthTransaction\.vanished"):
        sso.check_fastmcp_compatibility()


# ---------------------------------------------------------------------------
# Login page
# ---------------------------------------------------------------------------


def test_login_page_requires_a_live_transaction(harness: _Harness) -> None:
    assert harness.client.get(sso.SSO_LOGIN_PATH).status_code == 400
    assert harness.client.get(f"{sso.SSO_LOGIN_PATH}?txn_id=nope").status_code == 400


def test_login_page_renders_both_choices(harness: _Harness) -> None:
    txn_id = harness.start_login()
    response, csrf = harness.get_login_page(txn_id)

    assert response.status_code == 200
    assert response.headers["x-frame-options"] == "DENY"
    assert "Continue with Airbyte Cloud" in response.text
    assert "Sign in with SSO" in response.text
    assert "Test Client" in response.text
    assert MCP_REDIRECT_URI in response.text
    assert f'name="txn_id" value="{txn_id}"' in response.text
    assert csrf
    assert f"__Host-{sso.LOGIN_STATE_COOKIE}" in response.cookies
    txn = harness.transaction(txn_id)
    assert txn.csrf_token == csrf
    assert txn.consent_token is None


def test_login_page_prefills_last_sso_identifier(harness: _Harness) -> None:
    _login_sso(harness, "acme")
    txn_id = harness.start_login()
    response, _ = harness.get_login_page(txn_id)
    assert 'name="company_identifier"' in response.text
    assert 'value="acme"' in response.text


# ---------------------------------------------------------------------------
# Login submission
# ---------------------------------------------------------------------------


def test_default_choice_redirects_to_default_realm(harness: _Harness) -> None:
    txn_id, response = _login_default(harness)

    assert response.status_code == 302
    location = urlsplit(response.headers["location"])
    query = parse_qs(location.query)
    assert f"{location.scheme}://{location.netloc}{location.path}" == (
        f"{DEFAULT_ISSUER}/protocol/openid-connect/auth"
    )
    assert query["client_id"] == [CLIENT_ID]
    assert query["state"] == [txn_id]
    assert query["prompt"] == ["consent"]
    assert "kc_idp_hint" not in query
    assert query["redirect_uri"] == [f"{BASE_URL}/auth/callback"]
    assert "__Host-MCP_CONSENT_BINDING" in response.cookies
    assert harness.transaction(txn_id).consent_token
    assert harness.choice(txn_id) is None


def test_sso_choice_redirects_to_realm_with_same_client(harness: _Harness) -> None:
    txn_id, response = _login_sso(harness, "acme")

    assert response.status_code == 302
    location = urlsplit(response.headers["location"])
    query = parse_qs(location.query)
    assert f"{location.scheme}://{location.netloc}{location.path}" == (
        f"{_issuer('acme')}/protocol/openid-connect/auth"
    )
    assert query["client_id"] == [CLIENT_ID]
    assert query["state"] == [txn_id]
    assert query["kc_idp_hint"] == ["default"]
    assert "prompt" not in query
    assert query["scope"] == [" ".join(SCOPES)]
    assert query["code_challenge_method"] == ["S256"]
    txn = harness.transaction(txn_id)
    expected_challenge = (
        base64.urlsafe_b64encode(
            hashlib.sha256(txn.proxy_code_verifier.encode()).digest()
        )
        .decode()
        .rstrip("=")
    )
    assert query["code_challenge"] == [expected_challenge]
    assert txn.consent_token
    assert "__Host-MCP_CONSENT_BINDING" in response.cookies
    assert f"__Host-{sso.LAST_REALM_COOKIE}" in response.cookies
    choice = harness.choice(txn_id)
    assert choice is not None
    assert choice.realm == "acme"


@pytest.mark.parametrize(
    "identifier, expected_error",
    [
        pytest.param("", "Enter your company identifier", id="empty"),
        pytest.param("bad/realm", "letters, numbers", id="invalid-chars"),
        pytest.param("master", "not an SSO company identifier", id="reserved"),
        pytest.param(DEFAULT_REALM, "letters, numbers", id="default-realm-is-internal"),
        pytest.param(
            "unknown-co", "No SSO configuration was found", id="no-such-realm"
        ),
    ],
)
def test_sso_choice_re_renders_form_on_bad_identifier(
    harness: _Harness, identifier: str, expected_error: str
) -> None:
    txn_id, response = _login_sso(harness, identifier)

    assert response.status_code == 400
    assert expected_error in response.text
    assert "Sign in with SSO" in response.text
    assert f'name="txn_id" value="{txn_id}"' in response.text
    assert harness.choice(txn_id) is None
    assert harness.transaction(txn_id).consent_token is None


def test_sso_choice_returns_503_when_discovery_is_down(
    monkeypatch: MonkeyPatch,
) -> None:
    proxy = _make_proxy(
        monkeypatch, fetch=_FakeFetch({"acme": httpx.ConnectTimeout("down")})
    )
    with TestClient(
        Starlette(routes=proxy.get_routes("/mcp")), base_url=BASE_URL
    ) as client:
        h = _Harness(proxy, client)
        _, response = _login_sso(h, "acme")
    assert response.status_code == 503
    assert "temporarily unavailable" in response.text


def test_login_post_rejects_unknown_choice(harness: _Harness) -> None:
    txn_id = harness.start_login()
    _, csrf = harness.get_login_page(txn_id)
    response = harness.post_login({
        "txn_id": txn_id,
        "csrf_token": csrf,
        "choice": "other",
    })
    assert response.status_code == 400


def test_login_post_rejects_bad_csrf_token(harness: _Harness) -> None:
    txn_id = harness.start_login()
    harness.get_login_page(txn_id)
    response = harness.post_login({
        "txn_id": txn_id,
        "csrf_token": "wrong",
        "choice": "default",
    })
    assert response.status_code == 400
    assert harness.transaction(txn_id).consent_token is None


def test_login_post_rejects_csrf_token_without_matching_cookie(
    harness: _Harness,
) -> None:
    """A valid form token from another browser fails the double-submit check."""
    txn_id = harness.start_login()
    _, csrf = harness.get_login_page(txn_id)
    with TestClient(harness.client.app, base_url=BASE_URL) as other_browser:
        response = other_browser.post(
            sso.SSO_LOGIN_PATH,
            data={"txn_id": txn_id, "csrf_token": csrf, "choice": "default"},
            follow_redirects=False,
        )
    assert response.status_code == 403
    assert harness.transaction(txn_id).consent_token is None


def test_login_csrf_token_is_single_use(harness: _Harness) -> None:
    txn_id = harness.start_login()
    _, csrf = harness.get_login_page(txn_id)
    form = {"txn_id": txn_id, "csrf_token": csrf, "choice": "default"}
    assert harness.post_login(form).status_code == 302
    assert harness.post_login(form).status_code == 400


def test_login_post_rejects_missing_transaction(harness: _Harness) -> None:
    response = harness.post_login({
        "txn_id": "nope",
        "csrf_token": "x",
        "choice": "default",
    })
    assert response.status_code == 400


def test_login_post_passes_host_origin_guard(monkeypatch: MonkeyPatch) -> None:
    proxy = _make_proxy(monkeypatch)
    guarded = HostOriginGuardMiddleware(
        Starlette(routes=proxy.get_routes("/mcp")), allowed_hosts=("mcp.example",)
    )
    with TestClient(guarded, base_url=BASE_URL) as client:
        h = _Harness(proxy, client)
        txn_id = h.start_login()
        _, csrf = h.get_login_page(txn_id)
        form = {"txn_id": txn_id, "csrf_token": csrf, "choice": "default"}
        forged = h.post_login(form, headers={"origin": "https://attacker.example"})
        assert forged.status_code == 403
        genuine = h.post_login(form, headers={"origin": BASE_URL})
        assert genuine.status_code == 302


# ---------------------------------------------------------------------------
# IdP callback
# ---------------------------------------------------------------------------


def _install_fake_upstream(
    monkeypatch: MonkeyPatch, proxy: sso.AirbyteSsoOidcProxy, issuer: str
) -> _FakeUpstreamOAuthClient:
    fake = _FakeUpstreamOAuthClient(_unsigned_jwt({"iss": issuer, "sub": "user"}))
    monkeypatch.setattr(proxy, "_create_upstream_oauth_client", lambda: fake)
    return fake


def test_callback_rejects_browser_without_binding_cookie(harness: _Harness) -> None:
    txn_id, _ = _login_sso(harness, "acme")
    with TestClient(harness.client.app, base_url=BASE_URL) as other_browser:
        response = other_browser.get(
            f"/auth/callback?code=idp-code&state={txn_id}", follow_redirects=False
        )
    assert response.status_code == 403
    assert "session mismatch" in response.text
    assert harness.choice(txn_id) is not None


def test_callback_rejects_transaction_that_skipped_the_login_page(
    harness: _Harness,
) -> None:
    """A callback for a transaction with no login-page submission has no binding."""
    txn_id = harness.start_login()
    response = harness.client.get(
        f"/auth/callback?code=idp-code&state={txn_id}", follow_redirects=False
    )
    assert response.status_code == 403


def test_callback_still_renders_fastmcp_errors(harness: _Harness) -> None:
    assert harness.client.get("/auth/callback?error=access_denied").status_code == 400
    assert harness.client.get("/auth/callback?code=x").status_code == 400
    assert harness.client.get("/auth/callback?code=x&state=nope").status_code == 400


def test_sso_callback_exchanges_code_at_the_realm_token_endpoint(
    harness: _Harness, monkeypatch: MonkeyPatch
) -> None:
    txn_id, _ = _login_sso(harness, "acme")
    fake = _install_fake_upstream(monkeypatch, harness.proxy, _issuer("acme"))

    response = harness.client.get(
        f"/auth/callback?code=idp-code&state={txn_id}", follow_redirects=False
    )

    assert response.status_code == 302
    location = urlsplit(response.headers["location"])
    assert f"{location.scheme}://{location.netloc}{location.path}" == MCP_REDIRECT_URI
    query = parse_qs(location.query)
    assert query["state"] == ["client-state"]
    assert query["code"]
    assert len(fake.fetch_calls) == 1
    call = fake.fetch_calls[0]
    assert call["url"] == f"{_issuer('acme')}/protocol/openid-connect/token"
    assert call["code"] == "idp-code"
    assert call["redirect_uri"] == f"{BASE_URL}/auth/callback"
    assert call["code_verifier"]
    assert harness.choice(txn_id) is None
    assert harness.transaction(txn_id) is None


def test_default_callback_exchanges_code_at_the_default_token_endpoint(
    harness: _Harness, monkeypatch: MonkeyPatch
) -> None:
    txn_id, _ = _login_default(harness)
    fake = _install_fake_upstream(monkeypatch, harness.proxy, DEFAULT_ISSUER)

    response = harness.client.get(
        f"/auth/callback?code=idp-code&state={txn_id}", follow_redirects=False
    )

    assert response.status_code == 302
    assert (
        fake.fetch_calls[0]["url"] == f"{DEFAULT_ISSUER}/protocol/openid-connect/token"
    )


def test_sso_callback_returns_503_when_realm_discovery_is_down(
    monkeypatch: MonkeyPatch,
) -> None:
    fetch = _FakeFetch({"acme": _realm_ok("acme")})
    proxy = _make_proxy(
        monkeypatch, fetch=fetch, config=_config(discovery_cache_ttl_seconds=0)
    )
    with TestClient(
        Starlette(routes=proxy.get_routes("/mcp")), base_url=BASE_URL
    ) as client:
        h = _Harness(proxy, client)
        txn_id, _ = _login_sso(h, "acme")
        fetch.responses["acme"] = httpx.ConnectTimeout("down")
        response = client.get(
            f"/auth/callback?code=idp-code&state={txn_id}", follow_redirects=False
        )
    assert response.status_code == 503


# ---------------------------------------------------------------------------
# Refresh and revocation route by the upstream token's issuer
# ---------------------------------------------------------------------------


def _upstream_token_set(
    issuer: str, *, token_id: str = "upstream-1"
) -> UpstreamTokenSet:
    now = time.time()
    return UpstreamTokenSet(
        upstream_token_id=token_id,
        access_token=_unsigned_jwt({"iss": issuer, "sub": "user"}),
        refresh_token="upstream-refresh",
        refresh_token_expires_at=now + 3600,
        expires_at=now - 1,
        token_type="Bearer",
        scope=" ".join(SCOPES),
        client_id=MCP_CLIENT_ID,
        created_at=now,
    )


@pytest.mark.parametrize(
    "issuer, expected_realm_issuer",
    [
        pytest.param(_issuer("acme"), _issuer("acme"), id="sso-realm"),
        pytest.param(DEFAULT_ISSUER, DEFAULT_ISSUER, id="default-realm"),
        pytest.param(
            "https://evil.example/realms/acme", DEFAULT_ISSUER, id="foreign-falls-back"
        ),
    ],
)
def test_transparent_refresh_uses_the_issuing_realm(
    monkeypatch: MonkeyPatch, issuer: str, expected_realm_issuer: str
) -> None:
    proxy = _make_proxy(monkeypatch)
    fake = _install_fake_upstream(monkeypatch, proxy, issuer)
    token_set = _upstream_token_set(issuer)

    refreshed = asyncio.run(proxy._try_transparent_refresh(token_set))  # noqa: SLF001

    assert refreshed.access_token == fake.access_token
    assert fake.refresh_calls[0]["url"] == (
        f"{expected_realm_issuer}/protocol/openid-connect/token"
    )
    assert fake.refresh_calls[0]["refresh_token"] == "upstream-refresh"


def test_refresh_token_exchange_uses_the_issuing_realm(
    monkeypatch: MonkeyPatch,
) -> None:
    proxy = _make_proxy(monkeypatch)
    proxy.get_routes("/mcp")  # initializes the JWT issuer
    fake = _install_fake_upstream(monkeypatch, proxy, _issuer("acme"))

    async def scenario() -> list[dict[str, Any]]:
        await proxy.register_client(
            OAuthClientInformationFull(
                client_id=MCP_CLIENT_ID, redirect_uris=[AnyUrl(MCP_REDIRECT_URI)]
            )
        )
        client = await proxy.get_client(MCP_CLIENT_ID)
        assert client is not None
        await proxy._upstream_token_store.put(  # noqa: SLF001
            key="upstream-1", value=_upstream_token_set(_issuer("acme")), ttl=3600
        )
        await proxy._jti_mapping_store.put(  # noqa: SLF001
            key="refresh-jti",
            value=JTIMapping(
                jti="refresh-jti",
                upstream_token_id="upstream-1",
                created_at=time.time(),
            ),
            ttl=3600,
        )
        proxy_refresh = proxy.jwt_issuer.issue_refresh_token(
            client_id=MCP_CLIENT_ID, scopes=SCOPES, jti="refresh-jti", expires_in=3600
        )
        result = await proxy.exchange_refresh_token(
            client,
            RefreshToken(
                token=proxy_refresh,
                client_id=MCP_CLIENT_ID,
                scopes=SCOPES,
                expires_at=None,
            ),
            SCOPES,
        )
        assert result.access_token
        return fake.refresh_calls

    calls = asyncio.run(scenario())
    assert calls[0]["url"] == f"{_issuer('acme')}/protocol/openid-connect/token"


def test_transparent_refresh_fails_closed_when_realm_discovery_is_down(
    monkeypatch: MonkeyPatch,
) -> None:
    proxy = _make_proxy(
        monkeypatch, fetch=_FakeFetch({"acme": httpx.ConnectTimeout("down")})
    )
    fake = _install_fake_upstream(monkeypatch, proxy, _issuer("acme"))
    with pytest.raises(sso.RealmDiscoveryUnavailableError):
        asyncio.run(
            proxy._try_transparent_refresh(_upstream_token_set(_issuer("acme")))
        )  # noqa: SLF001
    assert fake.refresh_calls == []


def test_refresh_token_exchange_fails_closed_when_realm_discovery_is_down(
    monkeypatch: MonkeyPatch,
) -> None:
    """A refresh for an SSO token must not be retried against the default realm."""
    proxy = _make_proxy(
        monkeypatch, fetch=_FakeFetch({"acme": httpx.ConnectTimeout("down")})
    )
    proxy.get_routes("/mcp")
    fake = _install_fake_upstream(monkeypatch, proxy, _issuer("acme"))

    async def scenario() -> None:
        await proxy.register_client(
            OAuthClientInformationFull(
                client_id=MCP_CLIENT_ID, redirect_uris=[AnyUrl(MCP_REDIRECT_URI)]
            )
        )
        client = await proxy.get_client(MCP_CLIENT_ID)
        assert client is not None
        await proxy._upstream_token_store.put(  # noqa: SLF001
            key="upstream-1", value=_upstream_token_set(_issuer("acme")), ttl=3600
        )
        await proxy._jti_mapping_store.put(  # noqa: SLF001
            key="refresh-jti",
            value=JTIMapping(
                jti="refresh-jti",
                upstream_token_id="upstream-1",
                created_at=time.time(),
            ),
            ttl=3600,
        )
        proxy_refresh = proxy.jwt_issuer.issue_refresh_token(
            client_id=MCP_CLIENT_ID, scopes=SCOPES, jti="refresh-jti", expires_in=3600
        )
        with pytest.raises(TokenError) as excinfo:
            await proxy.exchange_refresh_token(
                client,
                RefreshToken(
                    token=proxy_refresh,
                    client_id=MCP_CLIENT_ID,
                    scopes=SCOPES,
                    expires_at=None,
                ),
                SCOPES,
            )
        assert excinfo.value.error == "invalid_grant"

    asyncio.run(scenario())
    assert fake.refresh_calls == []


def _capture_upstream_posts(monkeypatch: MonkeyPatch) -> list[str]:
    """Replace `httpx.AsyncClient` inside FastMCP's proxy with a stub that records POST URLs."""
    posts: list[str] = []

    class _FakeAsyncClient:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        async def __aenter__(self) -> _FakeAsyncClient:
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

        async def post(self, url: str, **_kwargs: Any) -> httpx.Response:
            posts.append(url)
            return httpx.Response(200)

    monkeypatch.setattr(fastmcp_proxy_module.httpx, "AsyncClient", _FakeAsyncClient)
    return posts


def test_revocation_of_upstream_access_token_posts_to_the_issuing_realm(
    monkeypatch: MonkeyPatch,
) -> None:
    proxy = _make_proxy(monkeypatch)
    _install_fake_upstream(monkeypatch, proxy, _issuer("acme"))
    posts = _capture_upstream_posts(monkeypatch)
    upstream_access = AccessToken(
        token=_unsigned_jwt({"iss": _issuer("acme")}),
        client_id=MCP_CLIENT_ID,
        scopes=SCOPES,
    )
    asyncio.run(proxy.revoke_token(upstream_access))
    assert posts == [f"{_issuer('acme')}/protocol/openid-connect/revoke"]


def test_revocation_skips_upstream_when_realm_discovery_is_down(
    monkeypatch: MonkeyPatch,
) -> None:
    """Revocation never falls back to the default realm; it just skips the upstream call."""
    proxy = _make_proxy(
        monkeypatch, fetch=_FakeFetch({"acme": httpx.ConnectTimeout("down")})
    )
    _install_fake_upstream(monkeypatch, proxy, _issuer("acme"))
    posts = _capture_upstream_posts(monkeypatch)
    upstream_access = AccessToken(
        token=_unsigned_jwt({"iss": _issuer("acme")}),
        client_id=MCP_CLIENT_ID,
        scopes=SCOPES,
    )
    asyncio.run(proxy.revoke_token(upstream_access))
    assert posts == []


def test_revocation_of_proxy_refresh_token_posts_to_the_issuing_realm(
    monkeypatch: MonkeyPatch,
) -> None:
    """A FastMCP refresh token names no realm itself; it resolves through the JTI mapping."""
    proxy = _make_proxy(monkeypatch)
    proxy.get_routes("/mcp")  # initializes the JWT issuer
    _install_fake_upstream(monkeypatch, proxy, _issuer("acme"))
    posts = _capture_upstream_posts(monkeypatch)

    async def scenario() -> None:
        await proxy._upstream_token_store.put(  # noqa: SLF001
            key="upstream-1", value=_upstream_token_set(_issuer("acme")), ttl=3600
        )
        await proxy._jti_mapping_store.put(  # noqa: SLF001
            key="refresh-jti",
            value=JTIMapping(
                jti="refresh-jti",
                upstream_token_id="upstream-1",
                created_at=time.time(),
            ),
            ttl=3600,
        )
        proxy_refresh = proxy.jwt_issuer.issue_refresh_token(
            client_id=MCP_CLIENT_ID, scopes=SCOPES, jti="refresh-jti", expires_in=3600
        )
        await proxy.revoke_token(
            RefreshToken(
                token=proxy_refresh,
                client_id=MCP_CLIENT_ID,
                scopes=SCOPES,
                expires_at=None,
            )
        )

    asyncio.run(scenario())
    assert posts == [f"{_issuer('acme')}/protocol/openid-connect/revoke"]
