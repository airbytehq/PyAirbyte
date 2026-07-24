# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for branded transport-auth resolution in `airbyte.mcp.server`.

These cover what this server owns: mapping its branded `AIRBYTE_MCP_*` env vars
into the typed `JWTAuthConfig` / `OIDCAuthConfig` objects it hands to
`fastmcp_extensions.build_mcp_auth`, activation of the headless and interactive
paths from env, blank-as-unset handling, and the durable-storage factory
injection on the interactive path. This server declares only env var names and
maps their values — it embeds no provider-specific configuration values; those
are supplied at deploy time by the deployment's own repo. The generic verifier
assembly lives in `fastmcp-extensions` and is tested there.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from fastmcp.server.auth.providers.jwt import JWTVerifier
from fastmcp_extensions import JWTAuthConfig, OIDCAuthConfig

from airbyte.mcp import server


if TYPE_CHECKING:
    from pytest import MonkeyPatch


_ALL_AUTH_ENV = (
    server.MCP_SERVER_URL_ENV,
    server.OIDC_CLIENT_ID_ENV,
    server.OIDC_CLIENT_SECRET_ENV,
    server.OIDC_CONFIG_URL_ENV,
    server.OIDC_CLIENT_STORAGE_FACTORY_ENV,
    server.JWKS_URI_ENV,
    server.JWT_PUBLIC_KEY_ENV,
    server.JWT_ISSUER_ENV,
    server.JWT_AUDIENCE_ENV,
    server.JWT_ALGORITHM_ENV,
)


def _clear_all_auth_env(monkeypatch: MonkeyPatch) -> None:
    for name in _ALL_AUTH_ENV:
        monkeypatch.delenv(name, raising=False)


def _capture_build_mcp_auth(monkeypatch: MonkeyPatch) -> dict[str, Any]:
    """Patch `build_mcp_auth` with a spy and return the dict it records into."""
    captured: dict[str, Any] = {}

    def _capture(**kwargs: Any) -> None:
        captured.update(kwargs)
        return None

    monkeypatch.setattr(server, "build_mcp_auth", _capture)
    return captured


def test_auth_env_names_are_branded() -> None:
    """The transport-auth env vars all use this server's `AIRBYTE_MCP_*` namespace."""
    assert server.OIDC_CLIENT_ID_ENV == "AIRBYTE_MCP_OIDC_CLIENT_ID"
    assert server.OIDC_CLIENT_SECRET_ENV == "AIRBYTE_MCP_OIDC_CLIENT_SECRET"
    assert server.OIDC_CONFIG_URL_ENV == "AIRBYTE_MCP_OIDC_CONFIG_URL"
    assert (
        server.OIDC_CLIENT_STORAGE_FACTORY_ENV
        == "AIRBYTE_MCP_OIDC_CLIENT_STORAGE_FACTORY"
    )
    assert server.JWKS_URI_ENV == "AIRBYTE_MCP_AUTH_JWKS_URI"
    assert server.JWT_PUBLIC_KEY_ENV == "AIRBYTE_MCP_AUTH_JWT_PUBLIC_KEY"
    assert server.JWT_ISSUER_ENV == "AIRBYTE_MCP_AUTH_ISSUER"
    assert server.JWT_AUDIENCE_ENV == "AIRBYTE_MCP_AUTH_AUDIENCE"
    assert server.JWT_ALGORITHM_ENV == "AIRBYTE_MCP_AUTH_ALGORITHM"


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(None, "fallback", id="unset"),
        pytest.param("", "fallback", id="empty"),
        pytest.param("   ", "fallback", id="spaces"),
        pytest.param("\t", "fallback", id="tab"),
        pytest.param("  actual  ", "actual", id="strips-and-returns"),
    ],
)
def test_env_or_default(
    value: str | None,
    expected: str,
    monkeypatch: MonkeyPatch,
) -> None:
    """Blank/unset values fall back to the default; real values are stripped.

    The default (`"fallback"`) is deliberately distinct from the stripped value
    (`"actual"`), so the `strips-and-returns` case fails if the env value is
    ignored and the default is returned instead.
    """
    if value is None:
        monkeypatch.delenv("SOME_VAR", raising=False)
    else:
        monkeypatch.setenv("SOME_VAR", value)
    assert server._env_or_default("SOME_VAR", "fallback") == expected


def test_create_auth_no_env_yields_no_provider(monkeypatch: MonkeyPatch) -> None:
    """With zero auth env, neither path is configured (no embedded defaults)."""
    _clear_all_auth_env(monkeypatch)
    captured = _capture_build_mcp_auth(monkeypatch)
    server._create_auth()

    assert captured["oidc"] is None
    assert captured["jwt"] is None


def test_create_auth_no_env_returns_none(monkeypatch: MonkeyPatch) -> None:
    """The real `build_mcp_auth` yields no provider when nothing is configured."""
    _clear_all_auth_env(monkeypatch)
    assert server._create_auth() is None


def test_create_auth_maps_jwt_env_when_jwks_set(monkeypatch: MonkeyPatch) -> None:
    """A JWKS URI activates the headless verifier; issuer/audience/algorithm map through."""
    _clear_all_auth_env(monkeypatch)
    monkeypatch.setenv(server.JWKS_URI_ENV, "https://idp.example/jwks")
    monkeypatch.setenv(server.JWT_ISSUER_ENV, "https://idp.example/")
    monkeypatch.setenv(server.JWT_AUDIENCE_ENV, "mcp-api")
    monkeypatch.setenv(server.JWT_ALGORITHM_ENV, "RS256")
    captured = _capture_build_mcp_auth(monkeypatch)
    server._create_auth()

    jwt = captured["jwt"]
    assert isinstance(jwt, JWTAuthConfig)
    assert jwt.jwks_uri == "https://idp.example/jwks"
    assert jwt.public_key is None
    assert jwt.issuer == "https://idp.example/"
    assert jwt.audience == "mcp-api"
    assert jwt.algorithm == "RS256"


def test_create_auth_activates_jwt_with_static_public_key(
    monkeypatch: MonkeyPatch,
) -> None:
    """A static public key activates the verifier without a JWKS URI."""
    _clear_all_auth_env(monkeypatch)
    monkeypatch.setenv(server.JWT_PUBLIC_KEY_ENV, "-----BEGIN PUBLIC KEY-----")
    captured = _capture_build_mcp_auth(monkeypatch)
    server._create_auth()

    jwt = captured["jwt"]
    assert isinstance(jwt, JWTAuthConfig)
    assert jwt.jwks_uri is None
    assert jwt.public_key == "-----BEGIN PUBLIC KEY-----"


def test_create_auth_returns_verifier_when_jwks_set(monkeypatch: MonkeyPatch) -> None:
    """The real `build_mcp_auth` yields a bearer-token verifier when a JWKS URI is set."""
    _clear_all_auth_env(monkeypatch)
    monkeypatch.setenv(server.JWKS_URI_ENV, "https://idp.example/jwks")
    auth = server._create_auth()
    assert isinstance(auth, JWTVerifier)


def test_create_auth_blank_jwt_env_yields_no_verifier(monkeypatch: MonkeyPatch) -> None:
    """Blank/whitespace signing-key vars are treated as unset (no verifier)."""
    _clear_all_auth_env(monkeypatch)
    monkeypatch.setenv(server.JWKS_URI_ENV, "   ")
    monkeypatch.setenv(server.JWT_PUBLIC_KEY_ENV, "  ")
    captured = _capture_build_mcp_auth(monkeypatch)
    server._create_auth()
    assert captured["jwt"] is None


def test_create_auth_activates_oidc_when_credentials_present(
    monkeypatch: MonkeyPatch,
) -> None:
    """OIDC client id + secret + discovery URL activate the interactive path."""
    _clear_all_auth_env(monkeypatch)
    monkeypatch.setenv(server.OIDC_CLIENT_ID_ENV, "cid")
    monkeypatch.setenv(server.OIDC_CLIENT_SECRET_ENV, "csecret")
    monkeypatch.setenv(server.OIDC_CONFIG_URL_ENV, "https://idp.example/.well-known")
    captured = _capture_build_mcp_auth(monkeypatch)
    server._create_auth()

    oidc = captured["oidc"]
    assert isinstance(oidc, OIDCAuthConfig)
    assert oidc.client_id == "cid"
    assert oidc.client_secret == "csecret"
    assert oidc.config_url == "https://idp.example/.well-known"
    # No storage factory configured -> in-memory default.
    assert oidc.client_storage is None


def test_create_auth_oidc_without_config_url_raises(monkeypatch: MonkeyPatch) -> None:
    """OIDC credentials without a discovery URL fail clearly, naming the env var."""
    _clear_all_auth_env(monkeypatch)
    monkeypatch.setenv(server.OIDC_CLIENT_ID_ENV, "cid")
    monkeypatch.setenv(server.OIDC_CLIENT_SECRET_ENV, "csecret")
    with pytest.raises(ValueError, match=server.OIDC_CONFIG_URL_ENV):
        server._create_auth()


def test_create_auth_partial_oidc_id_only_raises(monkeypatch: MonkeyPatch) -> None:
    """A client id alone (no secret) fails closed rather than starting unauthenticated."""
    _clear_all_auth_env(monkeypatch)
    monkeypatch.setenv(server.OIDC_CLIENT_ID_ENV, "cid")
    with pytest.raises(ValueError, match=server.OIDC_CLIENT_SECRET_ENV):
        server._create_auth()


def test_create_auth_partial_oidc_secret_only_raises(monkeypatch: MonkeyPatch) -> None:
    """A client secret alone (no id) fails closed rather than starting unauthenticated."""
    _clear_all_auth_env(monkeypatch)
    monkeypatch.setenv(server.OIDC_CLIENT_SECRET_ENV, "csecret")
    with pytest.raises(ValueError, match=server.OIDC_CLIENT_ID_ENV):
        server._create_auth()


def test_resolve_client_storage_returns_none_when_unset(
    monkeypatch: MonkeyPatch,
) -> None:
    """No factory env var means the interactive proxy keeps its in-memory default."""
    monkeypatch.delenv(server.OIDC_CLIENT_STORAGE_FACTORY_ENV, raising=False)
    assert server._resolve_client_storage(encryption_source_material="s") is None


_STORAGE_FACTORY_CALLS: list[str] = []
_SENTINEL_STORE = object()


def _fake_store_factory(*, encryption_source_material: str) -> object:
    """Test factory recording its `encryption_source_material` and returning a sentinel."""
    _STORAGE_FACTORY_CALLS.append(encryption_source_material)
    return _SENTINEL_STORE


def test_resolve_client_storage_imports_and_calls_factory(
    monkeypatch: MonkeyPatch,
) -> None:
    """The factory env var is resolved to a callable and invoked with the secret."""
    _STORAGE_FACTORY_CALLS.clear()
    monkeypatch.setenv(
        server.OIDC_CLIENT_STORAGE_FACTORY_ENV,
        f"{__name__}:_fake_store_factory",
    )
    store = server._resolve_client_storage(encryption_source_material="the-secret")
    assert store is _SENTINEL_STORE
    assert _STORAGE_FACTORY_CALLS == ["the-secret"]


@pytest.mark.parametrize(
    "factory_spec",
    [
        pytest.param("not-a-valid-reference", id="malformed_reference"),
        pytest.param(f"{__name__}:_does_not_exist", id="missing_symbol"),
    ],
)
def test_resolve_client_storage_raises_clear_error_on_bad_factory(
    monkeypatch: MonkeyPatch,
    factory_spec: str,
) -> None:
    """A malformed/unresolvable factory reference fails with the env var named."""
    monkeypatch.setenv(server.OIDC_CLIENT_STORAGE_FACTORY_ENV, factory_spec)
    with pytest.raises(ValueError, match=server.OIDC_CLIENT_STORAGE_FACTORY_ENV):
        server._resolve_client_storage(encryption_source_material="s")


def test_create_auth_injects_resolved_storage_on_oidc(monkeypatch: MonkeyPatch) -> None:
    """The resolved storage object is passed through to `OIDCAuthConfig.client_storage`."""
    _STORAGE_FACTORY_CALLS.clear()
    _clear_all_auth_env(monkeypatch)
    monkeypatch.setenv(server.OIDC_CLIENT_ID_ENV, "cid")
    monkeypatch.setenv(server.OIDC_CLIENT_SECRET_ENV, "csecret")
    monkeypatch.setenv(server.OIDC_CONFIG_URL_ENV, "https://idp.example/.well-known")
    monkeypatch.setenv(
        server.OIDC_CLIENT_STORAGE_FACTORY_ENV,
        f"{__name__}:_fake_store_factory",
    )
    captured = _capture_build_mcp_auth(monkeypatch)
    server._create_auth()

    oidc = captured["oidc"]
    assert isinstance(oidc, OIDCAuthConfig)
    assert oidc.client_storage is _SENTINEL_STORE
    # The OIDC client secret is the encryption source material.
    assert _STORAGE_FACTORY_CALLS == ["csecret"]
