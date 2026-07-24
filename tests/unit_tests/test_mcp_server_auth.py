# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for headless auth env resolution in `airbyte.mcp.server`."""

from __future__ import annotations

import os

import pytest
from fastmcp_extensions import JWTAuthConfig, OIDCAuthConfig


# Importing `airbyte.mcp.server` runs `load_secrets_to_env_vars()` at module
# import time, which raises if `AIRBYTE_MCP_ENV_FILE` points at a missing file.
# Drop any stale value from the ambient environment before importing so test
# collection doesn't fail on an external dotenv path unrelated to these tests,
# then restore it so this import doesn't permanently mutate the process env for
# other test modules.
_PRIOR_MCP_ENV_FILE = os.environ.pop("AIRBYTE_MCP_ENV_FILE", None)

from airbyte.mcp import server  # noqa: E402


if _PRIOR_MCP_ENV_FILE is not None:
    os.environ["AIRBYTE_MCP_ENV_FILE"] = _PRIOR_MCP_ENV_FILE


def test_env_or_default_uses_default_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SOME_VAR", raising=False)
    assert server._env_or_default("SOME_VAR", "fallback") == "fallback"


@pytest.mark.parametrize("blank", ["", "   ", "\t", "\n "])
def test_env_or_default_treats_blank_as_unset(
    blank: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOME_VAR", blank)
    assert server._env_or_default("SOME_VAR", "fallback") == "fallback"


def test_env_or_default_strips_and_returns_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOME_VAR", "  actual  ")
    assert server._env_or_default("SOME_VAR", "fallback") == "actual"


def test_resolve_signing_key_defaults_to_cloud_jwks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(server.JWKS_URI_ENV, raising=False)
    monkeypatch.delenv(server.JWT_PUBLIC_KEY_ENV, raising=False)
    jwks_uri, public_key = server._resolve_signing_key()
    assert jwks_uri == server.AIRBYTE_CLOUD_JWKS_URI
    assert public_key == ""


def test_resolve_signing_key_blank_values_fall_back_to_cloud(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(server.JWKS_URI_ENV, "   ")
    monkeypatch.setenv(server.JWT_PUBLIC_KEY_ENV, "  ")
    jwks_uri, public_key = server._resolve_signing_key()
    assert jwks_uri == server.AIRBYTE_CLOUD_JWKS_URI
    assert public_key == ""


def test_resolve_signing_key_honors_explicit_jwks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(server.JWKS_URI_ENV, "https://self-hosted/jwks")
    monkeypatch.delenv(server.JWT_PUBLIC_KEY_ENV, raising=False)
    jwks_uri, _public_key = server._resolve_signing_key()
    assert jwks_uri == "https://self-hosted/jwks"


def test_resolve_signing_key_static_key_not_shadowed_by_cloud_jwks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(server.JWKS_URI_ENV, raising=False)
    monkeypatch.setenv(server.JWT_PUBLIC_KEY_ENV, "-----BEGIN PUBLIC KEY-----")
    jwks_uri, public_key = server._resolve_signing_key()
    # Cloud JWKS default must not be injected when a static key is supplied.
    assert jwks_uri == ""
    assert public_key == "-----BEGIN PUBLIC KEY-----"


def _clear_all_auth_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        server.JWT_ISSUER_ENV,
        server.JWT_AUDIENCE_ENV,
        server.JWT_ALGORITHM_ENV,
        server.JWKS_URI_ENV,
        server.JWT_PUBLIC_KEY_ENV,
        server.OIDC_CLIENT_ID_ENV,
        server.OIDC_CLIENT_SECRET_ENV,
        server.OIDC_CONFIG_URL_ENV,
        server.MCP_SERVER_URL_ENV,
    ):
        monkeypatch.delenv(name, raising=False)


def _capture_build_mcp_auth(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """Patch `build_mcp_auth` with a spy and return the dict it records into."""
    captured: dict[str, object] = {}

    def _capture(**kwargs: object) -> None:
        captured.update(kwargs)
        return None

    monkeypatch.setattr(server, "build_mcp_auth", _capture)
    return captured


def test_create_auth_builds_cloud_jwt_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Zero auth env yields a JWT config carrying the Airbyte Cloud realm defaults."""
    _clear_all_auth_env(monkeypatch)
    captured = _capture_build_mcp_auth(monkeypatch)
    server._create_auth()

    jwt = captured["jwt"]
    assert isinstance(jwt, JWTAuthConfig)
    assert jwt.jwks_uri == server.AIRBYTE_CLOUD_JWKS_URI
    assert jwt.public_key is None
    assert jwt.issuer == server.AIRBYTE_CLOUD_ISSUER
    assert jwt.audience == server.AIRBYTE_CLOUD_AUDIENCE
    assert jwt.algorithm == server.AIRBYTE_CLOUD_ALGORITHM
    # No OIDC without client credentials.
    assert captured["oidc"] is None


def test_create_auth_overrides_jwt_claims_from_branded_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Branded `AIRBYTE_MCP_AUTH_*` env vars override the Cloud realm defaults."""
    _clear_all_auth_env(monkeypatch)
    monkeypatch.setenv(server.JWT_ISSUER_ENV, "https://self-hosted/realm")
    monkeypatch.setenv(server.JWT_AUDIENCE_ENV, "self-hosted-aud")
    monkeypatch.setenv(server.JWT_ALGORITHM_ENV, "ES256")
    captured = _capture_build_mcp_auth(monkeypatch)
    server._create_auth()

    jwt = captured["jwt"]
    assert isinstance(jwt, JWTAuthConfig)
    assert jwt.issuer == "https://self-hosted/realm"
    assert jwt.audience == "self-hosted-aud"
    assert jwt.algorithm == "ES256"


def test_create_auth_builds_oidc_when_credentials_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OIDC client creds activate an `OIDCAuthConfig` defaulting to Airbyte Cloud."""
    _clear_all_auth_env(monkeypatch)
    monkeypatch.setenv(server.OIDC_CLIENT_ID_ENV, "cid")
    monkeypatch.setenv(server.OIDC_CLIENT_SECRET_ENV, "sec")
    captured = _capture_build_mcp_auth(monkeypatch)
    server._create_auth()

    oidc = captured["oidc"]
    assert isinstance(oidc, OIDCAuthConfig)
    assert oidc.client_id == "cid"
    assert oidc.client_secret == "sec"
    assert oidc.config_url == server.AIRBYTE_CLOUD_OIDC_CONFIG_URL


def test_create_auth_omits_oidc_without_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A bare client id (no secret) does not activate the interactive OIDC path."""
    _clear_all_auth_env(monkeypatch)
    monkeypatch.setenv(server.OIDC_CLIENT_ID_ENV, "cid")
    captured = _capture_build_mcp_auth(monkeypatch)
    server._create_auth()
    assert captured["oidc"] is None
