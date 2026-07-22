# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for headless auth env resolution in `airbyte.mcp.server`."""

from __future__ import annotations

import os

import pytest


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
    resolved = server._resolve_signing_key()
    assert resolved["MCP_AUTH_JWKS_URI"] == server.AIRBYTE_CLOUD_JWKS_URI
    assert resolved["MCP_AUTH_JWT_PUBLIC_KEY"] == ""


def test_resolve_signing_key_blank_values_fall_back_to_cloud(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(server.JWKS_URI_ENV, "   ")
    monkeypatch.setenv(server.JWT_PUBLIC_KEY_ENV, "  ")
    resolved = server._resolve_signing_key()
    assert resolved["MCP_AUTH_JWKS_URI"] == server.AIRBYTE_CLOUD_JWKS_URI
    assert resolved["MCP_AUTH_JWT_PUBLIC_KEY"] == ""


def test_resolve_signing_key_honors_explicit_jwks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(server.JWKS_URI_ENV, "https://self-hosted/jwks")
    monkeypatch.delenv(server.JWT_PUBLIC_KEY_ENV, raising=False)
    resolved = server._resolve_signing_key()
    assert resolved["MCP_AUTH_JWKS_URI"] == "https://self-hosted/jwks"


def test_resolve_signing_key_static_key_not_shadowed_by_cloud_jwks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(server.JWKS_URI_ENV, raising=False)
    monkeypatch.setenv(server.JWT_PUBLIC_KEY_ENV, "-----BEGIN PUBLIC KEY-----")
    resolved = server._resolve_signing_key()
    # Cloud JWKS default must not be injected when a static key is supplied.
    assert resolved["MCP_AUTH_JWKS_URI"] == ""
    assert resolved["MCP_AUTH_JWT_PUBLIC_KEY"] == "-----BEGIN PUBLIC KEY-----"
