# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for the Airbyte-specific client-credentials transport policy.

The provider-neutral exchange middleware and its internals (Basic decoding,
token caching/locking, minting) live in `fastmcp_extensions` and are tested
there. These tests cover only what this module owns: the opt-in env toggle, the
Airbyte Cloud token-endpoint resolution, and the `wrap_if_enabled` wiring.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from airbyte.mcp import _client_credentials as cc


if TYPE_CHECKING:
    from starlette.types import Receive, Scope, Send


class _RecordingApp:
    """Minimal downstream ASGI app used as a wrap target."""

    async def __call__(self, scope: Scope, _receive: Receive, _send: Send) -> None:
        return None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("1", True, id="one"),
        pytest.param("true", True, id="true"),
        pytest.param("TRUE", True, id="true-uppercase"),
        pytest.param("  yes ", True, id="yes-padded"),
        pytest.param("on", True, id="on"),
        pytest.param("", False, id="empty"),
        pytest.param("0", False, id="zero"),
        pytest.param("false", False, id="false"),
        pytest.param("nope", False, id="arbitrary"),
    ],
)
def test_client_credentials_enabled(value: str, expected: bool) -> None:
    assert (
        cc.client_credentials_enabled({cc.ALLOW_CLIENT_CREDENTIALS_ENV: value})
        is expected
    )


def test_client_credentials_enabled_absent() -> None:
    assert cc.client_credentials_enabled({}) is False


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(None, cc.AIRBYTE_CLOUD_TOKEN_URL, id="unset-uses-default"),
        pytest.param("", cc.AIRBYTE_CLOUD_TOKEN_URL, id="empty-uses-default"),
        pytest.param("   ", cc.AIRBYTE_CLOUD_TOKEN_URL, id="whitespace-uses-default"),
        pytest.param(
            "https://self-hosted/token",
            "https://self-hosted/token",
            id="explicit-override-honored",
        ),
        pytest.param(
            "  https://self-hosted/token  ",
            "https://self-hosted/token",
            id="explicit-override-stripped",
        ),
    ],
)
def test_token_url(
    value: str | None,
    expected: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if value is None:
        monkeypatch.delenv(cc.TOKEN_URL_ENV, raising=False)
    else:
        monkeypatch.setenv(cc.TOKEN_URL_ENV, value)
    assert cc._token_url() == expected


def test_wrap_if_enabled_returns_app_unchanged_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(cc.ALLOW_CLIENT_CREDENTIALS_ENV, raising=False)
    app = _RecordingApp()
    assert cc.wrap_if_enabled(app) is app


def test_wrap_if_enabled_wraps_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(cc.ALLOW_CLIENT_CREDENTIALS_ENV, "true")
    app = _RecordingApp()
    wrapped = cc.wrap_if_enabled(app)
    assert wrapped is not app
