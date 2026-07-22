# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for the opt-in HTTP Basic client-credentials transport middleware."""

from __future__ import annotations

import asyncio
import base64
from typing import TYPE_CHECKING

import pytest

from airbyte.mcp import _client_credentials as cc


if TYPE_CHECKING:
    from starlette.types import Receive, Scope, Send


def _basic_header(client_id: str, client_secret: str) -> bytes:
    """Return a `Basic` header value encoding `client_id:client_secret`."""
    raw = f"{client_id}:{client_secret}".encode()
    return b"Basic " + base64.b64encode(raw)


def _http_scope(*header: tuple[bytes, bytes]) -> Scope:
    """Return a minimal HTTP ASGI scope carrying the given headers."""
    return {"type": "http", "headers": list(header)}


class _RecordingApp:
    """Downstream ASGI app that records the scope handed to it."""

    def __init__(self) -> None:
        self.seen_scope: Scope | None = None
        self.calls = 0

    async def __call__(self, scope: Scope, _receive: Receive, _send: Send) -> None:
        self.calls += 1
        self.seen_scope = scope


def _auth_header(scope: Scope) -> bytes | None:
    """Return the `authorization` header value from an ASGI scope, if present."""
    for name, value in scope["headers"]:
        if name == b"authorization":
            return value
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


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(900, 900.0, id="int"),
        pytest.param(900.5, 900.5, id="float"),
        pytest.param("900", 900.0, id="numeric-string"),
        pytest.param(None, 0.0, id="none"),
        pytest.param("not-a-number", 0.0, id="non-numeric-string"),
        pytest.param({}, 0.0, id="object"),
        pytest.param([], 0.0, id="list"),
        pytest.param(True, 0.0, id="bool-true"),
        pytest.param(0, 0.0, id="zero"),
        pytest.param(-5, 0.0, id="negative"),
    ],
)
def test_coerce_expires_in(value: object, expected: float) -> None:
    assert cc._coerce_expires_in(value) == expected


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        pytest.param(_basic_header("id", "secret"), ("id", "secret"), id="simple"),
        pytest.param(
            _basic_header("id", "sec:with:colons"),
            ("id", "sec:with:colons"),
            id="secret-with-colons",
        ),
        pytest.param(_basic_header("id", ""), ("id", ""), id="empty-secret"),
        pytest.param(b"Bearer sometoken", None, id="bearer-scheme"),
        pytest.param(b"Basic ", None, id="basic-no-payload"),
        pytest.param(b"Basic !!!notbase64!!!", None, id="not-base64"),
        pytest.param(
            b"Basic " + base64.b64encode(b"no-colon-here"),
            None,
            id="missing-colon",
        ),
    ],
)
def test_decode_basic(header: bytes, expected: tuple[str, str] | None) -> None:
    assert cc._decode_basic(header) == expected


def test_cache_key_is_stable_and_distinct() -> None:
    key = cc._cache_key("id", "secret")
    assert key == cc._cache_key("id", "secret")
    assert key != cc._cache_key("id", "other-secret")
    assert key != cc._cache_key("other-id", "secret")
    # The plaintext secret must not appear in the derived key.
    assert "secret" not in key


def test_with_bearer_replaces_authorization() -> None:
    scope = _http_scope(
        (b"content-type", b"application/json"),
        (b"authorization", _basic_header("id", "secret")),
    )
    rewritten = cc._with_bearer(scope, "minted-token")
    assert _auth_header(rewritten) == b"Bearer minted-token"
    # Original scope is left untouched (shallow copy).
    assert _auth_header(scope) != b"Bearer minted-token"
    # Non-auth headers are preserved.
    assert (b"content-type", b"application/json") in rewritten["headers"]


def _run(coro: object) -> object:
    return asyncio.run(coro)  # type: ignore[arg-type]


def test_non_http_scope_passes_through() -> None:
    app = _RecordingApp()
    mw = cc.ClientCredentialsExchangeMiddleware(app, token_url="https://example/token")

    async def scenario() -> None:
        await mw({"type": "lifespan"}, _noop_receive, _noop_send)

    _run(scenario())
    assert app.calls == 1
    assert app.seen_scope == {"type": "lifespan"}


def test_bearer_request_passes_through_unchanged() -> None:
    app = _RecordingApp()
    mw = cc.ClientCredentialsExchangeMiddleware(app, token_url="https://example/token")
    scope = _http_scope((b"authorization", b"Bearer client-token"))

    async def scenario() -> None:
        await mw(scope, _noop_receive, _noop_send)

    _run(scenario())
    assert app.calls == 1
    assert _auth_header(app.seen_scope) == b"Bearer client-token"


def test_basic_request_is_rewritten_to_bearer() -> None:
    app = _RecordingApp()
    mw = cc.ClientCredentialsExchangeMiddleware(app, token_url="https://example/token")
    scope = _http_scope((b"authorization", _basic_header("id", "secret")))
    mint_calls: list[tuple[str, str]] = []

    async def fake_mint(client_id: str, client_secret: str) -> tuple[str, float]:
        mint_calls.append((client_id, client_secret))
        return "minted-token", 900.0

    mw._mint_token = fake_mint  # type: ignore[assignment,method-assign]

    async def scenario() -> None:
        await mw(scope, _noop_receive, _noop_send)
        # A second identical request must reuse the cached token (no re-mint).
        await mw(scope, _noop_receive, _noop_send)

    _run(scenario())
    assert _auth_header(app.seen_scope) == b"Bearer minted-token"
    assert mint_calls == [("id", "secret")]


def test_concurrent_identical_requests_mint_once() -> None:
    app = _RecordingApp()
    mw = cc.ClientCredentialsExchangeMiddleware(app, token_url="https://example/token")
    scope = _http_scope((b"authorization", _basic_header("id", "secret")))
    mint_calls: list[tuple[str, str]] = []

    async def fake_mint(client_id: str, client_secret: str) -> tuple[str, float]:
        mint_calls.append((client_id, client_secret))
        # Yield so the second concurrent request can interleave; the
        # per-credential lock must still serialize them into a single mint.
        await asyncio.sleep(0)
        return "minted-token", 900.0

    mw._mint_token = fake_mint  # type: ignore[assignment,method-assign]

    async def scenario() -> None:
        await asyncio.gather(
            mw(scope, _noop_receive, _noop_send),
            mw(scope, _noop_receive, _noop_send),
        )

    _run(scenario())
    assert mint_calls == [("id", "secret")]
    assert _auth_header(app.seen_scope) == b"Bearer minted-token"


def test_failed_exchange_passes_request_through_unmodified() -> None:
    app = _RecordingApp()
    mw = cc.ClientCredentialsExchangeMiddleware(app, token_url="https://example/token")
    basic = _basic_header("id", "bad-secret")
    scope = _http_scope((b"authorization", basic))

    async def fake_mint(_client_id: str, _client_secret: str) -> None:
        return None

    mw._mint_token = fake_mint  # type: ignore[assignment,method-assign]

    async def scenario() -> None:
        await mw(scope, _noop_receive, _noop_send)

    _run(scenario())
    assert app.calls == 1
    # Unchanged: still the original Basic header for the verifier to reject.
    assert _auth_header(app.seen_scope) == basic


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
    assert isinstance(wrapped, cc.ClientCredentialsExchangeMiddleware)


async def _noop_receive() -> dict[str, object]:
    return {"type": "http.request"}


async def _noop_send(_message: dict[str, object]) -> None:
    return None
