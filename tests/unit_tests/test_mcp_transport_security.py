# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for HTTP MCP host and origin validation."""

from __future__ import annotations

import pytest
from starlette.responses import PlainTextResponse
from starlette.testclient import TestClient
from starlette.types import Receive, Scope, Send

from airbyte.mcp._transport_security import (
    ALLOWED_HOSTS_ENV,
    HostOriginGuardMiddleware,
    resolve_allowed_hosts,
)


async def _app(scope: Scope, receive: Receive, send: Send) -> None:
    if scope["type"] == "lifespan":
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return
    await PlainTextResponse("ok")(scope, receive, send)


def _client(allowed_hosts: tuple[str, ...]) -> TestClient:
    return TestClient(HostOriginGuardMiddleware(_app, allowed_hosts))


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1"])
def test_allowed_loopback_hosts_pass_through(host: str) -> None:
    with _client(("localhost", "127.0.0.1")) as client:
        response = client.get("/", headers={"host": host})

    assert response.status_code == 200
    assert response.text == "ok"


def test_unallowed_host_is_rejected() -> None:
    with _client(("localhost",)) as client:
        response = client.get("/", headers={"host": "attacker.example:8080"})

    assert response.status_code == 421
    assert response.text == "Misdirected Request"


def test_unallowed_origin_is_rejected_even_with_allowed_host() -> None:
    with _client(("localhost",)) as client:
        response = client.get(
            "/",
            headers={
                "host": "localhost",
                "origin": "http://attacker.example:8080",
            },
        )

    assert response.status_code == 403
    assert response.text == "Forbidden Origin"


def test_missing_origin_is_allowed() -> None:
    with _client(("localhost",)) as client:
        response = client.get("/", headers={"host": "localhost"})

    assert response.status_code == 200


def test_missing_host_is_rejected() -> None:
    with _client(("localhost",)) as client:
        response = client.get("/", headers={"host": ""})

    assert response.status_code == 421


def test_configured_hosts_are_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ALLOWED_HOSTS_ENV, "custom.example:8443,*.run.app")

    with _client(resolve_allowed_hosts("")) as client:
        custom_response = client.get("/", headers={"host": "custom.example"})
        pattern_response = client.get("/", headers={"host": "service.run.app"})

    assert custom_response.status_code == 200
    assert pattern_response.status_code == 200


def test_server_url_hostname_is_allowed() -> None:
    allowed_hosts = resolve_allowed_hosts("https://mcp.example.com:443/mcp")

    assert allowed_hosts[-1:] == ("mcp.example.com",)
