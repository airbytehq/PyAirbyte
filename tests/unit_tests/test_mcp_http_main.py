# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for the HTTP transport entry point in `airbyte.mcp.http_main`.

These cover `_advertise_root_mount_resource`, which normalizes the RFC 9728
protected-resource identifier when the MCP endpoint is mounted at root behind a
path-stripping load balancer. Without it FastMCP advertises a trailing-slash
resource (e.g. `.../cloud-mcp/`) that strict clients reject because they
canonicalize the connection URL to the slash-less form (`.../cloud-mcp`).
"""

from __future__ import annotations

import asyncio

import pytest
from fastmcp.server.auth import MultiAuth
from fastmcp.server.auth.auth import TokenVerifier
from fastmcp_extensions import run_mcp_http_server

from airbyte.mcp import http_main
from airbyte.mcp.http_main import _advertise_root_mount_resource


_BASE_URL = "https://mcp.example.com/cloud-mcp"


class _FakeProvider(TokenVerifier):
    """Minimal concrete provider that inherits FastMCP's resource-URL logic."""

    async def verify_token(self, token: str) -> None:
        """Never authenticates; the resource-URL logic is what is under test."""
        return None


@pytest.mark.parametrize(
    ("mcp_path", "expected"),
    [
        pytest.param("/", _BASE_URL, id="root_mount_drops_trailing_slash"),
        pytest.param("", _BASE_URL, id="empty_path_is_root"),
        pytest.param(None, _BASE_URL, id="none_path_is_root"),
        pytest.param("/mcp", f"{_BASE_URL}/mcp", id="non_root_path_unchanged"),
    ],
)
def test_advertise_root_mount_resource(mcp_path: str | None, expected: str) -> None:
    """The helper maps a root mount to the slash-less resource, leaving others intact."""
    provider = _FakeProvider(base_url=_BASE_URL)

    _advertise_root_mount_resource(provider)

    assert str(provider._get_resource_url(mcp_path)) == expected


def test_advertise_root_mount_resource_recurses_into_multiauth() -> None:
    """The fix reaches the interactive server and every headless verifier in the tree."""
    server = _FakeProvider(base_url=_BASE_URL)
    verifier = _FakeProvider(base_url=_BASE_URL)
    multi = MultiAuth(server=server, verifiers=[verifier])

    _advertise_root_mount_resource(multi)

    for provider in (multi, server, verifier):
        assert str(provider._get_resource_url("/")) == _BASE_URL
        assert str(provider._get_resource_url("/mcp")) == f"{_BASE_URL}/mcp"


@pytest.mark.parametrize(
    ("installed", "expected"),
    [
        pytest.param(
            "0.54.0",
            "https://github.com/airbytehq/PyAirbyte/releases/tag/v0.54.0",
            id="tagged_release_links_to_its_release_page",
        ),
        pytest.param(
            "0.54.0.post4.dev0+32b9886",
            "https://github.com/airbytehq/PyAirbyte/commit/32b9886",
            id="dev_build_links_to_the_commit_in_its_local_segment",
        ),
        pytest.param(
            "0.54.1a3",
            "https://github.com/airbytehq/PyAirbyte/releases",
            id="prerelease_build_without_a_sha_links_to_the_release_list",
        ),
        pytest.param(
            "0.54.1.dev3+1b1637b4",
            "https://github.com/airbytehq/PyAirbyte/commit/1b1637b4",
            id="dev_build_links_to_the_commit_it_was_cut_from",
        ),
        pytest.param(
            "0.54.1.dev3+dirty",
            "https://github.com/airbytehq/PyAirbyte/releases",
            id="local_segment_without_a_sha_links_to_the_release_list",
        ),
        pytest.param(
            "0.54.1.dev3+1b1637b4.dirty",
            "https://github.com/airbytehq/PyAirbyte/commit/1b1637b4",
            id="dirty_dev_build_links_to_the_bare_sha",
        ),
    ],
)
def test_landing_version_url(
    monkeypatch: pytest.MonkeyPatch,
    installed: str,
    expected: str,
) -> None:
    """Tagged versions link to a release page; dev builds link to their commit."""
    monkeypatch.setattr(http_main, "get_version", lambda: installed)

    assert http_main._landing_version_url() == expected
    assert http_main._landing_version_str() == f"v{installed}"


@pytest.mark.parametrize(
    ("accept", "expected_status", "expected_body"),
    [
        pytest.param(
            b"text/event-stream",
            405,
            b"",
            id="sse-get-is-rejected",
        ),
        pytest.param(
            b"text/html,application/xhtml+xml",
            200,
            b"<title>Airbyte MCP Server</title>",
            id="browser-get-reaches-landing-page",
        ),
    ],
)
def test_event_stream_get_content_negotiation(
    monkeypatch: pytest.MonkeyPatch,
    accept: bytes,
    expected_status: int,
    expected_body: bytes,
) -> None:
    """The default stateless HTTP stack rejects SSE GETs but serves browsers."""
    messages: list[dict[str, object]] = []

    async def receive() -> dict[str, object]:
        return {"type": "http.request", "body": b""}

    async def send(message: dict[str, object]) -> None:
        messages.append(message)

    async def landing_page(scope: object, receive: object, send: object) -> None:
        del scope, receive
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/html")],
        })
        await send({
            "type": "http.response.body",
            "body": b"<title>Airbyte MCP Server</title>",
        })

    class FakeServer:
        def http_app(self, **_: object) -> object:
            return landing_page

    captured: dict[str, object] = {}

    def capture_run(app: object, **_: object) -> None:
        captured["app"] = app

    monkeypatch.setattr(
        "fastmcp_extensions.http_server.uvicorn.run",
        capture_run,
    )
    run_mcp_http_server(
        FakeServer(),  # type: ignore[arg-type]
        transport="streamable-http",
        stateless_http=True,
    )
    app = captured["app"]
    assert app is not None
    asyncio.run(
        app(
            {
                "type": "http",
                "method": "GET",
                "headers": [(b"accept", accept)],
            },
            receive,
            send,
        )
    )

    assert messages[0]["status"] == expected_status
    assert messages[1]["body"] == expected_body
