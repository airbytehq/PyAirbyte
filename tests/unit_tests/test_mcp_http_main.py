# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for the HTTP transport entry point in `airbyte.mcp.http_main`.

These cover `_advertise_root_mount_resource`, which normalizes the RFC 9728
protected-resource identifier when the MCP endpoint is mounted at root behind a
path-stripping load balancer. Without it FastMCP advertises a trailing-slash
resource (e.g. `.../cloud-mcp/`) that strict clients reject because they
canonicalize the connection URL to the slash-less form (`.../cloud-mcp`).
"""

from __future__ import annotations

import pytest
from fastmcp.server.auth import MultiAuth
from fastmcp.server.auth.auth import TokenVerifier

from airbyte.mcp.http_main import _advertise_root_mount_resource


_BASE_URL = "https://mcp.internal.airbyte.ai/cloud-mcp"


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
