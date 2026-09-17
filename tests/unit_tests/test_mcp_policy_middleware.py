# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP policy middleware."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from airbyte.constants import MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR
from airbyte.exceptions import PipelineChangesDisabledError
from airbyte.mcp import _tool_utils
from airbyte.mcp._policy_middleware import PipelineChangesGuardMiddleware


class _FakeFastMCP:
    """Minimal FastMCP stand-in for middleware tool lookup."""

    def __init__(self, tool: Any) -> None:
        self.tool = tool

    async def get_tool(self, name: str) -> Any:  # noqa: ARG002
        """Return the configured tool."""
        return self.tool


def _context(*, read_only: bool) -> Any:
    """Build a minimal middleware context."""
    tool = SimpleNamespace(
        annotations=SimpleNamespace(readOnlyHint=read_only),
    )
    return SimpleNamespace(
        message=SimpleNamespace(name="example"),
        fastmcp_context=SimpleNamespace(fastmcp=_FakeFastMCP(tool)),
    )


def test_non_read_only_tool_is_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-read-only calls are rejected when pipeline changes are disabled."""
    monkeypatch.setenv(MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, "0")
    called = False

    async def call_next(context: Any) -> str:  # noqa: ARG001
        nonlocal called
        called = True
        return "ok"

    with pytest.raises(PipelineChangesDisabledError):
        asyncio.run(
            PipelineChangesGuardMiddleware().on_call_tool(
                _context(read_only=False),
                call_next,
            )
        )

    assert not called


def test_read_only_tool_passes_through(monkeypatch: pytest.MonkeyPatch) -> None:
    """Read-only calls pass through when pipeline changes are disabled."""
    monkeypatch.setenv(MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, "0")

    async def call_next(context: Any) -> str:  # noqa: ARG001
        return "ok"

    result = asyncio.run(
        PipelineChangesGuardMiddleware().on_call_tool(
            _context(read_only=True),
            call_next,
        )
    )

    assert result == "ok"


def test_unset_pipeline_policy_passes_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Calls pass through when pipeline permission is unset."""
    monkeypatch.delenv(MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, raising=False)
    monkeypatch.setattr(_tool_utils, "get_mcp_config", lambda *args, **kwargs: "")

    async def call_next(context: Any) -> str:  # noqa: ARG001
        return "ok"

    result = asyncio.run(
        PipelineChangesGuardMiddleware().on_call_tool(
            _context(read_only=False),
            call_next,
        )
    )

    assert result == "ok"
