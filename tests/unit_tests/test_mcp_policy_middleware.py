# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP policy middleware."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from airbyte.constants import (
    MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR,
    MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR,
)
from airbyte.exceptions import ExternalAccessDisabledError, PipelineChangesDisabledError
from airbyte.mcp import _tool_utils
from airbyte.mcp._policy_middleware import PolicyGuardMiddleware


class _FakeFastMCP:
    """Minimal FastMCP stand-in for middleware tool lookup."""

    def __init__(self, tool: Any) -> None:
        self.tool = tool

    async def get_tool(self, name: str) -> Any:  # noqa: ARG002
        """Return the configured tool."""
        return self.tool


def _context(
    *,
    read_only: bool,
    pipeline_change: bool | None = None,
    external_access: bool = False,
) -> Any:
    """Build a minimal middleware context."""
    model_extra = {"external_access": external_access}
    if pipeline_change is not None:
        model_extra["pipeline_change"] = pipeline_change
    tool = SimpleNamespace(
        annotations=SimpleNamespace(
            readOnlyHint=read_only,
            model_extra=model_extra,
        ),
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
            PolicyGuardMiddleware().on_call_tool(
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
        PolicyGuardMiddleware().on_call_tool(
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
        PolicyGuardMiddleware().on_call_tool(
            _context(read_only=False),
            call_next,
        )
    )

    assert result == "ok"


def test_sync_tool_passes_through_when_pipeline_changes_are_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sync operations are allowed when pipeline changes are disabled."""
    monkeypatch.setenv(MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, "0")

    async def call_next(context: Any) -> str:  # noqa: ARG001
        return "ok"

    result = asyncio.run(
        PolicyGuardMiddleware().on_call_tool(
            _context(read_only=False, pipeline_change=False),
            call_next,
        )
    )

    assert result == "ok"


def test_external_access_tool_is_blocked_when_external_access_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """External-access calls are rejected when external access is disabled."""
    monkeypatch.setenv(MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR, "0")
    monkeypatch.setattr(_tool_utils, "get_mcp_config", lambda *args, **kwargs: "")

    async def call_next(context: Any) -> str:  # noqa: ARG001
        return "ok"

    with pytest.raises(ExternalAccessDisabledError):
        asyncio.run(
            PolicyGuardMiddleware().on_call_tool(
                _context(read_only=True, external_access=True),
                call_next,
            )
        )
