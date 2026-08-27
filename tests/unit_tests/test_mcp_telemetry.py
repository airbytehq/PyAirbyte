# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP tool-call telemetry configuration."""

from __future__ import annotations

import os

import pytest
from fastmcp_extensions import ToolCallTelemetryMiddleware
from segment import analytics

from airbyte.mcp import server


_DUMMY_SEGMENT_WRITE_KEY = "dummy-segment-write-key"


def test_segment_write_key_defaults_to_app_tracking_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The telemetry key defaults to PyAirbyte's application key."""
    monkeypatch.delenv(server.SEGMENT_WRITE_KEY_ENV, raising=False)
    monkeypatch.delenv(server.DO_NOT_TRACK, raising=False)

    assert server._segment_write_key() == server.PYAIRBYTE_APP_TRACKING_KEY


def test_segment_write_key_uses_env_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The MCP server can use a deployment-specific Segment key."""
    monkeypatch.delenv(server.DO_NOT_TRACK, raising=False)
    monkeypatch.setenv(server.SEGMENT_WRITE_KEY_ENV, _DUMMY_SEGMENT_WRITE_KEY)

    assert server._segment_write_key() == _DUMMY_SEGMENT_WRITE_KEY


def test_segment_write_key_respects_do_not_track(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The telemetry sink is disabled when tracking is opted out."""
    monkeypatch.setenv(server.DO_NOT_TRACK, "1")
    monkeypatch.setenv(server.SEGMENT_WRITE_KEY_ENV, _DUMMY_SEGMENT_WRITE_KEY)

    assert server._segment_write_key() is None


def test_register_tool_call_telemetry_adds_middleware(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enabled telemetry adds exactly one middleware without sending an event."""
    monkeypatch.delenv(server.DO_NOT_TRACK, raising=False)
    monkeypatch.setenv(server.SEGMENT_WRITE_KEY_ENV, _DUMMY_SEGMENT_WRITE_KEY)

    def fail_if_called(*_: object, **__: object) -> None:
        pytest.fail("middleware registration must not send Segment traffic")

    monkeypatch.setattr(analytics, "track", fail_if_called)
    # Constructing the middleware configures the module-level Segment client, so
    # snapshot the client state that registration mutates.
    monkeypatch.setattr(analytics, "write_key", analytics.write_key)
    monkeypatch.setattr(analytics, "send", analytics.send)
    monkeypatch.setattr(analytics, "on_error", analytics.on_error)
    original_middleware = list(server.app.middleware)
    try:
        server._register_tool_call_telemetry()
        added_middleware = [
            middleware
            for middleware in server.app.middleware
            if middleware not in original_middleware
        ]
        assert len(added_middleware) == 1
        assert isinstance(added_middleware[0], ToolCallTelemetryMiddleware)
    finally:
        server.app.middleware[:] = original_middleware


def test_register_tool_call_telemetry_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Opting out leaves the MCP app middleware unchanged."""
    monkeypatch.setenv(server.DO_NOT_TRACK, "1")
    original_middleware = list(server.app.middleware)
    try:
        server._register_tool_call_telemetry()
        assert server.app.middleware == original_middleware
    finally:
        server.app.middleware[:] = original_middleware


def test_module_level_registration_adds_telemetry_middleware() -> None:
    """The shared MCP app registers telemetry when tracking is enabled."""
    if os.environ.get(server.DO_NOT_TRACK) or server.AIRBYTE_OFFLINE_MODE:
        pytest.skip("telemetry is disabled by the test environment")

    assert any(
        isinstance(middleware, ToolCallTelemetryMiddleware)
        for middleware in server.app.middleware
    )
