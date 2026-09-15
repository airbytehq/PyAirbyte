# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP tool-call telemetry configuration."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest
from fastmcp_extensions import ToolCallTelemetryMiddleware
from segment import analytics

from airbyte import constants
from airbyte._util import telemetry
from airbyte.constants import set_hosted_mcp_mode
from airbyte.mcp import server


_DUMMY_SEGMENT_WRITE_KEY = "dummy-segment-write-key"


@pytest.fixture(autouse=True)
def force_online_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep in-process telemetry tests independent of the runner environment."""
    monkeypatch.setattr(server, "AIRBYTE_OFFLINE_MODE", False)
    yield


@pytest.mark.parametrize(
    ("do_not_track", "segment_key", "offline_mode", "expected_key"),
    [
        pytest.param(
            None, None, False, server.PYAIRBYTE_APP_TRACKING_KEY, id="default"
        ),
        pytest.param(
            None,
            _DUMMY_SEGMENT_WRITE_KEY,
            False,
            _DUMMY_SEGMENT_WRITE_KEY,
            id="environment-override",
        ),
        pytest.param(
            "1",
            _DUMMY_SEGMENT_WRITE_KEY,
            False,
            None,
            id="do-not-track",
        ),
        pytest.param(
            None,
            _DUMMY_SEGMENT_WRITE_KEY,
            True,
            None,
            id="offline-mode",
        ),
    ],
)
def test_segment_write_key_respects_configuration(
    monkeypatch: pytest.MonkeyPatch,
    do_not_track: str | None,
    segment_key: str | None,
    offline_mode: bool,
    expected_key: str | None,
) -> None:
    """The Segment key reflects tracking, environment, and offline configuration."""
    monkeypatch.delenv(server.SEGMENT_WRITE_KEY_ENV, raising=False)
    monkeypatch.delenv(server.DO_NOT_TRACK, raising=False)
    if do_not_track is not None:
        monkeypatch.setenv(server.DO_NOT_TRACK, do_not_track)
    if segment_key is not None:
        monkeypatch.setenv(server.SEGMENT_WRITE_KEY_ENV, segment_key)
    monkeypatch.setattr(server, "AIRBYTE_OFFLINE_MODE", offline_mode)

    assert server._segment_write_key() == expected_key


def test_segment_write_key_rechecks_runtime_offline_mode() -> None:
    """Offline mode loaded after constants import still disables Segment."""
    child_env = os.environ.copy()
    child_env.pop("AIRBYTE_OFFLINE_MODE", None)
    child_env.pop("AIRBYTE_MCP_ENV_FILE", None)
    child_env.pop(server.DO_NOT_TRACK, None)
    child_env[server.SEGMENT_WRITE_KEY_ENV] = _DUMMY_SEGMENT_WRITE_KEY
    child_script = f"""
import os

from airbyte import constants
from airbyte.mcp import server

if constants.AIRBYTE_OFFLINE_MODE is not False:
    raise SystemExit(
        f"expected imported offline mode=False, got {{constants.AIRBYTE_OFFLINE_MODE!r}}"
    )

os.environ["AIRBYTE_OFFLINE_MODE"] = "true"
if server._segment_write_key() is not None:
    raise SystemExit("runtime offline mode did not disable Segment")

os.environ["AIRBYTE_OFFLINE_MODE"] = "false"
if server._segment_write_key() != {_DUMMY_SEGMENT_WRITE_KEY!r}:
    raise SystemExit("runtime false offline mode did not restore the Segment key")
"""
    result = subprocess.run(
        [sys.executable, "-c", child_script],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
        env=child_env,
    )

    assert result.returncode == 0, (
        f"child process failed with return code {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )


def test_shared_app_registers_telemetry_without_sending_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The shared app has one telemetry middleware for both transports."""

    def fail_if_called(*_: object, **__: object) -> None:
        pytest.fail("telemetry setup must not send Segment traffic")

    monkeypatch.setattr(analytics, "track", fail_if_called)
    telemetry_middleware = [
        middleware
        for middleware in server.app.middleware
        if isinstance(middleware, ToolCallTelemetryMiddleware)
    ]
    assert len(telemetry_middleware) == 1


def test_shared_app_passes_upstream_attribution_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The shared app configures attribution with PyAirbyte's analytics ID."""
    monkeypatch.setattr(telemetry, "_ANALYTICS_ID", "analytics-ulid")
    telemetry_middleware = next(
        middleware
        for middleware in server.app.middleware
        if isinstance(middleware, ToolCallTelemetryMiddleware)
    )
    attribution = telemetry_middleware._attribution

    assert attribution is not None
    assert attribution._known_public_mcp_domains == (
        "airbyte.ai",
        "airbyte.com",
        "airbyte.io",
    )
    assert attribution._anonymization_salt is server._mcp_anonymization_salt
    monkeypatch.setenv(server.ANONYMIZATION_SALT_ENV, "configured-salt")
    assert attribution._anonymization_salt() == "configured-salt"
    monkeypatch.delenv(server.ANONYMIZATION_SALT_ENV)
    assert attribution._anonymization_salt() == "analytics-ulid"
    telemetry_middleware = next(
        middleware
        for middleware in server.app.middleware
        if isinstance(middleware, ToolCallTelemetryMiddleware)
    )
    assert (
        telemetry_middleware._sinks._segment_anonymous_id
        is server._mcp_segment_anonymous_id
    )


def test_segment_anonymous_id_uses_local_analytics_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use the analytics ID locally, but not as a hosted shared user."""
    monkeypatch.setattr(telemetry, "_ANALYTICS_ID", "analytics-ulid")
    monkeypatch.setattr(constants, "_HOSTED_MCP_MODE_ENABLED", False)
    assert server._mcp_segment_anonymous_id() == "analytics-ulid"

    monkeypatch.setattr(constants, "_HOSTED_MCP_MODE_ENABLED", True)
    assert server._mcp_segment_anonymous_id() is None


def test_hosted_attribution_is_resolved_per_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hosted attribution reflects mode changes after module import."""
    monkeypatch.setattr(constants, "_HOSTED_MCP_MODE_ENABLED", False)
    telemetry_middleware = next(
        middleware
        for middleware in server.app.middleware
        if isinstance(middleware, ToolCallTelemetryMiddleware)
    )
    extra_properties = telemetry_middleware._extra_properties
    assert callable(extra_properties)
    assert extra_properties() == {"is_hosted_mcp": False}

    set_hosted_mcp_mode()

    assert extra_properties() == {"is_hosted_mcp": True}


@pytest.mark.parametrize(
    ("disabled_env", "expected_segment"),
    [
        pytest.param(None, True, id="enabled"),
        pytest.param("DO_NOT_TRACK", False, id="do-not-track"),
        pytest.param("AIRBYTE_OFFLINE_MODE", False, id="offline-mode"),
    ],
)
def test_module_level_registration_configures_telemetry(
    disabled_env: str | None,
    expected_segment: bool,
) -> None:
    """A clean import registers telemetry and respects external-sink opt-outs."""
    child_env = os.environ.copy()
    child_env.pop(server.DO_NOT_TRACK, None)
    child_env.pop("AIRBYTE_OFFLINE_MODE", None)
    child_env[server.SEGMENT_WRITE_KEY_ENV] = _DUMMY_SEGMENT_WRITE_KEY
    if disabled_env is not None:
        child_env[disabled_env] = "1"

    child_script = f"""
from fastmcp_extensions import ToolCallTelemetryMiddleware
from airbyte.mcp import server

has_telemetry = any(
    isinstance(middleware, ToolCallTelemetryMiddleware)
    for middleware in server.app.middleware
)
telemetry = next(
    middleware
    for middleware in server.app.middleware
    if isinstance(middleware, ToolCallTelemetryMiddleware)
)
if has_telemetry is not True or telemetry._segment_enabled is not {expected_segment!r}:
    raise SystemExit(
        "expected telemetry middleware and segment sink="
        f"True/{{expected_segment!r}}, got "
        f"{{has_telemetry!r}}/{{telemetry._segment_enabled!r}}"
    )
"""
    result = subprocess.run(
        [sys.executable, "-c", child_script],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
        env=child_env,
    )

    assert result.returncode == 0, (
        f"child process failed with return code {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
