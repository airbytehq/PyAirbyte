# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP tool-call telemetry configuration."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import textwrap

import pytest
from fastmcp_extensions import ToolCallTelemetryMiddleware
from segment import analytics

from airbyte import constants
from airbyte._util import meta, telemetry
from airbyte.constants import set_hosted_mcp_mode
from airbyte.mcp import server
from airbyte.secrets import config as secrets_config
from airbyte.secrets.prompt import SecretsPrompt


@pytest.fixture(autouse=True)
def force_online_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep in-process telemetry tests independent of the runner environment."""
    monkeypatch.setattr(server, "AIRBYTE_OFFLINE_MODE", False)


def test_segment_write_key_uses_mcp_tracking_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The MCP server uses PyAirbyte's MCP application key."""
    monkeypatch.delenv(server.DO_NOT_TRACK, raising=False)

    assert server._segment_write_key() == server.PYAIRBYTE_MCP_TRACKING_KEY


def test_segment_write_key_respects_do_not_track(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The telemetry sink is disabled when tracking is opted out."""
    monkeypatch.setenv(server.DO_NOT_TRACK, "1")

    assert server._segment_write_key() is None


def test_segment_write_key_respects_offline_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Offline mode disables the external Segment sink."""
    monkeypatch.delenv(server.DO_NOT_TRACK, raising=False)
    monkeypatch.setattr(server, "AIRBYTE_OFFLINE_MODE", True)

    assert server._segment_write_key() is None


def test_segment_write_key_rechecks_runtime_offline_mode() -> None:
    """Offline mode loaded after constants import still disables Segment."""
    child_env = os.environ.copy()
    child_env.pop("AIRBYTE_OFFLINE_MODE", None)
    child_env.pop("AIRBYTE_MCP_ENV_FILE", None)
    child_env.pop(server.DO_NOT_TRACK, None)
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
if server._segment_write_key() != {server.PYAIRBYTE_MCP_TRACKING_KEY!r}:
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


@pytest.mark.parametrize("mcp_mode", [False, True])
def test_get_segment_write_key_uses_runtime_mode(
    monkeypatch: pytest.MonkeyPatch,
    mcp_mode: bool,
) -> None:
    """The telemetry key follows the active runtime mode."""
    monkeypatch.setattr(meta, "_MCP_MODE_ENABLED", mcp_mode)

    expected_key = (
        telemetry.PYAIRBYTE_MCP_TRACKING_KEY
        if mcp_mode
        else telemetry.PYAIRBYTE_APP_TRACKING_KEY
    )
    assert telemetry.get_segment_write_key() == expected_key


def test_get_env_flags_reflects_mcp_mode_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MCP mode is refreshed even after environment flags are first read."""
    telemetry._get_static_env_flags.cache_clear()
    monkeypatch.setattr(meta, "_MCP_MODE_ENABLED", False)

    assert "MCP" not in telemetry.get_env_flags()

    monkeypatch.setattr(meta, "_MCP_MODE_ENABLED", True)

    assert telemetry.get_env_flags()["MCP"] is True


def test_send_telemetry_uses_mcp_tracking_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MCP telemetry is sent with the MCP application's Segment key."""
    post_calls: list[dict[str, object]] = []

    def fake_post(*_: object, **kwargs: object) -> None:
        post_calls.append(kwargs)

    monkeypatch.delenv(telemetry.DO_NOT_TRACK, raising=False)
    monkeypatch.setattr(telemetry, "AIRBYTE_OFFLINE_MODE", False)
    monkeypatch.setattr(meta, "_MCP_MODE_ENABLED", True)
    monkeypatch.setattr(telemetry.requests, "post", fake_post)

    telemetry.send_telemetry(
        source=None,
        destination=None,
        cache=None,
        state=telemetry.EventState.STARTED,
        event_type=telemetry.EventType.SYNC,
    )

    assert post_calls[0]["auth"] == (telemetry.PYAIRBYTE_MCP_TRACKING_KEY, "")


def test_importing_server_does_not_enable_mcp_mode() -> None:
    """Importing the server module must not flip the global MCP-mode flag."""
    script = textwrap.dedent(
        """
        import airbyte.mcp.server
        from airbyte._util.meta import is_mcp_mode

        print(is_mcp_mode())
        """
    )
    child_env = os.environ.copy()
    child_env.pop("AIRBYTE_MCP_ENV_FILE", None)
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
        env=child_env,
    )

    # Startup logging may precede the printed value on stdout.
    assert result.stdout.strip().splitlines()[-1] == "False"


def test_lifespan_enables_mcp_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Entering the server lifespan enables MCP mode and drops interactive secret prompts."""
    monkeypatch.setattr(meta, "_MCP_MODE_ENABLED", False)
    monkeypatch.setattr(secrets_config, "_SECRETS_SOURCES", [SecretsPrompt()])

    async def enter_lifespan() -> None:
        async with server._mcp_mode_lifespan(server.app):
            assert meta.is_mcp_mode()
            assert not any(
                isinstance(source, SecretsPrompt)
                for source in secrets_config._SECRETS_SOURCES
            )

    asyncio.run(enter_lifespan())


def test_shared_app_registers_telemetry_without_sending_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The shared app has one telemetry middleware for both transports."""

    def fail_if_called(*_: object, **__: object) -> None:
        pytest.fail("telemetry setup must not send Segment traffic")

    monkeypatch.setattr(analytics, "track", fail_if_called)
    # Keep the module-level Segment client state isolated from middleware setup.
    monkeypatch.setattr(analytics, "write_key", analytics.write_key)
    monkeypatch.setattr(analytics, "send", analytics.send)
    monkeypatch.setattr(analytics, "on_error", analytics.on_error)
    original_middleware = list(server.app.middleware)
    try:
        telemetry_middleware = [
            middleware
            for middleware in server.app.middleware
            if isinstance(middleware, ToolCallTelemetryMiddleware)
        ]
        assert len(telemetry_middleware) == 1
    finally:
        server.app.middleware[:] = original_middleware


def test_hosted_attribution_is_resolved_per_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hosted attribution reflects mode changes after module import."""
    monkeypatch.setattr(constants, "_HOSTED_MCP_MODE_ENABLED", False)
    telemetry = next(
        middleware
        for middleware in server.app.middleware
        if isinstance(middleware, ToolCallTelemetryMiddleware)
    )
    extra_properties = telemetry._extra_properties
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
