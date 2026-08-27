# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for MCP tool-call telemetry configuration."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import subprocess
import sys
from types import SimpleNamespace

import pytest
from fastmcp_extensions import ToolCallTelemetryMiddleware
from segment import analytics

from airbyte import constants
from airbyte.constants import set_hosted_mcp_mode
from airbyte.mcp import _telemetry_attribution as attribution
from airbyte.mcp import server


_DUMMY_SEGMENT_WRITE_KEY = "dummy-segment-write-key"


@pytest.fixture(autouse=True)
def force_online_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep in-process telemetry tests independent of the runner environment."""
    monkeypatch.setattr(server, "AIRBYTE_OFFLINE_MODE", False)
    attribution._get_telemetry_salt.cache_clear()
    yield
    attribution._get_telemetry_salt.cache_clear()


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


def test_segment_write_key_respects_offline_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Offline mode disables the external Segment sink."""
    monkeypatch.delenv(server.DO_NOT_TRACK, raising=False)
    monkeypatch.setenv(server.SEGMENT_WRITE_KEY_ENV, _DUMMY_SEGMENT_WRITE_KEY)
    monkeypatch.setattr(server, "AIRBYTE_OFFLINE_MODE", True)

    assert server._segment_write_key() is None


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


def _http_request(
    *,
    host: str = "mcp.airbyte.ai",
    path: str = "/",
    forwarded_for: str | None = None,
) -> SimpleNamespace:
    headers = {"host": host}
    if forwarded_for is not None:
        headers["x-forwarded-for"] = forwarded_for
    return SimpleNamespace(
        headers=headers,
        client=SimpleNamespace(host="127.0.0.1"),
        url=SimpleNamespace(path=path),
    )


def _context(
    *,
    session_id: str = "session-id",
    client_name: str | None = None,
    client_version: str | None = None,
) -> SimpleNamespace:
    client_info = (
        SimpleNamespace(name=client_name, version=client_version)
        if client_name is not None and client_version is not None
        else None
    )
    return SimpleNamespace(
        session_id=session_id,
        session=SimpleNamespace(
            client_params=(
                SimpleNamespace(clientInfo=client_info)
                if client_info is not None
                else None
            )
        ),
    )


def test_stdio_attribution_only_preserves_hosted_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stdio calls do not expose request-scoped attribution."""
    monkeypatch.setattr(attribution, "is_hosted_mcp_mode", lambda: True)
    monkeypatch.setattr(
        attribution,
        "get_http_request",
        lambda: (_ for _ in ()).throw(RuntimeError("no HTTP request")),
    )

    assert attribution.get_telemetry_attribution() == {"is_hosted_mcp": True}


def test_attribution_uses_first_forwarded_ip_and_airbyte_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HTTP attribution uses the original forwarded client and owned endpoint."""
    monkeypatch.setenv("AIRBYTE_MCP_TELEMETRY_SALT", "test-salt")
    request = _http_request(
        host="preview.airbyte.ai",
        path="/cloud-mcp",
        forwarded_for="198.51.100.23, 192.0.2.10",
    )
    monkeypatch.setattr(attribution, "get_http_request", lambda: request)
    monkeypatch.setattr(
        attribution,
        "get_context",
        lambda: _context(
            session_id="session-secret",
            client_name="Test Client",
            client_version="1.2.3",
        ),
    )
    monkeypatch.setattr(
        attribution,
        "get_access_token",
        lambda: SimpleNamespace(
            claims={"sub": "subject-secret"}, client_id="client-id"
        ),
    )

    payload = attribution.get_telemetry_attribution()

    assert payload["caller_hash"] == attribution._hash_value(
        "198.51.100.23", "ip", "preview.airbyte.ai"
    )
    assert payload["mcp_endpoint"] == "preview.airbyte.ai/cloud-mcp"
    assert payload["mcp_endpoint_hash"] == attribution._hash_value(
        "preview.airbyte.ai", "endpoint", "preview.airbyte.ai"
    )
    assert payload["session_id_hash"] == attribution._hash_value(
        "session-secret", "session", "preview.airbyte.ai"
    )
    assert payload["auth_subject_hash"] == attribution._hash_value(
        "subject-secret", "subject", "preview.airbyte.ai"
    )
    assert (
        payload["caller_hash"]
        == hmac.new(
            b"test-salt",
            b"ip|preview.airbyte.ai|198.51.100.23",
            hashlib.sha256,
        ).hexdigest()[:16]
    )
    assert payload["mcp_client_name"] == "Test Client"
    assert payload["mcp_client_version"] == "1.2.3"


def test_non_airbyte_endpoint_is_hashed_but_not_emitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Third-party deployment hostnames are never emitted in plaintext."""
    monkeypatch.setenv("AIRBYTE_MCP_TELEMETRY_SALT", "test-salt")
    host = "customer.example.com"
    request = _http_request(host=host)
    monkeypatch.setattr(attribution, "get_http_request", lambda: request)
    monkeypatch.setattr(attribution, "get_context", lambda: _context())
    monkeypatch.setattr(attribution, "get_access_token", lambda: None)

    payload = attribution.get_telemetry_attribution()

    assert "mcp_endpoint" not in payload
    assert payload["mcp_endpoint_hash"] == attribution._hash_value(
        host, "endpoint", host
    )


def test_hashes_are_stable_and_scoped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Attribution hashes are stable but scoped to their field."""
    monkeypatch.setenv("AIRBYTE_MCP_TELEMETRY_SALT", "test-salt")

    session_hash = attribution._hash_value("same-value", "session")
    assert session_hash == attribution._hash_value("same-value", "session")
    assert session_hash != attribution._hash_value("same-value", "ip")
    assert (
        session_hash
        == hmac.new(
            b"test-salt",
            b"session|local|same-value",
            hashlib.sha256,
        ).hexdigest()[:16]
    )
    monkeypatch.setenv("AIRBYTE_MCP_TELEMETRY_SALT", "different-salt")
    attribution._get_telemetry_salt.cache_clear()
    assert session_hash != attribution._hash_value("same-value", "session")


def test_caller_hash_is_bound_to_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """The same caller has different surrogates on different deployments."""
    monkeypatch.setenv("AIRBYTE_MCP_TELEMETRY_SALT", "test-salt")
    current_request = [
        _http_request(host="prod.airbyte.ai", forwarded_for="198.51.100.23")
    ]
    monkeypatch.setattr(attribution, "get_http_request", lambda: current_request[0])
    monkeypatch.setattr(attribution, "get_context", lambda: _context())
    monkeypatch.setattr(attribution, "get_access_token", lambda: None)

    first = attribution.get_telemetry_attribution()["caller_hash"]
    current_request[0] = _http_request(
        host="preview.airbyte.ai", forwarded_for="198.51.100.23"
    )
    second = attribution.get_telemetry_attribution()["caller_hash"]

    assert first != second


def test_attribution_payload_contains_no_raw_identifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raw request identifiers are absent from the emitted properties."""
    monkeypatch.setenv("AIRBYTE_MCP_TELEMETRY_SALT", "test-salt")
    raw_ip = "198.51.100.42"
    raw_session = "session-secret"
    raw_subject = "subject-secret"
    request = _http_request(host="customer.example.com", forwarded_for=raw_ip)
    monkeypatch.setattr(attribution, "get_http_request", lambda: request)
    monkeypatch.setattr(
        attribution, "get_context", lambda: _context(session_id=raw_session)
    )
    monkeypatch.setattr(
        attribution,
        "get_access_token",
        lambda: SimpleNamespace(claims={"sub": raw_subject}, client_id=None),
    )

    payload_text = json.dumps(attribution.get_telemetry_attribution())

    assert raw_ip not in payload_text
    assert raw_session not in payload_text
    assert raw_subject not in payload_text
    assert "customer.example.com" not in payload_text


def test_salt_prefers_environment_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit deployment salt is used before the analytics ID."""
    monkeypatch.setenv("AIRBYTE_MCP_TELEMETRY_SALT", "environment-salt")
    monkeypatch.setattr(
        attribution,
        "_get_analytics_id",
        lambda: pytest.fail("analytics ID should not be read when salt is configured"),
    )

    assert (
        attribution._hash_value("value", "session")
        == hmac.new(
            b"environment-salt",
            b"session|local|value",
            hashlib.sha256,
        ).hexdigest()[:16]
    )


def test_salt_falls_back_to_analytics_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """The persisted anonymous analytics ID provides the fallback salt."""
    monkeypatch.delenv("AIRBYTE_MCP_TELEMETRY_SALT", raising=False)
    monkeypatch.setattr(attribution, "_get_analytics_id", lambda: "analytics-id")

    assert (
        attribution._hash_value("value", "session")
        == hmac.new(
            b"analytics-id",
            b"session|local|value",
            hashlib.sha256,
        ).hexdigest()[:16]
    )


def test_opted_out_analytics_omits_attribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Opting out of analytics also omits attribution properties."""
    monkeypatch.delenv("AIRBYTE_MCP_TELEMETRY_SALT", raising=False)
    monkeypatch.setattr(attribution, "_get_analytics_id", lambda: None)
    monkeypatch.setattr(
        attribution,
        "get_http_request",
        lambda: _http_request(host="preview.airbyte.ai", forwarded_for="198.51.100.23"),
    )
    monkeypatch.setattr(attribution, "get_context", lambda: _context())

    assert attribution.get_telemetry_attribution() == {"is_hosted_mcp": False}


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
