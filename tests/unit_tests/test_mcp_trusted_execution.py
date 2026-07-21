# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for the MCP trusted-execution gate and its function-layer guards."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Callable

import pytest

from airbyte.constants import MCP_TRUSTED_EXECUTION_ENV_VAR
from airbyte.exceptions import (
    AirbyteTrustedExecutionRequiredError,
    PyAirbyteInputError,
)
from airbyte.mcp import local
from airbyte.mcp._arg_resolvers import resolve_connector_config
from airbyte.mcp._guards import (
    is_trusted_execution_enabled,
    raise_if_untrusted_execution_context,
)


if TYPE_CHECKING:
    from pytest import MonkeyPatch


TRUSTED_DOMAINS_INCLUDE_ENV = "AIRBYTE_MCP_DOMAINS"
TRUSTED_DOMAINS_EXCLUDE_ENV = "AIRBYTE_MCP_DOMAINS_DISABLED"


def _set_trusted(monkeypatch: MonkeyPatch, *, enabled: bool) -> None:
    if enabled:
        monkeypatch.setenv(MCP_TRUSTED_EXECUTION_ENV_VAR, "1")
    else:
        monkeypatch.delenv(MCP_TRUSTED_EXECUTION_ENV_VAR, raising=False)


def _write_config_json(tmp_path: Path) -> Path:
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({"host": "example.com"}))
    return config_file


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("1", True, id="one"),
        pytest.param("true", True, id="true"),
        pytest.param("YES", True, id="yes_uppercase"),
        pytest.param(" 1 ", True, id="whitespace_padded"),
        pytest.param("0", False, id="zero"),
        pytest.param("false", False, id="false"),
        pytest.param("", False, id="empty"),
        pytest.param("nope", False, id="arbitrary"),
        pytest.param(None, False, id="unset_defaults_off"),
    ],
)
def test_is_trusted_execution_enabled(
    monkeypatch: MonkeyPatch,
    value: str | None,
    expected: bool,
) -> None:
    """`is_trusted_execution_enabled` parses only explicit truthy env values and defaults off."""
    if value is None:
        monkeypatch.delenv(MCP_TRUSTED_EXECUTION_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(MCP_TRUSTED_EXECUTION_ENV_VAR, value)
    assert is_trusted_execution_enabled() is expected


@pytest.mark.parametrize("enabled", [False, True], ids=["disabled", "enabled"])
def test_raise_if_untrusted_execution_context(
    monkeypatch: MonkeyPatch,
    enabled: bool,
) -> None:
    """The guard hard-fails (recording the feature) when disabled and is a no-op when enabled."""
    _set_trusted(monkeypatch, enabled=enabled)
    if enabled:
        raise_if_untrusted_execution_context("some feature")
        return
    with pytest.raises(AirbyteTrustedExecutionRequiredError) as exc_info:
        raise_if_untrusted_execution_context("some feature")
    assert exc_info.value.feature == "some feature"


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        pytest.param(
            {"config": {"host": "example.com"}},
            {"host": "example.com"},
            id="inline_dict",
        ),
        pytest.param({}, {}, id="empty"),
    ],
)
def test_resolve_connector_config_allows_when_untrusted(
    monkeypatch: MonkeyPatch,
    kwargs: dict[str, object],
    expected: dict[str, object],
) -> None:
    """Plain inline/empty config never requires trusted execution (cloud tools rely on it)."""
    _set_trusted(monkeypatch, enabled=False)
    assert resolve_connector_config(**kwargs) == expected


@pytest.mark.parametrize(
    "build_kwargs",
    [
        pytest.param(
            lambda tmp_path: {"config_file": _write_config_json(tmp_path)},
            id="config_file",
        ),
        pytest.param(
            lambda tmp_path: {"config_secret_name": "MY_SECRET_CONFIG"},
            id="config_secret_name",
        ),
        pytest.param(
            lambda tmp_path: {"config": {"password": "secret_reference::MY_ENV_VAR"}},
            id="secret_reference",
        ),
    ],
)
def test_resolve_connector_config_rejects_when_untrusted(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
    build_kwargs: Callable[[Path], dict[str, object]],
) -> None:
    """Filesystem, server-side-secret, and `secret_reference::` paths are gated when untrusted."""
    _set_trusted(monkeypatch, enabled=False)
    with pytest.raises(AirbyteTrustedExecutionRequiredError):
        resolve_connector_config(**build_kwargs(tmp_path))


@pytest.mark.parametrize(
    "build_case",
    [
        pytest.param(
            lambda tmp_path: (
                {"config_file": _write_config_json(tmp_path)},
                {"host": "example.com"},
            ),
            id="config_file",
        ),
        pytest.param(
            lambda tmp_path: (
                {"config": {"password": "secret_reference::MY_ENV_VAR"}},
                {"password": "secret_reference::MY_ENV_VAR"},
            ),
            id="secret_reference",
        ),
    ],
)
def test_resolve_connector_config_allows_when_trusted(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
    build_case: Callable[[Path], tuple[dict[str, object], dict[str, object]]],
) -> None:
    """When trusted, gated paths pass the guard (`secret_reference::` is resolved downstream)."""
    _set_trusted(monkeypatch, enabled=True)
    kwargs, expected = build_case(tmp_path)
    assert resolve_connector_config(**kwargs) == expected


_UNTRUSTED_LOCAL_HELPERS: list[Callable[[], object]] = [
    pytest.param(
        lambda: local._get_mcp_source("source-faker", manifest_path=None),
        id="_get_mcp_source",
    ),
    pytest.param(lambda: local.list_cached_streams(), id="list_cached_streams"),
    pytest.param(lambda: local.describe_default_cache(), id="describe_default_cache"),
    pytest.param(lambda: local.run_sql_query("SELECT 1", 10), id="run_sql_query"),
    pytest.param(lambda: local.list_dotenv_secrets(), id="list_dotenv_secrets"),
    pytest.param(
        lambda: local.list_connector_config_secrets("source-faker"),
        id="list_connector_config_secrets",
    ),
    pytest.param(
        lambda: local.destination_smoke_test(
            "destination-dev-null",
            None,
            None,
            None,
            "fast",
            None,
            None,
            None,
            None,
            False,
        ),
        id="destination_smoke_test",
    ),
]


@pytest.mark.parametrize("call_helper", _UNTRUSTED_LOCAL_HELPERS)
def test_local_helpers_reject_when_untrusted(
    monkeypatch: MonkeyPatch,
    call_helper: Callable[[], object],
) -> None:
    """Trusted-machine local helpers hard-fail when trusted execution is disabled.

    This is independent of tool visibility: calling the backend directly must raise even
    though a registration mistake could leave the tool listed.
    """
    _set_trusted(monkeypatch, enabled=False)
    with pytest.raises(AirbyteTrustedExecutionRequiredError):
        call_helper()


@pytest.mark.parametrize("enabled", [False, True], ids=["disabled", "enabled"])
def test_assert_http_trusted_execution_disabled(
    monkeypatch: MonkeyPatch,
    enabled: bool,
) -> None:
    """The HTTP startup guard hard-fails only when trusted execution is explicitly enabled."""
    from fastmcp_extensions import assert_http_trusted_execution_disabled

    from airbyte.mcp.server import app

    _set_trusted(monkeypatch, enabled=enabled)
    if enabled:
        with pytest.raises(RuntimeError):
            assert_http_trusted_execution_disabled(app)
    else:
        assert_http_trusted_execution_disabled(app)


@pytest.mark.parametrize("trusted", [False, True], ids=["untrusted", "trusted"])
def test_load_secrets_registers_secret_managers_only_when_trusted(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
    trusted: bool,
) -> None:
    """Dotenv/GSM secret managers register only when trusted; env vars always load either way."""
    from airbyte.mcp import _config

    _set_trusted(monkeypatch, enabled=trusted)
    env_file = tmp_path / "custom.env"
    env_file.write_text("MY_SAFE_VAR=hello\n")
    monkeypatch.setenv(_config.AIRBYTE_MCP_DOTENV_PATH_ENVVAR, str(env_file))
    monkeypatch.delenv("MY_SAFE_VAR", raising=False)

    registered: list[object] = []
    monkeypatch.setattr(_config, "register_secret_manager", registered.append)

    _config.load_secrets_to_env_vars()

    # The env var loads regardless so safe, non-secret-manager config keeps working.
    assert os.environ["MY_SAFE_VAR"] == "hello"
    if trusted:
        assert any(isinstance(mgr, _config.DotenvSecretManager) for mgr in registered)
    else:
        assert registered == []


def test_validate_airbyte_domains_rejects_include_and_exclude(
    monkeypatch: MonkeyPatch,
) -> None:
    """Setting both include and exclude domain lists hard-fails with remediation guidance."""
    from airbyte.mcp._tool_utils import validate_airbyte_domains
    from airbyte.mcp.server import app

    monkeypatch.setenv(TRUSTED_DOMAINS_INCLUDE_ENV, "cloud")
    monkeypatch.setenv(TRUSTED_DOMAINS_EXCLUDE_ENV, "local")
    with pytest.raises(PyAirbyteInputError) as exc_info:
        validate_airbyte_domains(app)
    rendered = str(exc_info.value)
    assert "mutually exclusive" in rendered
    assert "restart" in rendered.lower()


def test_validate_airbyte_domains_rejects_unknown_domain(
    monkeypatch: MonkeyPatch,
) -> None:
    """Requesting a domain with no registered tools hard-fails instead of silently dropping it."""
    from airbyte.mcp._tool_utils import validate_airbyte_domains
    from airbyte.mcp.server import app

    monkeypatch.delenv(TRUSTED_DOMAINS_EXCLUDE_ENV, raising=False)
    monkeypatch.setenv(TRUSTED_DOMAINS_INCLUDE_ENV, "not_a_real_domain")
    with pytest.raises(PyAirbyteInputError) as exc_info:
        validate_airbyte_domains(app)
    assert "not_a_real_domain" in exc_info.value.context["unknown_domains"]


def test_validate_airbyte_domains_allows_known_single_domain(
    monkeypatch: MonkeyPatch,
) -> None:
    """A single valid include domain passes validation."""
    from airbyte.mcp._tool_utils import validate_airbyte_domains
    from airbyte.mcp.server import app

    monkeypatch.delenv(TRUSTED_DOMAINS_EXCLUDE_ENV, raising=False)
    monkeypatch.setenv(TRUSTED_DOMAINS_INCLUDE_ENV, "cloud")
    validate_airbyte_domains(app)
