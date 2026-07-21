# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for the MCP trusted-execution gate and its function-layer guards."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Callable

import pytest

from airbyte.constants import MCP_TRUSTED_EXECUTION_ENV_VAR
from airbyte.exceptions import (
    PyAirbyteInputError,
    AirbyteTrustedExecutionRequiredError,
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
    ],
)
def test_is_trusted_execution_enabled(
    monkeypatch: MonkeyPatch,
    value: str,
    expected: bool,
) -> None:
    """`is_trusted_execution_enabled` parses only explicit truthy env values."""
    monkeypatch.setenv(MCP_TRUSTED_EXECUTION_ENV_VAR, value)
    assert is_trusted_execution_enabled() is expected


def test_is_trusted_execution_enabled_defaults_off(monkeypatch: MonkeyPatch) -> None:
    """The gate defaults to off when the env var is unset."""
    monkeypatch.delenv(MCP_TRUSTED_EXECUTION_ENV_VAR, raising=False)
    assert is_trusted_execution_enabled() is False


def test_raise_if_untrusted_execution_context_raises_when_disabled(
    monkeypatch: MonkeyPatch,
) -> None:
    """`raise_if_untrusted_execution_context` hard-fails and records the feature when disabled."""
    _set_trusted(monkeypatch, enabled=False)
    with pytest.raises(AirbyteTrustedExecutionRequiredError) as exc_info:
        raise_if_untrusted_execution_context("some feature")
    assert exc_info.value.feature == "some feature"


def test_raise_if_untrusted_execution_context_passes_when_enabled(
    monkeypatch: MonkeyPatch,
) -> None:
    """`raise_if_untrusted_execution_context` is a no-op when trusted execution is enabled."""
    _set_trusted(monkeypatch, enabled=True)
    raise_if_untrusted_execution_context("some feature")


def test_resolve_connector_config_allows_inline_config_when_untrusted(
    monkeypatch: MonkeyPatch,
) -> None:
    """A plain inline config dict does not require trusted execution (cloud tools rely on it)."""
    _set_trusted(monkeypatch, enabled=False)
    assert resolve_connector_config(config={"host": "example.com"}) == {
        "host": "example.com"
    }


def test_resolve_connector_config_empty_when_untrusted(
    monkeypatch: MonkeyPatch,
) -> None:
    """Resolving with no inputs is always allowed and returns an empty dict."""
    _set_trusted(monkeypatch, enabled=False)
    assert resolve_connector_config() == {}


def test_resolve_connector_config_rejects_config_file_when_untrusted(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Reading config from a local file is gated behind trusted execution."""
    _set_trusted(monkeypatch, enabled=False)
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({"host": "example.com"}))
    with pytest.raises(AirbyteTrustedExecutionRequiredError):
        resolve_connector_config(config_file=config_file)


def test_resolve_connector_config_reads_config_file_when_trusted(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When trusted, reading a local config file works as before."""
    _set_trusted(monkeypatch, enabled=True)
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({"host": "example.com"}))
    assert resolve_connector_config(config_file=config_file) == {"host": "example.com"}


def test_resolve_connector_config_rejects_config_secret_name_when_untrusted(
    monkeypatch: MonkeyPatch,
) -> None:
    """Resolving config from a server-side secret is gated behind trusted execution."""
    _set_trusted(monkeypatch, enabled=False)
    with pytest.raises(AirbyteTrustedExecutionRequiredError):
        resolve_connector_config(config_secret_name="MY_SECRET_CONFIG")


def test_resolve_connector_config_rejects_secret_reference_when_untrusted(
    monkeypatch: MonkeyPatch,
) -> None:
    """Inline `secret_reference::` values are gated behind trusted execution."""
    _set_trusted(monkeypatch, enabled=False)
    with pytest.raises(AirbyteTrustedExecutionRequiredError):
        resolve_connector_config(config={"password": "secret_reference::MY_ENV_VAR"})


def test_resolve_connector_config_allows_secret_reference_when_trusted(
    monkeypatch: MonkeyPatch,
) -> None:
    """When trusted, an inline `secret_reference::` passes the guard (resolved downstream)."""
    _set_trusted(monkeypatch, enabled=True)
    resolved = resolve_connector_config(
        config={"password": "secret_reference::MY_ENV_VAR"}
    )
    assert resolved == {"password": "secret_reference::MY_ENV_VAR"}


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


def test_assert_http_trusted_execution_disabled_raises_when_enabled(
    monkeypatch: MonkeyPatch,
) -> None:
    """The HTTP entrypoint hard-fails at startup if trusted execution is explicitly enabled."""
    from fastmcp_extensions import assert_http_trusted_execution_disabled

    from airbyte.mcp.server import app

    _set_trusted(monkeypatch, enabled=True)
    with pytest.raises(RuntimeError):
        assert_http_trusted_execution_disabled(app)


def test_assert_http_trusted_execution_disabled_passes_when_off(
    monkeypatch: MonkeyPatch,
) -> None:
    """The HTTP startup guard is a no-op when trusted execution is disabled (the default)."""
    from fastmcp_extensions import assert_http_trusted_execution_disabled

    from airbyte.mcp.server import app

    _set_trusted(monkeypatch, enabled=False)
    assert_http_trusted_execution_disabled(app)


def test_load_secrets_suppresses_secret_managers_when_untrusted(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When untrusted, dotenv/GSM secret managers are not registered, but env still loads."""
    from airbyte.mcp import _config

    _set_trusted(monkeypatch, enabled=False)
    env_file = tmp_path / "custom.env"
    env_file.write_text("MY_SAFE_VAR=hello\n")
    monkeypatch.setenv(_config.AIRBYTE_MCP_DOTENV_PATH_ENVVAR, str(env_file))
    monkeypatch.delenv("MY_SAFE_VAR", raising=False)

    registered: list[object] = []
    monkeypatch.setattr(_config, "register_secret_manager", registered.append)

    _config.load_secrets_to_env_vars()

    assert registered == []
    # The env var is still loaded so safe, non-secret-manager config keeps working.
    import os

    assert os.environ["MY_SAFE_VAR"] == "hello"


def test_load_secrets_registers_dotenv_manager_when_trusted(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When trusted, a custom dotenv file is registered as a secret manager source."""
    from airbyte.mcp import _config

    _set_trusted(monkeypatch, enabled=True)
    env_file = tmp_path / "custom.env"
    env_file.write_text("MY_SAFE_VAR=hello\n")
    monkeypatch.setenv(_config.AIRBYTE_MCP_DOTENV_PATH_ENVVAR, str(env_file))

    registered: list[object] = []
    monkeypatch.setattr(_config, "register_secret_manager", registered.append)

    _config.load_secrets_to_env_vars()

    assert any(isinstance(mgr, _config.DotenvSecretManager) for mgr in registered)


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
