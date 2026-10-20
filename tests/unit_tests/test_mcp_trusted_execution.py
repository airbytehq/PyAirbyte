# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for the MCP trusted-execution gate and its function-layer guards."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Callable

import pytest
from fastmcp import Client, FastMCP

from airbyte.constants import MCP_TRUSTED_EXECUTION_ENV_VAR
from airbyte.exceptions import (
    AirbyteLibInputError,
    AirbyteTrustedExecutionRequiredError,
)
from airbyte.mcp import cloud, connector_builder, local
from airbyte.mcp._arg_resolvers import resolve_connector_config, resolve_manifest_yaml
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


def test_resolve_manifest_yaml_rejects_file_when_untrusted(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A local manifest file input requires trusted execution."""
    _set_trusted(monkeypatch, enabled=False)
    with pytest.raises(AirbyteTrustedExecutionRequiredError):
        resolve_manifest_yaml(
            manifest_yaml_text=None,
            manifest_yaml_file=str(tmp_path / "manifest.yaml"),
        )


@pytest.mark.parametrize(
    "manifest_yaml_text",
    [
        pytest.param("manifest.yaml", id="single-line-text"),
        pytest.param(
            '{"version": "0.1.0", "type": "DeclarativeSource"}', id="inline-json"
        ),
        pytest.param("version: 0.1.0\ntype: DeclarativeSource", id="multiline-yaml"),
    ],
)
@pytest.mark.parametrize("enabled", [False, True], ids=["untrusted", "trusted"])
def test_resolve_manifest_yaml_preserves_text(
    monkeypatch: MonkeyPatch,
    manifest_yaml_text: str,
    enabled: bool,
) -> None:
    """Manifest text is never reinterpreted as a path."""
    _set_trusted(monkeypatch, enabled=enabled)
    resolved = resolve_manifest_yaml(
        manifest_yaml_text=manifest_yaml_text,
        manifest_yaml_file=None,
    )
    assert resolved == manifest_yaml_text


def test_resolve_manifest_yaml_returns_file_path_when_trusted(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A local manifest file is resolved to a `Path` when trusted execution is enabled."""
    _set_trusted(monkeypatch, enabled=True)
    manifest_path = tmp_path / "manifest.yaml"
    assert (
        resolve_manifest_yaml(
            manifest_yaml_text=None,
            manifest_yaml_file=str(manifest_path),
        )
        == manifest_path
    )


def test_resolve_manifest_yaml_rejects_both_inputs(
    monkeypatch: MonkeyPatch,
) -> None:
    """Manifest text and a local file path cannot be supplied together."""
    _set_trusted(monkeypatch, enabled=True)
    with pytest.raises(
        AirbyteLibInputError,
        match="Provide only one of `manifest_yaml_text` or `manifest_yaml_file`\\.",
    ):
        resolve_manifest_yaml(
            manifest_yaml_text="version: 0.1.0",
            manifest_yaml_file="manifest.yaml",
        )


@pytest.mark.parametrize("enabled", [False, True], ids=["untrusted", "trusted"])
def test_resolve_manifest_yaml_returns_none_when_inputs_omitted(
    monkeypatch: MonkeyPatch,
    enabled: bool,
) -> None:
    """A missing manifest stays missing in either execution mode."""
    _set_trusted(monkeypatch, enabled=enabled)
    assert (
        resolve_manifest_yaml(
            manifest_yaml_text=None,
            manifest_yaml_file=None,
        )
        is None
    )


def test_manifest_file_schema_depends_on_trusted_execution(
    monkeypatch: MonkeyPatch,
) -> None:
    """The file argument is hidden from untrusted schemas and exposed to trusted callers."""
    for enabled in (False, True):
        _set_trusted(monkeypatch, enabled=enabled)
        app = FastMCP(f"manifest-schema-{enabled}")
        cloud.register_cloud_tools(app)
        connector_builder.register_connector_builder_tools(app)

        async def list_tools() -> dict[str, object]:
            async with Client(app) as client:
                return {tool.name: tool for tool in await client.list_tools()}

        tools = asyncio.run(list_tools())

        for tool_name in (
            "create_connector_builder_project",
            "publish_custom_source_definition",
            "update_custom_source_definition",
        ):
            tool = tools[tool_name]
            properties = tool.input_schema["properties"]  # type: ignore[attr-defined]
            assert "manifest_yaml_text" in properties
            assert ("manifest_yaml_file" in properties) is enabled
            assert "manifest_yaml" not in properties
            assert (
                cloud.MANIFEST_YAML_SOURCE_HINT
                in (getattr(tool, "description", None) or "")
            ) is enabled
            if enabled:
                assert (
                    "manifest_yaml_text"
                    not in properties["manifest_yaml_file"]["description"]
                )

        for tool_name in (
            "publish_custom_source_definition",
            "set_connector_builder_testing_values",
            "update_custom_source_definition",
        ):
            tool = tools[tool_name]
            properties = tool.input_schema["properties"]  # type: ignore[attr-defined]
            assert "testing_values" in properties
            assert ("testing_values_secret_name" in properties) is enabled
            assert (
                "secret_reference::" not in properties["testing_values"]["description"]
            )
            assert (
                cloud.TESTING_VALUES_SECRET_HINT
                in (getattr(tool, "description", None) or "")
            ) is enabled


@pytest.mark.parametrize("enabled", [False, True], ids=["untrusted", "trusted"])
def test_publish_missing_manifest_error_depends_on_trusted_execution(
    monkeypatch: MonkeyPatch,
    enabled: bool,
) -> None:
    """The missing-manifest error only mentions the file argument when it is available."""
    _set_trusted(monkeypatch, enabled=enabled)
    app = FastMCP(f"manifest-error-{enabled}")
    cloud.register_cloud_tools(app)

    async def call_publish() -> object:
        async with Client(app) as client:
            return await client.call_tool(
                "publish_custom_source_definition",
                {"name": "x"},
                raise_on_error=False,
            )

    result = asyncio.run(call_publish())
    assert result.is_error  # type: ignore[attr-defined]
    error_text = result.content[0].text  # type: ignore[attr-defined]
    assert ("manifest_yaml_file" in error_text) is enabled


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


def test_run_sql_query_log_omits_exception_text(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingCache:
        def run_sql_query(self, sql_query: str, max_records: int) -> None:
            raise RuntimeError("SENTINEL_SQL_ERROR")

    monkeypatch.setattr(local, "raise_if_untrusted_execution_context", lambda _: None)
    monkeypatch.setattr(local, "get_default_cache", FailingCache)

    monkeypatch.setattr(local.logger, "propagate", False)
    local.logger.addHandler(caplog.handler)
    try:
        caplog.set_level(logging.WARNING, logger=local.logger.name)
        local.run_sql_query("SELECT 1", 10)
    finally:
        local.logger.removeHandler(caplog.handler)

    assert "SENTINEL_SQL_ERROR" not in caplog.text
    assert "RuntimeError" in caplog.text
    assert len(caplog.records) == 1
    assert caplog.records[0].exc_info is None


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
    with pytest.raises(AirbyteLibInputError) as exc_info:
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
    with pytest.raises(AirbyteLibInputError) as exc_info:
        validate_airbyte_domains(app)
    assert "not_a_real_domain" in exc_info.value.context["unknown_domains"]


@pytest.mark.parametrize(
    ("include_domains", "disabled_domains"),
    [
        pytest.param("cloud", None, id="include-cloud"),
        pytest.param("cloud,guidance", None, id="include-cloud-and-guidance"),
        pytest.param(None, "guidance", id="disable-guidance"),
    ],
)
def test_validate_airbyte_domains_allows_known_single_domain(
    monkeypatch: MonkeyPatch,
    include_domains: str | None,
    disabled_domains: str | None,
) -> None:
    """Known domains pass validation when included or disabled."""
    from airbyte.mcp._tool_utils import validate_airbyte_domains
    from airbyte.mcp.server import app

    monkeypatch.delenv(TRUSTED_DOMAINS_EXCLUDE_ENV, raising=False)
    monkeypatch.delenv(TRUSTED_DOMAINS_INCLUDE_ENV, raising=False)
    if include_domains is not None:
        monkeypatch.setenv(TRUSTED_DOMAINS_INCLUDE_ENV, include_domains)
    if disabled_domains is not None:
        monkeypatch.setenv(TRUSTED_DOMAINS_EXCLUDE_ENV, disabled_domains)
    validate_airbyte_domains(app)
