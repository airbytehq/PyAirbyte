# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the Airbyte MCP tool module filters."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastmcp_extensions.tool_filters import CONFIG_INCLUDE_MODULES

from airbyte.constants import (
    MCP_CONFIG_API_URL,
    MCP_CONFIG_ALLOW_EXTERNAL_ACCESS,
    MCP_CONFIG_CONFIG_API_URL,
    MCP_CONFIG_EXCLUDE_MODULES,
    MCP_CONFIG_INCLUDE_MODULES,
    MCP_CONFIG_INSIDERS,
    MCP_INSIDERS_ENV_VAR,
    MCP_INSIDERS_HEADER,
    MCP_INSIDERS_MODULES,
    MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR,
    MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR,
    MCP_READONLY_MODE_ENV_VAR,
    _str_to_bool,
)
from airbyte.mcp import _tool_utils
from fastmcp import FastMCP
from mcp.types import Tool


APP = cast(FastMCP, object())
"""Stand-in for the app; the filter only passes it to `get_mcp_config`, which is patched."""


def _tool(
    mcp_module: str,
    *,
    read_only: bool = False,
    pipeline_change: bool | None = None,
    external_access: bool | None = None,
) -> Tool:
    """Return a tool-like object annotated with an MCP module name."""
    extra = {
        "external_access": mcp_module == "agents"
        if external_access is None
        else external_access,
    }
    if pipeline_change is not None:
        extra["pipeline_change"] = pipeline_change
    return cast(
        Tool,
        SimpleNamespace(
            annotations=SimpleNamespace(
                mcp_module=mcp_module,
                readOnlyHint=read_only,
                external_access=extra["external_access"],
                **(
                    {"pipeline_change": extra["pipeline_change"]}
                    if "pipeline_change" in extra
                    else {}
                ),
                model_extra=extra,
            )
        ),
    )


@pytest.fixture
def mcp_config(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Patch `get_mcp_config` so tests can set MCP config values directly."""
    monkeypatch.delenv(MCP_INSIDERS_ENV_VAR, raising=False)
    monkeypatch.delenv(MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, raising=False)
    monkeypatch.delenv(MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR, raising=False)
    monkeypatch.delenv(MCP_READONLY_MODE_ENV_VAR, raising=False)
    monkeypatch.setattr(_tool_utils, "AIRBYTE_CLOUD_MCP_SAFE_MODE", False)
    config: dict[str, str] = {}
    monkeypatch.setattr(
        _tool_utils,
        "get_mcp_config",
        lambda app, key, **kwargs: config.get(key),  # noqa: ARG005
    )
    return config


def _visible(module: str) -> bool:
    """Return whether a tool in the given module is advertised."""
    return _tool_utils.airbyte_module_filter(_tool(module), APP)


@pytest.mark.parametrize(
    ("config", "expected_visibility"),
    [
        pytest.param({}, {"agents": False, "cloud": True}, id="hidden_by_default"),
        *(
            pytest.param(
                {MCP_CONFIG_INSIDERS: config_value},
                {"agents": True, "cloud": True},
                id=f"insiders_on_{config_value}",
            )
            for config_value in ("1", "true", "TRUE", "yes")
        ),
        *(
            pytest.param(
                {MCP_CONFIG_INSIDERS: config_value},
                {"agents": False, "cloud": True},
                id=f"insiders_off_{config_value or 'empty'}",
            )
            for config_value in ("0", "false", "no", "")
        ),
        pytest.param(
            {MCP_CONFIG_INCLUDE_MODULES: "agents"},
            {"agents": True, "cloud": False},
            id="include_agents_only",
        ),
        pytest.param(
            {MCP_CONFIG_INCLUDE_MODULES: "cloud,local"},
            {"agents": False, "cloud": True, "local": True},
            id="include_without_agents",
        ),
        pytest.param(
            {MCP_CONFIG_INCLUDE_MODULES: "cloud,agents"},
            {"agents": True, "cloud": True, "local": False},
            id="include_agents_with_others",
        ),
        pytest.param(
            {MCP_CONFIG_INSIDERS: "1", MCP_CONFIG_EXCLUDE_MODULES: "agents"},
            {"agents": False, "cloud": True},
            id="exclude_beats_insiders",
        ),
        pytest.param(
            {
                MCP_CONFIG_INCLUDE_MODULES: "agents",
                MCP_CONFIG_EXCLUDE_MODULES: "agents",
            },
            {"agents": False},
            id="exclude_beats_include",
        ),
        pytest.param(
            {MCP_CONFIG_EXCLUDE_MODULES: "local"},
            {"agents": False, "cloud": True, "local": False},
            id="exclude_other_module",
        ),
        pytest.param(
            {CONFIG_INCLUDE_MODULES: "agents"},
            {"agents": True},
            id="include_agents_via_library_config",
        ),
        pytest.param(
            {
                MCP_CONFIG_INSIDERS: "1",
                MCP_CONFIG_API_URL: "https://airbyte.example.com/api/public/v1",
            },
            {"agents": False, "cloud": True},
            id="api_url_override_hides_agents",
        ),
        pytest.param(
            {
                MCP_CONFIG_INSIDERS: "1",
                MCP_CONFIG_CONFIG_API_URL: "https://airbyte.example.com/api/v1",
            },
            {"agents": False, "cloud": True},
            id="config_api_url_override_hides_agents",
        ),
        pytest.param(
            {
                MCP_CONFIG_INCLUDE_MODULES: "agents",
                MCP_CONFIG_API_URL: "https://airbyte.example.com/api/public/v1",
            },
            {"agents": False},
            id="api_url_override_beats_include_list",
        ),
        pytest.param(
            {
                MCP_CONFIG_INSIDERS: "1",
                MCP_CONFIG_API_URL: "https://api.airbyte.com/v1/",
                MCP_CONFIG_CONFIG_API_URL: "https://cloud.airbyte.com/api/v1/",
            },
            {"agents": True},
            id="public_cloud_roots_with_trailing_slash_keep_agents",
        ),
    ],
)
def test_module_visibility(
    mcp_config: dict[str, str],
    config: dict[str, str],
    expected_visibility: dict[str, bool],
) -> None:
    """Verify which modules are advertised for each combination of module config."""
    mcp_config.update(config)

    assert {
        module: _visible(module) for module in expected_visibility
    } == expected_visibility


def test_explicit_agents_api_root_keeps_agents_visible(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    """An explicit Agents API root keeps Agents tools visible on custom Cloud roots."""
    monkeypatch.setenv(MCP_INSIDERS_ENV_VAR, "1")
    monkeypatch.setenv("AIRBYTE_AGENTS_API_URL", "https://agents.example.com/api/v1")
    mcp_config.update({
        MCP_CONFIG_API_URL: "https://airbyte.example.com/api/public/v1",
        MCP_CONFIG_INSIDERS: "1",
    })

    assert _visible("agents")


@pytest.mark.parametrize(
    ("config", "env"),
    [
        pytest.param(
            {MCP_CONFIG_INSIDERS: "1"},
            {},
            id="insiders",
        ),
        pytest.param(
            {MCP_CONFIG_INCLUDE_MODULES: "agents"},
            {},
            id="legacy_include",
        ),
        pytest.param(
            {CONFIG_INCLUDE_MODULES: "agents"},
            {},
            id="library_include",
        ),
        pytest.param(
            {},
            {
                MCP_INSIDERS_ENV_VAR: "1",
                "AIRBYTE_AGENTS_API_URL": "https://agents.example.com/api/v1",
            },
            id="insiders_env_and_agents_api",
        ),
    ],
)
def test_external_access_disabled_hides_agents_regardless_of_other_settings(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
    config: dict[str, str],
    env: dict[str, str],
) -> None:
    """Disabled external access hides Agents tools regardless of other module settings."""
    monkeypatch.setenv(MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR, "0")
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    mcp_config.update(config)

    assert _visible("agents") is False


def test_external_access_enabled_bypasses_insiders(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    """Explicit external access permission advertises Agents without insiders mode."""
    monkeypatch.setenv(MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR, "1")
    mcp_config[MCP_CONFIG_API_URL] = "https://api.airbyte.com/v1/"
    mcp_config[MCP_CONFIG_CONFIG_API_URL] = "https://cloud.airbyte.com/api/v1/"

    assert _visible("agents")


def test_safe_mode_hides_external_access_by_default(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    """Safe mode disables external access unless explicitly allowed."""
    monkeypatch.setattr(_tool_utils, "AIRBYTE_CLOUD_MCP_SAFE_MODE", True)
    mcp_config[MCP_CONFIG_INSIDERS] = "1"

    assert not _visible("agents")


def test_safe_mode_external_access_override_bypasses_safe_mode(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    """Explicit external access permission overrides safe mode."""
    monkeypatch.setattr(_tool_utils, "AIRBYTE_CLOUD_MCP_SAFE_MODE", True)
    monkeypatch.setenv(MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR, "1")
    mcp_config[MCP_CONFIG_API_URL] = "https://api.airbyte.com/v1/"
    mcp_config[MCP_CONFIG_CONFIG_API_URL] = "https://cloud.airbyte.com/api/v1/"

    assert _visible("agents")


def test_safe_mode_disabled_preserves_unset_external_access_behavior(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    """Disabling safe mode restores the unset external-access behavior."""
    monkeypatch.setattr(_tool_utils, "AIRBYTE_CLOUD_MCP_SAFE_MODE", False)
    mcp_config[MCP_CONFIG_INSIDERS] = "1"

    assert _tool_utils.external_access_allowed(APP) is None
    assert _visible("agents")


def test_external_access_enabled_respects_insiders_hard_deny(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    """Explicit external access permission cannot override an insiders environment deny."""
    monkeypatch.setenv(MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR, "1")
    monkeypatch.setenv(MCP_INSIDERS_ENV_VAR, "0")

    assert not _visible("agents")


def test_external_access_enabled_respects_module_exclude(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    """Explicit external access permission cannot override a module exclusion."""
    monkeypatch.setenv(MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR, "1")
    mcp_config[MCP_CONFIG_EXCLUDE_MODULES] = "agents"

    assert not _visible("agents")


@pytest.mark.parametrize(
    ("query_value", "pipeline_value", "expected"),
    [
        pytest.param(None, "0", False, id="pipeline_denied"),
        pytest.param(None, None, True, id="insiders_default"),
        pytest.param("0", None, False, id="query_denied"),
        pytest.param("1", None, True, id="query_allowed"),
    ],
)
def test_external_access_derivation(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
    query_value: str | None,
    pipeline_value: str | None,
    expected: bool,
) -> None:
    """External access permission derives from pipeline permission when unset."""
    if query_value is not None:
        monkeypatch.setenv(MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR, query_value)
    if pipeline_value is not None:
        monkeypatch.setenv(MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, pipeline_value)
    mcp_config[MCP_CONFIG_INSIDERS] = "1"

    assert _visible("agents") is expected


def test_legacy_readonly_mode_hides_agents_when_query_is_unset(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    """Legacy read-only mode derives disabled external access."""
    monkeypatch.setenv(MCP_READONLY_MODE_ENV_VAR, "1")
    mcp_config[MCP_CONFIG_INSIDERS] = "1"

    assert not _visible("agents")


@pytest.mark.parametrize(
    ("env_value", "config_value", "expected"),
    [
        pytest.param("1", "0", False, id="header_narrows_env_allow"),
        pytest.param("0", "1", False, id="header_cannot_widen_env_deny"),
    ],
)
def test_external_access_header_precedence(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
    env_value: str,
    config_value: str,
    expected: bool,
) -> None:
    """External access request settings can only narrow an environment setting."""
    monkeypatch.setenv(MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR, env_value)
    mcp_config[MCP_CONFIG_ALLOW_EXTERNAL_ACCESS] = config_value
    mcp_config[MCP_CONFIG_API_URL] = "https://api.airbyte.com/v1/"
    mcp_config[MCP_CONFIG_CONFIG_API_URL] = "https://cloud.airbyte.com/api/v1/"

    assert _visible("agents") is expected


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        pytest.param("0", False, id="disabled"),
        pytest.param("1", True, id="enabled"),
        pytest.param(None, True, id="unset"),
    ],
)
def test_pipeline_changes_filter(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
    policy: str | None,
    expected: bool,
) -> None:
    """Pipeline policy controls visibility of non-read-only tools."""
    if policy is not None:
        monkeypatch.setenv(MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, policy)
    tool = _tool("cloud", read_only=False)

    assert _tool_utils.airbyte_readonly_mode_filter(tool, APP) is expected


@pytest.mark.parametrize(
    ("tool", "expected"),
    [
        pytest.param(
            _tool("cloud", read_only=False, pipeline_change=False),
            True,
            id="run_cloud_sync_is_not_a_pipeline_change",
        ),
        pytest.param(
            _tool("cloud", read_only=False, pipeline_change=False),
            True,
            id="cancel_cloud_sync_is_not_a_pipeline_change",
        ),
        pytest.param(
            _tool("cloud", read_only=False, pipeline_change=True),
            False,
            id="annotated_pipeline_change_is_hidden",
        ),
        pytest.param(
            _tool("cloud", read_only=True),
            True,
            id="read_only_tool_is_visible",
        ),
        pytest.param(
            _tool("cloud", read_only=False),
            False,
            id="unannotated_tool_uses_read_only_hint",
        ),
    ],
)
def test_pipeline_change_annotation_controls_readonly_filter(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
    tool: Tool,
    expected: bool,
) -> None:
    """Pipeline policy uses the PyAirbyte annotation with a read-only fallback."""
    monkeypatch.setenv(MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, "0")

    assert _tool_utils.airbyte_readonly_mode_filter(tool, APP) is expected


def test_legacy_readonly_mode_filter(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    """Legacy read-only mode continues hiding non-read-only tools."""
    monkeypatch.setenv(MCP_READONLY_MODE_ENV_VAR, "1")

    assert not _tool_utils.airbyte_readonly_mode_filter(_tool("cloud"), APP)


@pytest.mark.parametrize(
    ("env_value", "config", "expected_agents_visibility"),
    [
        *(
            pytest.param(
                env_value,
                {MCP_CONFIG_INSIDERS: "0"},
                False,
                id=f"header_off_narrows_hosted_on_{env_value.strip()}",
            )
            for env_value in ("1", "true", "TRUE", " Yes ", "on")
        ),
        *(
            pytest.param(
                env_value,
                {},
                True,
                id=f"hosted_on_without_header_{env_value.strip()}",
            )
            for env_value in ("1", "true", " Yes ")
        ),
        *(
            pytest.param(
                env_value,
                {MCP_CONFIG_INSIDERS: "1"},
                False,
                id=f"hosted_off_beats_header_on_{env_value}",
            )
            for env_value in ("0", "false", "FALSE", " No ", "off")
        ),
        pytest.param(
            "0",
            {MCP_CONFIG_INCLUDE_MODULES: "agents"},
            False,
            id="hosted_off_beats_include_list",
        ),
        pytest.param(
            "1",
            {MCP_CONFIG_EXCLUDE_MODULES: "agents"},
            False,
            id="exclude_still_narrows_hosted_on",
        ),
        pytest.param(
            "1",
            {MCP_CONFIG_API_URL: "https://airbyte.example.com/api/public/v1"},
            False,
            id="api_url_override_beats_hosted_insiders_on",
        ),
        *(
            pytest.param(
                env_value,
                {MCP_CONFIG_INSIDERS: "1"},
                True,
                id=f"unrecognized_defers_to_header_on_{env_value.strip() or 'blank'}",
            )
            for env_value in ("", "  ", "maybe")
        ),
        *(
            pytest.param(
                env_value,
                {},
                False,
                id=f"unrecognized_defers_to_header_off_{env_value.strip() or 'blank'}",
            )
            for env_value in ("", "  ", "maybe")
        ),
    ],
)
def test_hosted_insiders_env_var_sets_the_default(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
    env_value: str,
    config: dict[str, str],
    expected_agents_visibility: bool,
) -> None:
    """Verify the hosted env var sets the default and callers can only narrow it."""
    monkeypatch.setenv(MCP_INSIDERS_ENV_VAR, env_value)
    mcp_config.update(config)

    assert _visible("agents") is expected_agents_visibility


def test_unannotated_tools_are_always_visible(mcp_config: dict[str, str]) -> None:
    """A tool with no module annotation is never filtered by module."""
    tool = cast(Tool, SimpleNamespace(annotations=None))

    assert _tool_utils.airbyte_module_filter(tool, APP)


def test_insiders_gate_is_off_by_default() -> None:
    """Guards the hidden-module list and the config arg that opens the gate."""
    config_arg: Any = _tool_utils.INSIDERS_CONFIG_ARG

    assert set(MCP_INSIDERS_MODULES) == {"agents"}
    assert _str_to_bool(config_arg.default) is None
    assert not config_arg.required
    assert config_arg.http_header_key == MCP_INSIDERS_HEADER
    assert config_arg.env_var == MCP_INSIDERS_ENV_VAR
