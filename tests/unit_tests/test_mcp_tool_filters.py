# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the Airbyte MCP tool module filters."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastmcp_extensions import ToolTraits
from fastmcp_extensions.tool_filters import CONFIG_INCLUDE_MODULES

from airbyte.constants import (
    CLOUD_MCP_SAFE_MODE_ENV_VAR,
    MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR,
    MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR,
    MCP_CONFIG_ALLOW_EXTERNAL_ACCESS,
    MCP_CONFIG_ALLOW_PIPELINE_CHANGES,
    MCP_CONFIG_EXCLUDE_MODULES,
    MCP_CONFIG_INCLUDE_MODULES,
    MCP_CONFIG_INSIDERS,
    MCP_INSIDERS_ENV_VAR,
    MCP_INSIDERS_HEADER,
    MCP_INSIDERS_MODULES,
    MCP_READONLY_MODE_ENV_VAR,
    _str_to_bool,
)
from airbyte.mcp import _tool_utils
from fastmcp import FastMCP
from mcp.types import Tool


APP = cast(FastMCP, object())
"""Stand-in for the app; the filter only passes it to `get_mcp_config`, which is patched."""


def _tool(name: str) -> Tool:
    """Return a tool-like object named for the module it belongs to."""
    return cast(Tool, SimpleNamespace(name=name))


@pytest.fixture
def mcp_config(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Patch `get_mcp_config` so tests can set MCP config values directly."""
    monkeypatch.delenv(MCP_INSIDERS_ENV_VAR, raising=False)
    for env_var in (
        CLOUD_MCP_SAFE_MODE_ENV_VAR,
        MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR,
        MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR,
        MCP_READONLY_MODE_ENV_VAR,
    ):
        monkeypatch.delenv(env_var, raising=False)
    monkeypatch.setattr(_tool_utils, "_TOOL_POLICIES", {})
    config: dict[str, str] = {}
    monkeypatch.setattr(
        _tool_utils,
        "get_mcp_config",
        lambda _app, key, **_kwargs: config.get(key),
    )
    # `mcp_module` is internal state (never on the wire), so stub the traits
    # registry lookup with a tool-name -> module map.
    monkeypatch.setattr(
        _tool_utils,
        "get_tool_traits",
        lambda _app, name: ToolTraits(
            mcp_module=name if name != "unannotated" else None
        ),
    )
    return config


def _visible(module: str) -> bool:
    """Return whether a tool in the given module is advertised."""
    return _tool_utils.airbyte_module_filter(_tool(module), APP)


@pytest.mark.parametrize(
    ("config", "expected_visibility"),
    [
        pytest.param({}, {"other": True, "cloud": True}, id="visible_by_default"),
        pytest.param(
            {MCP_CONFIG_INSIDERS: "1"},
            {"other": True, "cloud": True},
            id="insiders_is_noop",
        ),
        pytest.param(
            {MCP_CONFIG_INCLUDE_MODULES: "other"},
            {"other": True, "cloud": False},
            id="include_other_only",
        ),
        pytest.param(
            {MCP_CONFIG_INCLUDE_MODULES: "cloud,local"},
            {"other": False, "cloud": True, "local": True},
            id="include_without_other",
        ),
        pytest.param(
            {MCP_CONFIG_EXCLUDE_MODULES: "other"},
            {"other": False, "cloud": True},
            id="exclude_other",
        ),
        pytest.param(
            {
                MCP_CONFIG_INCLUDE_MODULES: "other",
                MCP_CONFIG_EXCLUDE_MODULES: "other",
            },
            {"other": False},
            id="exclude_beats_include",
        ),
        pytest.param(
            {MCP_CONFIG_EXCLUDE_MODULES: "local"},
            {"other": True, "cloud": True, "local": False},
            id="exclude_local",
        ),
        pytest.param(
            {CONFIG_INCLUDE_MODULES: "other"},
            {"other": True},
            id="include_other_via_library_config",
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


def test_unannotated_tools_are_always_visible(mcp_config: dict[str, str]) -> None:
    """A tool with no mcp_module trait is never filtered by module."""
    tool = cast(Tool, SimpleNamespace(name="unannotated"))

    assert _tool_utils.airbyte_module_filter(tool, APP)


def test_ui_tool_wire_meta_carries_only_standard_ui_key() -> None:
    """UI tools put `ui` on the wire `_meta`; custom keys stay off the wire."""
    import asyncio

    from airbyte.mcp.server import app as airbyte_app

    tool = asyncio.run(airbyte_app.get_tool("show_connectors_list"))
    wire = tool.to_mcp_tool().model_dump(by_alias=True)

    assert (wire.get("_meta") or {}).get("ui", {}).get("resourceUri")
    serialized = str(wire)
    assert "'interactive-ui'" not in serialized
    assert "'mcp_module'" not in serialized
    assert "requiresClientFilesystem" not in serialized


def test_insiders_gate_is_empty() -> None:
    """No modules are insiders-gated; the config arg stays for compatibility."""
    config_arg: Any = _tool_utils.INSIDERS_CONFIG_ARG

    assert set(MCP_INSIDERS_MODULES) == set()
    assert _str_to_bool(config_arg.default) is None
    assert not config_arg.required
    assert config_arg.http_header_key == MCP_INSIDERS_HEADER
    assert config_arg.env_var == MCP_INSIDERS_ENV_VAR


def test_sync_tools_stay_visible_when_pipeline_changes_are_disabled(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    assert not mcp_config
    monkeypatch.setenv(MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, "0")
    for tool_name in ("run_cloud_sync", "cancel_cloud_sync"):
        tool = _policy_tool(tool_name, pipeline_change=False)
        assert _tool_utils.airbyte_readonly_mode_filter(tool, APP)


def _policy_tool(
    name: str,
    *,
    pipeline_change: bool | None,
    external_access: bool = False,
    read_only_hint: bool = False,
) -> Tool:
    if pipeline_change is not None or external_access:
        _tool_utils._TOOL_POLICIES[name] = _tool_utils.ToolPolicy(
            pipeline_change=bool(pipeline_change),
            external_access=external_access,
        )
    return cast(
        Tool,
        SimpleNamespace(
            name=name,
            annotations=SimpleNamespace(read_only_hint=read_only_hint),
        ),
    )


@pytest.mark.parametrize(
    (
        "pipeline_env",
        "header",
        "legacy_readonly",
        "policy",
        "read_only_hint",
        "visible",
    ),
    [
        pytest.param("0", None, None, True, False, False, id="env-deny"),
        pytest.param("1", "0", None, True, False, False, id="header-narrows-env-allow"),
        pytest.param(
            "0", "1", None, True, False, False, id="env-deny-wins-over-header"
        ),
        pytest.param(None, "0", None, True, False, False, id="header-deny-without-env"),
        pytest.param("1", None, None, True, False, True, id="env-allow"),
        pytest.param("1", None, "1", True, False, False, id="legacy-readonly-deny"),
        pytest.param("0", None, None, False, False, True, id="pipeline-change-false"),
        pytest.param("0", None, None, None, True, True, id="read-only-hint-fallback"),
    ],
)
def test_pipeline_change_filter(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
    pipeline_env: str | None,
    header: str | None,
    legacy_readonly: str | None,
    policy: bool | None,
    read_only_hint: bool,
    visible: bool,
) -> None:
    for env_var, value in (
        (MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, pipeline_env),
        (MCP_READONLY_MODE_ENV_VAR, legacy_readonly),
    ):
        if value is None:
            monkeypatch.delenv(env_var, raising=False)
        else:
            monkeypatch.setenv(env_var, value)
    if header is not None:
        mcp_config[MCP_CONFIG_ALLOW_PIPELINE_CHANGES] = header
    tool = _policy_tool(
        "pipeline-policy-tool",
        pipeline_change=policy,
        read_only_hint=read_only_hint,
    )

    assert _tool_utils.airbyte_readonly_mode_filter(tool, APP) is visible


@pytest.mark.parametrize(
    (
        "external_env",
        "header",
        "pipeline_env",
        "legacy_readonly",
        "safe_mode",
        "visible",
    ),
    [
        pytest.param("0", None, None, None, None, False, id="external-env-deny"),
        pytest.param(
            "1", "0", None, None, None, False, id="header-narrows-external-allow"
        ),
        pytest.param("0", "1", None, None, None, False, id="external-env-deny-wins"),
        pytest.param(None, "0", None, None, None, False, id="external-header-deny"),
        pytest.param("1", None, None, None, "1", True, id="explicit-external-allow"),
        pytest.param(None, None, None, None, "1", False, id="explicit-safe-mode-deny"),
        pytest.param(None, None, "0", None, None, False, id="pipeline-deny-default"),
        pytest.param(None, None, None, "1", None, False, id="legacy-readonly-default"),
        pytest.param(None, None, None, None, None, True, id="unset-default-allows"),
        pytest.param(None, None, None, None, "auto", True, id="auto-safe-mode-allows"),
    ],
)
def test_external_access_filter(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
    external_env: str | None,
    header: str | None,
    pipeline_env: str | None,
    legacy_readonly: str | None,
    safe_mode: str | None,
    visible: bool,
) -> None:
    for env_var, value in (
        (MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR, external_env),
        (MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, pipeline_env),
        (MCP_READONLY_MODE_ENV_VAR, legacy_readonly),
        (CLOUD_MCP_SAFE_MODE_ENV_VAR, safe_mode),
    ):
        if value is None:
            monkeypatch.delenv(env_var, raising=False)
        else:
            monkeypatch.setenv(env_var, value)
    if header is not None:
        mcp_config[MCP_CONFIG_ALLOW_EXTERNAL_ACCESS] = header
    tool = _policy_tool(
        "external-access-tool", pipeline_change=False, external_access=True
    )

    assert _tool_utils.airbyte_external_access_filter(tool, APP) is visible
