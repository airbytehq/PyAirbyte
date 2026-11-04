# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the Airbyte MCP tool module filters."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastmcp_extensions import ToolTraits
from fastmcp_extensions.tool_filters import CONFIG_INCLUDE_MODULES

from airbyte.constants import (
    MCP_ALLOW_DESTRUCTIVE_TOOLS_ENV_VAR,
    MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR,
    MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR,
    MCP_CONFIG_ALLOW_DESTRUCTIVE_TOOLS,
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
from airbyte.mcp import _tool_utils, cloud as cloud_mcp
from fastmcp import Context, FastMCP
from mcp.types import Tool


APP = cast(FastMCP, object())
"""Stand-in for the app; the filter only passes it to `get_mcp_config`, which is patched."""

_TOOL_MODULES: dict[str, str] = {}
"""Tool-name -> mcp_module map for the `get_tool_traits` stub, cleared per test."""


def _tool(name: str) -> Tool:
    """Return a tool-like object named for the module it belongs to."""
    return cast(Tool, SimpleNamespace(name=name))


@pytest.fixture
def mcp_config(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Patch `get_mcp_config` so tests can set MCP config values directly."""
    monkeypatch.delenv(MCP_INSIDERS_ENV_VAR, raising=False)
    for env_var in (
        MCP_ALLOW_DESTRUCTIVE_TOOLS_ENV_VAR,
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
    _TOOL_MODULES.clear()
    monkeypatch.setattr(
        _tool_utils,
        "get_tool_traits",
        lambda _app, name: ToolTraits(
            mcp_module=_TOOL_MODULES.get(name, name if name != "unannotated" else None)
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

    tool = asyncio.run(airbyte_app.get_tool("show_connector_definitions"))
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


@pytest.mark.parametrize(
    ("env_value", "caller_value", "expected"),
    [
        pytest.param(None, None, False, id="hidden_by_default"),
        pytest.param(None, "1", True, id="caller_header_enables"),
        pytest.param(None, "0", False, id="caller_header_disables"),
        pytest.param("1", None, True, id="env_enables"),
        pytest.param("1", "0", False, id="caller_narrows_env"),
        pytest.param("0", "1", False, id="env_off_wins"),
    ],
)
def test_insiders_tool_visibility(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
    env_value: str | None,
    caller_value: str | None,
    expected: bool,
) -> None:
    """Tools registered with `insiders_only=True` follow the insiders env var and header."""
    if env_value is not None:
        monkeypatch.setenv(MCP_INSIDERS_ENV_VAR, env_value)
    if caller_value is not None:
        mcp_config[MCP_CONFIG_INSIDERS] = caller_value
    _tool_utils._TOOL_POLICIES["insiders_tool"] = _tool_utils.ToolPolicy(
        pipeline_change=False,
        insiders_only=True,
    )
    _tool_utils._TOOL_POLICIES["regular_tool"] = _tool_utils.ToolPolicy(
        pipeline_change=False,
    )

    insiders_filter = _tool_utils.airbyte_insiders_tools_filter
    assert insiders_filter(_tool("insiders_tool"), APP) is expected
    assert insiders_filter(_tool("regular_tool"), APP) is True


@pytest.mark.parametrize(
    ("insiders", "destructive", "pipeline_changes", "readonly", "expected"),
    [
        pytest.param(None, None, None, None, False, id="insiders_unset"),
        pytest.param("1", None, None, None, True, id="insiders_on"),
        pytest.param("1", "0", None, None, False, id="destructive_tools_off"),
        pytest.param("1", None, "0", None, False, id="pipeline_changes_off"),
        pytest.param("1", None, None, "1", False, id="readonly_mode_on"),
    ],
)
def test_unsafe_connection_update_tool_enabled(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
    insiders: str | None,
    destructive: str | None,
    pipeline_changes: str | None,
    readonly: str | None,
    expected: bool,
) -> None:
    """The unsafe connection update is advertised only when every gate permits it."""
    for env_var, value in (
        (MCP_INSIDERS_ENV_VAR, insiders),
        (MCP_ALLOW_DESTRUCTIVE_TOOLS_ENV_VAR, destructive),
        (MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, pipeline_changes),
        (MCP_READONLY_MODE_ENV_VAR, readonly),
    ):
        if value is None:
            monkeypatch.delenv(env_var, raising=False)
        else:
            monkeypatch.setenv(env_var, value)

    ctx = Context(fastmcp=FastMCP("unsafe-tool-policy-test"))
    assert cloud_mcp._is_unsafe_connection_update_tool_enabled(ctx) is expected


def test_sync_tools_stay_visible_when_pipeline_changes_are_disabled(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    assert not mcp_config
    monkeypatch.setenv(MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, "0")
    for tool_name in ("run_connection_sync", "cancel_connection_sync"):
        tool = _policy_tool(tool_name, pipeline_change=False)
        assert _tool_utils.airbyte_readonly_mode_filter(tool, APP)


def test_non_read_only_tools_hidden_in_readonly_mode(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    """Read-only mode hides non-read-only tools but keeps read-only-annotated tools."""
    assert not mcp_config
    monkeypatch.setenv(MCP_READONLY_MODE_ENV_VAR, "1")
    for tool_name in ("run_connection_sync", "cancel_connection_sync"):
        tool = _policy_tool(tool_name, pipeline_change=False)
        assert not _tool_utils.airbyte_readonly_mode_filter(tool, APP)
    read_only_tool = _policy_tool(
        "list_workspaces", pipeline_change=False, read_only_hint=True
    )
    assert _tool_utils.airbyte_readonly_mode_filter(read_only_tool, APP)


def _policy_tool(
    name: str,
    *,
    pipeline_change: bool | None,
    external_access: bool = False,
    read_only_hint: bool = False,
    destructive_hint: bool = False,
    module: str = "cloud",
) -> Tool:
    _TOOL_MODULES[name] = module
    if pipeline_change is not None or external_access:
        _tool_utils._TOOL_POLICIES[name] = _tool_utils.ToolPolicy(
            pipeline_change=bool(pipeline_change),
            external_access=external_access,
        )
    return cast(
        Tool,
        SimpleNamespace(
            name=name,
            annotations=SimpleNamespace(
                read_only_hint=read_only_hint,
                destructive_hint=destructive_hint,
            ),
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
        "visible",
    ),
    [
        pytest.param("0", None, None, None, False, id="external-env-deny"),
        pytest.param("1", "0", None, None, False, id="header-narrows-external-allow"),
        pytest.param("0", "1", None, None, False, id="external-env-deny-wins"),
        pytest.param(None, "0", None, None, False, id="external-header-deny"),
        pytest.param("1", None, None, "1", True, id="explicit-external-allow"),
        pytest.param(
            None, None, "0", None, True, id="pipeline-deny-external-unaffected"
        ),
        pytest.param(None, None, None, "1", True, id="readonly-external-unaffected"),
        pytest.param(None, None, None, None, True, id="unset-default-allows"),
    ],
)
def test_external_access_filter(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
    external_env: str | None,
    header: str | None,
    pipeline_env: str | None,
    legacy_readonly: str | None,
    visible: bool,
) -> None:
    for env_var, value in (
        (MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR, external_env),
        (MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, pipeline_env),
        (MCP_READONLY_MODE_ENV_VAR, legacy_readonly),
    ):
        if value is None:
            monkeypatch.delenv(env_var, raising=False)
        else:
            monkeypatch.setenv(env_var, value)
    if header is not None:
        mcp_config[MCP_CONFIG_ALLOW_EXTERNAL_ACCESS] = header
    tool = _policy_tool(
        "external-access-tool",
        pipeline_change=False,
        external_access=True,
        read_only_hint=True,
    )

    assert _tool_utils.airbyte_external_access_filter(tool, APP) is visible


@pytest.mark.parametrize(
    ("env_value", "header", "destructive", "visible"),
    [
        pytest.param("0", None, True, False, id="env-deny"),
        pytest.param(None, "0", True, False, id="header-deny"),
        pytest.param("1", "0", True, False, id="header-narrows-env-allow"),
        pytest.param(None, None, True, True, id="unset-default-allows"),
        pytest.param("0", None, False, True, id="non-destructive-unaffected"),
    ],
)
def test_destructive_tools_filter(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
    env_value: str | None,
    header: str | None,
    destructive: bool,
    visible: bool,
) -> None:
    if env_value is None:
        monkeypatch.delenv(MCP_ALLOW_DESTRUCTIVE_TOOLS_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(MCP_ALLOW_DESTRUCTIVE_TOOLS_ENV_VAR, env_value)
    if header is not None:
        mcp_config[MCP_CONFIG_ALLOW_DESTRUCTIVE_TOOLS] = header
    tool = _policy_tool(
        "destructive-tool",
        pipeline_change=False,
        destructive_hint=destructive,
    )

    assert _tool_utils.airbyte_destructive_tools_filter(tool, APP) is visible


def test_local_tools_unaffected_by_readonly_and_destructive_policies(
    monkeypatch: pytest.MonkeyPatch,
    mcp_config: dict[str, str],
) -> None:
    """Local-module tools stay visible under Cloud read-only and destructive policies."""
    assert not mcp_config
    monkeypatch.setenv(MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, "0")
    monkeypatch.setenv(MCP_READONLY_MODE_ENV_VAR, "1")
    monkeypatch.setenv(MCP_ALLOW_DESTRUCTIVE_TOOLS_ENV_VAR, "0")

    # No `ToolPolicy` registration: the tools exercise the real default
    # (non-read-only -> pipeline_change=True).
    destructive_local_tool = _policy_tool(
        "run_local_destination_smoke_test",
        pipeline_change=None,
        destructive_hint=True,
        module="local",
    )
    assert _tool_utils.airbyte_destructive_tools_filter(destructive_local_tool, APP)
    assert _tool_utils.airbyte_readonly_mode_filter(destructive_local_tool, APP)

    non_readonly_local_tool = _policy_tool(
        "sync_connector",
        pipeline_change=None,
        module="local",
    )
    assert _tool_utils.airbyte_readonly_mode_filter(non_readonly_local_tool, APP)


def test_real_server_keeps_local_tools_listed_under_cloud_denials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real registrations: local tools survive Cloud denials; Cloud tools do not."""
    import asyncio

    from airbyte.mcp.server import app as airbyte_app

    monkeypatch.setenv(MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, "0")
    monkeypatch.setenv(MCP_READONLY_MODE_ENV_VAR, "1")
    monkeypatch.setenv(MCP_ALLOW_DESTRUCTIVE_TOOLS_ENV_VAR, "0")

    def visible(name: str) -> bool:
        tool = cast(Tool, asyncio.run(airbyte_app.get_tool(name)))
        return _tool_utils.airbyte_readonly_mode_filter(
            tool, airbyte_app
        ) and _tool_utils.airbyte_destructive_tools_filter(tool, airbyte_app)

    assert visible("run_local_destination_smoke_test")
    assert not visible("run_connection_sync")
