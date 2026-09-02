# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the Airbyte MCP tool module filters."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastmcp_extensions.tool_filters import CONFIG_INCLUDE_MODULES

from airbyte.constants import (
    MCP_CONFIG_EXCLUDE_MODULES,
    MCP_CONFIG_INCLUDE_MODULES,
    MCP_CONFIG_INSIDERS,
    MCP_INSIDERS_ENV_VAR,
    MCP_INSIDERS_HEADER,
    MCP_INSIDERS_MODULES,
    _str_to_bool,
)
from airbyte.mcp import _tool_utils
from fastmcp import FastMCP
from mcp.types import Tool


APP = cast(FastMCP, object())
"""Stand-in for the app; the filter only passes it to `get_mcp_config`, which is patched."""


def _tool(mcp_module: str) -> Tool:
    """Return a tool-like object annotated with an MCP module name."""
    return cast(
        Tool, SimpleNamespace(annotations=SimpleNamespace(mcp_module=mcp_module))
    )


@pytest.fixture
def mcp_config(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Patch `get_mcp_config` so tests can set MCP config values directly."""
    monkeypatch.delenv(MCP_INSIDERS_ENV_VAR, raising=False)
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
