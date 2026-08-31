# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the Airbyte MCP tool module filters."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from airbyte.constants import (
    MCP_CONFIG_EXCLUDE_MODULES,
    MCP_CONFIG_INCLUDE_MODULES,
    MCP_CONFIG_INSIDERS,
    MCP_INSIDERS_ENV_VAR,
    MCP_INSIDERS_HEADER,
    MCP_INSIDERS_MODULES,
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
    """A tool with no module annotation is never filtered by module."""
    tool = cast(Tool, SimpleNamespace(annotations=None))

    assert _tool_utils.airbyte_module_filter(tool, APP)


def test_insiders_gate_is_off_by_default() -> None:
    """Guards the hidden-module list and the config arg that opens the gate."""
    config_arg: Any = _tool_utils.INSIDERS_CONFIG_ARG

    assert set(MCP_INSIDERS_MODULES) == {"agents"}
    assert config_arg.default == "0"
    assert not config_arg.required
    assert config_arg.http_header_key == MCP_INSIDERS_HEADER
    assert config_arg.env_var == MCP_INSIDERS_ENV_VAR
