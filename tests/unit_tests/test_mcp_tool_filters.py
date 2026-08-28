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


def test_insiders_module_is_hidden_by_default(mcp_config: dict[str, str]) -> None:
    """Agents tools are not advertised unless something asks for them."""
    assert not _visible("agents")
    assert _visible("cloud")


@pytest.mark.parametrize("config_value", ["1", "true", "TRUE", "yes"])
def test_insiders_gate_advertises_agents(
    mcp_config: dict[str, str],
    config_value: str,
) -> None:
    """Insiders mode adds Agents tools to the normal surface."""
    mcp_config[MCP_CONFIG_INSIDERS] = config_value

    assert _visible("agents")
    assert _visible("cloud")


@pytest.mark.parametrize("config_value", ["0", "false", "no", ""])
def test_insiders_gate_off_values_keep_agents_hidden(
    mcp_config: dict[str, str],
    config_value: str,
) -> None:
    """Only affirmative values open the gate."""
    mcp_config[MCP_CONFIG_INSIDERS] = config_value

    assert not _visible("agents")


def test_explicit_include_advertises_agents_without_the_gate(
    mcp_config: dict[str, str],
) -> None:
    """Naming the module is itself an opt-in, and it stays exclusive."""
    mcp_config[MCP_CONFIG_INCLUDE_MODULES] = "agents"

    assert _visible("agents")
    assert not _visible("cloud")


def test_include_list_without_agents_keeps_agents_hidden(
    mcp_config: dict[str, str],
) -> None:
    """An include list that omits Agents does not accidentally opt in."""
    mcp_config[MCP_CONFIG_INCLUDE_MODULES] = "cloud,local"

    assert not _visible("agents")
    assert _visible("cloud")


def test_include_list_can_mix_agents_with_other_modules(
    mcp_config: dict[str, str],
) -> None:
    """Agents can be requested alongside other modules by naming them all."""
    mcp_config[MCP_CONFIG_INCLUDE_MODULES] = "cloud,agents"

    assert _visible("agents")
    assert _visible("cloud")
    assert not _visible("local")


def test_exclude_list_hides_agents_despite_the_gate(mcp_config: dict[str, str]) -> None:
    """Excluding a module wins over insiders mode."""
    mcp_config[MCP_CONFIG_INSIDERS] = "1"
    mcp_config[MCP_CONFIG_EXCLUDE_MODULES] = "agents"

    assert not _visible("agents")
    assert _visible("cloud")


def test_exclude_list_leaves_other_modules_visible(mcp_config: dict[str, str]) -> None:
    """An exclude list does not implicitly opt Agents in."""
    mcp_config[MCP_CONFIG_EXCLUDE_MODULES] = "local"

    assert not _visible("agents")
    assert _visible("cloud")
    assert not _visible("local")


def test_unannotated_tools_are_always_visible(mcp_config: dict[str, str]) -> None:
    """A tool with no module annotation is never filtered by module."""
    tool = cast(Tool, SimpleNamespace(annotations=None))

    assert _tool_utils.airbyte_module_filter(tool, APP)


def test_insiders_modules_lists_only_agents() -> None:
    """Guards against a module becoming hidden by default unintentionally."""
    assert set(MCP_INSIDERS_MODULES) == {"agents"}


def test_insiders_config_arg_defaults_to_off() -> None:
    """The gate is off unless the header or env var says otherwise."""
    config_arg: Any = _tool_utils.INSIDERS_CONFIG_ARG

    assert config_arg.default == "0"
    assert not config_arg.required
    assert config_arg.http_header_key == MCP_INSIDERS_HEADER
    assert config_arg.env_var == MCP_INSIDERS_ENV_VAR
