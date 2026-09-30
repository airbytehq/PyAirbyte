# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for the Airbyte MCP tool module filters."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastmcp_extensions import ToolTraits
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


def _tool(name: str) -> Tool:
    """Return a tool-like object named for the module it belongs to."""
    return cast(Tool, SimpleNamespace(name=name))


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
    # `mcp_module` is internal state (never on the wire), so stub the traits
    # registry lookup with a tool-name -> module map.
    monkeypatch.setattr(
        _tool_utils,
        "get_tool_traits",
        lambda app, name: ToolTraits(
            mcp_module=name if name != "unannotated" else None
        ),  # noqa: ARG005
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
