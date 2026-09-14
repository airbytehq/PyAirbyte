# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for concise user-facing MCP tool errors."""

from __future__ import annotations

import inspect
from typing import cast

import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError

from airbyte.exceptions import AirbyteAgentsUnavailableError, PyAirbyteInputError
from airbyte.mcp import agents as agents_mcp
from airbyte.mcp._tool_utils import mcp_tool


@mcp_tool()
def _raise_input_error() -> None:
    raise PyAirbyteInputError(message="bad input")


@mcp_tool()
def _raise_agents_unavailable_error() -> None:
    raise AirbyteAgentsUnavailableError(message="no agents")


@mcp_tool()
def _raise_runtime_error() -> None:
    raise RuntimeError("boom")


def _return_value(value: str, suffix: str = "!") -> str:
    return value + suffix


_return_value_signature = inspect.signature(_return_value)
return_value = mcp_tool()(_return_value)


def test_input_errors_become_concise_tool_errors() -> None:
    with pytest.raises(ToolError) as exc_info:
        _raise_input_error()

    assert "bad input" in str(exc_info.value)
    assert exc_info.value.__cause__ is None


def test_agents_unavailable_errors_become_concise_tool_errors() -> None:
    assert "AIRBYTE_AGENTS_API_URL" in (
        AirbyteAgentsUnavailableError(message="x").guidance or ""
    )

    with pytest.raises(ToolError) as exc_info:
        _raise_agents_unavailable_error()

    assert "no agents" in str(exc_info.value)
    assert "AIRBYTE_AGENTS_API_URL" in str(exc_info.value)


def test_other_errors_propagate_unchanged() -> None:
    with pytest.raises(RuntimeError, match="boom"):
        _raise_runtime_error()


def test_real_agents_tools_return_tool_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeConnector:
        def execute(self, **_: object) -> None:
            raise AssertionError("invalid API args should fail before execution")

    monkeypatch.setattr(
        agents_mcp, "_get_agent_connector", lambda **_: _FakeConnector()
    )

    with pytest.raises(ToolError, match="JSON object"):
        agents_mcp.execute_agent_connector_ro(
            ctx=cast(Context, object()),
            connector_id="connector-id",
            entity_type="issues",
            action="list",
            api_args="[1, 2]",
            select_fields=None,
            exclude_fields=None,
            page_size=None,
            cursor=None,
            intent=None,
            workspace_id="workspace-id",
            organization_id=None,
        )


def test_return_values_and_signatures_are_preserved() -> None:
    assert return_value("hello") == "hello!"
    assert inspect.signature(return_value) == _return_value_signature
