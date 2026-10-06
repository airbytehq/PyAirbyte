# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for runtime MCP policy enforcement."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError
from fastmcp_extensions import mcp_server

from airbyte.constants import (
    CLOUD_MCP_SAFE_MODE_ENV_VAR,
    MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR,
    MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR,
    MCP_READONLY_MODE_ENV_VAR,
)
from airbyte.exceptions import (
    AirbyteExternalAccessDisabledError,
    AirbytePipelineChangesDisabledError,
)
from airbyte.mcp import _tool_utils
from airbyte.mcp._error_handling import (
    MCP_TOOL_USER_FACING_ERRORS,
    format_user_facing_error,
)
from airbyte.mcp._policy_middleware import PolicyGuardMiddleware
from airbyte.mcp._tool_utils import ToolPolicy


class _FakeFastMCP:
    def __init__(self, tool: Any) -> None:
        self.tool = tool

    async def get_tool(self, name: str) -> Any:
        assert name == self.tool.name
        return self.tool


@pytest.fixture(autouse=True)
def clear_policy_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for env_var in (
        CLOUD_MCP_SAFE_MODE_ENV_VAR,
        MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR,
        MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR,
        MCP_READONLY_MODE_ENV_VAR,
    ):
        monkeypatch.delenv(env_var, raising=False)
    monkeypatch.setattr(_tool_utils, "_TOOL_POLICIES", {})


@pytest.mark.parametrize(
    (
        "tool_name",
        "policy",
        "read_only_hint",
        "environment",
        "request_config",
        "expected_error",
    ),
    [
        pytest.param(
            "pipeline_write",
            ToolPolicy(pipeline_change=True),
            False,
            {MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR: "0"},
            {},
            AirbytePipelineChangesDisabledError,
            id="pipeline-change-denied",
        ),
        pytest.param(
            "provider_read_only",
            None,
            True,
            {MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR: "0"},
            {},
            None,
            id="read-only-provider-pass-through",
        ),
        pytest.param(
            "sync_control",
            ToolPolicy(pipeline_change=False),
            False,
            {MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR: "0"},
            {},
            None,
            id="non-pipeline-change-tool-pass-through",
        ),
        pytest.param(
            "pipeline_write",
            ToolPolicy(pipeline_change=True),
            False,
            {MCP_READONLY_MODE_ENV_VAR: "1"},
            {},
            AirbytePipelineChangesDisabledError,
            id="legacy-read-only-denial",
        ),
        pytest.param(
            "external_query",
            ToolPolicy(pipeline_change=False, external_access=True),
            False,
            {MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR: "0"},
            {},
            AirbyteExternalAccessDisabledError,
            id="explicit-external-access-denial",
        ),
        pytest.param(
            "external_query",
            ToolPolicy(pipeline_change=False, external_access=True),
            False,
            {CLOUD_MCP_SAFE_MODE_ENV_VAR: "1"},
            {},
            AirbyteExternalAccessDisabledError,
            id="explicit-safe-mode-default-denial",
        ),
        pytest.param(
            "external_query",
            ToolPolicy(pipeline_change=False, external_access=True),
            False,
            {
                CLOUD_MCP_SAFE_MODE_ENV_VAR: "1",
                MCP_ALLOW_EXTERNAL_ACCESS_ENV_VAR: "1",
            },
            {},
            None,
            id="explicit-external-access-allow",
        ),
        pytest.param(
            "external_query",
            ToolPolicy(pipeline_change=False, external_access=True),
            False,
            {},
            {},
            None,
            id="unset-safe-mode-default-pass-through",
        ),
        pytest.param(
            "external_query",
            ToolPolicy(pipeline_change=False, external_access=True),
            False,
            {CLOUD_MCP_SAFE_MODE_ENV_VAR: "auto"},
            {},
            None,
            id="auto-safe-mode-default-pass-through",
        ),
        pytest.param(
            "external_query",
            ToolPolicy(pipeline_change=False, external_access=True),
            False,
            {MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR: "0"},
            {},
            AirbyteExternalAccessDisabledError,
            id="pipeline-changes-disabled-default-denial",
        ),
    ],
)
def test_policy_guard_middleware(
    monkeypatch: pytest.MonkeyPatch,
    tool_name: str,
    policy: ToolPolicy | None,
    read_only_hint: bool,
    environment: dict[str, str],
    request_config: dict[str, str],
    expected_error: type[Exception] | None,
) -> None:
    monkeypatch.setattr(
        _tool_utils,
        "get_mcp_config",
        lambda _app, key, **_kwargs: request_config.get(key),
    )
    for env_var, value in environment.items():
        monkeypatch.setenv(env_var, value)

    if policy is not None:
        _tool_utils._TOOL_POLICIES[tool_name] = policy
    tool = SimpleNamespace(
        name=tool_name,
        annotations=SimpleNamespace(read_only_hint=read_only_hint),
    )
    fastmcp_context = SimpleNamespace(fastmcp=_FakeFastMCP(tool))
    context = SimpleNamespace(
        fastmcp_context=fastmcp_context,
        message=SimpleNamespace(name=tool_name),
    )

    async def call_next(_context: Any) -> str:
        return "called"

    async def invoke() -> str:
        return await PolicyGuardMiddleware().on_call_tool(context, call_next)

    if expected_error is None:
        assert asyncio.run(invoke()) == "called"
    else:
        with pytest.raises(expected_error):
            asyncio.run(invoke())


def test_blocked_policy_error_is_formatted_for_mcp_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = mcp_server(
        "test",
        telemetry=False,
        user_facing_errors=MCP_TOOL_USER_FACING_ERRORS,
        user_facing_error_formatter=format_user_facing_error,
    )
    server.add_middleware(PolicyGuardMiddleware())
    monkeypatch.setenv(MCP_ALLOW_PIPELINE_CHANGES_ENV_VAR, "0")

    @server.tool
    def blocked_pipeline_tool() -> str:
        return "unexpectedly executed"

    _tool_utils._TOOL_POLICIES["blocked_pipeline_tool"] = ToolPolicy(
        pipeline_change=True
    )

    async def call_tool() -> None:
        async with Client(server) as client:
            with pytest.raises(ToolError) as error:
                await client.call_tool("blocked_pipeline_tool")
        assert "Pipeline-changing tools are disabled" in str(error.value)
        assert "AIRBYTE_CLOUD_MCP_ALLOW_PIPELINE_CHANGES=1" in str(error.value)

    asyncio.run(call_tool())
