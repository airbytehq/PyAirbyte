# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for concise user-facing MCP tool errors."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Literal

import pytest
import requests
from airbyte_api.errors import SDKError
from fastmcp import Client
from fastmcp.exceptions import ToolError

from airbyte._util import api_util
from airbyte.exceptions import (
    AirbyteCloudApiError,
    AirbyteConnectionSyncError,
    AirbyteConnectionSyncTimeoutError,
    AirbyteConnectorInUseError,
    AirbyteMissingResourceError,
    AirbyteLibError,
    AirbyteLibInputError,
)
from airbyte.mcp._error_handling import (
    MCP_TOOL_USER_FACING_ERRORS,
    AgentErrorTextMiddleware,
    agent_error_text,
    redact_agent_text,
    classify_mcp_tool_error,
    format_user_facing_error,
    mcp_tool_error_reason,
)
from airbyte.mcp._tool_utils import check_guid_created_in_session
from fastmcp_extensions import UserFacingErrorMiddleware, mcp_server


def test_unexpected_errors_keep_fastmcp_default_handling() -> None:
    server = mcp_server(
        "test",
        telemetry=False,
        user_facing_errors=MCP_TOOL_USER_FACING_ERRORS,
        user_facing_error_formatter=format_user_facing_error,
    )

    @server.tool
    def raise_runtime_error() -> None:
        raise RuntimeError("boom")

    async def call_tool() -> None:
        async with Client(server) as client:
            try:
                await client.call_tool("raise_runtime_error")
            except ToolError as error:
                assert "boom" in str(error)
                assert "fix it" not in str(error)
                return
        raise AssertionError("Expected ToolError")

    asyncio.run(call_tool())


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(
            AirbyteLibInputError(message="bad", guidance="fix it"),
            id="input-error",
        ),
        pytest.param(
            AirbyteConnectorInUseError(
                message="Connector is in use.",
                guidance="Delete its connections first.",
            ),
            id="connector-in-use",
        ),
        pytest.param(
            AirbyteMissingResourceError(
                resource_type="sync job",
                resource_name_or_id="42",
                message="Job 42 is unavailable.",
                guidance="Find a valid job ID.",
            ),
            id="missing-resource",
        ),
    ],
)
def test_expected_errors_are_presented_concisely(error: AirbyteLibError) -> None:
    server = mcp_server(
        "test",
        telemetry=False,
        user_facing_errors=MCP_TOOL_USER_FACING_ERRORS,
        user_facing_error_formatter=format_user_facing_error,
    )

    @server.tool
    def raise_resource_error() -> None:
        raise error

    async def call_tool() -> None:
        async with Client(server) as client:
            try:
                await client.call_tool("raise_resource_error")
            except ToolError as tool_error:
                text = str(tool_error)
                assert "Traceback" not in text
                assert error.get_message() in text
                assert error.guidance in text
                return
        raise AssertionError("Expected ToolError")

    asyncio.run(call_tool())


def test_format_user_facing_error() -> None:
    assert (
        format_user_facing_error(AirbyteLibInputError(message="bad", guidance="fix it"))
        == "bad fix it"
    )
    assert format_user_facing_error(AirbyteLibInputError(message="bad")) == "bad"
    assert format_user_facing_error(ValueError("plain")) == "plain"


def test_direct_cloud_api_error_uses_fixed_table_message_in_agent_text() -> None:
    error = AirbyteCloudApiError(
        status_code=404,
        message="Request failed at https://config.example/connections/secret-id",
        context={
            "full_url": "https://config.example/connections/secret-id",
            "response_text": json.dumps({
                "type": "https://reference.airbyte.com/reference/errors#resource-not-found",
                "data": {
                    "resourceType": "connection",
                    "message": "SENTINEL_RESPONSE_TEXT",
                },
            }),
        },
    )
    with pytest.raises(ToolError) as exc_info:
        raise ToolError("unformatted error") from error

    text = agent_error_text(exc_info.value)

    assert "The connection was not found." in text
    assert "resource-not-found, HTTP 404" in text
    assert "https://config.example" not in text
    assert "SENTINEL_RESPONSE_TEXT" not in text
    assert "response_text" not in text


def test_format_direct_cloud_api_error_keeps_fixed_forbidden_guidance() -> None:
    guidance = (
        "Authentication succeeded but access was denied; the workspace or connector "
        "may not be enabled for agent access in Airbyte Cloud."
    )
    error = AirbyteCloudApiError(
        status_code=403,
        message="Request failed at https://config.example/connections/secret-id",
        guidance=guidance,
        context={"full_url": "https://config.example/connections/secret-id"},
    )

    text = format_user_facing_error(error)

    assert guidance in text
    assert "https://config.example" not in text
    assert "full_url" not in text


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        pytest.param(
            AirbyteConnectionSyncError(
                connection_id="c", job_id=1, job_status="failed"
            ),
            "upstream_error",
            id="failed-job",
        ),
        pytest.param(
            AirbyteConnectionSyncTimeoutError(
                connection_id="c", job_id=1, job_status="running", timeout=5
            ),
            "upstream_timeout",
            id="timed-out-job",
        ),
        pytest.param(
            AirbyteConnectionSyncError(connection_id="c"),
            None,
            id="api-error-keeps-status-category",
        ),
        pytest.param(AirbyteLibInputError(message="bad input"), None, id="other-error"),
        pytest.param(ValueError("unexpected"), None, id="non-airbyte-error"),
    ],
)
def test_classify_mcp_tool_error(error: BaseException, expected: str | None) -> None:
    assert classify_mcp_tool_error(error) == expected


@pytest.mark.parametrize(
    ("problem", "expected"),
    [
        (
            {"type": "https://h/errors", "title": "unexpected-problem"},
            "unexpected-problem",
        ),
        # A slug that is, or holds, an ID names a resource and is never exported.
        ({"type": "https://h/errors", "title": "1234567"}, None),
        (
            {"type": "https://h/v1/workspaces/97953b90-8f3a-4c1e-9d2b-0a1b2c3d4e5f"},
            None,
        ),
        (
            {
                "type": "https://h/errors",
                "title": "ws-97953b90-8f3a-4c1e-9d2b-0a1b2c3d4e5f",
            },
            None,
        ),
    ],
)
def test_mcp_tool_error_reason_is_never_an_id(
    problem: dict[str, str], expected: str | None
) -> None:
    error = AirbyteCloudApiError(context={"response_text": json.dumps(problem)})
    assert mcp_tool_error_reason(error) == expected


def test_mcp_tool_error_reason_uses_validated_context_problem_type() -> None:
    error = AirbyteCloudApiError(context={"problem_type": "try-again-later"})
    error.__cause__ = requests.HTTPError("request failed")
    assert mcp_tool_error_reason(error) == "try-again-later"


@pytest.mark.parametrize(
    "problem_type",
    [
        "97953b90-8f3a-4c1e-9d2b-0a1b2c3d4e5f",
        "password=hunter2",
    ],
)
def test_mcp_tool_error_reason_rejects_invalid_context_slug(problem_type: str) -> None:
    error = AirbyteCloudApiError(context={"problem_type": problem_type})
    assert mcp_tool_error_reason(error) is None


def _server_with_agent_error_text():
    """Build a test server wired like `airbyte.mcp.server.app`."""
    server = mcp_server(
        "test",
        telemetry=False,
        user_facing_errors=MCP_TOOL_USER_FACING_ERRORS,
        user_facing_error_formatter=format_user_facing_error,
    )
    server.middleware.insert(
        next(
            i
            for i, middleware in enumerate(server.middleware)
            if isinstance(middleware, UserFacingErrorMiddleware)
        ),
        AgentErrorTextMiddleware(),
    )
    return server


def _call_error(server, name: str, arguments: dict | None = None) -> str:
    async def call_tool() -> str:
        async with Client(server) as client:
            try:
                await client.call_tool(name, arguments or {})
            except ToolError as error:
                return str(error)
        raise AssertionError("Expected ToolError")

    return asyncio.run(call_tool())


def test_argument_errors_name_the_parameter_without_the_value() -> None:
    server = _server_with_agent_error_text()

    @server.tool
    def typed(count: int) -> str:
        return "ok"

    text = _call_error(server, "typed", {"count": "sk_live_FAKE1234567890"})

    assert "sk_live_FAKE1234567890" not in text
    assert "input_value" not in text
    assert "`count`" in text


def test_enum_typo_is_masked_with_parameter_name() -> None:
    server = _server_with_agent_error_text()

    @server.tool
    def with_mode(mode: Literal["full", "incremental"]) -> str:
        return mode

    text = _call_error(server, "with_mode", {"mode": "incrimental-typo-value"})

    assert "incrimental-typo-value" not in text
    assert "input_value" not in text


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(["a"], id="list"),
        pytest.param({"k": "v"}, id="dict"),
    ],
)
def test_unhashable_literal_argument_is_redacted(value: object) -> None:
    server = _server_with_agent_error_text()

    @server.tool
    def with_mode(mode: Literal["a"]) -> str:
        return str(mode)

    text = _call_error(server, "with_mode", {"mode": value})

    assert repr(value) not in text
    assert "input_value" not in text


def test_cloud_error_is_formatted_without_debug_context() -> None:
    server = _server_with_agent_error_text()

    @server.tool
    def broken() -> str:
        raw = requests.Response()
        raw.status_code = 500
        raw.url = "https://api.airbyte.com/v1/connections"
        body = json.dumps({
            "type": "https://reference.airbyte.com/reference/errors",
            "title": "unexpected-problem",
            "data": {"message": "INTERNAL_MARKER select x from y"},
        })
        error = SDKError("API error occurred", 500, body, raw)
        raise api_util._wrap_sdk_error(error) from error

    text = _call_error(server, "broken")

    assert "Airbyte Cloud hit an unexpected error" in text
    assert "unexpected-problem, HTTP 500" in text
    assert "INTERNAL_MARKER" not in text
    assert "Status Code" not in text
    assert "api.airbyte.com" not in text


def test_unexpected_error_echoed_argument_is_masked() -> None:
    server = _server_with_agent_error_text()

    @server.tool
    def fail(connection_id: str) -> str:
        raise RuntimeError(f"boom with {connection_id}")

    text = _call_error(server, "fail", {"connection_id": "SENTINEL-arg-value-1234"})

    assert "boom with <value of connection_id>" in text
    assert "SENTINEL-arg-value-1234" not in text


def test_user_facing_error_echoed_id_is_masked() -> None:
    server = _server_with_agent_error_text()

    @server.tool
    def unsafe(connection_id: str) -> str:
        check_guid_created_in_session(connection_id)
        return "done"

    text = _call_error(
        server,
        "unsafe",
        {"connection_id": "00000000-1111-2222-3333-444444444444"},
    )

    assert "Cannot perform destructive operation" in text
    assert "00000000-1111-2222-3333-444444444444" not in text
    assert "<value of connection_id>" in text


def test_secret_valued_argument_never_shows_a_prefix() -> None:
    server = _server_with_agent_error_text()

    @server.tool
    def read_records(config: dict | str | None, stream_name: str) -> str:
        raise AirbyteLibInputError(
            message=f"Stream '{stream_name}' failed with key {config}"
        )

    secret = "sk_live_FAKE1234567890"
    text = _call_error(
        server,
        "read_records",
        {
            "config": {"api_key": secret},
            "stream_name": "stream_with_secret_value",
        },
    )

    assert secret not in text
    assert "sk_live" not in text
    assert "<value of config.api_key>" in text
    assert "stream_with_secret_value" not in text


def _tool_stub(fn):
    return SimpleNamespace(fn=fn, name="test-tool")


def _classify_args(stream_name: str, connection_id: str, password: str) -> None:
    """Hints so `classify_tool` gives HASH/PRESENCE modes like real tools."""


@pytest.mark.parametrize(
    ("arguments", "text", "expected"),
    [
        pytest.param(
            {"stream_name": "user"},
            "Ask the user; don't retry.",
            "Ask the user; don't retry.",
            id="non-secret-word-user",
        ),
        pytest.param(
            {"stream_name": "retry"},
            "Ask the user; don't retry.",
            "Ask the user; don't retry.",
            id="non-secret-word-retry",
        ),
        pytest.param(
            {"stream_name": "users"},
            "Ask the user; don't retry.",
            "Ask the user; don't retry.",
            id="non-secret-word-users",
        ),
        pytest.param(
            {"stream_name": "customer"},
            "Check the customers table first.",
            "Check the customers table first.",
            id="non-secret-token-inside-longer-token",
        ),
        pytest.param(
            {"connection_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479"},
            "Cannot perform destructive operation on "
            "'f47ac10b-58cc-4372-a567-0e02b2c3d479': Object was not created.",
            "Cannot perform destructive operation on "
            "'<value of connection_id>': Object was not created.",
            id="non-secret-uuid-echo-masked",
        ),
        pytest.param(
            {"password": "abc123secret"},
            "boom in xxabc123secretyy field",
            "boom in xx<value of password>yy field",
            id="secret-substring-masked",
        ),
    ],
)
def test_redact_agent_text_masks_whole_tokens_only(
    arguments: dict, text: str, expected: str
) -> None:
    assert redact_agent_text(text, arguments, _tool_stub(_classify_args)) == expected


def test_short_secret_is_masked_as_a_whole_token() -> None:
    assert (
        redact_agent_text(
            "Ask the user for a valid value",
            {"password": "a"},
            _tool_stub(_classify_args),
        )
        == "Ask the user for <value of password> valid value"
    )


def test_long_secret_is_masked_inside_a_token() -> None:
    assert (
        redact_agent_text(
            "Request failed with pw=hunter2xyz;",
            {"password": "hunter2xyz"},
            _tool_stub(_classify_args),
        )
        == "Request failed with pw=<value of password>;"
    )
