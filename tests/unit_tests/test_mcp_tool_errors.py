# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for concise user-facing MCP tool errors."""

from __future__ import annotations

import asyncio
import json
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


def test_direct_cloud_api_error_relays_problem_message_in_agent_text() -> None:
    error = AirbyteCloudApiError(
        status_code=404,
        message="Request failed at https://config.example/connections/secret-id",
        context={
            "full_url": "https://config.example/connections/secret-id",
            "response_text": json.dumps({
                "type": "https://reference.airbyte.com/reference/errors#resource-not-found",
                "detail": "Connection was removed.",
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

    assert text == "Connection was removed."
    assert "https://config.example" not in text
    assert "SENTINEL_RESPONSE_TEXT" not in text
    assert "response_text" not in text


def test_format_direct_cloud_api_error_uses_general_forbidden_guidance() -> None:
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

    assert (
        text
        == "Airbyte Cloud returned HTTP 403. " + api_util.FORBIDDEN_RESOURCE_GUIDANCE
    )
    assert guidance not in text
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

    assert text == "unexpected-problem"
    assert "INTERNAL_MARKER" not in text
    assert "Status Code" not in text
    assert "api.airbyte.com" not in text


def test_unexpected_error_echoed_argument_passes_through() -> None:
    server = _server_with_agent_error_text()

    @server.tool
    def fail(connection_id: str) -> str:
        raise RuntimeError(f"boom with {connection_id}")

    text = _call_error(server, "fail", {"connection_id": "SENTINEL-arg-value-1234"})

    assert "boom with SENTINEL-arg-value-1234" in text
    assert "<value of" not in text


def test_user_facing_error_echoed_id_passes_through() -> None:
    server = _server_with_agent_error_text()

    @server.tool
    def unsafe(connection_id: str) -> str:
        raise AirbyteLibInputError(
            message=f"Cannot perform destructive operation on {connection_id}"
        )

    text = _call_error(
        server,
        "unsafe",
        {"connection_id": "00000000-1111-2222-3333-444444444444"},
    )

    assert "Cannot perform destructive operation" in text
    assert "00000000-1111-2222-3333-444444444444" in text
    assert "<value of" not in text


def test_secret_valued_argument_passes_through_error_text() -> None:
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

    assert secret in text
    assert "sk_live" in text
    assert "stream_with_secret_value" in text
    assert "<value of" not in text


def test_nested_argument_echo_passes_through_without_mapping_key() -> None:
    server = _server_with_agent_error_text()
    hostile_key = "password-or-injected-text"
    secret = "sk_live_FAKE1234567890"

    @server.tool
    def read_records(config: dict[str, str]) -> str:
        raise AirbyteLibInputError(message=f"Failed to read key {config[hostile_key]}")

    text = _call_error(
        server,
        "read_records",
        {"config": {hostile_key: secret}},
    )

    assert secret in text
    assert hostile_key not in text
    assert "<value of" not in text


def test_redact_agent_text_keeps_connection_error_id_and_url() -> None:
    text = (
        "Connection 'c0bfcb08-bd3c-411e-8076-3d80fa18c137' not found. See "
        "https://cloud.airbyte.com/workspaces/266ebdfe-0d7b-4540-9817-de7e4505ba61/"
        "source/c0bfcb08-bd3c-411e-8076-3d80fa18c137"
    )

    assert redact_agent_text(text) == text


@pytest.mark.parametrize(
    ("status", "payload", "message", "guidance"),
    [
        (422, {"detail": "  Cursor é\nmissing.  "}, "  Cursor é\nmissing.  ", None),
        (
            500,
            {"detail": " ", "title": "  Temporarily unavailable\n"},
            "  Temporarily unavailable\n",
            None,
        ),
        (
            409,
            {"title": "actor-not-ready", "detail": "Finish authentication.  "},
            "Finish authentication.  ",
            api_util.ACTOR_NOT_READY_MCP_GUIDANCE,
        ),
        (
            403,
            {"detail": "Resource is unavailable.  "},
            "Resource is unavailable.  ",
            api_util.FORBIDDEN_RESOURCE_GUIDANCE,
        ),
        (404, {"detail": "Connection was removed."}, "Connection was removed.", None),
        (
            429,
            {"message": "ignored non-problem text"},
            "Airbyte Cloud returned HTTP 429 (rate limited).",
            None,
        ),
    ],
)
@pytest.mark.parametrize(
    "carrier", ["sdk", "wrapped", "nested", "direct", "bodyless-direct", "http"]
)
def test_cloud_chain_uses_exact_message_for_agent_and_span(
    status: int,
    payload: dict[str, str],
    message: str,
    guidance: str | None,
    carrier: str,
) -> None:
    from airbyte.mcp._error_handling import cloud_error_trace_message

    body = json.dumps(payload)
    response = requests.Response()
    response.status_code = status
    response._content = body.encode()
    sdk = SDKError("ignored SDK message", status, body, response)
    error: BaseException = sdk
    if carrier in {"wrapped", "nested"}:
        error = api_util._wrap_sdk_error(sdk)
        error.__cause__ = sdk
        if carrier == "nested":
            outer = AirbyteMissingResourceError(
                message="local override", guidance="local advice"
            )
            outer.__cause__ = error
            error = outer
    elif carrier in {"direct", "bodyless-direct"}:
        error = AirbyteCloudApiError(
            status_code=status,
            guidance="ignored advice",
            context={"response_text": body} if carrier == "direct" else {},
        )
        if carrier == "bodyless-direct":
            error.__cause__ = sdk
    elif carrier == "http":
        error = api_util.AirbyteCloudError(
            message="ignored rewording",
            context={"problem_type": None, "status_code": status},
        )
        error.__cause__ = requests.HTTPError(response=response)
    expected = f"{message} {guidance}".rstrip() if guidance else message
    assert format_user_facing_error(error) == expected
    tool = ToolError("ignored wrapper")
    tool.__cause__ = error
    assert agent_error_text(tool) == expected
    assert cloud_error_trace_message(tool) == message


@pytest.mark.parametrize("field", ["detail", "title"])
def test_real_middleware_redacts_selected_cloud_message_after_nested_rewrap(
    field: str,
) -> None:
    server = _server_with_agent_error_text()

    @server.tool
    def broken() -> str:
        response = requests.Response()
        response.status_code = 403
        sdk = SDKError(
            "ignored", 403, json.dumps({field: "Denied Bearer FAKE_SECRET"}), response
        )
        wrapped = api_util._wrap_sdk_error(sdk)
        wrapped.__cause__ = sdk
        raise AirbyteMissingResourceError(message="local rewording") from wrapped

    text = _call_error(server, "broken")
    assert text == "Denied Bearer <redacted> " + api_util.FORBIDDEN_RESOURCE_GUIDANCE
    assert "FAKE_SECRET" not in text


@pytest.mark.parametrize("field", ["detail", "title"])
def test_real_middleware_serializes_fallback_for_escaped_lone_surrogate(
    field: str,
) -> None:
    server = _server_with_agent_error_text()

    @server.tool
    def broken() -> str:
        response = requests.Response()
        response.status_code = 500
        sdk = SDKError("ignored", 500, json.dumps({field: "\ud800"}), response)
        raise api_util._wrap_sdk_error(sdk) from sdk

    assert _call_error(server, "broken") == "Airbyte Cloud returned HTTP 500."


def test_unrelated_http_error_is_not_a_cloud_carrier() -> None:
    from airbyte.mcp._error_handling import cloud_error_trace_message

    response = requests.Response()
    response.status_code = 500
    response._content = b'{"detail":"unrelated HTTP body"}'
    error = AirbyteLibInputError(message="local input error")
    error.__cause__ = requests.HTTPError(response=response)
    assert cloud_error_trace_message(error) is None
    assert format_user_facing_error(error) == "local input error"


@pytest.mark.parametrize("status", [None, 403, 429, 502])
def test_bodyless_direct_error_has_only_status_fallback(status: int | None) -> None:
    from airbyte.mcp._error_handling import cloud_error_trace_message

    error = AirbyteCloudApiError(
        status_code=status,
        message="ignored local wording",
        guidance="ignored local guidance",
    )
    message = (
        "Airbyte Cloud request failed."
        if status is None
        else "Airbyte Cloud returned HTTP 429 (rate limited)."
        if status == 429
        else f"Airbyte Cloud returned HTTP {status}."
    )
    expected = (
        message + " " + api_util.FORBIDDEN_RESOURCE_GUIDANCE
        if status == 403
        else message
    )
    assert format_user_facing_error(error) == expected
    tool = ToolError("outer text")
    tool.__cause__ = error
    assert agent_error_text(tool) == expected
    assert cloud_error_trace_message(tool) == message


def test_cloud_chain_remains_bounded_and_tolerates_cycles() -> None:
    from airbyte.mcp._error_handling import cloud_error_trace_message

    error = ToolError("local")
    error.__cause__ = error
    assert cloud_error_trace_message(error) is None
    assert agent_error_text(error) == "local"
    cloud = AirbyteCloudApiError(status_code=500)
    outer: BaseException = cloud
    for _ in range(5):
        wrapped = ToolError("outer")
        wrapped.__cause__ = outer
        outer = wrapped
    assert cloud_error_trace_message(outer) is None


@pytest.mark.parametrize("transport", ["config", "public", "direct", "sdk"])
@pytest.mark.parametrize(
    ("content", "encoding", "expected"),
    [
        (
            b'{"detail":"bad\xff","type":"error:future"}',
            None,
            "Airbyte Cloud returned HTTP 500.",
        ),
        (
            b'{"detail":"bad\xff","type":"error:future"}',
            "utf-8",
            "Airbyte Cloud returned HTTP 500.",
        ),
        (
            b'{"detail":"caf\xe9","type":"error:future"}',
            "iso-8859-1",
            "Airbyte Cloud returned HTTP 500.",
        ),
        (b'{"detail":"caf\xc3\xa9","type":"error:future"}', "iso-8859-1", "café"),
        (b'{"detail":"caf\xc3\xa9","type":"error:future"}', None, "café"),
    ],
)
def test_raw_cloud_errors_validate_utf8_before_python_agent_and_trace_relay(
    monkeypatch: pytest.MonkeyPatch,
    transport: str,
    content: bytes,
    encoding: str | None,
    expected: str,
) -> None:
    from airbyte._direct_connectors import api_util as direct_api_util
    from airbyte.cloud._credentials import _AirbyteCredentials
    from airbyte.mcp._error_handling import cloud_error_trace_message

    response = requests.Response()
    response.status_code = 500
    response._content = content
    response.encoding = encoding
    response.headers["Content-Type"] = "application/problem+json" + (
        f"; charset={encoding}" if encoding else ""
    )
    response.request = requests.Request(
        "GET", "https://api.airbyte.com/v1/test"
    ).prepare()
    monkeypatch.setattr(api_util.requests, "request", lambda **_: response)
    monkeypatch.setattr(api_util.requests, "get", lambda *_, **__: response)
    monkeypatch.setattr(
        direct_api_util.deployment, "is_agents_api_available", lambda **_: True
    )
    kwargs = dict(
        api_root="https://api.airbyte.com/v1",
        path="/test",
        client_id=None,
        client_secret=None,
        bearer_token=api_util.SecretString("token"),
    )
    with pytest.raises(api_util.AirbyteCloudError) as caught:
        if transport == "sdk":
            sdk = SDKError("ignored SDK wording", 500, response.text, response)
            assert api_util.sdk_error_message(sdk) == expected
            assert cloud_error_trace_message(sdk) == expected
            raise api_util._wrap_sdk_error(sdk) from sdk
        elif transport == "config":
            api_util._make_config_api_request(**kwargs, json={})
        elif transport == "public":
            api_util._get_public_api_json(**kwargs, params=None)
        else:
            direct_api_util.make_cloud_agent_request(
                method="GET",
                path="/test",
                credentials=_AirbyteCredentials.from_auth(
                    bearer_token="token", env_vars=False
                ),
            )
    error = caught.value
    assert error.message == expected
    assert error.guidance is None
    assert format_user_facing_error(error) == expected
    tool = ToolError("outer wrapper")
    tool.__cause__ = error
    assert agent_error_text(tool) == expected
    assert cloud_error_trace_message(tool) == expected
    valid_utf8 = expected == "café"
    assert mcp_tool_error_reason(tool) == ("error:future" if valid_utf8 else None)
    if transport == "direct":
        assert error.context["response_body_is_utf8"] is valid_utf8
        assert error.context["response_text"] == (
            content.decode("utf-8") if valid_utf8 else response.text
        )
    else:
        assert "response_text" not in error.context
        if transport == "sdk":
            assert isinstance(error.__cause__, SDKError)
            assert error.__cause__.raw_response is response
        else:
            assert isinstance(error.__cause__, requests.HTTPError)
            assert error.__cause__.response is response
