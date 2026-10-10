# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""PyAirbyte-specific handling and trace classification of MCP tool errors."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from fastmcp.exceptions import ToolError, ValidationError
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp_extensions.otel._arg_digests import classify_tool  # noqa: PLC2701
from fastmcp_extensions.otel._extras import _chain, declared_parameters  # noqa: PLC2701
from fastmcp_extensions.otel.models import TraceArg

from airbyte._util.api_util import (
    SDKError,
    error_response_body,
    sdk_error_message,
    sdk_error_response,
)
from airbyte._util.cloud_errors import (
    describe_cloud_error,
    is_valid_problem_slug,
    parse_cloud_error,
)
from airbyte.exceptions import (
    AirbyteAgentsUnavailableError,
    AirbyteCloudApiError,
    AirbyteConnectionSyncError,
    AirbyteConnectionSyncTimeoutError,
    AirbyteConnectorInUseError,
    AirbyteConnectorNotReadyError,
    AirbyteExternalAccessDisabledError,
    AirbyteLibError,
    AirbyteLibInputError,
    AirbyteMCPError,
    AirbyteMissingResourceError,
    AirbytePipelineChangesDisabledError,
    AirbyteSafeModeError,
)


if TYPE_CHECKING:
    from fastmcp.tools import Tool, ToolResult
    from mcp import types as mt


logger = logging.getLogger(__name__)


MCP_TOOL_USER_FACING_ERRORS: tuple[type[AirbyteLibError], ...] = (
    AirbyteLibInputError,
    AirbyteMCPError,
    AirbyteSafeModeError,
    AirbytePipelineChangesDisabledError,
    AirbyteExternalAccessDisabledError,
    AirbyteAgentsUnavailableError,
    AirbyteConnectorNotReadyError,
    AirbyteConnectorInUseError,
    AirbyteMissingResourceError,
)
"""Expected errors returned to MCP clients as concise message and guidance text."""


def cloud_error_trace_message(error: BaseException) -> str | None:
    """Return the fixed Cloud error message for a failed call's span, if any.

    Only the message half: guidance can carry Cloud's error ID.
    """
    cloud_error: AirbyteCloudApiError | None = None
    for cause in _chain(error):
        if isinstance(cause, AirbyteCloudApiError):
            body = error_response_body(cause)
            if body is not None:
                message, _ = describe_cloud_error(parse_cloud_error(cause.status_code, body))
                return message
            cloud_error = cause
        elif isinstance(cause, SDKError):
            return sdk_error_message(cause)

    if cloud_error is not None:
        message, _ = describe_cloud_error(parse_cloud_error(cloud_error.status_code, None))
        return message
    return None


def _cloud_api_error_text(error: AirbyteCloudApiError) -> str:
    """Return fixed Cloud error text, retaining direct-access guidance when present."""
    message, table_guidance = describe_cloud_error(
        parse_cloud_error(error.status_code, error_response_body(error))
    )
    guidance = error.guidance if error.guidance is not None else table_guidance
    return f"{message} {guidance}".rstrip()


def format_user_facing_error(error: BaseException) -> str:
    """Return the error message followed by its guidance, when present."""
    if isinstance(error, AirbyteCloudApiError):
        return _cloud_api_error_text(error)
    if not isinstance(error, AirbyteLibError):
        return str(error)
    text = error.get_message()
    if error.guidance:
        text = f"{text} {error.guidance}"
    return text


def classify_mcp_tool_error(error: BaseException) -> str | None:
    """Return a trace error category the built-in rules cannot infer, else `None`.

    A failed or timed-out sync job carries no HTTP status, so the built-in
    rules would report it as `unclassified` with an unknown fault. Sync errors raised
    from an API call have no `job_status` and keep the status-code category.
    """
    if not isinstance(error, AirbyteConnectionSyncError) or error.job_status is None:
        return None
    if isinstance(error, AirbyteConnectionSyncTimeoutError):
        return "upstream_timeout"
    return "upstream_error"


def mcp_tool_error_reason(error: BaseException) -> str | None:
    """Return the problem slug from the first Airbyte API error body in the chain.

    Only the problem `type`, or its `title` when `type` is the generic errors
    page, is read. `detail` and `data` quote IDs and payloads, so they never are.
    """
    body = next(
        (body for body in map(error_response_body, _chain(error)) if body is not None),
        None,
    )
    if body is not None:
        return parse_cloud_error(None, body).slug
    for cause in _chain(error):
        if isinstance(cause, AirbyteLibError):
            problem_type = (cause.context or {}).get("problem_type")
            if isinstance(problem_type, str):
                return problem_type if is_valid_problem_slug(problem_type) else None
    return None


def agent_error_text(error: ToolError) -> str:
    """Return the text an agent sees for a tool error before redaction."""
    cause = error.__cause__
    if isinstance(cause, AirbyteCloudApiError):
        return _cloud_api_error_text(cause)
    if isinstance(cause, AirbyteLibError):
        return format_user_facing_error(cause)
    sdk_response = sdk_error_response(cause) if cause is not None else None
    if sdk_response is not None:
        status_code, body = sdk_response
        message, guidance = describe_cloud_error(parse_cloud_error(status_code, body))
        return f"{message} {guidance}"
    return str(error)


def format_validation_error(error: ValidationError) -> str:
    """Rebuild a FastMCP argument-validation error without `input_value` echoes."""
    cause = error.__cause__
    errors = getattr(cause, "errors", None)
    parts: list[str] = []
    if callable(errors):
        try:
            details = errors(include_input=False, include_url=False, include_context=False)
        except Exception:
            details = []
        for detail in details if isinstance(details, list) else []:
            msg = detail.get("msg") if isinstance(detail, Mapping) else None
            loc = detail.get("loc", ()) if isinstance(detail, Mapping) else ()
            dotted = ".".join(str(part) for part in loc)
            if msg and dotted:
                parts.append(f"Invalid value for `{dotted}`: {msg}.")
            elif msg:
                parts.append(f"Invalid value: {msg}.")
    return " ".join(parts) if parts else "Invalid arguments."


_JSON_LITERALS = frozenset({"true", "false", "null"})
_INPUT_VALUE = re.compile(r"input_value=(?:'[^']*'|\"[^\"]*\"|[^\s,\])]+)")
_BEARER_TOKEN = re.compile(r"Bearer\s+[^\s'\")]+")
_URL_CREDENTIALS = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://)[^\s/?#'\")]+@")
_URL_QUERY = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://[^\s?#'\")]*)\?[^\s'\")]*")


def _secret_leaf(leaf: str) -> bool:
    """Whether a secret-class leaf is masked: everything but JSON literals."""
    return bool(leaf) and not (
        leaf in _JSON_LITERALS or (leaf.isdigit() and len(leaf) < 4)  # noqa: PLR2004
    )


def _collect_leaves(
    value: Any,  # noqa: ANN401
    path: str,
    *,
    secret: bool,
    leaves: list[tuple[str, str, bool]],
) -> None:
    """Collect (placeholder path, leaf text, secret) tuples for values to hide."""
    if isinstance(value, bool):
        return  # `true`/`false` are JSON literals, never masked.
    if isinstance(value, (int, float)):
        leaf = str(value)
        if (secret and _secret_leaf(leaf)) or (not secret and len(leaf) >= 8):  # noqa: PLR2004
            leaves.append((path, leaf, secret))
        return
    if isinstance(value, str):
        if (secret and _secret_leaf(value)) or (not secret and len(value) >= 8):  # noqa: PLR2004
            leaves.append((path, value, secret))
        # A JSON-string config may hold nested secrets; index its leaves too.
        if value[:1] in {"{", "["}:
            try:
                nested = json.loads(value)
            except ValueError:
                return
            if isinstance(nested, (dict, list)):
                _collect_leaves(nested, path, secret=secret, leaves=leaves)
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if isinstance(key, str):
                _collect_leaves(item, f"{path}.{key}", secret=secret, leaves=leaves)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _collect_leaves(item, path, secret=secret, leaves=leaves)


def _allowed_choices(
    tool: Tool | None,
    name: str,
    arg_class: Any,  # noqa: ANN401
) -> tuple[Any, ...]:
    """Return the declared `enum`/`const` choices of a parameter, if any."""
    if tool is not None:
        schema = declared_parameters(tool).get(name)
        if isinstance(schema, Mapping):
            if isinstance(schema.get("enum"), list):
                return tuple(schema["enum"])
            if "const" in schema:
                return (schema["const"],)
    allowed = getattr(arg_class, "allowed", None)
    return tuple(allowed) if allowed is not None else ()


def redact_agent_text(
    text: str,
    arguments: Mapping[str, Any] | None,
    tool: Tool | None,
) -> str:
    """Mask every echoed argument value in agent-facing error text.

    Each parameter is classified by the same rules arg tracing uses, so error
    text never shows a value tracing would not record in clear: secret-class
    parameters lose every leaf, other parameters lose leaves of eight or more
    characters, and `VALUE` (enum-like) parameters stay clear only when they
    hold one of the declared choices. Secret leaves shorter than four
    characters and non-secret leaves are masked as whole tokens.
    """
    if arguments:
        func = getattr(tool, "fn", None) if tool is not None else None
        try:
            classes = classify_tool(func, sorted(arguments), tool=getattr(tool, "name", ""))
        except Exception:
            classes = {}
        leaves: list[tuple[str, str, bool]] = []
        for name, value in arguments.items():
            arg_class = classes.get(name)
            mode = arg_class.mode if arg_class is not None else TraceArg.PRESENCE
            if mode is TraceArg.VALUE:
                if any(value == choice for choice in _allowed_choices(tool, name, arg_class)):
                    continue
                secret = False
            else:
                secret = mode in {TraceArg.OMIT, TraceArg.PRESENCE}
            _collect_leaves(value, name, secret=secret, leaves=leaves)
        seen: set[str] = set()
        for path, leaf, secret in sorted(leaves, key=lambda item: -len(item[1])):
            if leaf in seen:
                continue
            seen.add(leaf)
            placeholder = f"<value of {path.split('.', maxsplit=1)[0]}>"
            if secret and len(leaf) >= 4:  # noqa: PLR2004
                text = text.replace(leaf, placeholder)
            else:
                text = re.sub(
                    rf"(?<![A-Za-z0-9_-]){re.escape(leaf)}(?![A-Za-z0-9_-])",
                    placeholder.replace("\\", "\\\\"),
                    text,
                )

    text = _INPUT_VALUE.sub("input_value=<redacted>", text)
    text = _BEARER_TOKEN.sub("Bearer <redacted>", text)
    text = _URL_CREDENTIALS.sub(r"\1<redacted>@", text)
    return _URL_QUERY.sub(r"\1", text)


async def _call_arguments(
    context: MiddlewareContext[mt.CallToolRequestParams],
) -> tuple[Mapping[str, Any] | None, Tool | None]:
    """Return the call's arguments and tool, either `None` when unavailable."""
    message = context.message
    arguments = getattr(message, "arguments", None)
    tool = None
    fastmcp_context = context.fastmcp_context
    if fastmcp_context is not None:
        try:
            tool = await fastmcp_context.fastmcp.get_tool(message.name)
        except Exception:
            logger.debug("Could not resolve the called tool for error redaction")
    return arguments if isinstance(arguments, Mapping) else None, tool


class AgentErrorTextMiddleware(Middleware):
    """Rewrite tool error text for the agent and mask echoed argument values.

    Runs outside `UserFacingErrorMiddleware` and telemetry: telemetry has
    already classified the original exception when this middleware replaces it.
    """

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        try:
            return await call_next(context)
        except ValidationError as error:
            arguments, tool = await _call_arguments(context)
            raise ValidationError(
                redact_agent_text(format_validation_error(error), arguments, tool)
            ) from None
        except ToolError as error:
            arguments, tool = await _call_arguments(context)
            raise ToolError(redact_agent_text(agent_error_text(error), arguments, tool)) from None
