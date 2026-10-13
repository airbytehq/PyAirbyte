# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""PyAirbyte-specific handling and trace classification of MCP tool errors."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Mapping
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import requests
from fastmcp.exceptions import DisabledError, NotFoundError, ToolError, ValidationError
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp_extensions.otel._extras import _chain  # noqa: PLC2701

from airbyte._util.api_util import (
    SDKError,
    cloud_error_guidance,
    decode_cloud_error_body,
    error_response_body,
)
from airbyte._util.cloud_errors import (
    CloudErrorInfo,
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
    AirbyteDestructiveToolsDisabledError,
    AirbyteExternalAccessDisabledError,
    AirbyteLibError,
    AirbyteLibInputError,
    AirbyteLocalDemoModeError,
    AirbyteMCPError,
    AirbyteMissingResourceError,
    AirbytePipelineChangesDisabledError,
    AirbyteReadOnlyModeError,
)


if TYPE_CHECKING:
    from fastmcp.tools import ToolResult
    from mcp import types as mt


_UPSTREAM_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE"})
# Literal segments come from path templates in airbyte_api.
_CLOUD_API_ROUTE_SEGMENTS = frozenset(
    {
        "connections",
        "destinations",
        "health",
        "jobs",
        "organizations",
        "oauthCredentials",
        "permissions",
        "sources",
        "initiateOAuth",
        "streams",
        "tags",
        "users",
        "workspaces",
        "definitions",
        "declarative_sources",
    }
)
_MAX_UPSTREAM_ROUTE_LENGTH = 200

_NON_AIRBYTE_TOOL_ERROR_MESSAGES: dict[type[BaseException], str] = {
    asyncio.CancelledError: "The tool call was cancelled.",
    NotImplementedError: "This operation isn't supported for this resource type yet.",
    DisabledError: "The requested tool is disabled on this server.",
    NotFoundError: "The requested tool doesn't exist on this server.",
    ValidationError: "The tool arguments failed validation.",
}


MCP_TOOL_USER_FACING_ERRORS: tuple[type[AirbyteLibError], ...] = (
    AirbyteLibInputError,
    AirbyteMCPError,
    AirbyteLocalDemoModeError,
    AirbytePipelineChangesDisabledError,
    AirbyteReadOnlyModeError,
    AirbyteDestructiveToolsDisabledError,
    AirbyteExternalAccessDisabledError,
    AirbyteAgentsUnavailableError,
    AirbyteConnectorNotReadyError,
    AirbyteConnectorInUseError,
    AirbyteMissingResourceError,
)
"""Expected errors returned to MCP clients as concise message and guidance text."""


def _cloud_error_info(error: BaseException) -> CloudErrorInfo | None:
    """Resolve known Cloud carriers, preferring a concrete body to a bodyless wrapper."""
    fallback = None
    for cause in _chain(error):
        if isinstance(cause, SDKError):
            return parse_cloud_error(cause.status_code, error_response_body(cause))
        if isinstance(cause, AirbyteCloudApiError):
            body = error_response_body(cause)
            problem = parse_cloud_error(cause.status_code, body)
            if body is not None:
                return problem
            fallback = problem
        elif (
            isinstance(cause, AirbyteLibError)
            and isinstance(cause.__cause__, requests.HTTPError)
            and "problem_type" in (cause.context or {})
        ):
            response = cause.__cause__.response
            if response is not None:
                return parse_cloud_error(response.status_code, decode_cloud_error_body(response))
            status = (cause.context or {}).get("status_code")
            return parse_cloud_error(status if isinstance(status, int) else None, None)
    return fallback


def cloud_error_trace_message(error: BaseException) -> str | None:
    """Return the selected Cloud message without workflow guidance for a failed span."""
    problem = _cloud_error_info(error)
    return problem.message if problem is not None else None


def tool_error_trace_message(error: BaseException) -> str | None:
    """Return the Cloud message or a fixed non-Cloud trace message, if any."""
    if (message := cloud_error_trace_message(error)) is not None:
        return message
    for cause in _chain(error):
        for error_class in type(cause).__mro__:
            if (
                error_class is not AirbyteLibError
                and issubclass(error_class, AirbyteLibError)
                and error_class.__doc__
            ):
                return error_class.__doc__.split("\n")[0].strip()
            if error_class in _NON_AIRBYTE_TOOL_ERROR_MESSAGES:
                return _NON_AIRBYTE_TOOL_ERROR_MESSAGES[error_class]
    return None


def cloud_error_route(error: BaseException) -> tuple[str, str] | None:
    """Return a bounded HTTP method and sanitized path for an upstream SDK error."""
    for cause in _chain(error):
        if not isinstance(cause, SDKError):
            continue
        try:
            response = getattr(cause, "raw_response", None)
            request = getattr(response, "request", None)
            if request is None:
                continue

            method = request.method.upper()
            if method not in _UPSTREAM_METHODS:
                return None

            segments = [
                segment for segment in urlsplit(str(request.url)).path.split("/") if segment
            ]
            version_index = next(
                (
                    index
                    for index in range(len(segments) - 1, -1, -1)
                    if re.fullmatch(r"v[0-9]{1,3}", segments[index])
                ),
                None,
            )
            route_segments: list[str] = []
            for index, segment in enumerate(segments):
                if version_index is None or index < version_index:
                    route_segments.append("{id}")
                elif index == version_index or segment in _CLOUD_API_ROUTE_SEGMENTS:
                    route_segments.append(segment)
                else:
                    route_segments.append("{id}")
            route = "/" + "/".join(route_segments)
            if len(route) > _MAX_UPSTREAM_ROUTE_LENGTH:
                return None
        except Exception:
            return None
        else:
            return method, route
    return None


def _cloud_problem_text(problem: CloudErrorInfo) -> str:
    guidance = cloud_error_guidance(problem, for_mcp=True)
    return f"{problem.message} {guidance}".rstrip() if guidance else problem.message


def format_user_facing_error(error: BaseException) -> str:
    """Return the error message followed by its guidance, when present."""
    if (problem := _cloud_error_info(error)) is not None:
        return _cloud_problem_text(problem)
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
    if (problem := _cloud_error_info(error)) is not None:
        return _cloud_problem_text(problem)
    cause = error.__cause__
    if isinstance(cause, AirbyteLibError):
        return format_user_facing_error(cause)
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


_INPUT_VALUE = re.compile(r"input_value=(?:'[^']*'|\"[^\"]*\"|[^\s,\])]+)")
_BEARER_TOKEN = re.compile(r"Bearer\s+[^\s'\")]+")
_URL_CREDENTIALS = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://)[^\s/?#'\")]+@")
_URL_QUERY = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://[^\s?#'\")]*)\?[^\s'\")]*")


def redact_agent_text(text: str) -> str:
    """Redact input_value echoes, bearer tokens, URL credentials, and URL query strings."""
    text = _INPUT_VALUE.sub("input_value=<redacted>", text)
    text = _BEARER_TOKEN.sub("Bearer <redacted>", text)
    text = _URL_CREDENTIALS.sub(r"\1<redacted>@", text)
    return _URL_QUERY.sub(r"\1", text)


class AgentErrorTextMiddleware(Middleware):
    """Rewrite tool error text for the agent and redact credentials.

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
            raise ValidationError(redact_agent_text(format_validation_error(error))) from None
        except ToolError as error:
            raise ToolError(redact_agent_text(agent_error_text(error))) from None
