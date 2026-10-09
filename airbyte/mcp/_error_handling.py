# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""PyAirbyte-specific handling and trace classification of MCP tool errors."""

from __future__ import annotations

import json
import re

from fastmcp_extensions.otel._extras import _chain  # noqa: PLC2701

from airbyte._util.api_util import error_response_body
from airbyte.exceptions import (
    AirbyteAgentsUnavailableError,
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


def format_user_facing_error(error: BaseException) -> str:
    """Return the error message followed by its guidance, when present."""
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


_PROBLEM_SLUG = re.compile(r"[a-z0-9][a-z0-9:._-]{0,99}")
# The last path segment of the generic problem `type` URL; `title` is the slug then.
_GENERIC_PROBLEM_SLUG = "errors"
_MAX_PROBLEM_BODY = 65536


def mcp_tool_error_reason(error: BaseException) -> str | None:
    """Return the problem slug from the first Airbyte API error body in the chain.

    Only the problem `type`, or its `title` when `type` is the generic errors
    page, is read. `detail` and `data` quote IDs and payloads, so they never are.
    """
    body = next(
        (body for body in map(error_response_body, _chain(error)) if body is not None),
        None,
    )
    if body is None or len(body) > _MAX_PROBLEM_BODY:
        return None
    try:
        problem = json.loads(body)
    except (ValueError, RecursionError):
        return None
    if not isinstance(problem, dict):
        return None
    for field in ("type", "title"):
        value = problem.get(field)
        if not isinstance(value, str):
            continue
        slug = re.split(r"[/#]", value)[-1]
        if slug != _GENERIC_PROBLEM_SLUG and _PROBLEM_SLUG.fullmatch(slug):
            return slug
    return None
