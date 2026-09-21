# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""PyAirbyte-specific configuration for user-facing MCP errors."""

from __future__ import annotations

from airbyte.exceptions import (
    AirbyteAgentsUnavailableError,
    AirbyteMCPError,
    PipelineChangesDisabledError,
    PyAirbyteError,
    PyAirbyteInputError,
)
from airbyte.mcp._tool_utils import SafeModeError


MCP_TOOL_USER_FACING_ERRORS: tuple[type[Exception], ...] = (
    PyAirbyteInputError,
    AirbyteMCPError,
    AirbyteAgentsUnavailableError,
    PipelineChangesDisabledError,
    SafeModeError,
)
"""Expected errors returned to MCP clients as concise message and guidance text."""


def format_user_facing_error(error: BaseException) -> str:
    """Return the error message followed by its guidance, when present."""
    if not isinstance(error, PyAirbyteError):
        return str(error)
    text = error.get_message()
    if error.guidance:
        text = f"{text} {error.guidance}"
    return text
