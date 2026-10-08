# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""PyAirbyte-specific configuration for user-facing MCP errors."""

from __future__ import annotations

from airbyte.exceptions import (
    AirbyteAgentsUnavailableError,
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
