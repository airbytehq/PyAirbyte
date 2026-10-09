# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""PyAirbyte-specific handling and trace classification of MCP tool errors."""

from __future__ import annotations

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
