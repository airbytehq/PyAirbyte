# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Safe mode filtering for MCP tools.

This module provides filtering logic to restrict destructive and write operations
on Cloud resources based on environment variables.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

from airbyte.mcp._annotations import DESTRUCTIVE_HINT, READ_ONLY_HINT


F = TypeVar("F", bound=Callable[..., Any])


class SafeModeError(Exception):
    """Raised when a tool is blocked by safe mode restrictions."""

    pass


def is_readonly_mode_enabled() -> bool:
    """Check if read-only mode is enabled via environment variable.

    Returns:
        True if AIRBYTE_CLOUD_MCP_READONLY_MODE is set to "1", False otherwise.
    """
    return os.environ.get("AIRBYTE_CLOUD_MCP_READONLY_MODE", "").strip() == "1"


def is_safe_mode_enabled() -> bool:
    """Check if safe mode is enabled via environment variable.

    Returns:
        True if AIRBYTE_CLOUD_MCP_SAFE_MODE is set to "1", False otherwise.
    """
    return os.environ.get("AIRBYTE_CLOUD_MCP_SAFE_MODE", "").strip() == "1"


def check_cloud_tool_allowed(annotations: dict[str, Any], tool_name: str) -> None:
    """Check if a Cloud ops tool is allowed to execute based on safe mode settings.

    Args:
        annotations: Tool annotations dict containing readOnlyHint and destructiveHint
        tool_name: Name of the tool being checked

    Raises:
        SafeModeError: If the tool is blocked by safe mode restrictions
    """
    readonly_mode = is_readonly_mode_enabled()
    safe_mode = is_safe_mode_enabled()

    if not readonly_mode and not safe_mode:
        return

    if readonly_mode:
        is_readonly = annotations.get(READ_ONLY_HINT, False)
        if not is_readonly:
            raise SafeModeError(
                f"Tool '{tool_name}' is blocked by AIRBYTE_CLOUD_MCP_READONLY_MODE. "
                f"This tool performs write operations on Cloud resources. "
                f"To allow write operations, set AIRBYTE_CLOUD_MCP_READONLY_MODE=0 or unset it."
            )

    if safe_mode:
        is_destructive = annotations.get(DESTRUCTIVE_HINT, True)  # Default is True per FastMCP
        if is_destructive:
            raise SafeModeError(
                f"Tool '{tool_name}' is blocked by AIRBYTE_CLOUD_MCP_SAFE_MODE. "
                f"This tool performs destructive operations (updates/deletes) on Cloud resources. "
                f"To allow destructive operations, set AIRBYTE_CLOUD_MCP_SAFE_MODE=0 or unset it."
            )


def enforce_cloud_safe_mode(annotations: dict[str, Any]) -> Callable[[F], F]:
    """Decorator to enforce safe mode restrictions on Cloud ops tools.

    Args:
        annotations: Tool annotations dict containing readOnlyHint and destructiveHint

    Returns:
        Decorator function that wraps the tool and checks safe mode before execution
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            check_cloud_tool_allowed(annotations, func.__name__)
            return func(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator
