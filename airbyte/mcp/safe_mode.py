# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Safe mode filtering for MCP tools.

This module provides a decorator to tag tool functions with MCP annotations
for deferred registration with safe mode filtering.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any, TypeVar

from airbyte.mcp._annotations import DESTRUCTIVE_HINT, READ_ONLY_HINT


F = TypeVar("F", bound=Callable[..., Any])

_CLOUD_TOOLS: list[tuple[Callable[..., Any], dict[str, Any]]] = []


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


def should_register_cloud_tool(annotations: dict[str, Any]) -> bool:
    """Check if a Cloud ops tool should be registered based on safe mode settings.

    Args:
        annotations: Tool annotations dict containing readOnlyHint and destructiveHint

    Returns:
        True if the tool should be registered, False if it should be filtered out
    """
    readonly_mode = is_readonly_mode_enabled()
    safe_mode = is_safe_mode_enabled()

    if not readonly_mode and not safe_mode:
        return True

    if readonly_mode:
        is_readonly = annotations.get(READ_ONLY_HINT, False)
        if not is_readonly:
            return False

    if safe_mode:
        is_destructive = annotations.get(DESTRUCTIVE_HINT, True)  # Default is True per FastMCP
        if is_destructive:
            return False

    return True


def get_registered_cloud_tools() -> list[tuple[Callable[..., Any], dict[str, Any]]]:
    """Get all registered cloud tools with their annotations.

    Returns:
        List of tuples containing (function, annotations) for each registered cloud tool
    """
    return _CLOUD_TOOLS.copy()


def cloud_tool(annotations: dict[str, Any]) -> Callable[[F], F]:
    """Decorator to tag a Cloud ops tool function with MCP annotations.

    This decorator stores the annotations on the function for later use during
    deferred registration. It does not register the tool immediately.

    Args:
        annotations: Tool annotations dict containing readOnlyHint, destructiveHint, etc.

    Returns:
        Decorator function that tags the tool with annotations

    Example:
        @cloud_tool({READ_ONLY_HINT: True, IDEMPOTENT_HINT: True})
        def list_sources():
            ...
    """

    def decorator(func: F) -> F:
        func._mcp_annotations = annotations  # type: ignore[attr-defined]  # noqa: SLF001
        func._is_cloud_tool = True  # type: ignore[attr-defined]  # noqa: SLF001
        _CLOUD_TOOLS.append((func, annotations))
        return func

    return decorator
