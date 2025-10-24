# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MCP tool utility functions.

This module provides a decorator to tag tool functions with MCP annotations
for deferred registration with safe mode filtering.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any, Literal, TypeVar

from airbyte.mcp._annotations import (
    AIRBYTE_INTERNAL,
    DESTRUCTIVE_HINT,
    IDEMPOTENT_HINT,
    OPEN_WORLD_HINT,
    READ_ONLY_HINT,
)


F = TypeVar("F", bound=Callable[..., Any])

AIRBYTE_CLOUD_MCP_READONLY_MODE = (
    os.environ.get("AIRBYTE_CLOUD_MCP_READONLY_MODE", "").strip() == "1"
)
AIRBYTE_CLOUD_MCP_SAFE_MODE = os.environ.get("AIRBYTE_CLOUD_MCP_SAFE_MODE", "").strip() == "1"

_REGISTERED_TOOLS: list[tuple[Callable[..., Any], dict[str, Any]]] = []


class SafeModeError(Exception):
    """Raised when a tool is blocked by safe mode restrictions."""

    pass


def should_register_tool(annotations: dict[str, Any]) -> bool:
    """Check if a tool should be registered based on safe mode settings.

    Args:
        annotations: Tool annotations dict containing domain, readOnlyHint, destructiveHint,
            and airbyte_internal

    Returns:
        True if the tool should be registered, False if it should be filtered out
    """
    if annotations.get(AIRBYTE_INTERNAL):
        admin_flag = os.environ.get("AIRBYTE_INTERNAL_ADMIN_FLAG")
        admin_user = os.environ.get("AIRBYTE_INTERNAL_ADMIN_USER")
        if admin_flag != "airbyte.io" or not admin_user or not admin_user.endswith("@airbyte.io"):
            return False

    if annotations.get("domain") != "cloud":
        return True

    if not AIRBYTE_CLOUD_MCP_READONLY_MODE and not AIRBYTE_CLOUD_MCP_SAFE_MODE:
        return True

    if AIRBYTE_CLOUD_MCP_READONLY_MODE:
        is_readonly = annotations.get(READ_ONLY_HINT, False)
        if not is_readonly:
            return False

    if AIRBYTE_CLOUD_MCP_SAFE_MODE:
        is_destructive = annotations.get(DESTRUCTIVE_HINT, True)  # Default is True per FastMCP
        if is_destructive:
            return False

    return True


def get_registered_tools(
    domain: Literal["cloud", "local", "registry"] | None = None,
) -> list[tuple[Callable[..., Any], dict[str, Any]]]:
    """Get all registered tools, optionally filtered by domain.

    Args:
        domain: The domain to filter by (e.g., "cloud", "local", "registry").
            If None, returns all tools.

    Returns:
        List of tuples containing (function, annotations) for each registered tool
    """
    if domain is None:
        return _REGISTERED_TOOLS.copy()
    return [(func, ann) for func, ann in _REGISTERED_TOOLS if ann.get("domain") == domain]


def mcp_tool(
    domain: Literal["cloud", "local", "registry"],
    *,
    read_only: bool = False,
    destructive: bool = False,
    idempotent: bool = False,
    open_world: bool = False,
    airbyte_internal: bool = False,
    extra_help_text: str | None = None,
) -> Callable[[F], F]:
    """Decorator to tag an MCP tool function with annotations for deferred registration.

    This decorator stores the annotations on the function for later use during
    deferred registration. It does not register the tool immediately.

    Args:
        domain: The domain this tool belongs to (e.g., "cloud", "local", "registry")
        read_only: If True, tool only reads without making changes (default: False)
        destructive: If True, tool modifies/deletes existing data (default: False)
        idempotent: If True, repeated calls have same effect (default: False)
        open_world: If True, tool interacts with external systems (default: False)
        airbyte_internal: If True, tool is only for internal Airbyte admin use.
        extra_help_text: Optional text to append to the function's docstring
            with a newline delimiter

    Returns:
        Decorator function that tags the tool with annotations

    Example:
        @mcp_tool("cloud", read_only=True, idempotent=True)
        def list_sources():
            ...
    """
    annotations: dict[str, Any] = {
        "domain": domain,
        READ_ONLY_HINT: read_only,
        DESTRUCTIVE_HINT: destructive,
        IDEMPOTENT_HINT: idempotent,
        OPEN_WORLD_HINT: open_world,
        AIRBYTE_INTERNAL: airbyte_internal,
    }

    def decorator(func: F) -> F:
        func._mcp_annotations = annotations  # type: ignore[attr-defined]  # noqa: SLF001
        func._mcp_domain = domain  # type: ignore[attr-defined]  # noqa: SLF001
        func._mcp_extra_help_text = extra_help_text  # type: ignore[attr-defined]  # noqa: SLF001
        _REGISTERED_TOOLS.append((func, annotations))
        return func

    return decorator


def register_tools(app: Any, domain: Literal["cloud", "local", "registry"]) -> None:  # noqa: ANN401
    """Register tools with the FastMCP app, filtered by domain and safe mode settings.

    Args:
        app: The FastMCP app instance
        domain: The domain to register tools for (e.g., "cloud", "local", "registry")
    """
    for func, tool_annotations in get_registered_tools(domain):
        if should_register_tool(tool_annotations):
            extra_help_text = getattr(func, "_mcp_extra_help_text", None)
            if extra_help_text:
                description = (func.__doc__ or "").rstrip() + "\n" + extra_help_text
                app.tool(func, annotations=tool_annotations, description=description)
            else:
                app.tool(func, annotations=tool_annotations)
