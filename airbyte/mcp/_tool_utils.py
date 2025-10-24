# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Safe mode filtering for MCP tools.

This module provides a decorator to tag tool functions with MCP annotations
for deferred registration with safe mode filtering.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any, Literal, TypeVar

from airbyte.mcp._annotations import (
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
        annotations: Tool annotations dict containing domain, readOnlyHint, and destructiveHint

    Returns:
        True if the tool should be registered, False if it should be filtered out
    """
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
    read_only: bool | None = None,
    destructive: bool | None = None,
    idempotent: bool | None = None,
    open_world: bool | None = None,
) -> Callable[[F], F]:
    """Decorator to tag an MCP tool function with annotations for deferred registration.

    This decorator stores the annotations on the function for later use during
    deferred registration. It does not register the tool immediately.

    Args:
        domain: The domain this tool belongs to (e.g., "cloud", "local", "registry")
        read_only: If True, tool only reads without making changes (default: False)
        destructive: If True, tool modifies/deletes existing data (default: True)
        idempotent: If True, repeated calls have same effect (default: False)
        open_world: If True, tool interacts with external systems (default: True)

    Returns:
        Decorator function that tags the tool with annotations

    Example:
        @mcp_tool("cloud", read_only=True, idempotent=True)
        def list_sources():
            ...
    """
    annotations: dict[str, Any] = {"domain": domain}
    if read_only is not None:
        annotations[READ_ONLY_HINT] = read_only
    if destructive is not None:
        annotations[DESTRUCTIVE_HINT] = destructive
    if idempotent is not None:
        annotations[IDEMPOTENT_HINT] = idempotent
    if open_world is not None:
        annotations[OPEN_WORLD_HINT] = open_world

    def decorator(func: F) -> F:
        func._mcp_annotations = annotations  # type: ignore[attr-defined]  # noqa: SLF001
        func._mcp_domain = domain  # type: ignore[attr-defined]  # noqa: SLF001
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
            app.tool(func, annotations=tool_annotations)
