# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MCP tool annotation constants.

These constants define the standard MCP annotations for tools, following the
FastMCP 2.2.7+ specification.

For more information, see:
https://gofastmcp.com/concepts/tools#mcp-annotations
"""

from __future__ import annotations


READ_ONLY_HINT = "readOnlyHint"
"""Indicates if the tool only reads data without making any changes.

When True, the tool performs read-only operations and does not modify any state.
When False, the tool may write, create, update, or delete data.

FastMCP default if not specified: False
"""

DESTRUCTIVE_HINT = "destructiveHint"
"""Signals if the tool's changes are destructive (updates or deletes existing data).

This hint is only relevant for non-read-only tools (readOnlyHint=False).
When True, the tool modifies or deletes existing data in a way that may be
difficult or impossible to reverse.
When False, the tool creates new data or performs non-destructive operations.

FastMCP default if not specified: True
"""

IDEMPOTENT_HINT = "idempotentHint"
"""Indicates if repeated calls with the same parameters have the same effect.

When True, calling the tool multiple times with identical parameters produces
the same result and side effects as calling it once.
When False, each call may produce different results or side effects.

FastMCP default if not specified: False
"""

OPEN_WORLD_HINT = "openWorldHint"
"""Specifies if the tool interacts with external systems.

When True, the tool communicates with external services, APIs, or systems
outside the local environment (e.g., cloud APIs, remote databases, internet).
When False, the tool only operates on local state or resources.

FastMCP default if not specified: True
"""
