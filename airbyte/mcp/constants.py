# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Constants for MCP (Model Context Protocol) server configuration.

This module provides environment variable-based configuration for the MCP server,
including domain filtering options that control which tool domains are advertised.
"""

from __future__ import annotations

import os


# Domain filtering environment variables
_AIRBYTE_MCP_DOMAINS_RAW = os.environ.get("AIRBYTE_MCP_DOMAINS", "").strip()
_AIRBYTE_MCP_DOMAINS_DISABLED_RAW = os.environ.get("AIRBYTE_MCP_DOMAINS_DISABLED", "").strip()

AIRBYTE_MCP_DOMAINS: set[str] = (
    {d.strip().lower() for d in _AIRBYTE_MCP_DOMAINS_RAW.split(",") if d.strip()}
    if _AIRBYTE_MCP_DOMAINS_RAW
    else set()
)
"""Set of enabled MCP tool domains parsed from the `AIRBYTE_MCP_DOMAINS` environment variable.

This accepts a comma-separated list of domain names (e.g., "registry,cloud").
If set, only tools from these domains will be advertised by the MCP server.
If not set (empty), all domains are enabled by default.

Values are case-insensitive and whitespace is trimmed.

Example:
    AIRBYTE_MCP_DOMAINS=registry,cloud  # Only registry and cloud tools enabled
"""

AIRBYTE_MCP_DOMAINS_DISABLED: set[str] = (
    {d.strip().lower() for d in _AIRBYTE_MCP_DOMAINS_DISABLED_RAW.split(",") if d.strip()}
    if _AIRBYTE_MCP_DOMAINS_DISABLED_RAW
    else set()
)
"""Set of disabled MCP tool domains from `AIRBYTE_MCP_DOMAINS_DISABLED` env var.

This accepts a comma-separated list of domain names (e.g., "registry").
Tools from these domains will not be advertised by the MCP server.

When both `AIRBYTE_MCP_DOMAINS` and `AIRBYTE_MCP_DOMAINS_DISABLED` are set,
the disabled list takes precedence (subtracts from the enabled list).

Values are case-insensitive and whitespace is trimmed.

Example:
    AIRBYTE_MCP_DOMAINS_DISABLED=registry  # All domains except registry enabled
"""
