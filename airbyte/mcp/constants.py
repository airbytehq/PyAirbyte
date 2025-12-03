# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Constants for MCP (Model Context Protocol) server configuration.

This module provides environment variable-based configuration for the MCP server,
including domain filtering options that control which tool domains are advertised.
"""

from __future__ import annotations

import os
import warnings
from enum import Enum


class MCPToolDomain(str, Enum):
    """MCP tool domains available in the server.

    Each domain represents a category of tools that can be enabled or disabled
    via the `AIRBYTE_MCP_DOMAINS` and `AIRBYTE_MCP_DOMAINS_DISABLED` environment variables.
    """

    CLOUD = "cloud"
    """Tools for managing Airbyte Cloud resources (sources, destinations, connections)."""

    LOCAL = "local"
    """Tools for local operations (connector validation, caching, SQL queries)."""

    REGISTRY = "registry"
    """Tools for querying the Airbyte connector registry."""


MCP_TOOL_DOMAINS: set[str] = {d.value for d in MCPToolDomain}
"""Set of valid MCP tool domain values, derived from the MCPToolDomain enum."""

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

# Validate domain values and warn about unknown domains
_unknown_enabled = AIRBYTE_MCP_DOMAINS - MCP_TOOL_DOMAINS
_unknown_disabled = AIRBYTE_MCP_DOMAINS_DISABLED - MCP_TOOL_DOMAINS

if _unknown_enabled or _unknown_disabled:
    _parts: list[str] = []
    if _unknown_enabled:
        _parts.append(f"AIRBYTE_MCP_DOMAINS contains unknown domain(s): {sorted(_unknown_enabled)}")
    if _unknown_disabled:
        _parts.append(
            f"AIRBYTE_MCP_DOMAINS_DISABLED contains unknown domain(s): {sorted(_unknown_disabled)}"
        )
    _known_domains = ", ".join(sorted(MCP_TOOL_DOMAINS))
    _warning_message = "; ".join(_parts) + f". Known MCP domains are: [{_known_domains}]."
    warnings.warn(_warning_message, stacklevel=1)
