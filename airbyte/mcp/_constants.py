# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Private constants used by the Airbyte MCP server."""

from __future__ import annotations


MCP_READONLY_MODE_ENV_VAR: str = "AIRBYTE_CLOUD_MCP_READONLY_MODE"
"""Environment variable to enable read-only mode for the MCP server.

When set to "1" or "true", only tools with readOnlyHint=True will be available.
"""

MCP_DOMAINS_DISABLED_ENV_VAR: str = "AIRBYTE_MCP_DOMAINS_DISABLED"
"""Environment variable to disable specific MCP tool domains.

Accepts a comma-separated list of domain names (e.g., "local,registry").
Tools from these domains will not be advertised by the MCP server.
"""

MCP_DOMAINS_ENV_VAR: str = "AIRBYTE_MCP_DOMAINS"
"""Environment variable to enable specific MCP tool domains.

Accepts a comma-separated list of domain names (e.g., "cloud,registry").
If set, only tools from these domains will be advertised by the MCP server.
"""

MCP_TRUSTED_EXECUTION_ENV_VAR: str = "AIRBYTE_MCP_TRUSTED_EXECUTION"
"""Environment variable that enables trusted (local) execution for the MCP server.

Values `1`, `true`, `t`, `yes`, `y`, and `on` (case-insensitive) enable the server's
trusted-machine capabilities: local filesystem access, local connector installation/execution,
and server-side secret resolution. Values `0`, `false`, `f`, `no`, `n`, and `off`
(case-insensitive), as well as unset or unrecognized values, leave it disabled. It is
permanently unavailable over the HTTP transport (a hosted deployment can never enable it).
This gate is server-owned and is deliberately never read from a request header, because it
*widens* the surface and so must never be caller-controllable.
"""

MCP_WORKSPACE_ID_HEADER: str = "X-Airbyte-Workspace-Id"
"""HTTP header key for passing workspace ID to the MCP server.

This allows per-request workspace ID configuration when using HTTP transport.
"""

MCP_ORGANIZATION_ID_HEADER: str = "X-Airbyte-Organization-Id"
"""HTTP header key for passing organization ID to the MCP server.

This allows per-request organization ID configuration when using HTTP transport, for the
tools that scope a listing to an organization rather than a workspace.
"""

MCP_INSIDERS_MODULES: frozenset[str] = frozenset()
"""MCP tool modules that are hidden unless insiders mode is enabled.

Enable them with `AIRBYTE_MCP_INSIDERS` / `X-MCP-Insiders`, or by naming the module in
the include list.
"""

MCP_INSIDERS_ENV_VAR: str = "AIRBYTE_MCP_INSIDERS"
"""Environment variable that advertises insiders MCP tools. Off by default.

Set to `1`/`true`/`yes` to advertise the tools in `MCP_INSIDERS_MODULES` to every
caller, or to `0`/`false`/`no` to hide them from every caller. Either value overrides
`MCP_INSIDERS_HEADER`; any other value, including an empty string, leaves the decision
to that header.
"""

MCP_INSIDERS_HEADER: str = "X-MCP-Insiders"
"""HTTP header key that advertises insiders MCP tools, per request.

Set to `1`/`true`/`yes` to add the tools in `MCP_INSIDERS_MODULES` to the advertised
tool surface. This selects which tools are advertised and is not an access-control
boundary: every insiders tool authorizes each call against the Airbyte API.
`MCP_INSIDERS_ENV_VAR` overrides this header when explicitly set.
"""

# MCP Config Arg Names (used with get_mcp_config)

MCP_CONFIG_READONLY_MODE: str = "airbyte_readonly_mode"
"""Config arg name for the legacy AIRBYTE_CLOUD_MCP_READONLY_MODE setting."""

MCP_CONFIG_EXCLUDE_MODULES: str = "airbyte_exclude_modules"
"""Config arg name for the legacy AIRBYTE_MCP_DOMAINS_DISABLED setting."""

MCP_CONFIG_INCLUDE_MODULES: str = "airbyte_include_modules"
"""Config arg name for the legacy AIRBYTE_MCP_DOMAINS setting."""

MCP_CONFIG_WORKSPACE_ID: str = "workspace_id"
"""Config arg name for the workspace ID setting."""

MCP_CONFIG_ORGANIZATION_ID: str = "organization_id"
"""Config arg name for the organization ID setting."""

MCP_CONFIG_INSIDERS: str = "insiders"
"""Config arg name for the insiders tools gate."""

MCP_CONFIG_BEARER_TOKEN: str = "bearer_token"
"""Config arg name for the bearer token setting."""

MCP_CONFIG_CLIENT_ID: str = "client_id"
"""Config arg name for the client ID setting."""

MCP_CONFIG_CLIENT_SECRET: str = "client_secret"
"""Config arg name for the client secret setting."""

MCP_CONFIG_API_URL: str = "api_url"
"""Config arg name for the API URL setting."""

MCP_CONFIG_CONFIG_API_URL: str = "config_api_url"
"""Config arg name for the Config API URL setting."""

# MCP HTTP Header Keys for credentials

MCP_BEARER_TOKEN_HEADER: str = "Authorization"
"""HTTP header key for bearer token (standard Authorization header)."""

MCP_EXTENSIONS_HEADER: str = "X-MCP-Extensions"
"""HTTP header key for client-declared MCP extension IDs."""

# Security Note: The API root and Config API root are intentionally NOT exposed as HTTP
# headers. Each hosted MCP deployment is paired to a single backend, so allowing
# a caller to override these URLs per-request would let them redirect the
# server's credentialed requests to an arbitrary host and exfiltrate secrets.
# These base URLs remain configurable via env var for local (stdio) use only.
