# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Request context management for MCP server HTTP/SSE modes.

This module provides ContextVars for storing per-request authentication values
extracted from HTTP headers. These values are scoped to a single request and
do not pollute the global environment.
"""

from __future__ import annotations

from contextvars import ContextVar

from airbyte.cloud.auth import (
    resolve_cloud_api_url,
    resolve_cloud_client_id,
    resolve_cloud_client_secret,
    resolve_cloud_workspace_id,
)
from airbyte.secrets import SecretString


CLOUD_CLIENT_ID_CVAR: ContextVar[str | SecretString | None] = ContextVar(
    "cloud_client_id", default=None
)
CLOUD_CLIENT_SECRET_CVAR: ContextVar[str | SecretString | None] = ContextVar(
    "cloud_client_secret", default=None
)
CLOUD_WORKSPACE_ID_CVAR: ContextVar[str | None] = ContextVar("cloud_workspace_id", default=None)
CLOUD_API_URL_CVAR: ContextVar[str | None] = ContextVar("cloud_api_url", default=None)


def get_effective_cloud_client_id() -> SecretString:
    """Get the effective cloud client ID from request context or environment.

    Returns:
        Client ID from HTTP headers (if set), otherwise from environment variables
    """
    header_value = CLOUD_CLIENT_ID_CVAR.get()
    if header_value is not None:
        return SecretString(header_value)
    return resolve_cloud_client_id()


def get_effective_cloud_client_secret() -> SecretString:
    """Get the effective cloud client secret from request context or environment.

    Returns:
        Client secret from HTTP headers (if set), otherwise from environment variables
    """
    header_value = CLOUD_CLIENT_SECRET_CVAR.get()
    if header_value is not None:
        return SecretString(header_value)
    return resolve_cloud_client_secret()


def get_effective_cloud_workspace_id() -> str:
    """Get the effective cloud workspace ID from request context or environment.

    Returns:
        Workspace ID from HTTP headers (if set), otherwise from environment variables
    """
    header_value = CLOUD_WORKSPACE_ID_CVAR.get()
    if header_value is not None:
        return str(header_value)
    return resolve_cloud_workspace_id()


def get_effective_cloud_api_url() -> str:
    """Get the effective cloud API URL from request context or environment.

    Returns:
        API URL from HTTP headers (if set), otherwise from environment variables
    """
    header_value = CLOUD_API_URL_CVAR.get()
    if header_value is not None:
        return str(header_value)
    return resolve_cloud_api_url()
