# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""HTTP transport configuration helpers for hosted MCP deployment."""

from __future__ import annotations

import os

from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware


MCP_CORS_ORIGINS_ENV = "MCP_CORS_ORIGINS"

# Headers used by remote MCP clients over streamable HTTP.
_MCP_CLIENT_HEADERS = [
    "mcp-protocol-version",
    "mcp-session-id",
    "Authorization",
    "Content-Type",
    "X-Airbyte-Workspace-Id",
    "X-Airbyte-Cloud-Client-Id",
    "X-Airbyte-Cloud-Client-Secret",
    "X-Airbyte-Cloud-Api-Url",
    "X-Airbyte-Cloud-Config-Api-Url",
]


def parse_cors_origins(raw: str | None) -> list[str]:
    """Parse a comma-separated list of CORS origins."""
    if not raw:
        return []
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


def build_http_middleware(
    *,
    cors_origins: str | None = None,
) -> list[Middleware]:
    """Build ASGI middleware for the hosted MCP HTTP transport.

    When ``MCP_CORS_ORIGINS`` is set (comma-separated), enables CORS for
    remote MCP clients that connect from a browser context (for example,
    browser-based OAuth flows).
    """
    origins = parse_cors_origins(cors_origins or os.getenv(MCP_CORS_ORIGINS_ENV, ""))
    if not origins:
        return []

    return [
        Middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
            allow_headers=_MCP_CLIENT_HEADERS,
            expose_headers=["mcp-protocol-version", "mcp-session-id"],
        ),
    ]
