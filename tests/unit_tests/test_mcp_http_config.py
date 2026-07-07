# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Unit tests for hosted MCP HTTP configuration."""

from __future__ import annotations

from starlette.middleware.cors import CORSMiddleware

from airbyte.mcp._http_config import build_http_middleware, parse_cors_origins


def test_parse_cors_origins_empty() -> None:
    """Empty or whitespace-only input returns no origins."""
    assert parse_cors_origins("") == []
    assert parse_cors_origins(None) == []
    assert parse_cors_origins("  ,  , ") == []


def test_parse_cors_origins_trims_and_splits() -> None:
    """Origins are split on commas and trimmed."""
    assert parse_cors_origins("https://app.example.com, https://admin.example.com") == [
        "https://app.example.com",
        "https://admin.example.com",
    ]


def test_build_http_middleware_without_origins() -> None:
    """No CORS middleware is added when origins are unset."""
    assert build_http_middleware(cors_origins="") == []


def test_build_http_middleware_with_origins() -> None:
    """CORS middleware is configured when origins are provided."""
    middleware = build_http_middleware(cors_origins="https://app.example.com")
    assert len(middleware) == 1
    assert middleware[0].cls is CORSMiddleware
    assert middleware[0].kwargs["allow_origins"] == ["https://app.example.com"]
    assert "mcp-protocol-version" in middleware[0].kwargs["allow_headers"]
