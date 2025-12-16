# HTTP Header Bearer Token Authentication for MCP

## Overview
Enable PyAirbyte MCP tools to authenticate via `Authorization` header when running over HTTP transport, allowing a single shared MCP server to handle requests from multiple users with different bearer tokens.

## Requirements

1. Add HTTP header extraction utility in `airbyte/mcp/_util.py`
   - Use `fastmcp.server.dependencies.get_http_headers` to extract headers
   - Return bearer token from `Authorization: Bearer <token>` header
   - Return None if not running over HTTP transport or no header present

2. Update `resolve_cloud_bearer_token` in `airbyte/cloud/auth.py`
   - Check HTTP Authorization header first (for MCP HTTP transport)
   - Fall back to environment variable if no header present
   - Resolution order: explicit input_value > HTTP header > env var

## Expected Behavior After Changes

| Transport | Token Source |
|-----------|-------------|
| stdio (subprocess) | `AIRBYTE_CLOUD_BEARER_TOKEN` env var |
| HTTP | `Authorization: Bearer <token>` header, fallback to env var |
