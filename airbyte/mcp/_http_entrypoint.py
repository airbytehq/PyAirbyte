# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Console-script entrypoint for `airbyte-mcp-http`.

Importing `airbyte.mcp.http_main` imports `airbyte.mcp.server`, which builds
the FastMCP app and its auth provider at import time, and those log. Logging is
configured here first so that import-time output already honors
`AIRBYTE_MCP_LOG_FORMAT`.
"""

from __future__ import annotations

from airbyte.mcp._logging import configure_logging, resolve_log_format


def main() -> None:
    """Configure logging, then start the Airbyte MCP server with HTTP transport."""
    configure_logging(resolve_log_format())

    from airbyte.mcp import http_main  # noqa: PLC0415

    http_main.main()
