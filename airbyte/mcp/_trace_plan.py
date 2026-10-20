# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Write the MCP argument trace plan and its classification sources."""

import asyncio
import json
from pathlib import Path

from fastmcp_extensions.otel.middleware import trace_plan

from airbyte.mcp import _otel as observability
from airbyte.mcp.server import app


def main() -> None:
    """Write the trusted-execution MCP trace plan to the repository root."""
    observability.install(app, environ={"AIRBYTE_MCP_TRACING_BACKEND": "otel"})
    plan = asyncio.run(trace_plan(app, include_sources=True))
    target = Path(__file__).resolve().parents[2] / "arg_trace_plan.json"
    target.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
