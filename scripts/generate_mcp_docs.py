#!/usr/bin/env python3
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Generate static HTML docs for the PyAirbyte MCP server using `mcpdocs-gen`.

This script:

1. Starts the PyAirbyte MCP server in SSE mode as a background subprocess.
2. Waits for the SSE endpoint to become reachable.
3. Shells out to `mcpdocs generate` to emit a static HTML site.
4. Tears down the server subprocess cleanly, even on failure or `Ctrl+C`.

Usage:

```
uv run python scripts/generate_mcp_docs.py [--port 8765] [--output docs/mcp-generated]
```

Or via the project's poe task:

```
poe mcp-docs-generate
```
"""

from __future__ import annotations

import argparse
import contextlib
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path


DEFAULT_PORT = 8765
DEFAULT_OUTPUT = Path("docs/mcp-generated")
STARTUP_TIMEOUT_SECONDS = 60.0


def _wait_for_port(host: str, port: int, timeout: float) -> None:
    """Block until `host:port` accepts TCP connections or `timeout` elapses."""
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError as ex:
            last_error = ex
            time.sleep(0.5)
    raise TimeoutError(
        f"MCP SSE server did not become reachable at {host}:{port} "
        f"within {timeout:.0f}s (last error: {last_error!r})."
    )


def _start_mcp_server(host: str, port: int) -> subprocess.Popen[bytes]:
    """Start the PyAirbyte MCP server in SSE mode as a background subprocess."""
    cmd = [
        sys.executable,
        "-c",
        (
            "from airbyte.mcp.server import app; "
            f"app.run(transport='sse', host={host!r}, port={port})"
        ),
    ]
    # Start in its own process group so we can signal the whole tree on shutdown.
    return subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


def _stop_mcp_server(proc: subprocess.Popen[bytes]) -> None:
    """Terminate the MCP server subprocess tree cleanly."""
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=10.0)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=5.0)


def _run_mcpdocs(url: str, output: Path) -> None:
    """Invoke the `mcpdocs generate` CLI to emit a static HTML site."""
    mcpdocs_bin = shutil.which("mcpdocs")
    if mcpdocs_bin is None:
        raise RuntimeError(
            "`mcpdocs` CLI not found on PATH. Install it with "
            "`uv pip install mcpdocs-gen` (or `pip install mcpdocs-gen`)."
        )
    output.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [mcpdocs_bin, "generate", "--url", url, "--output", str(output)],
        check=True,
    )


def generate(host: str, port: int, output: Path) -> None:
    """Start the MCP server, generate docs with mcpdocs-gen, then shut down."""
    print(f"Starting PyAirbyte MCP server on http://{host}:{port} (SSE)...")
    proc = _start_mcp_server(host=host, port=port)
    try:
        _wait_for_port(host=host, port=port, timeout=STARTUP_TIMEOUT_SECONDS)
        print(f"Generating MCP docs into {output}/ ...")
        _run_mcpdocs(url=f"http://{host}:{port}/sse", output=output)
        print(f"MCP docs written to {output}/index.html")
    finally:
        print("Stopping MCP server...")
        _stop_mcp_server(proc)


def main() -> int:
    """CLI entrypoint for the MCP docs generator."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind the MCP SSE server to (default: 127.0.0.1).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"Port to bind the MCP SSE server to (default: {DEFAULT_PORT}).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output directory for generated HTML (default: {DEFAULT_OUTPUT}).",
    )
    args = parser.parse_args()
    try:
        generate(host=args.host, port=args.port, output=args.output)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except (subprocess.CalledProcessError, TimeoutError, RuntimeError) as ex:
        print(f"MCP docs generation failed: {ex}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
