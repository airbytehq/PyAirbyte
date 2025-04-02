# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MITM proxy functionality for HTTP caching."""

from __future__ import annotations

import atexit
import logging
import os
import socket
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from airbyte._util.text_util import generate_ulid
from airbyte.http_caching.modes import HttpCacheMode


if TYPE_CHECKING:
    from collections.abc import Callable


logger = logging.getLogger(__name__)

LOCALHOST = "localhost"
DOCKER_HOST = "host.docker.internal"


def find_free_port() -> int:
    """Find a free port to use for the proxy."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def get_proxy_host() -> str:
    """Return 'localhost' or another host name."""
    return LOCALHOST


def mitm_get_proxy_env_vars(proxy_port: int) -> dict[str, str]:
    """Get the environment variables to apply to processes using the HTTP proxy.

    Args:
        proxy_port: The port number the proxy is listening on.

    Returns:
        A dictionary of environment variables.
    """
    env_vars = {}
    if proxy_port:
        proxy_url = f"http://host.docker.internal:{proxy_port}"
        env_vars = {
            "HTTP_PROXY": proxy_url,
            "HTTPS_PROXY": proxy_url,
            "NO_PROXY": "localhost,127.0.0.1,http://host.docker.internal",
        }
    return env_vars


def mitm_start_proxy(
    cache_dir: Path,
    read_dir: Path,
    mode: HttpCacheMode,
    serialization_format: str,
    proxy_port: int,
    stop_callback: Callable[[], None],
) -> subprocess.Popen:
    """Start the HTTP proxy.

    Args:
        cache_dir: The directory where cache files are stored for writing.
        read_dir: Directory for reading cached responses.
        mode: The cache mode to use.
        serialization_format: The format to use for serializing cached data.
        proxy_port: The port number to use for the proxy.
        stop_callback: A callback function to register with atexit for cleanup.

    Returns:
        A tuple containing the process, temporary directory, and CA certificate path.
    """
    # Create a unique ID for this session
    session_id = generate_ulid().lower()

    # Set environment variables for the mitmdump process
    env = os.environ.copy()
    env.update(
        {
            "AIRBYTE_HTTP_CACHE_DIR": str(cache_dir.absolute()),
            "AIRBYTE_HTTP_CACHE_READ_DIR": str(read_dir.absolute()),
            "AIRBYTE_HTTP_CACHE_MODE": mode,
            "AIRBYTE_HTTP_CACHE_FORMAT": serialization_format,
        }
    )

    # Start mitmdump with our script
    script_path = Path(__file__).parent / "mitm_addons_script.py"

    session_dir = Path(".airbyte-http-cache/sessions")
    session_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(":", "-")
    flow_output_file = session_dir / f"{timestamp}.flows"

    replay_files = sorted(session_dir.glob("*.flows"))

    cmd = [
        "mitmdump",
        "--mode",
        "regular",
        "--listen-host",
        "127.0.0.1",
        "--listen-port",
        str(proxy_port),
        "--ssl-insecure",
        "--set",
        f"confdir={cache_dir.absolute()}",
        "--save-stream-file",
        str(flow_output_file),
        "--set",
        "server_replay.match=host,port,path",
        "--set",
        "server_replay.nopop=true",
        "--scripts",
        str(script_path.absolute()),
    ]
    if mode == HttpCacheMode.READ_ONLY_FAIL_ON_MISS:
        cmd.extend(("--set", "server_replay.kill_extra=true"))

    if replay_files:
        for replay_file in replay_files:
            if replay_file != flow_output_file:
                # Remove old replay files
                cmd.extend(
                    ["--server-replay", str(replay_file.absolute())],
                )

    print(f"Starting mitmdump with command: {' '.join(cmd)}")

    # Register cleanup function to ensure the process is terminated
    atexit.register(stop_callback)

    mitm_process = subprocess.Popen(
        cmd,
        env=env,
        stdin=subprocess.PIPE,  # Open a dummy STDIN pipe to mitmdump doesn't immediately exit
        stderr=subprocess.STDOUT,
        text=True,
    )

    # Check if the process is still running
    if mitm_process.poll() is not None:
        _, stderr = mitm_process.communicate()
        logger.error(f"Failed to start mitmdump: {stderr}")
        raise RuntimeError(f"Failed to start mitmdump: {stderr}")

    logger.info(f"HTTP cache proxy started on port {proxy_port}")
    return mitm_process


def mitm_stop_proxy(
    mitm_process: subprocess.Popen | None,
    stop_callback: Callable[[], None],
) -> None:
    """Stop the HTTP proxy.

    Args:
        mitm_process: The mitmdump process to stop.
        stop_callback: The callback function to unregister from atexit.
    """
    if mitm_process is not None:
        logger.info("Stopping HTTP cache proxy")
        mitm_process.terminate()
        try:
            mitm_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            mitm_process.kill()

        # Remove the cleanup function
        atexit.unregister(stop_callback)
