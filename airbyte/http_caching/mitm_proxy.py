# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""MITM proxy functionality for HTTP caching."""

from __future__ import annotations

import atexit
import logging
import os
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

from airbyte._util.text_util import generate_ulid


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


def get_proxy_env_vars(proxy_port: int) -> dict[str, str]:
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
            "no_proxy": "localhost,127.0.0.1,http://host.docker.internal",
        }
    return env_vars


def extract_ca_cert(temp_path: Path, cache_dir: Path) -> Path:
    """Extract the mitmproxy CA certificate to use for SSL verification.

    Args:
        temp_path: The temporary directory where mitmproxy stores its files.
        cache_dir: The directory where to store the extracted certificate.

    Returns:
        The path to the CA certificate.
    """
    # Wait for mitmproxy to generate the CA certificate
    time.sleep(2)

    # Find the CA certificate
    ca_cert_path = temp_path / "mitmproxy-ca-cert.pem"

    if not ca_cert_path.exists():
        # Try to find it in the mitmproxy directory
        mitmproxy_dir = Path.home() / ".mitmproxy"
        if mitmproxy_dir.exists():
            source_cert = mitmproxy_dir / "mitmproxy-ca-cert.pem"
            if source_cert.exists():
                # Copy the certificate to our cache directory
                ca_cert_path = cache_dir / "mitmproxy-ca-cert.pem"
                shutil.copy(source_cert, ca_cert_path)
                logger.info(f"Copied mitmproxy CA certificate to {ca_cert_path}")
                return ca_cert_path

    # If we found the certificate in the temp directory, copy it to our cache directory
    if ca_cert_path.exists():
        dest_cert = cache_dir / "mitmproxy-ca-cert.pem"
        shutil.copy(ca_cert_path, dest_cert)
        logger.info(f"Copied mitmproxy CA certificate to {dest_cert}")
        return dest_cert

    # If we couldn't find the certificate, try to generate it
    logger.warning("Could not find mitmproxy CA certificate, attempting to generate it")
    try:
        # Run mitmproxy to generate the certificate
        subprocess.run(
            ["mitmdump", "--set", f"confdir={temp_path}", "--version"],
            check=True,
            capture_output=True,
            text=True,
        )

        # Check if the certificate was generated
        if (temp_path / "mitmproxy-ca-cert.pem").exists():
            dest_cert = cache_dir / "mitmproxy-ca-cert.pem"
            shutil.copy(temp_path / "mitmproxy-ca-cert.pem", dest_cert)
            logger.info(f"Generated and copied mitmproxy CA certificate to {dest_cert}")
            return dest_cert
    except Exception as e:
        logger.exception("Failed to generate mitmproxy CA certificate.")

    logger.error("Could not find or generate mitmproxy CA certificate")
    raise RuntimeError("Could not find or generate mitmproxy CA certificate")


def start_proxy(
    cache_dir: Path,
    read_dir: Path,
    mode: str,
    serialization_format: str,
    proxy_port: int,
    stop_callback: Callable[[], None],
) -> tuple[subprocess.Popen, tempfile.TemporaryDirectory, Path]:
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
    # Create a temporary directory for mitmdump configuration
    temp_dir = tempfile.TemporaryDirectory()
    temp_path = Path(temp_dir.name)

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

    cmd = [
        "mitmdump",
        "--listen-host",
        "127.0.0.1",
        "--listen-port",
        str(proxy_port),
        "--ssl-insecure",
        "--set",
        f"confdir={temp_path}",
        "--scripts",
        str(script_path.absolute()),
    ]

    logger.info(f"Starting mitmdump with command: {' '.join(cmd)}")

    mitm_process = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Register cleanup function to ensure the process is terminated
    atexit.register(stop_callback)

    # Wait a moment for the proxy to start
    time.sleep(1)

    # Check if the process is still running
    if mitm_process.poll() is not None:
        stdout, stderr = mitm_process.communicate()
        logger.error(f"Failed to start mitmdump: {stderr}")
        raise RuntimeError(f"Failed to start mitmdump: {stderr}")

    # Extract the CA certificate for SSL verification
    ca_cert_path = extract_ca_cert(temp_path, cache_dir)

    logger.info(f"HTTP cache proxy started on port {proxy_port}")
    return mitm_process, temp_dir, ca_cert_path


def get_docker_volumes(ca_cert_path: Path | None) -> dict[str, str]:
    """Get the Docker volume mappings needed for the proxy.

    Args:
        ca_cert_path: The path to the CA certificate.

    Returns:
        A dictionary of host paths to container paths for Docker volume mappings.
    """
    volumes = {}

    # Add the CA certificate volume if available
    if ca_cert_path and ca_cert_path.exists():
        volumes[str(ca_cert_path.absolute())] = "/usr/local/lib/python3.11/site-packages/certifi/cacert.pem"

    return volumes


def stop_proxy(mitm_process: subprocess.Popen | None, temp_dir: tempfile.TemporaryDirectory | None, stop_callback: Callable[[], None]) -> None:
    """Stop the HTTP proxy.

    Args:
        mitm_process: The mitmdump process to stop.
        temp_dir: The temporary directory to clean up.
        stop_callback: The callback function to unregister from atexit.
    """
    if mitm_process is not None:
        logger.info("Stopping HTTP cache proxy")
        mitm_process.terminate()
        try:
            mitm_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            mitm_process.kill()

        # Clean up temporary directory
        if temp_dir:
            temp_dir.cleanup()

        # Remove the cleanup function
        atexit.unregister(stop_callback)
