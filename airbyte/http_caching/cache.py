# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""HTTP caching for Airbyte connectors."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from airbyte.constants import DEFAULT_HTTP_CACHE_DIR, DEFAULT_HTTP_CACHE_READ_DIR
from airbyte.http_caching.mitm_proxy import (
    find_free_port,
    get_proxy_host,
)
from airbyte.http_caching.mitm_proxy import (
    get_docker_volumes as mitm_get_docker_volumes,
)
from airbyte.http_caching.mitm_proxy import (
    get_proxy_env_vars as mitm_get_env_vars,
)
from airbyte.http_caching.mitm_proxy import (
    start_proxy as mitm_start_proxy,
)
from airbyte.http_caching.mitm_proxy import (
    stop_proxy as mitm_stop_proxy,
)
from airbyte.http_caching.modes import HttpCacheMode, SerializationFormat


if TYPE_CHECKING:
    import subprocess
    import tempfile


class AirbyteConnectorCache:
    """Cache for Airbyte connector HTTP requests and responses.

    This class manages an HTTP proxy that intercepts requests from connectors and either
    serves them from the cache or forwards them to the server based on the cache mode.
    """

    def __init__(
        self,
        cache_dir: str | Path | None = None,
        read_dir: str | Path | None = None,
        mode: str | HttpCacheMode = HttpCacheMode.READ_WRITE,
        serialization_format: str | SerializationFormat = SerializationFormat.NATIVE,
    ) -> None:
        """Initialize the cache.

        Args:
            cache_dir: The directory where cache files are stored for writing. If not provided,
                       the default directory will be used.
            read_dir: Optional separate directory for reading cached responses. If not provided,
                      cache_dir will be used for both reading and writing.
            mode: The cache mode to use. Can be one of 'read_only', 'write_only',
                  'read_write', or 'read_only_fail_on_miss'.
            serialization_format: The format to use for serializing cached data. Can be
                                 'json' or 'native'.
        """
        self.cache_dir = Path(cache_dir) if cache_dir else DEFAULT_HTTP_CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        if read_dir:
            self.read_dir = Path(read_dir)
        elif DEFAULT_HTTP_CACHE_READ_DIR:
            self.read_dir = DEFAULT_HTTP_CACHE_READ_DIR
        else:
            self.read_dir = self.cache_dir

        self.read_dir.mkdir(parents=True, exist_ok=True)

        self.mode = HttpCacheMode(mode) if isinstance(mode, str) else mode

        self.serialization_format = (
            SerializationFormat(serialization_format)
            if isinstance(serialization_format, str)
            else serialization_format
        )

        self._proxy_port: int | None = None
        self._mitm_process: subprocess.Popen | None = None
        self._temp_dir: tempfile.TemporaryDirectory | None = None
        self._ca_cert_path: Path | None = None

    @property
    def proxy_host(self) -> str:
        """Return 'localhost' or another host name."""
        return get_proxy_host()

    @property
    def proxy_port(self) -> int:
        """Return the port number the proxy is listening on."""
        if self._proxy_port is None:
            self._proxy_port = find_free_port()
        assert self._proxy_port is not None, "Proxy port should be set at this point"
        return self._proxy_port

    def get_proxy_env_vars(self) -> dict[str, str]:
        """Get the environment variables to apply to processes using the HTTP proxy."""
        return mitm_get_env_vars(self.proxy_port)

    def start(self) -> int:
        """Start the HTTP proxy.

        Returns:
            The port number the proxy is listening on.
        """
        if self._mitm_process is not None and self._proxy_port is not None:
            # Proxy is already running
            return self._proxy_port

        self._mitm_process, self._temp_dir, self._ca_cert_path = mitm_start_proxy(
            cache_dir=self.cache_dir,
            read_dir=self.read_dir,
            mode=self.mode.value,
            serialization_format=self.serialization_format.value,
            proxy_port=self.proxy_port,
            stop_callback=self.stop,
        )

        return self.proxy_port

    def get_docker_volumes(self) -> dict[str, str]:
        """Get the Docker volume mappings needed for the proxy.

        Returns:
            A dictionary of host paths to container paths for Docker volume mappings.
        """
        return mitm_get_docker_volumes(self._ca_cert_path)

    def stop(self) -> None:
        """Stop the HTTP proxy."""
        mitm_stop_proxy(self._mitm_process, self._temp_dir, self.stop)
        self._mitm_process = None
        self._proxy_port = None
        self._temp_dir = None
        self._ca_cert_path = None
