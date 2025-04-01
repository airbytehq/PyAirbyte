# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""HTTP caching for Airbyte connectors."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from pathlib import Path
from typing import Optional

from mitmproxy import options
from mitmproxy.tools.dump import DumpMaster

from airbyte.constants import DEFAULT_HTTP_CACHE_DIR, DEFAULT_HTTP_CACHE_READ_DIR
from airbyte.http_caching.proxy import HttpCacheMode, HttpCachingAddon
from airbyte.http_caching.serialization import SerializationFormat


logger = logging.getLogger(__name__)


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
                                 'json' or 'binary'.
        """
        self.cache_dir = Path(cache_dir) if cache_dir else DEFAULT_HTTP_CACHE_DIR

        if read_dir:
            self.read_dir = Path(read_dir)
        elif DEFAULT_HTTP_CACHE_READ_DIR:
            self.read_dir = DEFAULT_HTTP_CACHE_READ_DIR
        else:
            self.read_dir = self.cache_dir

        self.mode = HttpCacheMode(mode) if isinstance(mode, str) else mode

        self.serialization_format = (
            SerializationFormat(serialization_format)
            if isinstance(serialization_format, str)
            else serialization_format
        )

        self._proxy_port: int | None = None
        self._proxy_thread: threading.Thread | None = None
        self._proxy: DumpMaster | None = None
        self._addon: HttpCachingAddon | None = None

    def start(self) -> int:
        """Start the HTTP proxy.

        Returns:
            The port number the proxy is listening on.
        """
        if self._proxy_port is not None:
            return self._proxy_port

        port = 8080

        opts = options.Options(
            listen_host="127.0.0.1",
            listen_port=port,
            ssl_insecure=True,  # Allow self-signed certificates
            confdir=str(self.cache_dir),  # Store certificates in the cache directory
        )

        addon = HttpCachingAddon(
            cache_dir=self.cache_dir,
            read_dir=self.read_dir,
            mode=self.mode,
            serialization_format=self.serialization_format,
        )
        self._addon = addon

        def run_proxy() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                proxy = DumpMaster(opts)
                self._proxy = proxy
                proxy.addons.add(addon)
                
                loop.run_until_complete(proxy.run())
            except Exception:
                logger.exception("Error running proxy")

        thread = threading.Thread(target=run_proxy, daemon=True)
        self._proxy_thread = thread
        thread.start()

        time.sleep(1)

        self._proxy_port = port
        return port

    def stop(self) -> None:
        """Stop the HTTP proxy."""
        if self._proxy is not None:
            self._proxy.shutdown()
            self._proxy_thread = None
            self._proxy = None
            self._addon = None
            self._proxy_port = None
