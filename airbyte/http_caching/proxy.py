# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""HTTP proxy implementation for caching requests and responses."""

from __future__ import annotations

import hashlib
import logging
from enum import Enum
from typing import TYPE_CHECKING, Any

from mitmproxy.http import HTTPFlow, Response

from airbyte.http_caching.serialization import (
    JsonSerializer,
    NativeSerializer,
    SerializationFormat,
)


if TYPE_CHECKING:
    from pathlib import Path


class HttpCacheMode(str, Enum):
    """The mode for the HTTP cache."""

    READ_ONLY = "read_only"
    """Only read from the cache. If a request is not in the cache, it will be made to the server."""

    WRITE_ONLY = "write_only"
    """Only write to the cache. All requests will be made to the server and cached."""

    READ_WRITE = "read_write"
    """Read from the cache if available, otherwise make the request to the server and cache it."""

    READ_ONLY_FAIL_ON_MISS = "read_only_fail_on_miss"
    """Only read from the cache. If a request is not in the cache, an exception will be raised."""


logger = logging.getLogger(__name__)


class HttpCachingAddon:
    """Addon for mitmproxy that caches HTTP requests and responses.

    This addon intercepts HTTP requests and either serves them from the cache or forwards them
    to the server based on the cache mode.
    """

    def __init__(
        self,
        cache_dir: Path,
        mode: HttpCacheMode,
        read_dir: Path | None = None,
        serialization_format: SerializationFormat = SerializationFormat.NATIVE,
    ) -> None:
        """Initialize the addon.

        Args:
            cache_dir: The directory where cache files are stored.
            mode: The cache mode to use.
            read_dir: Optional separate directory for reading cached responses.
                      If not provided, cache_dir will be used for both reading and writing.
            serialization_format: The format to use for serializing cached data.
        """
        self.cache_dir = cache_dir
        self.read_dir = read_dir or cache_dir
        self.mode = mode
        self.serialization_format = serialization_format

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        if self.read_dir != self.cache_dir:
            self.read_dir.mkdir(parents=True, exist_ok=True)

        if serialization_format == SerializationFormat.JSON:
            self.serializer: JsonSerializer | NativeSerializer = JsonSerializer()
        else:
            self.serializer = NativeSerializer()

    def _get_cache_key(self, flow: HTTPFlow) -> str:
        """Generate a unique key for the request.

        Args:
            flow: The HTTP flow to generate a key for.

        Returns:
            A string key that uniquely identifies the request.
        """
        request_hash = hashlib.md5()
        request_hash.update(flow.request.method.encode())
        request_hash.update(flow.request.url.encode())
        request_hash.update(flow.request.content or b"")

        headers_str = ",".join(f"{k}" for k in flow.request.headers)
        request_hash.update(headers_str.encode())

        return request_hash.hexdigest()

    def _get_cache_path(self, key: str, *, is_read: bool = False) -> Path:
        """Get the path to the cache file for the given key.

        Args:
            key: The cache key.
            is_read: Whether this path is for reading (True) or writing (False).

        Returns:
            The path to the cache file.
        """
        base_dir = self.read_dir if is_read else self.cache_dir
        extension = ".json" if self.serialization_format == SerializationFormat.JSON else ".mitm"
        return base_dir / f"{key}{extension}"

    def request(self, flow: HTTPFlow) -> None:
        """Process an HTTP request.

        Args:
            flow: The HTTP flow to process.
        """
        key = self._get_cache_key(flow)
        cache_path = self._get_cache_path(key, is_read=True)

        if self.mode in {
            HttpCacheMode.READ_ONLY,
            HttpCacheMode.READ_WRITE,
            HttpCacheMode.READ_ONLY_FAIL_ON_MISS,
        }:
            if cache_path.exists():
                try:
                    cached_data: dict[str, Any] = self.serializer.deserialize(cache_path)
                    cached_flow = HTTPFlow.from_state(cached_data)
                    if hasattr(cached_flow, "response") and cached_flow.response:
                        flow.response = cached_flow.response
                    logger.info(f"Serving {flow.request.url} from cache")
                except Exception as e:
                    logger.warning(f"Failed to load cached response: {e}")
                else:
                    return

            if self.mode == HttpCacheMode.READ_ONLY_FAIL_ON_MISS:
                flow.response = Response.make(
                    status_code=404,
                    content=f"Cache miss for {flow.request.url}".encode(),
                )
                logger.error(f"Cache miss for {flow.request.url} in READ_ONLY_FAIL_ON_MISS mode")

    def response(self, flow: HTTPFlow) -> None:
        """Process an HTTP response.

        Args:
            flow: The HTTP flow to process.
        """
        if self.mode in {HttpCacheMode.WRITE_ONLY, HttpCacheMode.READ_WRITE}:
            key = self._get_cache_key(flow)
            cache_path = self._get_cache_path(key, is_read=False)

            try:
                self.serializer.serialize(flow.get_state(), cache_path)
                logger.info(f"Cached response for {flow.request.url}")
            except Exception as e:
                logger.warning(f"Failed to cache response: {e}")
