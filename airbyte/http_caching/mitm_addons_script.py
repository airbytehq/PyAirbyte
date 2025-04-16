# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Standalone script for mitmproxy CLI to handle HTTP caching."""

from __future__ import annotations

import asyncio
from typing import Any

# import hashlib
# import json
# import os
# from pathlib import Path
from mitmproxy import ctx, http


def load(l: Any) -> None:  # noqa: ANN401, E741
    """Run when the script is loaded."""
    _ = l
    ctx.log.info("✅ mitmproxy addon loaded.")


def done() -> None:
    """Perform actions when the script is done."""
    ctx.log.info("💤 mitmproxy done...")


class ReplayStatsLogger:
    """Replay statistics logger for cache hits and misses."""

    def __init__(self) -> None:
        """Initialize the logger."""
        self.hits = 0
        self.misses = 0

    def response(self, flow: http.HTTPFlow) -> None:
        """Perform actions during HTTP response events."""
        if flow.is_replay:
            ctx.log.info(f"Replay cache hit for {flow.request.url}")
            self.hits += 1
        else:
            ctx.log.info(f"Replay cache miss for {flow.request.url}")
            self.misses += 1

    def done(self) -> None:
        """Perform actions when the script is done."""
        ctx.log.info("💤 mitmproxy shutting down...")
        ctx.log.info("📊 mitmproxy replay stats:")
        ctx.log.info(f"   🔁 Replayed (cache hits): {self.hits}")
        ctx.log.info(f"   🌍 Forwarded (cache misses): {self.misses}")


class KeepAlive:
    def running(self):
        self.task = asyncio.create_task(self._block())

    async def _block(self):
        while True:
            await asyncio.sleep(1)


# class HttpCachingAddon:
#     """Addon for mitmproxy that caches HTTP requests and responses."""

#     def __init__(self) -> None:
#         """Initialize the addon from environment variables."""
#         # Get configuration from environment variables
#         self.cache_dir = Path(os.environ.get("AIRBYTE_HTTP_CACHE_DIR", ".airbyte-http-cache"))
#         self.read_dir = Path(
#             os.environ.get(
#                 "AIRBYTE_HTTP_CACHE_READ_DIR",
#                 os.environ.get("AIRBYTE_HTTP_CACHE_DIR", ".airbyte-http-cache"),
#             )
#         )
#         self.mode = os.environ.get("AIRBYTE_HTTP_CACHE_MODE", HttpCacheMode.READ_WRITE)
#         self.serialization_format = os.environ.get(
#             "AIRBYTE_HTTP_CACHE_FORMAT", SerializationFormat.NATIVE
#         )

#         # Create directories if they don't exist
#         self.cache_dir.mkdir(parents=True, exist_ok=True)
#         if self.read_dir != self.cache_dir:
#             self.read_dir.mkdir(parents=True, exist_ok=True)

#         ctx.log.info(f"HTTP Cache initialized with mode: {self.mode}")
#         ctx.log.info(f"Cache directory: {self.cache_dir}")
#         ctx.log.info(f"Read directory: {self.read_dir}")
#         ctx.log.info(f"Serialization format: {self.serialization_format}")

#     def _get_cache_key(self, flow: http.HTTPFlow) -> str:
#         """Generate a unique key for the request."""
#         request_hash = hashlib.md5()
#         request_hash.update(flow.request.method.encode())
#         request_hash.update(flow.request.url.encode())
#         request_hash.update(flow.request.content or b"")

#         headers_str = ",".join(f"{k}" for k in flow.request.headers)
#         request_hash.update(headers_str.encode())

#         return request_hash.hexdigest()

#     def _get_cache_path(self, key: str, *, is_read: bool = False) -> Path:
#         """Get the path to the cache file for the given key."""
#         base_dir = self.read_dir if is_read else self.cache_dir
#         extension = ".json" if self.serialization_format == SerializationFormat.JSON else ".mitm"

#         ctx.log.info(f"Cache path: {base_dir / f'{key}{extension}'}")
#         return base_dir / f"{key}{extension}"

#     def _serialize_flow(self, flow: http.HTTPFlow, path: Path) -> None:
#         """Serialize a flow to a file."""
#         if self.serialization_format == SerializationFormat.JSON:
#             # JSON serialization
#             data = {
#                 "request": {
#                     "method": flow.request.method,
#                     "url": flow.request.url,
#                     "headers": dict(flow.request.headers),
#                     "content": flow.request.content.decode("utf-8", errors="replace")
#                     if flow.request.content
#                     else None,
#                 },
#                 "response": None,
#             }

#             if flow.response:
#                 data["response"] = {
#                     "status_code": flow.response.status_code,
#                     "headers": dict(flow.response.headers),
#                     "content": flow.response.content.decode("utf-8", errors="replace")
#                     if flow.response.content
#                     else None,
#                 }

#             with path.open("w", encoding="utf-8") as f:
#                 json.dump(data, f, indent=2)
#         else:
#             # Use mitmproxy's built-in flow export
#             flow.backup()
#             flow.export_file(str(path))

#     def _deserialize_flow(self, path: Path) -> http.HTTPFlow | None:
#         """Deserialize a flow from a file."""
#         if not path.exists():
#             return None

#         try:
#             if self.serialization_format == SerializationFormat.JSON:
#                 # JSON deserialization
#                 with path.open(encoding="utf-8") as f:
#                     data = json.load(f)

#                 # Create a new flow
#                 flow = http.HTTPFlow(None, None)

#                 # Set request
#                 req_data = data["request"]
#                 flow.request = http.Request(
#                     "http",
#                     req_data["method"],
#                     req_data["url"],
#                     "",
#                     "",
#                     http.Headers([(k, v) for k, v in req_data["headers"].items()]),
#                     req_data["content"].encode("utf-8") if req_data["content"] else b"",
#                 )  # type: ignore [reportCallIssue]

#                 # Set response if available
#                 if data["response"]:
#                     resp_data = data["response"]
#                     flow.response = http.Response(
#                         resp_data["status_code"],
#                         "",
#                         "",
#                         http.Headers([(k, v) for k, v in resp_data["headers"].items()]),
#                         resp_data["content"].encode("utf-8") if resp_data["content"] else b"",
#                     )  # type: ignore [reportCallIssue]

#                 return flow
#             # Use mitmproxy's built-in flow import
#             return http.HTTPFlow.load(str(path))
#         except Exception as e:
#             ctx.log.error(f"Failed to deserialize flow from {path}: {e}")
#             return None

#     def request(self, flow: http.HTTPFlow) -> None:
#         """Process an HTTP request."""
#         key = self._get_cache_key(flow)
#         cache_path = self._get_cache_path(key, is_read=True)

#         if self.mode in {
#             HttpCacheMode.READ_ONLY,
#             HttpCacheMode.READ_WRITE,
#             HttpCacheMode.READ_ONLY_FAIL_ON_MISS,
#         }:
#             if cache_path.exists():
#                 cached_flow = self._deserialize_flow(cache_path)
#                 if cached_flow and cached_flow.response:
#                     flow.response = cached_flow.response
#                     ctx.log.info(f"Serving {flow.request.url} from cache")
#                     return

#             if self.mode == HttpCacheMode.READ_ONLY_FAIL_ON_MISS:
#                 flow.response = http.Response.make(
#                     status_code=404,
#                     content=f"Cache miss for {flow.request.url}".encode(),
#                 )
#                 ctx.log.error(f"Cache miss for {flow.request.url} in READ_ONLY_FAIL_ON_MISS mode")

#     def response(self, flow: http.HTTPFlow) -> None:
#         """Process an HTTP response."""
#         if self.mode not in {HttpCacheMode.WRITE_ONLY, HttpCacheMode.READ_WRITE}:
#             ctx.log.info(f"Cache mode {self.mode} does not support writing responses.")
#             return

#         key = self._get_cache_key(flow)
#         cache_path = self._get_cache_path(key, is_read=False)

#         ctx.log.info(f"Caching response for {flow.request.url}")
#         try:
#             cache_path.parent.mkdir(parents=True, exist_ok=True)
#             self._serialize_flow(flow, cache_path)
#             ctx.log.info(f"Cached response for {flow.request.url}")
#         except Exception as e:
#             ctx.log.error(f"Failed to cache response: {e}")

addons = [
    ReplayStatsLogger(),
    KeepAlive(),
    # HttpCachingAddon(),
]
