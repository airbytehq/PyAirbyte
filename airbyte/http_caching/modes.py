# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""HTTP proxy implementation for caching requests and responses."""

from __future__ import annotations

from enum import Enum


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


class SerializationFormat(str, Enum):
    """The format to use for serializing HTTP cache data."""

    JSON = "json"
    """Human-readable JSON format."""

    NATIVE = "native"
    """Native mitmproxy format, interoperable with mitmproxy tools."""
