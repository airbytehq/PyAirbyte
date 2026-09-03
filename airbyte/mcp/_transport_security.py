# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""ASGI transport security for the HTTP MCP server."""

from __future__ import annotations

import fnmatch
import ipaddress
import os
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from starlette.responses import Response


if TYPE_CHECKING:
    from collections.abc import Sequence

    from starlette.types import ASGIApp, Receive, Scope, Send


ALLOWED_HOSTS_ENV = "AIRBYTE_MCP_ALLOWED_HOSTS"
HTTP_HOST_ENV = "AIRBYTE_MCP_HTTP_HOST"

_DEFAULT_ALLOWED_HOSTS = ("127.0.0.1", "localhost", "::1")


def _strip_host_port(value: str) -> str:
    """Normalize a host or host pattern for comparison."""
    normalized = value.strip().lower()
    if normalized.startswith("["):
        closing_bracket = normalized.find("]")
        if closing_bracket != -1:
            return normalized[1:closing_bracket]
    if normalized.count(":") == 1:
        return normalized.rsplit(":", 1)[0]
    return normalized


def _is_unspecified_address(hostname: str) -> bool:
    try:
        return ipaddress.ip_address(hostname).is_unspecified
    except ValueError:
        return False


def resolve_allowed_hosts(server_url: str) -> tuple[str, ...]:
    """Resolve allowed hostnames from defaults, the server URL, and the environment."""
    resolved: list[str] = []
    seen: set[str] = set()

    def add(host: str) -> None:
        cleaned = host.strip()
        if not cleaned:
            return
        key = _strip_host_port(cleaned)
        if key and key not in seen:
            resolved.append(cleaned)
            seen.add(key)

    for host in _DEFAULT_ALLOWED_HOSTS:
        add(host)

    hostname = urlparse(server_url).hostname
    if hostname and not _is_unspecified_address(hostname):
        add(hostname)

    for host in os.getenv(ALLOWED_HOSTS_ENV, "").split(","):
        add(host)

    return tuple(resolved)


def _request_host(scope: Scope) -> str | None:
    hosts = [
        value.decode("latin-1")
        for name, value in scope.get("headers", [])
        if name.lower() == b"host"
    ]
    if len(hosts) != 1:
        return None
    return _strip_host_port(hosts[0]) or None


def _origin_hosts(scope: Scope) -> tuple[str | None, ...]:
    origins: list[str | None] = []
    for name, value in scope.get("headers", []):
        if name.lower() == b"origin":
            origin = value.decode("latin-1").strip()
            try:
                hostname = urlparse(origin).hostname
            except ValueError:
                origins.append(None)
            else:
                origins.append(_strip_host_port(hostname) if hostname else None)
    return tuple(origins)


class HostOriginGuardMiddleware:
    """Reject HTTP requests whose host or origin is outside the allowlist."""

    def __init__(self, app: ASGIApp, allowed_hosts: Sequence[str]) -> None:
        self.app = app
        self.allowed_hosts = tuple(
            normalized
            for normalized in (_strip_host_port(host) for host in allowed_hosts)
            if normalized
        )

    def _is_allowed(self, hostname: str | None) -> bool:
        return hostname is not None and any(
            fnmatch.fnmatchcase(hostname, allowed_host) for allowed_host in self.allowed_hosts
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        if not self._is_allowed(_request_host(scope)):
            await Response("Misdirected Request", status_code=421)(scope, receive, send)
            return

        origin_hosts = _origin_hosts(scope)
        # Both headers are attacker-controlled; do not fall back to request Host.
        if any(not self._is_allowed(origin_host) for origin_host in origin_hosts):
            await Response("Forbidden Origin", status_code=403)(scope, receive, send)
            return

        await self.app(scope, receive, send)
