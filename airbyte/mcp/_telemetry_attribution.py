# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Privacy-safe attribution properties for MCP tool-call telemetry.

Hosted deployments should set `AIRBYTE_TELEMETRY_ANONYMIZATION_SEED` explicitly.
The analytics-ID fallback is regenerated per container, which would otherwise
make the surrogates identify instances instead of callers.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from collections.abc import Callable  # noqa: TC003
from functools import lru_cache
from typing import TypeVar
from urllib.parse import urlsplit

from fastmcp.server.dependencies import (
    get_access_token,
    get_context,
    get_http_request,
)

from airbyte._util.telemetry import _get_analytics_id
from airbyte.constants import is_hosted_mcp_mode


_TELEMETRY_SALT_ENV = "AIRBYTE_TELEMETRY_ANONYMIZATION_SEED"
_AIRBYTE_OWNED_DOMAINS = ("airbyte.ai", "airbyte.com", "airbyte.io")
_T = TypeVar("_T")


@lru_cache(maxsize=1)
def _get_telemetry_salt() -> str | None:
    """Return the process-wide attribution salt, if telemetry is available."""
    salt = os.environ.get(_TELEMETRY_SALT_ENV)
    if salt is None:
        salt = _get_analytics_id()
    return salt or None


def _hash_value(value: str, scope_label: str, endpoint: str = "local") -> str | None:
    try:
        salt = _get_telemetry_salt()
        if salt is None:
            return None
        message = f"{scope_label}|{endpoint}|{value}".encode()
        return hmac.new(salt.encode(), message, hashlib.sha256).hexdigest()[:16]
    except RuntimeError:
        return None


def _safe_value(resolver: Callable[[], _T]) -> _T | None:
    try:
        value = resolver()
    except RuntimeError:
        return None
    return value or None


def _session_id() -> str | None:
    return get_context().session_id


def _caller_ip() -> str | None:
    request = get_http_request()
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        first_hop = forwarded_for.split(",", 1)[0].strip()
        if first_hop:
            return first_hop
    return request.client.host if request.client else None


def _auth_subject() -> str | None:
    access_token = get_access_token()
    if access_token is None:
        return None
    claims = access_token.claims
    subject = claims.get("sub") or claims.get("client_id") or access_token.client_id
    return subject if isinstance(subject, str) else None


def _request_host() -> str | None:
    request = get_http_request()
    host = request.headers.get("host")
    if not host:
        return None
    host = host.strip()
    if not host:
        return None
    return host


def _request_endpoint(host: str) -> str:
    request = get_http_request()
    path = request.url.path
    return f"{host}{path}" if path and path != "/" else host


def _client_info() -> tuple[str | None, str | None]:
    context = get_context()
    client_params = context.session.client_params
    if client_params is None:
        return None, None
    client_info = client_params.clientInfo
    return client_info.name, client_info.version


def _is_airbyte_owned(host: str) -> bool:
    hostname = urlsplit(f"//{host}").hostname
    if not hostname:
        return False
    hostname = hostname.rstrip(".").lower()
    return any(
        hostname == domain or hostname.endswith(f".{domain}") for domain in _AIRBYTE_OWNED_DOMAINS
    )


def get_telemetry_attribution() -> dict[str, str | bool]:
    """Return privacy-safe attribution properties for an MCP tool call."""
    properties: dict[str, str | bool] = {}
    properties["is_hosted_mcp"] = is_hosted_mcp_mode()
    if _safe_value(_get_telemetry_salt) is None:
        return properties

    host = _safe_value(_request_host)
    endpoint_context = host or "local"

    session_id = _safe_value(_session_id)
    if session_id is not None:
        session_id_hash = _hash_value(session_id, "session", endpoint_context)
        if session_id_hash is not None:
            properties["session_id_hash"] = session_id_hash

    caller_ip = _safe_value(_caller_ip)
    if caller_ip is not None:
        caller_hash = _hash_value(caller_ip, "ip", endpoint_context)
        if caller_hash is not None:
            properties["caller_hash"] = caller_hash

    auth_subject = _safe_value(_auth_subject)
    if auth_subject is not None:
        auth_subject_hash = _hash_value(auth_subject, "subject", endpoint_context)
        if auth_subject_hash is not None:
            properties["auth_subject_hash"] = auth_subject_hash

    if host is not None:
        endpoint_hash = _hash_value(host, "endpoint", host)
        if endpoint_hash is not None:
            properties["mcp_endpoint_hash"] = endpoint_hash
        endpoint = _safe_value(lambda: _request_endpoint(host)) or host
        if _safe_value(lambda: _is_airbyte_owned(host)):
            properties["mcp_endpoint"] = endpoint

    client_data = _safe_value(_client_info)
    if client_data is not None:
        client_name, client_version = client_data
        if client_name:
            properties["mcp_client_name"] = client_name
        if client_version:
            properties["mcp_client_version"] = client_version

    return properties
