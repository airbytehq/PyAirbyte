# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Self-describing capability tokens for stateless MCP HTTP transport."""

from __future__ import annotations

import base64
import binascii
import json
import re
import uuid
from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

_TOKEN_SEPARATOR = "."
_SESSION_HEADER = b"mcp-session-id"
_BASE64URL_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
_HTTP_REQUEST = "http"
_HTTP_REQUEST_MESSAGE = "http.request"
_HTTP_RESPONSE_START = "http.response.start"
_HTTP_REQUEST_METHOD = "method"
_HTTP_REQUEST_BODY = "body"
_HTTP_REQUEST_MORE_BODY = "more_body"
_HTTP_RESPONSE_HEADERS = "headers"
_UUID4_VERSION = 4


def encode_capability_token(extension_ids: AbstractSet[str]) -> str:
    """Encode extension IDs as a visible-ASCII capability token.

    The token contains a random UUID4 component and a base64url-encoded,
    space-separated payload. An empty extension set returns an empty string.
    """
    normalized_ids = sorted(
        extension_id
        for extension_id in extension_ids
        if extension_id and not any(char.isspace() for char in extension_id)
    )
    if not normalized_ids:
        return ""
    payload = " ".join(normalized_ids).encode("utf-8")
    encoded_payload = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    return f"{uuid.uuid4().hex}{_TOKEN_SEPARATOR}{encoded_payload}"


def decode_capability_token(token: str) -> set[str]:
    """Decode extension IDs from a capability token, failing closed on errors."""
    if not token or _TOKEN_SEPARATOR not in token:
        return set()

    nonce, encoded_payload = token.split(_TOKEN_SEPARATOR, maxsplit=1)
    if not _is_uuid4_hex(nonce) or not encoded_payload:
        return set()
    if not _BASE64URL_PATTERN.fullmatch(encoded_payload):
        return set()

    try:
        padding = "=" * (-len(encoded_payload) % 4)
        payload = base64.urlsafe_b64decode(encoded_payload + padding).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return set()

    extension_ids = payload.split()
    if not extension_ids:
        return set()
    return set(extension_ids)


def _is_uuid4_hex(value: str) -> bool:
    try:
        parsed = uuid.UUID(hex=value)
    except ValueError:
        return False
    return parsed.hex == value.lower() and parsed.version == _UUID4_VERSION


def _mapping(value: object) -> Mapping[str, object] | None:
    if isinstance(value, Mapping):
        return value
    return None


def _initialize_extension_ids(body: bytes) -> set[str]:
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return set()

    payload_mapping = _mapping(payload)
    if payload_mapping is None or payload_mapping.get("method") != "initialize":
        return set()
    params = _mapping(payload_mapping.get("params"))
    capabilities = _mapping(params.get("capabilities")) if params is not None else None
    extensions = _mapping(capabilities.get("extensions")) if capabilities is not None else None
    if extensions is None:
        return set()
    return {
        extension_id
        for extension_id in extensions
        if isinstance(extension_id, str) and extension_id
    }


class CapabilityTokenMiddleware:
    """Carry initialize extension declarations through stateless HTTP requests."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != _HTTP_REQUEST or scope.get(_HTTP_REQUEST_METHOD) != "POST":
            await self.app(scope, receive, send)
            return

        body_parts: list[bytes] = []
        while True:
            message = await receive()
            body_parts.append(message.get(_HTTP_REQUEST_BODY, b""))
            if not message.get(_HTTP_REQUEST_MORE_BODY, False):
                break
        body = b"".join(body_parts)
        extension_ids = _initialize_extension_ids(body)
        token = encode_capability_token(extension_ids)
        replayed = False

        async def replay_receive() -> Message:
            nonlocal replayed
            if replayed:
                return await receive()
            replayed = True
            return {
                "type": _HTTP_REQUEST_MESSAGE,
                "body": body,
                "more_body": False,
            }

        async def send_response(message: Message) -> None:
            if (
                message.get("type") == _HTTP_RESPONSE_START
                and token
                and isinstance(message.get(_HTTP_RESPONSE_HEADERS), list)
            ):
                headers = [
                    (name, value)
                    for name, value in message[_HTTP_RESPONSE_HEADERS]
                    if name.lower() != _SESSION_HEADER
                ]
                headers.append((_SESSION_HEADER, token.encode("ascii")))
                message = {**message, _HTTP_RESPONSE_HEADERS: headers}
            await send(message)

        await self.app(scope, replay_receive, send_response)
