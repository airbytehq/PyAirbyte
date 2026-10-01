# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Phase P placeholder interfaces: shared key loader and grouping id."""

from __future__ import annotations

import asyncio
import base64
import logging
import uuid

import httpx
import pytest
from fastmcp_extensions import TelemetrySinks
from fastmcp_extensions.capability_tokens import encode_session_token

from airbyte.mcp import _telemetry
from airbyte.mcp._telemetry import (
    GroupingId,
    McpRequestTelemetryMiddleware,
    current_grouping_id,
    session_id_digest,
)
from airbyte.mcp._telemetry_key import TELEMETRY_HMAC_KEY_ENV, load_master


@pytest.fixture(autouse=True)
def _propagate_airbyte_logs(monkeypatch):
    monkeypatch.setattr(logging.getLogger("airbyte"), "propagate", True)


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


VALID_KEY = bytes(range(32))


def test_load_master_accepts_unpadded_base64url_32_bytes(caplog):
    with caplog.at_level(logging.DEBUG):
        assert load_master({TELEMETRY_HMAC_KEY_ENV: _b64(VALID_KEY)}) == VALID_KEY
    assert caplog.records == []


@pytest.mark.parametrize("environ", [{}, {TELEMETRY_HMAC_KEY_ENV: ""}])
def test_load_master_unset_is_silent(environ, caplog):
    with caplog.at_level(logging.DEBUG):
        assert load_master(environ) is None
    assert caplog.records == []


@pytest.mark.parametrize(
    "raw",
    [
        _b64(b"\x02" * 31),
        _b64(b"\x02" * 33),
        _b64(VALID_KEY) + "=",
        base64.urlsafe_b64encode(VALID_KEY).decode(),
        base64.b64encode(b"\xfb" * 32).decode().rstrip("="),
        _b64(VALID_KEY)[:-1] + "!",
        _b64(VALID_KEY) + " extra",
        _b64(b"\x01" * 32),
    ],
)
def test_load_master_rejects_invalid_and_test_key_without_logging_value(raw, caplog):
    with caplog.at_level(logging.DEBUG):
        assert load_master({TELEMETRY_HMAC_KEY_ENV: raw}) is None
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.WARNING
    assert raw not in caplog.text
    assert raw[:8] not in caplog.text


def _state_for(headers: dict[str, str]) -> dict[str, object]:
    captured: dict[str, object] = {}

    async def inner(scope, receive, send):
        captured.update(scope["state"])
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    app = McpRequestTelemetryMiddleware(
        inner, sinks=TelemetrySinks(package_name="airbyte"), mcp_path="/mcp"
    )

    async def run() -> None:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            await client.post("/other", headers=headers)

    asyncio.run(run())
    return captured


def test_current_grouping_id_none_on_stdio():
    assert current_grouping_id() == GroupingId(kind="none", raw_digest=None)


@pytest.mark.parametrize(
    "headers",
    [{}, {"mcp-session-id": "test"}, {"mcp-session-id": str(uuid.uuid4())}],
)
def test_current_grouping_id_none_for_hand_made_ids(monkeypatch, headers):
    state = _state_for(headers)
    monkeypatch.setattr(_telemetry, "_mutable_request_state", lambda: state)
    assert current_grouping_id() == GroupingId(kind="none", raw_digest=None)


def test_current_grouping_id_transport_session_for_minted_token(monkeypatch):
    token = encode_session_token(client_name="Synthetic", client_version="1.0")
    state = _state_for({"mcp-session-id": token})
    monkeypatch.setattr(_telemetry, "_mutable_request_state", lambda: state)
    assert current_grouping_id() == GroupingId(
        kind="transport_session", raw_digest=session_id_digest(token)
    )


def test_current_grouping_id_never_raises(monkeypatch):
    def boom():
        raise RuntimeError("synthetic")

    monkeypatch.setattr(_telemetry, "_mutable_request_state", boom)
    assert current_grouping_id().kind == "none"
