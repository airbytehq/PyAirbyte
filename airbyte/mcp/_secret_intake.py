# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Out-of-band intake for connector secret values."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from starlette.responses import JSONResponse
from starlette.routing import Route

from airbyte.mcp._tool_utils import _resolve_transport_bearer_token


if TYPE_CHECKING:
    from collections.abc import Iterable

    from starlette.requests import Request

DEFAULT_INTAKE_TTL_SECONDS = 600
SECRET_INTAKE_PREFIX = "secret_intake::"
_SIGNING_KEY_ENV = "AIRBYTE_MCP_SECRET_INTAKE_SIGNING_KEY"
_PROCESS_SIGNING_KEY = secrets.token_bytes(32)


class SecretIntakeError(ValueError):
    """Raised when a secret intake token or reference is invalid."""


@dataclass
class _IntakeRecord:
    allowed_fields: frozenset[str]
    expires_at: float
    tenant_claim: str
    secrets: dict[str, str]
    used: bool = False


_INTAKES: dict[str, _IntakeRecord] = {}
_INTAKES_LOCK = threading.Lock()


def _signing_key() -> bytes:
    configured = os.getenv(_SIGNING_KEY_ENV, "")
    return configured.encode() if configured else _PROCESS_SIGNING_KEY


def _tenant_claim(token: str | None = None) -> str:
    if token is None:
        try:
            token = _resolve_transport_bearer_token()
        except Exception:
            token = ""
    return hashlib.sha256(token.encode()).hexdigest() if token else ""


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def mint_intake_token(
    allowed_secret_fields: Iterable[str],
    *,
    tenant_claim: str | None = None,
    ttl_seconds: int = DEFAULT_INTAKE_TTL_SECONDS,
) -> str:
    """Mint a signed, one-time token for collecting connector secrets.

    The signing key comes from `AIRBYTE_MCP_SECRET_INTAKE_SIGNING_KEY`. When
    unset, a process-random key is used, which is suitable only for a
    single-process spike.
    """
    fields = frozenset(allowed_secret_fields)
    if not fields or any(not field for field in fields):
        raise SecretIntakeError("At least one non-empty secret field is required.")
    if ttl_seconds <= 0:
        raise SecretIntakeError("The intake token TTL must be positive.")

    intake_id = secrets.token_urlsafe(18)
    expires_at = time.time() + ttl_seconds
    claim = tenant_claim if tenant_claim is not None else _tenant_claim()
    payload = {
        "intake_id": intake_id,
        "allowed_fields": sorted(fields),
        "tenant_claim": claim,
        "expires_at": expires_at,
    }
    encoded_payload = _encode(json.dumps(payload, separators=(",", ":")).encode())
    signature = hmac.new(
        _signing_key(),
        encoded_payload.encode(),
        hashlib.sha256,
    ).digest()
    with _INTAKES_LOCK:
        _INTAKES[intake_id] = _IntakeRecord(
            allowed_fields=fields,
            expires_at=expires_at,
            tenant_claim=claim,
            secrets={},
        )
        _prune_expired_locked(now=expires_at)
    return f"{encoded_payload}.{_encode(signature)}"


def _decode_and_verify(token: str) -> tuple[str, _IntakeRecord]:
    try:
        encoded_payload, encoded_signature = token.split(".", 1)
        expected_signature = hmac.new(
            _signing_key(),
            encoded_payload.encode(),
            hashlib.sha256,
        ).digest()
        if not hmac.compare_digest(_decode(encoded_signature), expected_signature):
            raise SecretIntakeError("Invalid intake token.")
        payload = json.loads(_decode(encoded_payload))
        intake_id = payload["intake_id"]
        allowed_fields = frozenset(payload["allowed_fields"])
        tenant_claim = payload["tenant_claim"]
        expires_at = float(payload["expires_at"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise SecretIntakeError("Invalid intake token.") from error

    if not isinstance(intake_id, str) or not isinstance(tenant_claim, str):
        raise SecretIntakeError("Invalid intake token.")
    if time.time() >= expires_at:
        raise SecretIntakeError("Intake token expired.")
    with _INTAKES_LOCK:
        record = _INTAKES.get(intake_id)
        if (
            record is None
            or record.used
            or record.expires_at != expires_at
            or record.allowed_fields != allowed_fields
            or record.tenant_claim != tenant_claim
        ):
            raise SecretIntakeError("Invalid intake token.")
    return intake_id, record


def _prune_expired_locked(*, now: float | None = None) -> None:
    current_time = time.time() if now is None else now
    for intake_id, record in list(_INTAKES.items()):
        if record.expires_at <= current_time:
            del _INTAKES[intake_id]


def store_intake_secrets(token: str, values: dict[str, str]) -> dict[str, str]:
    """Validate and consume a token after storing its submitted values."""
    intake_id, _ = _decode_and_verify(token)
    if not isinstance(values, dict) or not values:
        raise SecretIntakeError("Secret values are required.")
    if any(
        not isinstance(field, str) or not isinstance(value, str) for field, value in values.items()
    ):
        raise SecretIntakeError("Secret field names and values must be strings.")

    with _INTAKES_LOCK:
        record = _INTAKES.get(intake_id)
        if record is None or record.used or time.time() >= record.expires_at:
            raise SecretIntakeError("Invalid intake token.")
        if not set(values).issubset(record.allowed_fields):
            raise SecretIntakeError("One or more secret fields are not allowed.")
        record.secrets = dict(values)
        record.used = True
        return {field: f"{SECRET_INTAKE_PREFIX}{intake_id}/{field}" for field in values}


def resolve_intake_secrets(config: dict[str, Any]) -> dict[str, Any]:
    """Resolve `secret_intake::` references for the current transport tenant."""
    caller_claim = _tenant_claim()

    def resolve(value: object) -> object:
        if isinstance(value, dict):
            return {key: resolve(item) for key, item in value.items()}
        if isinstance(value, list):
            return [resolve(item) for item in value]
        if not isinstance(value, str) or not value.startswith(SECRET_INTAKE_PREFIX):
            return value

        reference = value[len(SECRET_INTAKE_PREFIX) :]
        try:
            intake_id, field = reference.split("/", 1)
        except ValueError as error:
            raise SecretIntakeError("Invalid secret intake reference.") from error
        with _INTAKES_LOCK:
            record = _INTAKES.get(intake_id)
            if (
                record is None
                or not record.used
                or time.time() >= record.expires_at
                or record.tenant_claim != caller_claim
                or field not in record.secrets
            ):
                raise SecretIntakeError("Invalid secret intake reference.")
            return record.secrets[field]

    return resolve(config)


async def _secret_intake_endpoint(request: Request) -> JSONResponse:
    """Accept secret values without returning them to the caller."""
    if request.method == "OPTIONS":
        return _cors_response({}, status_code=204)
    authorization = request.headers.get("authorization", "")
    if not authorization.lower().startswith("bearer "):
        return _cors_response({"error": "Unauthorized."}, status_code=401)
    token = authorization[7:].strip()
    try:
        body = await request.json()
        secret_values = body.get("secrets") if isinstance(body, dict) else None
        secret_refs = store_intake_secrets(token, secret_values)
    except (SecretIntakeError, ValueError, TypeError):
        return _cors_response({"error": "Invalid secret intake request."}, status_code=403)
    return _cors_response({"secret_refs": secret_refs})


def _cors_response(body: dict[str, Any], *, status_code: int = 200) -> JSONResponse:
    return JSONResponse(
        body,
        status_code=status_code,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Authorization, Content-Type",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
        },
    )


def secret_intake_routes() -> list[Route]:
    """Return Starlette routes for the out-of-band secret intake endpoint."""
    return [
        Route(
            "/secret-intake",
            _secret_intake_endpoint,
            methods=["POST", "OPTIONS"],
        ),
    ]


__all__ = [
    "DEFAULT_INTAKE_TTL_SECONDS",
    "SECRET_INTAKE_PREFIX",
    "SecretIntakeError",
    "mint_intake_token",
    "resolve_intake_secrets",
    "secret_intake_routes",
    "store_intake_secrets",
]
