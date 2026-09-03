# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Stateless encrypted action capabilities for connector configuration forms.

The action token is encrypted with a process-shared key so its claims, including
Cloud credentials, are not visible to the MCP client. The process-random fallback
is suitable only for a single-process spike. Multi-replica deployments must
configure the same `AIRBYTE_MCP_FORM_SIGNING_KEY` on every replica; replay
tracking is still process-local best effort, while token expiration is the actual
lifetime bound.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import secrets
import threading
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Literal, cast
from urllib.parse import urlencode

import jsonschema
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Route

from airbyte import get_source
from airbyte._util import api_util
from airbyte._util.api_util import SDKError
from airbyte._util.registry_spec import get_connector_spec_from_registry
from airbyte.cloud.client import CloudClient
from airbyte.exceptions import AirbyteError, PyAirbyteInputError
from airbyte.mcp._tool_utils import (
    SafeModeError,
    _resolve_transport_bearer_token,
    check_guid_created_in_session,
)


if TYPE_CHECKING:
    from starlette.requests import Request


DEFAULT_FORM_TOKEN_TTL_SECONDS = 600
_FORM_SIGNING_KEY_ENV = "AIRBYTE_MCP_FORM_SIGNING_KEY"
_PROCESS_SIGNING_KEY = secrets.token_bytes(32)
_ACTION_NAMES = frozenset({"create", "update", "validate"})
_AES_GCM_NONCE_SIZE = 12
_SEEN_JTIS: dict[str, int] = {}
_SEEN_JTIS_LOCK = threading.Lock()
_OAUTH_SUCCESS_HTML = (
    "<!doctype html><html><body><p>Authentication complete. Your "
    "{connector_name} source was {action}. You can close this tab and return to your agent."
    "</p><p>Connector ID: {connector_id}</p></body></html>"
)
_OAUTH_FAILURE_HTML = (
    "<!doctype html><html><body><p>Could not complete authentication.</p></body></html>"
)
_OAUTH_INVALID_HTML = (
    "<!doctype html><html><body><p>This link is invalid or has expired.</p></body></html>"
)


class ConfigSubmitError(ValueError):
    """Raised when an encrypted configuration-submit capability is invalid."""


def _signing_key() -> bytes:
    configured = os.getenv(_FORM_SIGNING_KEY_ENV, "")
    return configured.encode() if configured else _PROCESS_SIGNING_KEY


def _encryption_key() -> bytes:
    return hashlib.sha256(_signing_key()).digest()


def _tenant_claim(token: str | None = None) -> str:
    if token is None:
        token = _resolve_transport_bearer_token()
    return hashlib.sha256(token.encode()).hexdigest() if token else ""


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def mint_action_token(  # noqa: PLR0913
    action: Literal["create", "update", "validate"],
    connector_name: str,
    *,
    non_secret_defaults: Mapping[str, Any] | None = None,
    workspace_id: str | None = None,
    source_id: str | None = None,
    source_name: str | None = None,
    bearer_token: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
    api_url: str | None = None,
    config_api_url: str | None = None,
    tenant_claim: str | None = None,
    oauth: bool | None = None,
    ttl_seconds: int = DEFAULT_FORM_TOKEN_TTL_SECONDS,
) -> str:
    """Mint an encrypted, one-shot action capability for the connector form."""
    if action not in _ACTION_NAMES:
        raise ConfigSubmitError("Invalid configuration action.")
    if not connector_name:
        raise ConfigSubmitError("Connector name is required.")
    if ttl_seconds <= 0:
        raise ConfigSubmitError("The form token TTL must be positive.")
    if (client_id is None) != (client_secret is None):
        raise ConfigSubmitError("Client credentials must be provided together.")

    claims: dict[str, Any] = {
        "action": action,
        "connector_name": connector_name,
        "tenant_claim": tenant_claim if tenant_claim is not None else _tenant_claim(),
        "exp": int(time.time()) + ttl_seconds,
        "jti": secrets.token_urlsafe(18),
    }
    optional_claims = {
        "non_secret_defaults": dict(non_secret_defaults)
        if non_secret_defaults is not None
        else None,
        "workspace_id": workspace_id,
        "source_id": source_id,
        "source_name": source_name,
        "bearer_token": bearer_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "api_url": api_url,
        "config_api_url": config_api_url,
        "oauth": oauth,
    }
    claims.update({key: value for key, value in optional_claims.items() if value is not None})
    plaintext = json.dumps(claims, separators=(",", ":")).encode()
    nonce = secrets.token_bytes(12)
    ciphertext = AESGCM(_encryption_key()).encrypt(nonce, plaintext, None)
    return _encode(nonce + ciphertext)


def _validate_claims(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ConfigSubmitError("Invalid configuration submit token.")
    required = ("action", "connector_name", "tenant_claim", "exp", "jti")
    if any(key not in payload for key in required):
        raise ConfigSubmitError("Invalid configuration submit token.")
    valid_types = (
        isinstance(payload["action"], str)
        and payload["action"] in _ACTION_NAMES
        and isinstance(payload["connector_name"], str)
        and isinstance(payload["tenant_claim"], str)
        and isinstance(payload["exp"], int)
        and not isinstance(payload["exp"], bool)
        and isinstance(payload["jti"], str)
    )
    if not valid_types:
        raise ConfigSubmitError("Invalid configuration submit token.")
    for key in (
        "non_secret_defaults",
        "workspace_id",
        "source_id",
        "source_name",
        "bearer_token",
        "client_id",
        "client_secret",
        "api_url",
        "config_api_url",
        "oauth",
    ):
        if key == "non_secret_defaults":
            if key in payload and not isinstance(payload[key], dict):
                raise ConfigSubmitError("Invalid configuration submit token.")
            continue
        if key == "oauth":
            if key in payload and not isinstance(payload[key], bool):
                raise ConfigSubmitError("Invalid configuration submit token.")
            continue
        if key in payload and not isinstance(payload[key], str):
            raise ConfigSubmitError("Invalid configuration submit token.")
    if ("client_id" in payload) != ("client_secret" in payload):
        raise ConfigSubmitError("Invalid configuration submit token.")
    return cast(dict[str, Any], payload)


def _prune_seen_jtis_locked(now: int) -> None:
    for jti, expires_at in list(_SEEN_JTIS.items()):
        if expires_at <= now:
            del _SEEN_JTIS[jti]


def decrypt_action_token(token: str, *, consume: bool = True) -> dict[str, Any]:
    """Decrypt and validate a one-shot action capability, optionally consuming it."""
    try:
        encoded = _decode(token)
        if len(encoded) <= _AES_GCM_NONCE_SIZE:
            raise ConfigSubmitError("Invalid configuration submit token.")
        plaintext = AESGCM(_encryption_key()).decrypt(
            encoded[:_AES_GCM_NONCE_SIZE],
            encoded[_AES_GCM_NONCE_SIZE:],
            None,
        )
        claims = _validate_claims(json.loads(plaintext))
    except (binascii.Error, InvalidTag, json.JSONDecodeError, TypeError, ValueError) as error:
        if isinstance(error, ConfigSubmitError):
            raise
        raise ConfigSubmitError("Invalid configuration submit token.") from error

    now = int(time.time())
    if claims["exp"] <= now:
        raise ConfigSubmitError("Configuration submit token expired.")
    if consume:
        with _SEEN_JTIS_LOCK:
            _prune_seen_jtis_locked(now)
            if claims["jti"] in _SEEN_JTIS:
                raise ConfigSubmitError("Configuration submit token already used.")
            _SEEN_JTIS[claims["jti"]] = claims["exp"]
    return claims


def _schema_secret_paths(schema: Mapping[str, Any], prefix: str = "") -> set[str]:
    paths: set[str] = set()
    if prefix and any(
        (
            schema.get("airbyte_secret") is True,
            schema.get("writeOnly") is True,
            schema.get("format") == "password",
        )
    ):
        paths.add(prefix)
    properties = schema.get("properties", {})
    if isinstance(properties, Mapping):
        for name, child in properties.items():
            if not isinstance(name, str) or not isinstance(child, Mapping):
                continue
            path = f"{prefix}.{name}" if prefix else name
            paths.update(_schema_secret_paths(child, path))
    items = schema.get("items")
    if isinstance(items, Mapping):
        paths.update(_schema_secret_paths(items, prefix))
    for branch_key in ("oneOf", "anyOf", "allOf"):
        branches = schema.get(branch_key, [])
        if not isinstance(branches, list):
            continue
        for branch in branches:
            if isinstance(branch, Mapping):
                paths.update(_schema_secret_paths(branch, prefix))
    return paths


def _strip_secret_values(
    value: object,
    secret_paths: set[str],
    prefix: str = "",
) -> object:
    if isinstance(value, list):
        return [_strip_secret_values(item, secret_paths, prefix) for item in value]
    if not isinstance(value, Mapping):
        return value
    result: dict[str, object] = {}
    for name, child in value.items():
        if not isinstance(name, str):
            continue
        path = f"{prefix}.{name}" if prefix else name
        if path in secret_paths:
            continue
        result[name] = _strip_secret_values(child, secret_paths, path)
    return result


def initiate_oauth_flow(
    claims: Mapping[str, Any],
    submitted_config: Mapping[str, Any],
    *,
    server_url: str,
) -> tuple[str, str]:
    """Mint a derived OAuth capability and obtain the Cloud consent URL."""
    if claims["action"] not in {"create", "update"} or not claims.get("workspace_id"):
        raise ConfigSubmitError("Invalid OAuth request.")
    source_type = claims["connector_name"].removeprefix("source-")
    if not api_util.is_oauth_source_type(source_type):
        raise ConfigSubmitError("Invalid OAuth request.")
    schema = get_connector_spec_from_registry(claims["connector_name"], platform="cloud")
    if schema is None:
        schema = get_connector_spec_from_registry(claims["connector_name"], platform="oss")
    if not schema:
        raise ConfigSubmitError("Invalid OAuth request.")
    clean_config = _strip_secret_values(submitted_config, _schema_secret_paths(schema))
    defaults = dict(claims.get("non_secret_defaults", {}))
    derived_token = mint_action_token(
        claims["action"],
        claims["connector_name"],
        non_secret_defaults=_merge_config(defaults, clean_config),
        workspace_id=claims.get("workspace_id"),
        source_id=claims.get("source_id"),
        source_name=claims.get("source_name"),
        bearer_token=claims.get("bearer_token"),
        client_id=claims.get("client_id"),
        client_secret=claims.get("client_secret"),
        api_url=claims.get("api_url"),
        config_api_url=claims.get("config_api_url"),
        tenant_claim=claims["tenant_claim"],
        oauth=True,
    )
    response = api_util.initiate_oauth(
        redirect_url=(
            f"{server_url.rstrip('/')}/connector-config-oauth-callback?"
            f"{urlencode({'state': derived_token})}"
        ),
        source_type=source_type,
        workspace_id=claims["workspace_id"],
        api_root=claims.get("api_url") or "",
        client_id=claims.get("client_id"),
        client_secret=claims.get("client_secret"),
        bearer_token=claims.get("bearer_token"),
    )
    raw = response.raw_response.json()
    consent_url = next(
        (raw.get(key) for key in ("consentUrl", "consent_url", "redirectUrl") if raw.get(key)),
        None,
    )
    if not isinstance(consent_url, str):
        raise ConfigSubmitError("OAuth consent URL is unavailable.")
    return consent_url, derived_token


def _cloud_client_from_claims(claims: Mapping[str, Any]) -> CloudClient:
    return CloudClient(
        bearer_token=claims.get("bearer_token"),
        client_id=claims.get("client_id"),
        client_secret=claims.get("client_secret"),
        public_api_root=claims.get("api_url"),
        config_api_root=claims.get("config_api_url"),
    )


def _execute_action(claims: Mapping[str, Any], config: dict[str, Any]) -> dict[str, str]:
    action = claims["action"]
    connector_name = claims["connector_name"]
    if action == "validate":
        source = get_source(connector_name, no_executor=True)
        source.set_config(config, validate=True)
        return {"status": "success", "action": "validated"}

    workspace_id = claims.get("workspace_id")
    if not workspace_id:
        raise ConfigSubmitError("Cloud workspace is unavailable.")
    workspace = _cloud_client_from_claims(claims).get_workspace(workspace_id)
    if action == "create":
        source = get_source(connector_name, no_executor=True)
        source.set_config(config, validate=True)
        deployed_source = workspace.deploy_source(
            name=claims.get("source_name") or connector_name,
            source=source,
            unique=True,
        )
        return {
            "status": "success",
            "action": "created",
            "connector_id": deployed_source.connector_id,
            "connector_url": deployed_source.connector_url,
        }

    source_id = claims.get("source_id")
    if not source_id:
        raise ConfigSubmitError("Cloud source is unavailable.")
    check_guid_created_in_session(source_id)
    source = workspace.get_source(source_id=source_id)
    source.update_config(config=config)
    return {
        "status": "success",
        "action": "updated",
        "connector_id": source.connector_id,
        "connector_url": source.connector_url,
    }


async def connector_config_submit_endpoint(request: Request) -> Response:  # noqa: PLR0911
    """Execute a one-shot connector configuration action without echoing config."""
    if request.method == "OPTIONS":
        return Response(status_code=204, headers=_cors_headers())
    authorization = request.headers.get("authorization", "")
    if not authorization.lower().startswith("bearer "):
        return _cors_response({"error": "Unauthorized."}, status_code=401)
    token = authorization[7:].strip()
    try:
        claims = decrypt_action_token(token)
    except ConfigSubmitError:
        return _cors_response({"error": "Invalid configuration submit request."}, status_code=403)

    try:
        body = await request.json()
    except (TypeError, ValueError):
        return _cors_response({"error": "Invalid configuration submit request."}, status_code=400)
    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        return _cors_response({"error": "Invalid configuration submit request."}, status_code=400)
    try:
        return _cors_response(_execute_action(claims, config))
    except (
        AirbyteError,
        ConfigSubmitError,
        KeyError,
        PyAirbyteInputError,
        SafeModeError,
        SDKError,
        TypeError,
        ValueError,
        jsonschema.ValidationError,
    ):
        return _cors_response(
            {"error": "Connector configuration could not be submitted."}, status_code=422
        )


async def connector_config_oauth_start_endpoint(request: Request) -> Response:  # noqa: PLR0911
    """Start an OAuth consent flow without consuming the form capability."""
    if request.method == "OPTIONS":
        return Response(status_code=204, headers=_cors_headers())
    authorization = request.headers.get("authorization", "")
    if not authorization.lower().startswith("bearer "):
        return _cors_response({"error": "Unauthorized."}, status_code=401)
    token = authorization[7:].strip()
    try:
        claims = decrypt_action_token(token, consume=False)
    except ConfigSubmitError:
        return _cors_response({"error": "Invalid OAuth request."}, status_code=403)
    if claims["action"] not in {"create", "update"} or not claims.get("workspace_id"):
        return _cors_response({"error": "Invalid OAuth request."}, status_code=403)
    if not api_util.is_oauth_source_type(claims["connector_name"].removeprefix("source-")):
        return _cors_response({"error": "Invalid OAuth request."}, status_code=403)
    try:
        body = await request.json()
        submitted = body.get("config") if isinstance(body, dict) else None
        if not isinstance(submitted, dict):
            return _cors_response({"error": "Invalid OAuth request."}, status_code=400)
        consent_url, _ = initiate_oauth_flow(
            claims,
            submitted,
            server_url=_server_url_from_request(request),
        )
        return _cors_response({"consent_url": consent_url})
    except (
        AirbyteError,
        ConfigSubmitError,
        KeyError,
        SDKError,
        TypeError,
        ValueError,
        jsonschema.ValidationError,
    ):
        return _cors_response({"error": "Could not start authentication."}, status_code=422)


def connector_config_oauth_callback_endpoint(request: Request) -> HTMLResponse:
    """Complete an OAuth flow and create or update the Cloud source."""
    state = request.query_params.get("state", "")
    secret_id = request.query_params.get("secret_id", "")
    if not state or not secret_id or not _safe_request_url(request):
        return HTMLResponse(_OAUTH_INVALID_HTML, status_code=403)
    try:
        claims = decrypt_action_token(state)
    except ConfigSubmitError:
        return HTMLResponse(_OAUTH_INVALID_HTML, status_code=403)
    try:
        if claims.get("oauth") is not True or claims["action"] not in {"create", "update"}:
            return HTMLResponse(_OAUTH_INVALID_HTML, status_code=403)
        config = claims.get("non_secret_defaults", {})
        if not isinstance(config, dict):
            return HTMLResponse(_OAUTH_INVALID_HTML, status_code=403)
        api_kwargs = {
            "api_root": claims.get("api_url") or "",
            "client_id": claims.get("client_id"),
            "client_secret": claims.get("client_secret"),
            "bearer_token": claims.get("bearer_token"),
        }
        if claims["action"] == "create":
            source = api_util.create_source(
                claims.get("source_name") or claims["connector_name"],
                workspace_id=claims["workspace_id"],
                config=config,
                secret_id=secret_id,
                **api_kwargs,
            )
        else:
            source = api_util.patch_source(
                claims["source_id"],
                config=config,
                secret_id=secret_id,
                **api_kwargs,
            )
        action = "created" if claims["action"] == "create" else "updated"
        connector_id = getattr(source, "source_id", None) or getattr(source, "connector_id", "")
        html = _OAUTH_SUCCESS_HTML.format(
            connector_name=claims["connector_name"],
            action=action,
            connector_id=connector_id,
        )
        return HTMLResponse(html)
    except (
        AirbyteError,
        ConfigSubmitError,
        KeyError,
        SDKError,
        TypeError,
        ValueError,
    ):
        return HTMLResponse(_OAUTH_FAILURE_HTML, status_code=422)


def _merge_config(base: dict[str, Any], update: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in update.items():
        if isinstance(value, Mapping) and isinstance(result.get(key), dict):
            result[key] = _merge_config(result[key], value)
        else:
            result[key] = value
    return result


def _server_url_from_request(request: Request) -> str:
    if not _safe_request_url(request):
        raise ConfigSubmitError("OAuth callback requires HTTPS.")
    return f"{request.url.scheme}://{request.url.netloc}"


def _safe_request_url(request: Request) -> bool:
    return request.url.scheme == "https" or request.url.hostname in {"localhost", "127.0.0.1"}


def _cors_response(body: dict[str, Any], *, status_code: int = 200) -> JSONResponse:
    return JSONResponse(
        body,
        status_code=status_code,
        headers=_cors_headers(),
    )


def _cors_headers() -> dict[str, str]:
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Authorization, Content-Type",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
    }


def connector_config_submit_routes() -> list[Route]:
    """Return the Starlette routes for connector configuration submission."""
    return [
        Route(
            "/connector-config-submit",
            connector_config_submit_endpoint,
            methods=["POST", "OPTIONS"],
        ),
        Route(
            "/connector-config-oauth-start",
            connector_config_oauth_start_endpoint,
            methods=["POST", "OPTIONS"],
        ),
        Route(
            "/connector-config-oauth-callback",
            connector_config_oauth_callback_endpoint,
            methods=["GET"],
        ),
    ]


__all__ = [
    "ConfigSubmitError",
    "DEFAULT_FORM_TOKEN_TTL_SECONDS",
    "_schema_secret_paths",
    "connector_config_submit_endpoint",
    "connector_config_submit_routes",
    "decrypt_action_token",
    "initiate_oauth_flow",
    "mint_action_token",
]
