# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for stateless MCP connector configuration submission."""

from __future__ import annotations

import base64
import time
from typing import Any, cast

import pytest
from airbyte_api.errors import SDKError
from starlette.applications import Starlette
from starlette.testclient import TestClient

from airbyte.mcp import _config_submit
from airbyte.mcp._config_submit import (
    ConfigSubmitError,
    _schema_secret_paths,
    connector_config_submit_routes,
    decrypt_action_token,
    mint_action_token,
)


def test_token_encrypts_and_decrypts_claims(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIRBYTE_MCP_FORM_SIGNING_KEY", "test-signing-key")
    token = mint_action_token(
        "create",
        "source-example",
        workspace_id="workspace-id",
        source_name="Example",
        bearer_token="bearer-secret",
        api_url="https://api.example.com",
    )

    assert "bearer-secret" not in token
    claims = decrypt_action_token(token)
    assert claims["action"] == "create"
    assert claims["connector_name"] == "source-example"
    assert claims["workspace_id"] == "workspace-id"
    assert claims["bearer_token"] == "bearer-secret"


def test_token_rejects_expiry_and_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    token = mint_action_token("validate", "source-example", ttl_seconds=1)
    now = time.time()
    monkeypatch.setattr(_config_submit.time, "time", lambda: now + 2)

    with pytest.raises(ConfigSubmitError, match="expired"):
        decrypt_action_token(token)

    monkeypatch.setattr(_config_submit.time, "time", lambda: now)
    token = mint_action_token("validate", "source-example")
    decrypt_action_token(token)
    with pytest.raises(ConfigSubmitError, match="already used"):
        decrypt_action_token(token)


def test_token_rejects_ciphertext_tampering() -> None:
    token = mint_action_token("validate", "source-example")
    encoded = bytearray(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)))
    encoded[-1] ^= 1
    tampered = base64.urlsafe_b64encode(encoded).rstrip(b"=").decode()

    with pytest.raises(ConfigSubmitError, match="Invalid"):
        decrypt_action_token(tampered)


def test_token_rejects_invalid_claims() -> None:
    with pytest.raises(ConfigSubmitError, match="Invalid configuration action"):
        mint_action_token("invalid", "source-example")  # type: ignore[arg-type]


def test_schema_secret_paths_include_nested_branches() -> None:
    schema = {
        "oneOf": [
            {
                "type": "object",
                "properties": {
                    "credentials": {
                        "type": "object",
                        "properties": {
                            "api_key": {"type": "string", "airbyte_secret": True},
                        },
                    },
                },
            },
            {
                "type": "object",
                "properties": {
                    "credentials": {
                        "type": "object",
                        "properties": {
                            "password": {"type": "string", "airbyte_secret": True},
                        },
                    },
                },
            },
        ]
    }

    assert _schema_secret_paths(schema) == {
        "credentials.api_key",
        "credentials.password",
    }


def test_schema_secret_paths_include_hydration_markers_and_items() -> None:
    schema = {
        "type": "object",
        "properties": {
            "write_only": {"type": "string", "writeOnly": True},
            "password": {"type": "string", "format": "password"},
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "token": {"type": "string", "format": "password"},
                    },
                },
            },
        },
    }

    assert _schema_secret_paths(schema) == {
        "write_only",
        "password",
        "items.token",
    }


class _SourceStub:
    def __init__(self) -> None:
        self.config: dict[str, object] | None = None

    def set_config(self, config: dict[str, object], *, validate: bool = True) -> None:
        self.config = config
        assert validate


def test_validate_endpoint_executes_full_config_without_echo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _SourceStub()
    monkeypatch.setattr(_config_submit, "get_source", lambda *args, **kwargs: source)
    token = mint_action_token("validate", "source-example")
    app = Starlette(routes=connector_config_submit_routes())
    config = {"credentials": {"api_key": "secret-value"}, "region": "us-east-1"}

    with TestClient(app) as client:
        response = client.post(
            "/connector-config-submit",
            headers={"Authorization": f"Bearer {token}"},
            json={"config": config},
        )

    assert response.status_code == 200
    assert response.json() == {"status": "success", "action": "validated"}
    assert source.config == config
    assert "secret-value" not in response.text


def test_endpoint_returns_safe_422_for_cloud_sdk_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_sdk_error(*args: Any, **kwargs: Any) -> dict[str, str]:
        raise SDKError("Cloud rejected the configuration.", 422, "", cast(Any, None))

    monkeypatch.setattr(_config_submit, "_execute_action", raise_sdk_error)
    app = Starlette(routes=connector_config_submit_routes())
    token = mint_action_token("validate", "source-example")

    with TestClient(app) as client:
        response = client.post(
            "/connector-config-submit",
            headers={"Authorization": f"Bearer {token}"},
            json={"config": {"api_key": "secret-value"}},
        )

    assert response.status_code == 422
    assert response.json() == {
        "error": "Connector configuration could not be submitted.",
    }
    assert "secret-value" not in response.text


def test_endpoint_rejects_missing_malformed_and_reused_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        _config_submit, "get_source", lambda *args, **kwargs: _SourceStub()
    )
    app = Starlette(routes=connector_config_submit_routes())

    with TestClient(app) as client:
        missing = client.post(
            "/connector-config-submit",
            json={"config": {"password": "value"}},
        )
        malformed = client.post(
            "/connector-config-submit",
            headers={"Authorization": "Bearer invalid"},
            json={"config": {}},
        )
        token = mint_action_token("validate", "source-example")
        first = client.post(
            "/connector-config-submit",
            headers={"Authorization": f"Bearer {token}"},
            json={"config": {}},
        )
        reused = client.post(
            "/connector-config-submit",
            headers={"Authorization": f"Bearer {token}"},
            json={"config": {}},
        )

    assert missing.status_code == 401
    assert malformed.status_code == 403
    assert first.status_code == 200
    assert reused.status_code == 403


def test_endpoint_rejects_missing_config_without_echoing_secrets() -> None:
    app = Starlette(routes=connector_config_submit_routes())
    token = mint_action_token("validate", "source-example")

    with TestClient(app) as client:
        response = client.post(
            "/connector-config-submit",
            headers={"Authorization": f"Bearer {token}"},
            json={"not_config": "secret-value"},
        )

    assert response.status_code == 400
    assert "secret-value" not in response.text


def test_options_response_is_bodyless_and_keeps_cors_headers() -> None:
    app = Starlette(routes=connector_config_submit_routes())

    with TestClient(app) as client:
        response = client.options(
            "/connector-config-submit",
            headers={
                "Origin": "https://example.com",
                "Access-Control-Request-Method": "POST",
            },
        )

    assert response.status_code == 204
    assert response.content == b""
    assert response.headers["access-control-allow-origin"] == "*"
    assert (
        response.headers["access-control-allow-headers"]
        == "Authorization, Content-Type"
    )
    assert response.headers["access-control-allow-methods"] == "POST, OPTIONS"
