# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for out-of-band MCP secret intake."""

from __future__ import annotations

import json
import time

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from airbyte.mcp import _secret_intake
from airbyte.mcp._secret_intake import (
    SecretIntakeError,
    mint_intake_token,
    resolve_intake_secrets,
    secret_intake_routes,
    store_intake_secrets,
)
from airbyte.mcp._arg_resolvers import resolve_connector_config


def _intake_id(token: str) -> str:
    payload = json.loads(_secret_intake._decode(token.split(".", 1)[0]))
    return payload["intake_id"]


@pytest.fixture
def local_tenant(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_secret_intake, "_resolve_transport_bearer_token", lambda: "")


def test_token_roundtrip_and_replay(local_tenant: None) -> None:
    token = mint_intake_token(["password"])

    assert store_intake_secrets(token, {"password": "secret-value"}) == {
        "password": f"secret_intake::{_intake_id(token)}/password"
    }
    with pytest.raises(SecretIntakeError, match="Invalid intake token"):
        store_intake_secrets(token, {"password": "another-value"})


def test_token_rejects_tampering_and_disallowed_fields(local_tenant: None) -> None:
    token = mint_intake_token(["password"])
    encoded_payload, signature = token.split(".")

    with pytest.raises(SecretIntakeError, match="Invalid intake token"):
        store_intake_secrets(f"{encoded_payload}x.{signature}", {"password": "value"})
    token = mint_intake_token(["password"])
    with pytest.raises(SecretIntakeError, match="not allowed"):
        store_intake_secrets(token, {"other": "value"})


def test_expired_token_fails(
    local_tenant: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = mint_intake_token(["password"], ttl_seconds=1)
    now = time.time()
    monkeypatch.setattr(_secret_intake.time, "time", lambda: now + 2)

    with pytest.raises(SecretIntakeError, match="expired"):
        store_intake_secrets(token, {"password": "value"})


def test_endpoint_returns_only_opaque_references(local_tenant: None) -> None:
    app = Starlette(routes=secret_intake_routes())
    token = mint_intake_token(["password"])

    with TestClient(app) as client:
        response = client.post(
            "/secret-intake",
            headers={"Authorization": f"Bearer {token}"},
            json={"secrets": {"password": "secret-value"}},
        )

    assert response.status_code == 200
    assert "secret-value" not in response.text
    assert response.json()["secret_refs"]["password"].startswith("secret_intake::")


def test_endpoint_rejects_missing_and_reused_tokens(local_tenant: None) -> None:
    app = Starlette(routes=secret_intake_routes())

    with TestClient(app) as client:
        missing = client.post("/secret-intake", json={"secrets": {"password": "value"}})
        token = mint_intake_token(["password"])
        first = client.post(
            "/secret-intake",
            headers={"Authorization": f"Bearer {token}"},
            json={"secrets": {"password": "value"}},
        )
        reused = client.post(
            "/secret-intake",
            headers={"Authorization": f"Bearer {token}"},
            json={"secrets": {"password": "value"}},
        )

    assert missing.status_code == 401
    assert first.status_code == 200
    assert reused.status_code == 403


def test_resolve_intake_secrets_enforces_tenant(
    local_tenant: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = mint_intake_token(["password"], tenant_claim="")
    refs = store_intake_secrets(token, {"password": "secret-value"})
    config = {"credentials": {"password": refs["password"]}}

    assert resolve_intake_secrets(config)["credentials"]["password"] == "secret-value"

    monkeypatch.setattr(
        _secret_intake, "_resolve_transport_bearer_token", lambda: "tenant-b"
    )
    with pytest.raises(SecretIntakeError, match="Invalid secret intake reference"):
        resolve_intake_secrets(config)


def test_unknown_reference_fails(local_tenant: None) -> None:
    with pytest.raises(SecretIntakeError, match="Invalid secret intake reference"):
        resolve_intake_secrets({"password": "secret_intake::unknown/password"})


def test_resolve_intake_secrets_rejects_wrong_field_path(local_tenant: None) -> None:
    token = mint_intake_token(["password"])
    refs = store_intake_secrets(token, {"password": "secret-value"})

    with pytest.raises(SecretIntakeError, match="Invalid secret intake reference"):
        resolve_intake_secrets({"username": refs["password"]})


def test_connector_config_resolves_intake_references(
    local_tenant: None,
) -> None:
    token = mint_intake_token(["password"])
    refs = store_intake_secrets(token, {"password": "secret-value"})

    assert resolve_connector_config({"password": refs["password"]}) == {
        "password": "secret-value"
    }


def test_connector_config_resolves_intake_references_before_secret_check(
    local_tenant: None,
) -> None:
    token = mint_intake_token(["password"])
    refs = store_intake_secrets(token, {"password": "secret-value"})
    schema = {
        "type": "object",
        "properties": {
            "credentials": {
                "type": "object",
                "properties": {
                    "password": {"type": "string", "airbyte_secret": True},
                },
            },
        },
    }

    assert resolve_connector_config(
        {"credentials": {"password": refs["password"]}},
        config_spec_jsonschema=schema,
    ) == {"credentials": {"password": "secret-value"}}


def test_connector_config_rejects_intake_reference_at_nonsecret_path(
    local_tenant: None,
) -> None:
    token = mint_intake_token(["password"])
    refs = store_intake_secrets(token, {"password": "secret-value"})
    schema = {
        "type": "object",
        "properties": {
            "credentials": {
                "type": "object",
                "properties": {
                    "password": {"type": "string", "airbyte_secret": True},
                    "username": {"type": "string"},
                },
            },
        },
    }

    with pytest.raises(SecretIntakeError, match="Invalid secret intake reference"):
        resolve_connector_config(
            {"credentials": {"username": refs["password"]}},
            config_spec_jsonschema=schema,
        )
