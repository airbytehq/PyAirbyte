# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for the connector configuration form MCP App."""

from __future__ import annotations

import json

import pytest

from airbyte.exceptions import PyAirbyteInputError
from airbyte.mcp.interactive import _connector_config_form_ui as form


SCHEMA = {
    "type": "object",
    "properties": {
        "credentials": {
            "type": "object",
            "properties": {
                "api_key": {"type": "string", "airbyte_secret": True},
                "username": {"type": "string"},
            },
        },
        "region": {"type": "string"},
    },
}


def test_form_result_contains_schema_and_opaque_intake_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        form, "get_connector_spec_from_registry", lambda *args, **kwargs: SCHEMA
    )

    result = form.show_connector_config_form(
        "source-example",
        {"credentials": {"username": "user"}, "region": "us-east-1"},
    )

    content = result.content[0].text
    structured = result.structured_content
    assert "source-example" in content
    assert "api_key" in content
    assert structured["spec_schema"] == SCHEMA
    assert structured["intake_endpoint"].endswith("/secret-intake")
    assert structured["intake_token"]
    assert "secret-value" not in content
    assert "secret-value" not in json.dumps(structured)


def test_form_rejects_secret_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        form, "get_connector_spec_from_registry", lambda *args, **kwargs: SCHEMA
    )

    with pytest.raises(PyAirbyteInputError, match="Secret values cannot"):
        form.show_connector_config_form(
            "source-example",
            {"credentials": {"api_key": "secret-value"}},
        )


def test_intake_endpoint_preserves_server_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(form.MCP_SERVER_URL_ENV, "https://example.com/cloud-mcp/")

    assert form._intake_endpoint() == "https://example.com/cloud-mcp/secret-intake"
    assert form._server_origin() == "https://example.com"


def test_server_origin_rejects_insecure_nonlocal_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(form.MCP_SERVER_URL_ENV, "http://example.com/cloud-mcp")

    with pytest.raises(PyAirbyteInputError, match="HTTPS"):
        form._server_origin()


@pytest.mark.parametrize(
    "server_url",
    ["//attacker.example", "ftp://example.com", "example.com"],
)
def test_server_origin_rejects_invalid_url(
    monkeypatch: pytest.MonkeyPatch,
    server_url: str,
) -> None:
    monkeypatch.setenv(form.MCP_SERVER_URL_ENV, server_url)

    with pytest.raises(PyAirbyteInputError, match="valid HTTP"):
        form._server_origin()


def test_server_origin_allows_local_http_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(form.MCP_SERVER_URL_ENV, "http://localhost:8080/cloud-mcp")

    assert form._server_origin() == "http://localhost:8080"
    assert form._intake_endpoint() == "http://localhost:8080/cloud-mcp/secret-intake"


def test_form_blocks_prototype_pollution_paths() -> None:
    html = form.connector_config_form_resource()

    assert '["__proto__", "constructor", "prototype"]' in html


def test_form_handles_one_of_schema_without_plaintext_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = {
        "type": "object",
        "properties": {
            "credentials": {
                "type": "object",
                "oneOf": [
                    {
                        "properties": {
                            "api_key": {"type": "string", "airbyte_secret": True},
                        }
                    }
                ],
            }
        },
    }
    monkeypatch.setattr(
        form, "get_connector_spec_from_registry", lambda *args, **kwargs: schema
    )

    result = form.show_connector_config_form("source-example")

    assert result.structured_content["secret_fields"] == ["credentials.api_key"]
    html = form.connector_config_form_resource()
    assert "Complex authentication objects are not supported by this form." in html
    assert "Array.isArray(child[key])" in html
