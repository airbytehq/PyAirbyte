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
