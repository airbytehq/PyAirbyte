# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for deferred Cloud connector authentication."""

from __future__ import annotations

import json
from typing import Any

import pytest

from airbyte.exceptions import AirbyteMissingWorkspaceContextError, PyAirbyteInputError
from airbyte.mcp import cloud as cloud_mcp
from airbyte.mcp._deferred_auth import (
    SECRET_PLACEHOLDER,
    _schema_secret_paths,
    _stub_missing_secrets,
)


def _google_sheets_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "spreadsheet_id": {"type": "string"},
            "credentials": {
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {
                            "auth_type": {"const": "Client"},
                            "client_id": {
                                "type": "string",
                                "airbyte_secret": True,
                            },
                            "client_secret": {
                                "type": "string",
                                "airbyte_secret": True,
                            },
                            "refresh_token": {
                                "type": "string",
                                "airbyte_secret": True,
                            },
                        },
                    }
                ]
            },
        },
    }


def _enum_auth_schema() -> dict[str, Any]:
    return {
        "oneOf": [
            {
                "type": "object",
                "properties": {
                    "auth_type": {"enum": ["Client"]},
                    "client_id": {"type": "string", "airbyte_secret": True},
                },
            },
            {
                "type": "object",
                "properties": {
                    "auth_type": {"enum": ["Service"]},
                    "service_account": {"type": "string", "airbyte_secret": True},
                },
            },
        ]
    }


def test_deferred_auth_rejects_secret_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cloud_mcp,
        "get_connector_spec_from_registry",
        lambda *args, **kwargs: _google_sheets_schema(),
    )

    with pytest.raises(
        PyAirbyteInputError, match="Secret values cannot be provided in config"
    ):
        cloud_mcp.create_source_with_deferred_auth(
            object(),
            "Google Sheets",
            "source-google-sheets",
            config={"credentials": {"auth_type": "Client", "client_id": "secret"}},
        )


def test_deferred_auth_accepts_json_config_and_rejects_invalid_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cloud_mcp,
        "get_connector_spec_from_registry",
        lambda *args, **kwargs: _google_sheets_schema(),
    )
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: (_ for _ in ()).throw(
            AirbyteMissingWorkspaceContextError()
        ),
    )
    with pytest.raises(AirbyteMissingWorkspaceContextError):
        cloud_mcp.create_source_with_deferred_auth(
            object(),
            "Google Sheets",
            "source-google-sheets",
            config='{"spreadsheet_id":"sheet-id"}',
        )

    with pytest.raises(PyAirbyteInputError, match="config must be a JSON object"):
        cloud_mcp.create_source_with_deferred_auth(
            object(),
            "Google Sheets",
            "source-google-sheets",
            config="{",
        )


def test_stub_missing_secrets_matches_single_value_enum_branch() -> None:
    result = _stub_missing_secrets({"auth_type": "Client"}, _enum_auth_schema())

    assert result == {
        "auth_type": "Client",
        "client_id": SECRET_PLACEHOLDER,
    }


def test_stub_missing_secrets_leaves_unknown_branch_unchanged() -> None:
    schema = _enum_auth_schema()
    value = {"auth_type": "Unknown"}

    assert _stub_missing_secrets(value, schema) == value


def test_stub_missing_secrets_preserves_present_secret() -> None:
    result = _stub_missing_secrets(
        {
            "spreadsheet_id": "sheet-id",
            "credentials": {
                "auth_type": "Client",
                "client_id": "already-present",
            },
        },
        _google_sheets_schema(),
    )

    assert result == {
        "spreadsheet_id": "sheet-id",
        "credentials": {
            "auth_type": "Client",
            "client_id": "already-present",
            "client_secret": SECRET_PLACEHOLDER,
            "refresh_token": SECRET_PLACEHOLDER,
        },
    }


def test_schema_secret_paths_include_nested_branch_secrets() -> None:
    assert _schema_secret_paths(_google_sheets_schema()) == {
        "credentials.client_id",
        "credentials.client_secret",
        "credentials.refresh_token",
    }


def test_deferred_auth_creates_source_with_safe_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = {"credentials": {"auth_type": "Client"}, "spreadsheet_id": "sheet-id"}
    captured: dict[str, Any] = {}

    class Source:
        def set_config(self, value: dict[str, Any], *, validate: bool) -> None:
            captured["config"] = value
            captured["validate"] = validate

    class Workspace:
        def deploy_source(self, *, name: str, source: Source, unique: bool) -> Any:
            captured["name"] = name
            captured["source"] = source
            captured["unique"] = unique
            return type(
                "DeployedSource",
                (),
                {
                    "connector_id": "source-id",
                    "connector_url": "https://cloud.airbyte.com/workspaces/ws/source/source-id",
                },
            )()

    monkeypatch.setattr(
        cloud_mcp,
        "get_connector_spec_from_registry",
        lambda *args, **kwargs: _google_sheets_schema(),
    )
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: Workspace(),
    )
    monkeypatch.setattr(cloud_mcp, "get_source", lambda *args, **kwargs: Source())

    result = cloud_mcp.create_source_with_deferred_auth(
        object(),
        "Google Sheets",
        "source-google-sheets",
        config=config,
        workspace_id="ws",
    )

    assert captured["config"] == {
        "credentials": {
            "auth_type": "Client",
            "client_id": SECRET_PLACEHOLDER,
            "client_secret": SECRET_PLACEHOLDER,
            "refresh_token": SECRET_PLACEHOLDER,
        },
        "spreadsheet_id": "sheet-id",
    }
    assert captured["validate"] is False
    assert captured["name"] == "Google Sheets"
    assert captured["unique"] is True
    assert result.id == "source-id"
    assert result.name == "Google Sheets"
    assert result.url == "https://cloud.airbyte.com/workspaces/ws/source/source-id"
    assert result.non_secret_config == config
    assert "complete authentication" in result.note
    serialized = result.model_dump_json()
    assert "__airbyte_placeholder__" not in serialized
    assert "client-secret" not in serialized
    assert "bearer-token" not in serialized
    assert json.loads(serialized)["non_secret_config"] == config


def test_deferred_auth_requires_workspace_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cloud_mcp,
        "get_connector_spec_from_registry",
        lambda *args, **kwargs: _google_sheets_schema(),
    )
    monkeypatch.setattr(cloud_mcp, "get_mcp_config", lambda *args, **kwargs: None)

    with pytest.raises(AirbyteMissingWorkspaceContextError):
        cloud_mcp.create_source_with_deferred_auth(
            object(),
            "Google Sheets",
            "source-google-sheets",
        )
