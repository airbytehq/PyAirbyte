# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for deferred Cloud connector authentication."""

from __future__ import annotations

import json
from typing import Any

import pytest

from airbyte.cloud import workspaces as cloud_workspaces
from airbyte.cloud._deferred_auth import (
    SECRET_PLACEHOLDER,
    _schema_secret_paths,
    _stub_missing_secrets,
)
from airbyte.destinations.base import Destination as DestinationBase
from airbyte.exceptions import AirbyteMissingWorkspaceContextError, PyAirbyteInputError
from airbyte.mcp import cloud as cloud_mcp
from airbyte.sources.base import Source as SourceBase


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


def _workspace() -> cloud_workspaces.CloudWorkspace:
    workspace = object.__new__(cloud_workspaces.CloudWorkspace)
    workspace.api_root = "https://api.airbyte.com"
    workspace.workspace_id = "workspace-id"
    workspace.client_id = None
    workspace.client_secret = None
    workspace.bearer_token = None
    return workspace


def test_deferred_auth_rejects_secret_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = object.__new__(SourceBase)
    source._name = "source-google-sheets"  # noqa: SLF001
    source._config_dict = {  # noqa: SLF001
        "credentials": {"auth_type": "Client", "client_id": "secret"}
    }
    workspace = _workspace()

    monkeypatch.setattr(
        cloud_workspaces,
        "get_connector_spec_from_registry",
        lambda *args, **kwargs: _google_sheets_schema(),
    )

    with pytest.raises(
        PyAirbyteInputError, match="Secret values cannot be provided in config"
    ):
        workspace.deploy_source(
            name="Google Sheets",
            source=source,
            unique=False,
            deferred_auth=True,
        )


def test_deferred_auth_accepts_json_config_and_rejects_invalid_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Source:
        def set_config(self, value: dict[str, Any], *, validate: bool) -> None:
            assert value == {"spreadsheet_id": "sheet-id"}
            assert validate is False

    monkeypatch.setattr(cloud_mcp, "get_source", lambda *args, **kwargs: Source())
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: (_ for _ in ()).throw(
            AirbyteMissingWorkspaceContextError()
        ),
    )
    with pytest.raises(AirbyteMissingWorkspaceContextError):
        cloud_mcp.deploy_source_to_cloud(
            object(),
            "Google Sheets",
            "source-google-sheets",
            workspace_id=None,
            config='{"spreadsheet_id":"sheet-id"}',
            config_secret_name=None,
            unique=True,
            deferred_auth=True,
        )

    with pytest.raises(PyAirbyteInputError, match="config must be a JSON object"):
        cloud_mcp.deploy_source_to_cloud(
            object(),
            "Google Sheets",
            "source-google-sheets",
            workspace_id=None,
            config="{",
            config_secret_name=None,
            unique=True,
            deferred_auth=True,
        )


def test_stub_missing_secrets_matches_single_value_enum_branch() -> None:
    result = _stub_missing_secrets({"auth_type": "Client"}, _enum_auth_schema())

    assert result == {
        "auth_type": "Client",
        "client_id": SECRET_PLACEHOLDER,
    }


def test_stub_missing_secrets_matches_null_const_branch() -> None:
    schema = {
        "oneOf": [
            {
                "properties": {
                    "auth_type": {"const": None},
                    "client_id": {"airbyte_secret": True},
                }
            }
        ]
    }

    assert _stub_missing_secrets({"auth_type": None}, schema) == {
        "auth_type": None,
        "client_id": SECRET_PLACEHOLDER,
    }


def test_stub_missing_secrets_does_not_match_omitted_null_const() -> None:
    schema = {
        "oneOf": [
            {
                "properties": {
                    "auth_type": {"const": None},
                    "client_id": {"airbyte_secret": True},
                }
            }
        ]
    }

    assert _stub_missing_secrets({}, schema) == {}


def test_stub_missing_secrets_handles_all_of() -> None:
    schema = {
        "allOf": [
            {
                "properties": {
                    "api_key": {"airbyte_secret": True},
                }
            }
        ]
    }

    assert _stub_missing_secrets({}, schema) == {"api_key": SECRET_PLACEHOLDER}


def test_stub_missing_secrets_handles_arrays() -> None:
    schema = {
        "properties": {
            "credentials": {
                "type": "array",
                "items": {
                    "properties": {
                        "api_key": {"airbyte_secret": True},
                    }
                },
            }
        }
    }

    assert _stub_missing_secrets({"credentials": [{}]}, schema) == {
        "credentials": [{"api_key": SECRET_PLACEHOLDER}]
    }


def test_stub_missing_secrets_stubs_selected_branch_and_parent_properties() -> None:
    schema = {
        "properties": {
            "api_key": {"airbyte_secret": True},
            "credentials": {
                "oneOf": [
                    {
                        "properties": {
                            "auth_type": {"const": "Client"},
                            "client_id": {"airbyte_secret": True},
                        }
                    }
                ]
            },
        }
    }
    value = {"credentials": {"auth_type": "Client"}}

    assert _stub_missing_secrets(value, schema) == {
        "api_key": SECRET_PLACEHOLDER,
        "credentials": {
            "auth_type": "Client",
            "client_id": SECRET_PLACEHOLDER,
        },
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


def test_stub_missing_secrets_preserves_empty_secret_value() -> None:
    schema = {
        "properties": {
            "api_key": {"airbyte_secret": True},
        }
    }

    assert _stub_missing_secrets({"api_key": ""}, schema) == {"api_key": ""}


def test_schema_secret_paths_include_nested_branch_secrets() -> None:
    assert _schema_secret_paths(_google_sheets_schema()) == {
        "credentials.client_id",
        "credentials.client_secret",
        "credentials.refresh_token",
    }


def test_workspace_deferred_auth_stubs_source_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = object.__new__(SourceBase)
    source._name = "source-google-sheets"  # noqa: SLF001
    source._config_dict = {  # noqa: SLF001
        "credentials": {"auth_type": "Client"}
    }
    workspace = _workspace()
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        cloud_workspaces,
        "get_connector_spec_from_registry",
        lambda *args, **kwargs: _google_sheets_schema(),
    )

    def create_source(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return type("SourceResponse", (), {"source_id": "source-id"})()

    monkeypatch.setattr(cloud_workspaces.api_util, "create_source", create_source)

    deployed = workspace.deploy_source(
        name="Google Sheets",
        source=source,
        unique=False,
        deferred_auth=True,
    )

    assert captured["config"] == {
        "credentials": {
            "auth_type": "Client",
            "client_id": SECRET_PLACEHOLDER,
            "client_secret": SECRET_PLACEHOLDER,
            "refresh_token": SECRET_PLACEHOLDER,
        },
        "sourceType": "google-sheets",
    }
    assert deployed.connector_id == "source-id"


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
        def deploy_source(
            self,
            *,
            name: str,
            source: Source,
            unique: bool,
            deferred_auth: bool,
        ) -> Any:
            captured["name"] = name
            captured["source"] = source
            captured["unique"] = unique
            captured["deferred_auth"] = deferred_auth
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
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: Workspace(),
    )
    monkeypatch.setattr(cloud_mcp, "get_source", lambda *args, **kwargs: Source())

    result = cloud_mcp.deploy_source_to_cloud(
        object(),
        "Google Sheets",
        "source-google-sheets",
        config=config,
        workspace_id="ws",
        config_secret_name=None,
        unique=True,
        deferred_auth=True,
    )

    assert captured["config"] == config
    assert captured["validate"] is False
    assert captured["name"] == "Google Sheets"
    assert captured["unique"] is True
    assert captured["deferred_auth"] is True
    assert result.startswith(
        "Successfully deployed source 'Google Sheets' with ID 'source-id'"
    )
    assert "Source created without working credentials (deferred auth)." in result
    assert SECRET_PLACEHOLDER not in result
    serialized = json.dumps(result)
    assert "__airbyte_placeholder__" not in serialized
    assert "client-secret" not in serialized
    assert "bearer-token" not in serialized


def test_deferred_auth_rejects_config_secret_name() -> None:
    with pytest.raises(
        PyAirbyteInputError,
        match="config_secret_name cannot be used with deferred_auth",
    ):
        cloud_mcp.deploy_source_to_cloud(
            object(),
            "Google Sheets",
            "source-google-sheets",
            workspace_id=None,
            config=None,
            config_secret_name="source-config",
            unique=True,
            deferred_auth=True,
        )


def test_deferred_auth_rejects_destination_secret_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = object.__new__(DestinationBase)
    destination._name = "destination-postgres"  # noqa: SLF001
    destination._config_dict = {  # noqa: SLF001
        "credentials": {"auth_type": "Client", "client_id": "secret"}
    }
    workspace = _workspace()

    monkeypatch.setattr(
        cloud_workspaces,
        "get_connector_spec_from_registry",
        lambda *args, **kwargs: _google_sheets_schema(),
    )

    with pytest.raises(
        PyAirbyteInputError, match="Secret values cannot be provided in config"
    ):
        workspace.deploy_destination(
            name="Postgres",
            destination=destination,
            unique=False,
            deferred_auth=True,
        )


def test_deferred_auth_rejects_destination_config_secret_name() -> None:
    with pytest.raises(
        PyAirbyteInputError,
        match="config_secret_name cannot be used with deferred_auth",
    ):
        cloud_mcp.deploy_destination_to_cloud(
            object(),
            "Postgres",
            "destination-postgres",
            workspace_id=None,
            config=None,
            config_secret_name="destination-config",
            unique=True,
            deferred_auth=True,
        )


def test_deferred_auth_creates_destination_with_safe_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = {"credentials": {"auth_type": "Client"}, "database": "warehouse"}
    captured: dict[str, Any] = {}

    class Destination:
        def set_config(self, value: dict[str, Any], *, validate: bool) -> None:
            captured["config"] = value
            captured["validate"] = validate

    class Workspace:
        def deploy_destination(
            self,
            *,
            name: str,
            destination: Destination,
            unique: bool,
            deferred_auth: bool,
        ) -> Any:
            captured["name"] = name
            captured["destination"] = destination
            captured["unique"] = unique
            captured["deferred_auth"] = deferred_auth
            return type(
                "DeployedDestination",
                (),
                {
                    "connector_id": "destination-id",
                    "connector_url": "https://cloud.airbyte.com/destinations/destination-id",
                },
            )()

    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: Workspace(),
    )
    monkeypatch.setattr(
        cloud_mcp,
        "get_destination",
        lambda *args, **kwargs: Destination(),
    )

    result = cloud_mcp.deploy_destination_to_cloud(
        object(),
        "Postgres",
        "destination-postgres",
        config=config,
        workspace_id="ws",
        config_secret_name=None,
        unique=True,
        deferred_auth=True,
    )

    assert captured["config"] == config
    assert captured["validate"] is False
    assert captured["name"] == "Postgres"
    assert captured["unique"] is True
    assert captured["deferred_auth"] is True
    assert result.startswith(
        "Successfully deployed destination 'Postgres' with ID 'destination-id' "
        "and URL: https://cloud.airbyte.com/destinations/destination-id"
    )
    assert "Destination created without working credentials (deferred auth)." in result
    assert SECRET_PLACEHOLDER not in result
    assert "__airbyte_placeholder__" not in json.dumps(result)
    assert "client-secret" not in result
    assert "bearer-token" not in result


def test_deploy_source_without_deferred_auth_has_no_note(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Source:
        config_spec: dict[str, Any] = {}

        def set_config(self, value: dict[str, Any], *, validate: bool) -> None:
            assert value == {}
            assert validate is True

    class Workspace:
        def deploy_source(
            self,
            *,
            name: str,
            source: Source,
            unique: bool,
            deferred_auth: bool,
        ) -> Any:
            assert name == "Source"
            assert unique is True
            assert deferred_auth is False
            return type(
                "DeployedSource",
                (),
                {
                    "connector_id": "source-id",
                    "connector_url": "https://cloud.airbyte.com/source/source-id",
                },
            )()

    monkeypatch.setattr(cloud_mcp, "get_source", lambda *args, **kwargs: Source())
    monkeypatch.setattr(
        cloud_mcp,
        "_get_cloud_workspace",
        lambda ctx, workspace_id=None: Workspace(),
    )

    result = cloud_mcp.deploy_source_to_cloud(
        object(),
        "Source",
        "source-faker",
        workspace_id=None,
        config=None,
        config_secret_name=None,
        unique=True,
    )

    assert "deferred auth" not in result


def test_deferred_auth_requires_workspace_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Source:
        def set_config(self, value: dict[str, Any], *, validate: bool) -> None:
            assert value == {}
            assert validate is False

    monkeypatch.setattr(cloud_mcp, "get_source", lambda *args, **kwargs: Source())
    monkeypatch.setattr(cloud_mcp, "get_mcp_config", lambda *args, **kwargs: None)

    with pytest.raises(AirbyteMissingWorkspaceContextError):
        cloud_mcp.deploy_source_to_cloud(
            object(),
            "Google Sheets",
            "source-google-sheets",
            workspace_id=None,
            config=None,
            config_secret_name=None,
            unique=True,
            deferred_auth=True,
        )
