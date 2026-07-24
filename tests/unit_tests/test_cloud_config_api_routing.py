# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for routing Cloud reads through the Config API for bearer-only auth.

Interactive OIDC logins produce a user-realm bearer token with no client credentials.
Such tokens are rejected by the public API but accepted by the Config API, so metadata
reads must route through the Config API in that case while continuing to use the public
API for application-client credentials.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from airbyte._util import api_util
from airbyte.cloud import CloudWorkspace
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.models import CloudConnectionInfo, CloudWorkspaceInfo
from airbyte.secrets.base import SecretString


WORKSPACE_ID = "266ebdfe-0d7b-4540-9817-de7e4505ba61"
CONNECTION_ID = "991db3a4-c432-4aa9-8e10-31b74921d4b5"
SOURCE_ID = "11111111-1111-1111-1111-111111111111"
DESTINATION_ID = "22222222-2222-2222-2222-222222222222"


def _web_backend_connection_payload() -> dict[str, Any]:
    """Return a minimal `WebBackendConnectionRead`-shaped Config API response."""
    return {
        "connectionId": CONNECTION_ID,
        "name": "My Postgres to Snowflake",
        "sourceId": SOURCE_ID,
        "destinationId": DESTINATION_ID,
        "prefix": "raw_",
        "status": "active",
        "source": {
            "sourceId": SOURCE_ID,
            "workspaceId": WORKSPACE_ID,
            "name": "My Postgres",
            "sourceName": "Postgres",
        },
        "destination": {
            "destinationId": DESTINATION_ID,
            "workspaceId": WORKSPACE_ID,
            "name": "My Snowflake",
            "destinationName": "Snowflake",
        },
        "syncCatalog": {
            "streams": [
                {"stream": {"name": "users"}, "config": {"selected": True}},
                {"stream": {"name": "orders"}, "config": {"selected": True}},
            ]
        },
    }


@pytest.mark.parametrize(
    "client_id,client_secret,bearer_token,expected",
    [
        pytest.param(None, None, "token", True, id="bearer_only"),
        pytest.param("id", "secret", None, False, id="client_credentials"),
    ],
)
def test_uses_bearer_only_auth(
    client_id: str | None,
    client_secret: str | None,
    bearer_token: str | None,
    expected: bool,
) -> None:
    workspace = CloudWorkspace(
        workspace_id=WORKSPACE_ID,
        client_id=client_id,
        client_secret=client_secret,
        bearer_token=bearer_token,
    )
    assert workspace._uses_bearer_only_auth is expected


def test_workspace_info_from_config_api_mapping() -> None:
    info = CloudWorkspaceInfo.from_mapping({
        "workspaceId": WORKSPACE_ID,
        "name": "Acme Workspace",
        "organizationId": "org-123",
        "slug": "acme",
        "initialSetupComplete": True,
    })
    assert info.workspace_id == WORKSPACE_ID
    assert info.name == "Acme Workspace"
    assert info.organization_id == "org-123"


def test_connection_info_from_config_api_response() -> None:
    info = CloudConnectionInfo.from_config_api_response(
        _web_backend_connection_payload()
    )

    assert info.connection_id == CONNECTION_ID
    assert info.workspace_id == WORKSPACE_ID
    assert info.source_id == SOURCE_ID
    assert info.destination_id == DESTINATION_ID
    assert info.name == "My Postgres to Snowflake"
    assert info.prefix == "raw_"
    assert info.status == "active"
    assert info.source_name == "My Postgres"
    assert info.destination_name == "My Snowflake"
    assert info.stream_names == ["users", "orders"]


def test_connection_info_from_config_api_response_uses_fallback_workspace_id() -> None:
    payload = _web_backend_connection_payload()
    payload["source"].pop("workspaceId")
    payload["destination"].pop("workspaceId")

    info = CloudConnectionInfo.from_config_api_response(
        payload,
        fallback_workspace_id=WORKSPACE_ID,
    )
    assert info.workspace_id == WORKSPACE_ID


def test_get_workspace_info_routes_bearer_only_to_config_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def _config_api(**_kwargs: Any) -> dict[str, Any]:
        calls.append("config")
        return {"workspaceId": WORKSPACE_ID, "name": "Acme"}

    def _public_api(**_kwargs: Any) -> object:
        calls.append("public")
        raise AssertionError("Public API should not be called for bearer-only auth.")

    monkeypatch.setattr(api_util, "get_workspace_via_config_api", _config_api)
    monkeypatch.setattr(api_util, "get_workspace", _public_api)

    workspace = CloudWorkspace(
        workspace_id=WORKSPACE_ID,
        bearer_token=SecretString("token"),
    )
    info = workspace.get_workspace_info()

    assert calls == ["config"]
    assert info.workspace_id == WORKSPACE_ID
    assert info.name == "Acme"


def test_get_workspace_info_routes_client_credentials_to_public_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def _config_api(**_kwargs: Any) -> dict[str, Any]:
        calls.append("config")
        raise AssertionError("Config API should not be called for client credentials.")

    def _public_api(**_kwargs: Any) -> object:
        calls.append("public")
        return SimpleNamespace(
            workspace_id=WORKSPACE_ID,
            name="Acme",
            data_residency=None,
            organization_id="org-123",
            notifications=None,
        )

    monkeypatch.setattr(api_util, "get_workspace_via_config_api", _config_api)
    monkeypatch.setattr(api_util, "get_workspace", _public_api)

    workspace = CloudWorkspace(
        workspace_id=WORKSPACE_ID,
        client_id=SecretString("id"),
        client_secret=SecretString("secret"),
    )
    info = workspace.get_workspace_info()

    assert calls == ["public"]
    assert info.workspace_id == WORKSPACE_ID


def test_fetch_connection_info_routes_bearer_only_to_config_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def _config_api(**_kwargs: Any) -> dict[str, Any]:
        calls.append("config")
        return _web_backend_connection_payload()

    def _public_api(**_kwargs: Any) -> object:
        calls.append("public")
        raise AssertionError("Public API should not be called for bearer-only auth.")

    monkeypatch.setattr(api_util, "get_connection_via_config_api", _config_api)
    monkeypatch.setattr(api_util, "get_connection", _public_api)

    workspace = CloudWorkspace(
        workspace_id=WORKSPACE_ID,
        bearer_token=SecretString("token"),
    )
    connection = CloudConnection(workspace=workspace, connection_id=CONNECTION_ID)

    assert connection.source_name == "My Postgres"
    assert connection.destination_name == "My Snowflake"
    assert connection.stream_names == ["users", "orders"]
    assert connection.table_prefix == "raw_"
    assert calls == ["config"]
