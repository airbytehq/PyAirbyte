# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for editing and refreshing Cloud connection catalogs."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from airbyte._util import api_util
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteLibInputError


_CONNECTION_ID = "connection-id"
_API_ROOT = "https://api.example/v1"
_CONFIG_API_ROOT = "https://config.example/v1"


def _workspace() -> CloudWorkspace:
    return CloudWorkspace(
        workspace_id="workspace-id",
        bearer_token="token",
        api_root=_API_ROOT,
        config_api_root=_CONFIG_API_ROOT,
    )


def _connection() -> CloudConnection:
    return CloudConnection(workspace=_workspace(), connection_id=_CONNECTION_ID)


def _stream_entry(
    name: str,
    *,
    namespace: str | None = None,
) -> dict[str, Any]:
    stream: dict[str, Any] = {
        "name": name,
        "jsonSchema": {"type": "object"},
        "supportedSyncModes": ["full_refresh", "incremental"],
    }
    if namespace is not None:
        stream["namespace"] = namespace
    return {
        "stream": stream,
        "config": {
            "syncMode": "full_refresh",
            "destinationSyncMode": "append",
            "selectedFields": [],
            "fieldSelectionEnabled": False,
            "primaryKey": [["id"]],
            "selected": True,
        },
    }


def _catalog(*entries: dict[str, Any]) -> dict[str, Any]:
    return {"streams": list(entries)}


def test_refresh_catalog_gets_and_replaces_catalog_with_config_api_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refresh the source catalog and persist the raw result."""
    refreshed_catalog = _catalog(_stream_entry("users", namespace="public"))
    get_refreshed = MagicMock(return_value={"syncCatalog": refreshed_catalog})
    replace_catalog = MagicMock(return_value={})
    monkeypatch.setattr(api_util, "get_refreshed_connection_catalog", get_refreshed)
    monkeypatch.setattr(api_util, "replace_connection_catalog", replace_catalog)

    result = _connection().refresh_catalog()

    assert result == refreshed_catalog
    get_refreshed.assert_called_once_with(
        connection_id=_CONNECTION_ID,
        api_root=_API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token="token",
        config_api_root=_CONFIG_API_ROOT,
    )
    replace_catalog.assert_called_once_with(
        connection_id=_CONNECTION_ID,
        configured_catalog_dict=refreshed_catalog,
        api_root=_API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token="token",
        config_api_root=_CONFIG_API_ROOT,
    )


@pytest.mark.parametrize(
    "response",
    [
        pytest.param({}, id="missing-sync-catalog"),
        pytest.param({"syncCatalog": None}, id="null-sync-catalog"),
        pytest.param({"syncCatalog": {"streams": []}}, id="empty-sync-catalog"),
    ],
)
def test_refresh_catalog_rejects_empty_or_missing_catalog(
    monkeypatch: pytest.MonkeyPatch,
    response: dict[str, Any],
) -> None:
    """An absent or empty refreshed catalog is never written."""
    get_refreshed = MagicMock(return_value=response)
    replace_catalog = MagicMock(return_value={})
    monkeypatch.setattr(api_util, "get_refreshed_connection_catalog", get_refreshed)
    monkeypatch.setattr(api_util, "replace_connection_catalog", replace_catalog)

    with pytest.raises(
        AirbyteLibInputError, match="Refreshed catalog is empty or missing"
    ):
        _connection().refresh_catalog()

    replace_catalog.assert_not_called()


def test_get_refreshed_connection_catalog_forwards_config_api_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Request discovery through the Config API catalog endpoint."""
    request = MagicMock(return_value={"syncCatalog": {"streams": []}})
    monkeypatch.setattr(api_util, "_make_config_api_request", request)

    result = api_util.get_refreshed_connection_catalog(
        connection_id=_CONNECTION_ID,
        api_root=_API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token="token",
        config_api_root=_CONFIG_API_ROOT,
    )

    assert result == {"syncCatalog": {"streams": []}}
    request.assert_called_once_with(
        path="/web_backend/connections/get",
        json={"connectionId": _CONNECTION_ID, "withRefreshedCatalog": True},
        api_root=_API_ROOT,
        config_api_root=_CONFIG_API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token="token",
    )
