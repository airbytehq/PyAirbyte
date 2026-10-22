# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for editing Cloud connection catalogs."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from airbyte._util import api_util
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.workspaces import CloudWorkspace


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


def test_import_raw_catalog_clears_cached_connection_info(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Importing a catalog invalidates previously cached connection information."""
    connection = _connection()
    stale_connection_info = MagicMock()
    stale_connection_info.configurations.streams = [MagicMock(name="old-stream")]
    connection._connection_info = stale_connection_info
    replace_catalog = MagicMock(return_value={})
    monkeypatch.setattr(api_util, "replace_connection_catalog", replace_catalog)

    connection.import_raw_catalog(_catalog(_stream_entry("new-stream")))

    replace_catalog.assert_called_once()
    assert connection._connection_info is None
