# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for editing and refreshing Cloud connection catalogs."""

from __future__ import annotations

from copy import deepcopy
from typing import Any
from unittest.mock import MagicMock

import pytest
from airbyte_protocol.models import DestinationSyncMode, SyncMode

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
    sync_modes: list[str] | None = None,
    source_defined_cursor: bool = False,
    default_cursor_field: list[str] | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    stream: dict[str, Any] = {
        "name": name,
        "jsonSchema": {"type": "object"},
        "supportedSyncModes": sync_modes or ["full_refresh", "incremental"],
    }
    if namespace is not None:
        stream["namespace"] = namespace
    if source_defined_cursor:
        stream["sourceDefinedCursor"] = True
    if default_cursor_field is not None:
        stream["defaultCursorField"] = default_cursor_field
    return {
        "stream": stream,
        "config": config
        or {
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


def _mock_catalog_api(
    monkeypatch: pytest.MonkeyPatch,
    catalog: dict[str, Any],
) -> tuple[MagicMock, MagicMock]:
    get_catalog = MagicMock(return_value={"syncCatalog": catalog})
    replace_catalog = MagicMock(return_value={})
    monkeypatch.setattr(api_util, "get_connection_catalog", get_catalog)
    monkeypatch.setattr(api_util, "replace_connection_catalog", replace_catalog)
    return get_catalog, replace_catalog


def test_set_stream_sync_mode_updates_only_target_and_saves_raw_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Update the target stream without normalizing or changing other catalog data."""
    target = _stream_entry("users", namespace="public")
    other = _stream_entry(
        "orders",
        namespace="sales",
        config={
            "syncMode": "incremental",
            "destinationSyncMode": "append_dedup",
            "cursorField": ["changed_at"],
            "selectedFields": [{"fieldPath": ["id"]}],
            "fieldSelectionEnabled": True,
            "primaryKey": [["order_id"]],
            "selected": False,
        },
    )
    original_other = deepcopy(other)
    catalog = _catalog(target, other)
    get_catalog, replace_catalog = _mock_catalog_api(monkeypatch, catalog)

    _connection().set_stream_sync_mode(
        stream_name="users",
        stream_namespace="public",
        sync_mode=SyncMode.incremental,
        destination_sync_mode=DestinationSyncMode.append_dedup,
        cursor_field="updated_at",
    )

    get_catalog.assert_called_once_with(
        connection_id=_CONNECTION_ID,
        api_root=_API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token="token",
        config_api_root=_CONFIG_API_ROOT,
    )
    assert target["config"] == {
        "syncMode": "incremental",
        "destinationSyncMode": "append_dedup",
        "cursorField": ["updated_at"],
        "selectedFields": [],
        "fieldSelectionEnabled": False,
        "primaryKey": [["id"]],
        "selected": True,
    }
    assert other == original_other

    replace_catalog.assert_called_once()
    saved_catalog = replace_catalog.call_args.kwargs["configured_catalog_dict"]
    assert saved_catalog == catalog
    assert saved_catalog["streams"][1] == original_other
    assert "sync_mode" not in saved_catalog["streams"][0]["config"]
    assert "supported_sync_modes" not in saved_catalog["streams"][0]["stream"]
    assert replace_catalog.call_args.kwargs["config_api_root"] == _CONFIG_API_ROOT


@pytest.mark.parametrize(
    (
        "catalog",
        "stream_name",
        "stream_namespace",
        "sync_mode",
        "cursor_field",
        "error_message",
        "expected_context",
    ),
    [
        pytest.param(
            _catalog(_stream_entry("users", namespace="public")),
            "missing",
            None,
            SyncMode.full_refresh,
            None,
            "not found",
            {"available_streams": ["public.users"]},
            id="missing-stream",
        ),
        pytest.param(
            _catalog(
                _stream_entry("users", namespace="public"),
                _stream_entry("users", namespace="sales"),
            ),
            "users",
            None,
            SyncMode.full_refresh,
            None,
            "ambiguous",
            {"matching_namespaces": ["public", "sales"]},
            id="ambiguous-name",
        ),
        pytest.param(
            _catalog(
                _stream_entry("users", namespace="public", sync_modes=["full_refresh"])
            ),
            "users",
            "public",
            SyncMode.incremental,
            None,
            "not supported",
            {"requested_sync_mode": "incremental"},
            id="unsupported-sync-mode",
        ),
        pytest.param(
            _catalog(
                _stream_entry(
                    "users",
                    namespace="public",
                    config={
                        "syncMode": "full_refresh",
                        "destinationSyncMode": "append",
                    },
                )
            ),
            "users",
            "public",
            SyncMode.incremental,
            None,
            "needs a cursor",
            {"stream_namespace": "public"},
            id="incremental-without-cursor",
        ),
    ],
)
def test_set_stream_sync_mode_rejects_invalid_updates(
    monkeypatch: pytest.MonkeyPatch,
    catalog: dict[str, Any],
    stream_name: str,
    stream_namespace: str | None,
    sync_mode: SyncMode,
    cursor_field: str | None,
    error_message: str,
    expected_context: dict[str, Any],
) -> None:
    """Input errors leave the Config API catalog untouched."""
    _, replace_catalog = _mock_catalog_api(monkeypatch, deepcopy(catalog))

    with pytest.raises(AirbyteLibInputError, match=error_message) as exc_info:
        _connection().set_stream_sync_mode(
            stream_name=stream_name,
            stream_namespace=stream_namespace,
            sync_mode=sync_mode,
            cursor_field=cursor_field,
        )

    assert exc_info.value.context is not None
    for key, value in expected_context.items():
        assert exc_info.value.context[key] == value
    replace_catalog.assert_not_called()


def test_set_stream_sync_mode_namespace_disambiguates_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A namespace selects the intended same-named stream."""
    public = _stream_entry(
        "users",
        namespace="public",
        config={
            "syncMode": "incremental",
            "destinationSyncMode": "append",
            "cursorField": ["updated_at"],
        },
    )
    sales = _stream_entry("users", namespace="sales")
    catalog = _catalog(public, sales)
    _, replace_catalog = _mock_catalog_api(monkeypatch, catalog)

    _connection().set_stream_sync_mode(
        stream_name="users",
        stream_namespace="sales",
        sync_mode=SyncMode.full_refresh,
    )

    assert public["config"]["syncMode"] == "incremental"
    assert sales["config"]["syncMode"] == "full_refresh"
    replace_catalog.assert_called_once()


@pytest.mark.parametrize(
    "stream_options",
    [
        pytest.param({"source_defined_cursor": True}, id="source-defined-cursor"),
        pytest.param(
            {"default_cursor_field": ["updated_at"]}, id="default-cursor-field"
        ),
    ],
)
def test_incremental_sync_accepts_stream_defined_cursor(
    monkeypatch: pytest.MonkeyPatch,
    stream_options: dict[str, Any],
) -> None:
    """Incremental sync can use a source-defined or default cursor without an override."""
    entry = _stream_entry(
        "users",
        namespace="public",
        config={"syncMode": "full_refresh", "destinationSyncMode": "append"},
        **stream_options,
    )
    _, replace_catalog = _mock_catalog_api(monkeypatch, _catalog(entry))

    _connection().set_stream_sync_mode(
        stream_name="users",
        stream_namespace="public",
        sync_mode=SyncMode.incremental,
    )

    assert entry["config"]["syncMode"] == "incremental"
    if "default_cursor_field" in stream_options:
        assert entry["config"]["cursorField"] == ["updated_at"]
    else:
        assert "cursorField" not in entry["config"]
    replace_catalog.assert_called_once()


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


def test_refresh_catalog_clears_cached_connection_info(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A refreshed catalog invalidates previously cached connection information."""
    connection = _connection()
    stale_connection_info = MagicMock()
    stale_connection_info.configurations.streams = [MagicMock(name="old-stream")]
    connection._connection_info = stale_connection_info
    refreshed_catalog = _catalog(_stream_entry("new-stream"))
    monkeypatch.setattr(
        api_util,
        "get_refreshed_connection_catalog",
        MagicMock(return_value={"syncCatalog": refreshed_catalog}),
    )
    replace_catalog = MagicMock(return_value={})
    monkeypatch.setattr(api_util, "replace_connection_catalog", replace_catalog)

    connection.refresh_catalog()

    replace_catalog.assert_called_once()
    assert connection._connection_info is None


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
