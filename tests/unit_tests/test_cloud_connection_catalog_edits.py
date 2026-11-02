# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for editing Cloud connection catalogs."""

from __future__ import annotations

from copy import deepcopy
from typing import Any
from unittest.mock import MagicMock

import pytest
from airbyte_protocol.models import (
    ConfiguredAirbyteCatalog,
    DestinationSyncMode,
    SyncMode,
)
from pydantic import ValidationError

from airbyte._util import api_util
from airbyte.cloud._connection_catalog import _normalize_catalog_to_protocol
from airbyte.cloud._stream_config_overrides import _find_unsafe_stream_changes
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.models import StreamConfigOverride
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import (
    AirbyteLibInputError,
    AirbyteUnsafeStreamConfigChangeError,
)


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
) -> tuple[MagicMock, MagicMock, MagicMock]:
    get_catalog = MagicMock(return_value={"syncCatalog": catalog})
    replace_catalog = MagicMock(return_value={})
    get_state = MagicMock(
        return_value={"stateType": "not_set", "connectionId": _CONNECTION_ID}
    )
    monkeypatch.setattr(api_util, "get_connection_catalog", get_catalog)
    monkeypatch.setattr(api_util, "replace_connection_catalog", replace_catalog)
    monkeypatch.setattr(api_util, "get_connection_state", get_state)
    return get_catalog, replace_catalog, get_state


def test_batch_overrides_write_once_and_preserve_untouched_catalog_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _stream_entry("users", namespace="public")
    other_target = _stream_entry(
        "orders",
        namespace="sales",
        config={
            "syncMode": "full_refresh",
            "destinationSyncMode": "append",
            "selectedFields": [],
            "fieldSelectionEnabled": False,
            "primaryKey": [["order_id"]],
            "selected": True,
        },
    )
    untouched = _stream_entry(
        "products",
        config={
            "syncMode": "incremental",
            "destinationSyncMode": "append_dedup",
            "cursorField": ["updated_at"],
            "selectedFields": [{"fieldPath": ["id"]}],
            "fieldSelectionEnabled": True,
            "primaryKey": [["sku"]],
            "selected": False,
        },
    )
    original_untouched = deepcopy(untouched)
    catalog = _catalog(target, other_target, untouched)
    _, replace_catalog, get_state = _mock_catalog_api(monkeypatch, catalog)

    result = _connection().update_stream_configs([
        StreamConfigOverride(
            stream_name="users",
            stream_namespace="public",
            sync_mode=SyncMode.incremental,
            destination_sync_mode=DestinationSyncMode.append_dedup,
            cursor_field="updated_at",
        ),
        StreamConfigOverride(
            stream_name="orders",
            stream_namespace="sales",
            destination_sync_mode=DestinationSyncMode.overwrite,
        ),
    ])

    replace_catalog.assert_called_once()
    saved_catalog = replace_catalog.call_args.kwargs["configured_catalog_dict"]
    assert saved_catalog["streams"][0]["config"]["cursorField"] == ["updated_at"]
    assert saved_catalog["streams"][0]["config"]["selectedFields"] == []
    assert saved_catalog["streams"][0]["config"]["fieldSelectionEnabled"] is False
    assert saved_catalog["streams"][0]["config"]["primaryKey"] == [["id"]]
    assert saved_catalog["streams"][0]["config"]["selected"] is True
    assert saved_catalog["streams"][2] == original_untouched
    assert "sync_mode" not in saved_catalog["streams"][0]["config"]
    assert "supported_sync_modes" not in saved_catalog["streams"][0]["stream"]
    assert replace_catalog.call_args.kwargs["skip_reset"] is True
    assert len(result.updated_streams) == 2
    get_state.assert_called_once()


def test_stream_config_override_round_trips_api_only_catalog_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    users = _stream_entry(
        "users",
        namespace="public",
        config={
            "syncMode": "full_refresh",
            "destinationSyncMode": "append",
            "cursorField": [],
            "primaryKey": [["id"]],
            "selected": True,
            "selectedFields": [],
            "fieldSelectionEnabled": False,
            "mappers": [{"name": "users_mapper", "config": {"field": "id"}}],
        },
    )
    users["stream"].update({
        "aliasName": "users_alias",
        "hashedFields": [{"path": ["id"]}],
        "suggested": True,
        "selected": True,
    })
    orders = _stream_entry(
        "orders",
        namespace="sales",
        config={
            "syncMode": "full_refresh",
            "destinationSyncMode": "append",
            "primaryKey": [["order_id"]],
            "selected": False,
            "selectedFields": [{"fieldPath": ["order_id"]}],
            "fieldSelectionEnabled": True,
            "mappers": [{"name": "orders_mapper", "config": {"field": "order_id"}}],
        },
    )
    orders["stream"].update({
        "aliasName": "orders_alias",
        "hashedFields": [{"path": ["order_id"]}],
        "suggested": False,
        "selected": False,
    })
    catalog = _catalog(users, orders)
    expected = deepcopy(catalog)
    expected["streams"][0]["config"]["syncMode"] = "incremental"
    expected["streams"][0]["config"]["cursorField"] = ["updated_at"]
    _, replace_catalog, _ = _mock_catalog_api(monkeypatch, catalog)

    _connection().update_stream_configs([
        StreamConfigOverride(
            stream_name="users",
            stream_namespace="public",
            sync_mode=SyncMode.incremental,
            cursor_field="updated_at",
        )
    ])

    assert replace_catalog.call_args.kwargs["configured_catalog_dict"] == expected


def test_namespace_selects_same_named_stream_and_sets_top_level_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    public = _stream_entry("users", namespace="public")
    sales = _stream_entry("users", namespace="sales")
    _, replace_catalog, _ = _mock_catalog_api(monkeypatch, _catalog(public, sales))

    _connection().update_stream_configs([
        StreamConfigOverride(
            stream_name="users",
            stream_namespace="sales",
            cursor_field="updated_at",
        )
    ])

    saved = replace_catalog.call_args.kwargs["configured_catalog_dict"]["streams"]
    assert saved[0]["config"].get("cursorField") is None
    assert saved[1]["config"]["cursorField"] == ["updated_at"]


def test_empty_namespace_selects_unnamespaced_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unnamespaced = _stream_entry("users")
    public = _stream_entry("users", namespace="public")
    _, replace_catalog, _ = _mock_catalog_api(
        monkeypatch,
        _catalog(unnamespaced, public),
    )

    _connection().update_stream_configs([
        StreamConfigOverride(
            stream_name="users",
            stream_namespace="",
            destination_sync_mode=DestinationSyncMode.overwrite,
        )
    ])

    saved = replace_catalog.call_args.kwargs["configured_catalog_dict"]["streams"]
    assert saved[0]["config"]["destinationSyncMode"] == "overwrite"
    assert saved[1]["config"]["destinationSyncMode"] == "append"


def test_cursor_override_rejects_nested_paths() -> None:
    with pytest.raises(ValidationError):
        StreamConfigOverride(
            stream_name="users",
            cursor_field=["nested", "updated_at"],
        )


def test_ambiguous_bare_name_is_rejected_without_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, replace_catalog, _ = _mock_catalog_api(
        monkeypatch,
        _catalog(
            _stream_entry("users", namespace="public"),
            _stream_entry("users", namespace="sales"),
        ),
    )

    with pytest.raises(AirbyteLibInputError, match="ambiguous"):
        _connection().update_stream_configs([
            StreamConfigOverride(stream_name="users", sync_mode=SyncMode.full_refresh)
        ])

    replace_catalog.assert_not_called()


def test_duplicate_catalog_stream_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, replace_catalog, _ = _mock_catalog_api(
        monkeypatch,
        _catalog(_stream_entry("users"), _stream_entry("users")),
    )

    with pytest.raises(AirbyteLibInputError, match="duplicate stream"):
        _connection().update_stream_configs([
            StreamConfigOverride(stream_name="users", sync_mode=SyncMode.incremental)
        ])

    replace_catalog.assert_not_called()


def test_stream_override_requires_at_least_one_setting() -> None:
    with pytest.raises(ValidationError, match="At least one"):
        StreamConfigOverride(stream_name="users")


def test_dedup_destination_mode_requires_a_primary_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = _stream_entry(
        "users",
        config={
            "syncMode": "full_refresh",
            "destinationSyncMode": "append",
            "primaryKey": [],
        },
    )
    _, replace_catalog, _ = _mock_catalog_api(monkeypatch, _catalog(entry))

    with pytest.raises(AirbyteLibInputError, match="requires a primary key"):
        _connection().update_stream_configs([
            StreamConfigOverride(
                stream_name="users",
                destination_sync_mode=DestinationSyncMode.append_dedup,
            )
        ])

    replace_catalog.assert_not_called()


def test_all_invalid_overrides_are_reported_before_writing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, replace_catalog, get_state = _mock_catalog_api(
        monkeypatch,
        _catalog(
            _stream_entry("users", sync_modes=["full_refresh"]),
            _stream_entry("orders"),
        ),
    )

    with pytest.raises(AirbyteLibInputError) as exc_info:
        _connection().update_stream_configs([
            StreamConfigOverride(
                stream_name="missing", sync_mode=SyncMode.full_refresh
            ),
            StreamConfigOverride(stream_name="users", sync_mode=SyncMode.incremental),
        ])

    message = exc_info.value.get_message()
    assert "missing" in message
    assert "unsupported" in message
    assert len(exc_info.value.context["validation_errors"]) == 2
    replace_catalog.assert_not_called()
    get_state.assert_not_called()


def test_multiple_missing_streams_list_available_streams_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, replace_catalog, _ = _mock_catalog_api(
        monkeypatch,
        _catalog(_stream_entry("users", namespace="public")),
    )

    with pytest.raises(AirbyteLibInputError) as exc_info:
        _connection().update_stream_configs([
            StreamConfigOverride(
                stream_name="missing-one",
                sync_mode=SyncMode.full_refresh,
            ),
            StreamConfigOverride(
                stream_name="missing-two",
                sync_mode=SyncMode.full_refresh,
            ),
        ])

    message = exc_info.value.get_message()
    assert "missing-one" in message
    assert "missing-two" in message
    assert message.count("Available streams:") == 1
    assert exc_info.value.context["available_streams"] == ["public.users"]
    replace_catalog.assert_not_called()


@pytest.mark.parametrize(
    "state",
    [
        {
            "stateType": "stream",
            "connectionId": _CONNECTION_ID,
            "streamState": [
                {
                    "streamDescriptor": {"name": "users", "namespace": "public"},
                    "streamState": {"cursor": "saved"},
                }
            ],
        },
        {
            "stateType": "global",
            "connectionId": _CONNECTION_ID,
            "globalState": {"sharedState": {"cursor": "saved"}, "streamStates": []},
        },
    ],
    ids=["per-stream-state", "global-state"],
)
def test_saved_state_rejects_unsafe_config_changes(
    monkeypatch: pytest.MonkeyPatch,
    state: dict[str, Any],
) -> None:
    _, replace_catalog, _ = _mock_catalog_api(
        monkeypatch,
        _catalog(_stream_entry("users", namespace="public")),
    )
    monkeypatch.setattr(
        api_util,
        "get_connection_state",
        MagicMock(return_value=state),
    )

    with pytest.raises(AirbyteUnsafeStreamConfigChangeError) as exc_info:
        _connection().update_stream_configs([
            StreamConfigOverride(
                stream_name="users",
                stream_namespace="public",
                sync_mode=SyncMode.incremental,
                cursor_field="updated_at",
            )
        ])

    unsafe = exc_info.value.context["unsafe_streams"]
    assert unsafe[0]["name"] == "users"
    assert unsafe[0]["namespace"] == "public"
    assert unsafe[0]["changed_fields"] == ["syncMode", "cursorField"]
    assert "public.users (syncMode, cursorField)" in exc_info.value.get_message()
    replace_catalog.assert_not_called()


def test_find_unsafe_stream_changes_compares_catalogs_to_normalized_state() -> None:
    old_catalog = _catalog(
        _stream_entry(
            "users",
            namespace="public",
            config={"syncMode": "full_refresh", "destinationSyncMode": "append"},
        ),
        _stream_entry(
            "orders",
            config={"syncMode": "full_refresh", "destinationSyncMode": "append"},
        ),
    )
    new_catalog = deepcopy(old_catalog)
    new_catalog["streams"][0]["config"].update({
        "syncMode": "incremental",
        "cursorField": ["updated_at"],
    })
    new_catalog["streams"][1]["config"]["destinationSyncMode"] = "overwrite"
    state_messages = [
        {
            "type": "STREAM",
            "stream": {
                "stream_descriptor": {"name": "users", "namespace": "public"},
                "stream_state": {"cursor": "saved"},
            },
        }
    ]

    old_typed_catalog = ConfiguredAirbyteCatalog.model_validate(
        _normalize_catalog_to_protocol(old_catalog)
    )
    new_typed_catalog = ConfiguredAirbyteCatalog.model_validate(
        _normalize_catalog_to_protocol(new_catalog)
    )
    unsafe = _find_unsafe_stream_changes(
        old_typed_catalog,
        new_typed_catalog,
        state_messages,
    )

    assert [
        (stream.name, stream.namespace, stream.changed_fields) for stream in unsafe
    ] == [("users", "public", ["syncMode", "cursorField"])]


def test_no_state_lookup_when_override_does_not_change_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, replace_catalog, get_state = _mock_catalog_api(
        monkeypatch,
        _catalog(_stream_entry("users")),
    )

    result = _connection().update_stream_configs([
        StreamConfigOverride(stream_name="users", sync_mode=SyncMode.full_refresh)
    ])

    get_state.assert_not_called()
    replace_catalog.assert_called_once()
    assert result.reset_streams == []


def test_changed_stream_without_state_is_safe_and_does_not_request_reset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, replace_catalog, get_state = _mock_catalog_api(
        monkeypatch,
        _catalog(_stream_entry("users")),
    )

    result = _connection().update_stream_configs([
        StreamConfigOverride(
            stream_name="users",
            destination_sync_mode=DestinationSyncMode.overwrite,
        )
    ])

    get_state.assert_called_once()
    assert replace_catalog.call_args.kwargs["skip_reset"] is True
    assert result.unsafe_streams == []
    assert result.reset_requested is False
    assert result.reset_streams == []


@pytest.mark.parametrize(
    "stream_options,expected_cursor",
    [
        ({"source_defined_cursor": True}, None),
        ({"default_cursor_field": ["updated_at"]}, ["updated_at"]),
    ],
    ids=["source-defined-cursor", "default-cursor-field"],
)
def test_incremental_sync_uses_default_but_not_source_defined_cursor(
    monkeypatch: pytest.MonkeyPatch,
    stream_options: dict[str, Any],
    expected_cursor: list[str] | None,
) -> None:
    entry = _stream_entry(
        "users",
        namespace="public",
        config={"syncMode": "full_refresh", "destinationSyncMode": "append"},
        **stream_options,
    )
    _, replace_catalog, _ = _mock_catalog_api(monkeypatch, _catalog(entry))

    _connection().update_stream_configs([
        StreamConfigOverride(
            stream_name="users",
            stream_namespace="public",
            sync_mode=SyncMode.incremental,
        )
    ])

    saved_config = replace_catalog.call_args.kwargs["configured_catalog_dict"][
        "streams"
    ][0]["config"]
    assert saved_config.get("cursorField") == expected_cursor


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


def test_replace_connection_catalog_forwards_skip_reset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = MagicMock(return_value={})
    monkeypatch.setattr(api_util, "_make_config_api_request", request)

    api_util.replace_connection_catalog(
        connection_id=_CONNECTION_ID,
        configured_catalog_dict={"streams": []},
        api_root=_API_ROOT,
        config_api_root=_CONFIG_API_ROOT,
        client_id=None,
        client_secret=None,
        bearer_token="token",
        skip_reset=False,
    )

    assert request.call_args.kwargs["json"]["skipReset"] is False
