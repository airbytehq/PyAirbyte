# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Tests for applying a completed discover catalog to a Cloud connection."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from airbyte._util import api_util
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.models import StreamConfigOverride
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import (
    AirbyteLibInputError,
    AirbyteUnsafeStreamConfigChangeError,
)
from airbyte_protocol.models import DestinationSyncMode


_API_ROOT = "https://api.example/v1"
_CONFIG_API_ROOT = "https://config.example/v1"
_DISCOVER_REQUEST_ID = "discover-request-id"
_SOURCE_CATALOG_ID = "source-catalog-id"


def _connection() -> CloudConnection:
    workspace = CloudWorkspace(
        workspace_id="workspace-id",
        bearer_token="token",
        api_root=_API_ROOT,
        config_api_root=_CONFIG_API_ROOT,
    )
    return CloudConnection(workspace=workspace, connection_id="connection-id")


def _stream_entry(
    name: str,
    *,
    namespace: str | None = None,
    supported_sync_modes: tuple[str, ...] = ("full_refresh", "incremental"),
    sync_mode: str = "full_refresh",
    destination_sync_mode: str = "append",
    cursor_field: list[str] | None = None,
    primary_key: list[list[str]] | None = None,
    source_defined_cursor: bool | None = None,
    selected: bool = True,
    schema_fields: tuple[str, ...] = ("id",),
    include_namespace: bool = False,
) -> dict[str, Any]:
    stream: dict[str, Any] = {
        "name": name,
        "jsonSchema": {
            "type": "object",
            "properties": {field: {"type": "string"} for field in schema_fields},
        },
        "supportedSyncModes": list(supported_sync_modes),
    }
    if namespace is not None or include_namespace:
        stream["namespace"] = namespace
    if source_defined_cursor is not None:
        stream["sourceDefinedCursor"] = source_defined_cursor

    config: dict[str, Any] = {
        "syncMode": sync_mode,
        "destinationSyncMode": destination_sync_mode,
        "selected": selected,
        "selectedFields": [],
        "fieldSelectionEnabled": False,
    }
    if cursor_field is not None:
        config["cursorField"] = cursor_field
    if primary_key is not None:
        config["primaryKey"] = primary_key
    return {"stream": stream, "config": config}


def _configure_discover_api(
    monkeypatch: pytest.MonkeyPatch,
    *,
    output: dict[str, Any] | None,
    status: str = "completed",
) -> tuple[MagicMock, MagicMock]:
    get_status = MagicMock(return_value={"status": status})
    get_output = MagicMock(return_value=output or {})
    monkeypatch.setattr(api_util, "get_command_status", get_status)
    monkeypatch.setattr(api_util, "get_discover_command_output", get_output)
    return get_status, get_output


def test_incomplete_discover_is_rejected_without_writing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_status, get_output = _configure_discover_api(
        monkeypatch, output=None, status="running"
    )
    replace_catalog = MagicMock()
    monkeypatch.setattr(api_util, "replace_connection_catalog", replace_catalog)
    connection = _connection()
    connection.dump_raw_catalog = MagicMock()  # type: ignore[method-assign]

    with pytest.raises(AirbyteLibInputError, match="still running") as error:
        connection.update_stream_configs([], discover_request_id=_DISCOVER_REQUEST_ID)

    assert "discover_source_catalog" in str(error.value)
    get_status.assert_called_once()
    get_output.assert_not_called()
    connection.dump_raw_catalog.assert_not_called()
    replace_catalog.assert_not_called()


def test_cancelled_discover_requires_a_new_request_without_writing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_status, get_output = _configure_discover_api(
        monkeypatch, output=None, status="cancelled"
    )
    replace_catalog = MagicMock()
    monkeypatch.setattr(api_util, "replace_connection_catalog", replace_catalog)
    connection = _connection()
    connection.dump_raw_catalog = MagicMock()  # type: ignore[method-assign]

    with pytest.raises(AirbyteLibInputError, match="ended without a catalog") as error:
        connection.update_stream_configs([], discover_request_id=_DISCOVER_REQUEST_ID)

    assert "Start a new `discover_source_catalog` request" in str(error.value)
    assert "poll" not in str(error.value).lower()
    get_status.assert_called_once()
    get_output.assert_not_called()
    connection.dump_raw_catalog.assert_not_called()
    replace_catalog.assert_not_called()


def test_failed_discover_is_rejected_without_writing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_discover_api(
        monkeypatch,
        output={
            "status": "failed",
            "failureReason": {"externalMessage": "The source could not be read."},
        },
    )
    replace_catalog = MagicMock()
    monkeypatch.setattr(api_util, "replace_connection_catalog", replace_catalog)
    connection = _connection()
    connection.dump_raw_catalog = MagicMock()  # type: ignore[method-assign]

    with pytest.raises(
        AirbyteLibInputError, match="The source could not be read"
    ) as error:
        connection.update_stream_configs([], discover_request_id=_DISCOVER_REQUEST_ID)

    assert "source catalog discover failed" in str(error.value).lower()
    connection.dump_raw_catalog.assert_not_called()
    replace_catalog.assert_not_called()


@pytest.mark.parametrize(
    "output",
    [
        pytest.param(
            {"status": "succeeded", "catalogId": _SOURCE_CATALOG_ID},
            id="missing-catalog",
        ),
        pytest.param(
            {"status": "succeeded", "catalog": {"streams": []}},
            id="missing-catalog-id",
        ),
    ],
)
def test_discover_output_without_catalog_or_id_is_rejected_without_writing(
    monkeypatch: pytest.MonkeyPatch,
    output: dict[str, Any],
) -> None:
    _configure_discover_api(monkeypatch, output=output)
    replace_catalog = MagicMock()
    monkeypatch.setattr(api_util, "replace_connection_catalog", replace_catalog)
    connection = _connection()
    connection.dump_raw_catalog = MagicMock()  # type: ignore[method-assign]

    with pytest.raises(AirbyteLibInputError, match="no longer available") as error:
        connection.update_stream_configs([], discover_request_id=_DISCOVER_REQUEST_ID)

    assert "discover_source_catalog" in str(error.value)
    connection.dump_raw_catalog.assert_not_called()
    replace_catalog.assert_not_called()


def _set_catalog_and_discover(
    monkeypatch: pytest.MonkeyPatch,
    connection: CloudConnection,
    *,
    configured_catalog: dict[str, Any],
    discovered_catalog: dict[str, Any],
    state_messages: list[dict[str, Any]],
) -> MagicMock:
    monkeypatch.setattr(
        api_util,
        "get_command_status",
        MagicMock(return_value={"status": "completed"}),
    )
    monkeypatch.setattr(
        api_util,
        "get_discover_command_output",
        MagicMock(
            return_value={
                "status": "succeeded",
                "catalogId": _SOURCE_CATALOG_ID,
                "catalog": discovered_catalog,
            }
        ),
    )
    connection.dump_raw_catalog = MagicMock(  # type: ignore[method-assign]
        return_value=configured_catalog
    )
    connection.dump_raw_state = MagicMock(  # type: ignore[method-assign]
        return_value=state_messages
    )
    replace_catalog = MagicMock(return_value={})
    monkeypatch.setattr(api_util, "replace_connection_catalog", replace_catalog)
    return replace_catalog


def _state_for_users() -> list[dict[str, Any]]:
    return [
        {
            "type": "STREAM",
            "stream": {
                "stream_descriptor": {"name": "users", "namespace": "public"},
                "stream_state": {"cursor": "saved"},
            },
        }
    ]


def test_safe_update_rejects_merge_induced_sync_mode_fallback_with_saved_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured_catalog = {
        "streams": [
            _stream_entry(
                "users",
                namespace="public",
                sync_mode="incremental",
                cursor_field=["updated_at"],
            )
        ]
    }
    discovered_catalog = {
        "streams": [
            _stream_entry(
                "users",
                namespace="public",
                supported_sync_modes=("full_refresh",),
                sync_mode="full_refresh",
            )
        ]
    }
    connection = _connection()
    replace_catalog = _set_catalog_and_discover(
        monkeypatch,
        connection,
        configured_catalog=configured_catalog,
        discovered_catalog=discovered_catalog,
        state_messages=_state_for_users(),
    )

    with pytest.raises(AirbyteUnsafeStreamConfigChangeError, match="syncMode"):
        connection.update_stream_configs([], discover_request_id=_DISCOVER_REQUEST_ID)

    replace_catalog.assert_not_called()


def test_unsafe_update_writes_catalog_id_and_reports_reset_added_and_removed_streams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured_catalog = {
        "streams": [
            _stream_entry(
                "users",
                namespace="public",
                sync_mode="incremental",
                cursor_field=["updated_at"],
            ),
            _stream_entry("removed"),
        ]
    }
    discovered_catalog = {
        "streams": [
            _stream_entry(
                "users",
                namespace="public",
                supported_sync_modes=("full_refresh",),
                sync_mode="full_refresh",
            ),
            _stream_entry("new"),
        ]
    }
    connection = _connection()
    replace_catalog = _set_catalog_and_discover(
        monkeypatch,
        connection,
        configured_catalog=configured_catalog,
        discovered_catalog=discovered_catalog,
        state_messages=_state_for_users(),
    )

    result = connection.update_stream_configs(
        [
            StreamConfigOverride(
                stream_name="users",
                stream_namespace="public",
                destination_sync_mode=DestinationSyncMode.overwrite,
            )
        ],
        allow_unsafe=True,
        discover_request_id=_DISCOVER_REQUEST_ID,
    )

    replace_catalog.assert_called_once()
    assert replace_catalog.call_args.kwargs["source_catalog_id"] == _SOURCE_CATALOG_ID
    assert replace_catalog.call_args.kwargs["skip_reset"] is False
    saved_catalog = replace_catalog.call_args.kwargs["configured_catalog_dict"]
    assert saved_catalog["streams"][0]["config"]["destinationSyncMode"] == "overwrite"
    assert saved_catalog["streams"][1]["config"]["selected"] is False
    assert result.applied_source_catalog_id == _SOURCE_CATALOG_ID
    assert [stream.label for stream in result.reset_streams] == ["public.users"]
    assert result.added_streams == ["new"]
    assert result.removed_streams == ["removed"]
    summary = " ".join(result.summary_lines())
    assert _SOURCE_CATALOG_ID in summary
    assert "added streams ['new']" in summary
    assert "removed streams ['removed']" in summary


@pytest.mark.parametrize(
    "include_discovered_namespace",
    [
        pytest.param(False, id="missing-namespace"),
        pytest.param(True, id="null-namespace"),
    ],
)
def test_empty_namespace_matches_missing_or_null_namespace(
    monkeypatch: pytest.MonkeyPatch,
    include_discovered_namespace: bool,
) -> None:
    configured_catalog = {
        "streams": [_stream_entry("users", namespace="", selected=True)]
    }
    discovered_entry = _stream_entry("users", selected=False)
    if include_discovered_namespace:
        discovered_entry["stream"]["namespace"] = None
    connection = _connection()
    replace_catalog = _set_catalog_and_discover(
        monkeypatch,
        connection,
        configured_catalog=configured_catalog,
        discovered_catalog={"streams": [discovered_entry]},
        state_messages=[],
    )

    result = connection.update_stream_configs(
        [], discover_request_id=_DISCOVER_REQUEST_ID
    )

    updated_catalog = replace_catalog.call_args.kwargs["configured_catalog_dict"]
    assert updated_catalog["streams"][0]["config"]["selected"] is True
    assert result.added_streams == []
    assert result.removed_streams == []
    replace_catalog.assert_called_once()


@pytest.mark.parametrize("allow_unsafe", [False, True])
def test_removed_incremental_cursor_is_rejected_without_writing(
    monkeypatch: pytest.MonkeyPatch,
    allow_unsafe: bool,
) -> None:
    configured_catalog = {
        "streams": [
            _stream_entry(
                "users",
                sync_mode="incremental",
                cursor_field=["removed_cursor"],
            )
        ]
    }
    discovered_catalog = {
        "streams": [
            _stream_entry(
                "users",
                sync_mode="incremental",
                cursor_field=["removed_cursor"],
                schema_fields=("id",),
            )
        ]
    }
    connection = _connection()
    replace_catalog = _set_catalog_and_discover(
        monkeypatch,
        connection,
        configured_catalog=configured_catalog,
        discovered_catalog=discovered_catalog,
        state_messages=_state_for_users(),
    )

    with pytest.raises(AirbyteLibInputError, match="removed_cursor") as error:
        connection.update_stream_configs(
            [],
            allow_unsafe=allow_unsafe,
            discover_request_id=_DISCOVER_REQUEST_ID,
        )

    assert "override_stream_configs" in str(error.value)
    assert "users" in str(error.value)
    replace_catalog.assert_not_called()


@pytest.mark.parametrize(
    "cursor_field",
    [
        pytest.param(None, id="missing-cursor-field"),
        pytest.param([], id="empty-cursor-field"),
    ],
)
def test_incremental_stream_without_cursor_field_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    cursor_field: list[str] | None,
) -> None:
    configured_catalog = {
        "streams": [
            _stream_entry("users", sync_mode="incremental", cursor_field=cursor_field)
        ]
    }
    discovered_catalog = {
        "streams": [
            _stream_entry("users", sync_mode="incremental", cursor_field=cursor_field)
        ]
    }
    connection = _connection()
    replace_catalog = _set_catalog_and_discover(
        monkeypatch,
        connection,
        configured_catalog=configured_catalog,
        discovered_catalog=discovered_catalog,
        state_messages=[],
    )

    with pytest.raises(AirbyteLibInputError, match="empty or missing `cursorField`"):
        connection.update_stream_configs([], discover_request_id=_DISCOVER_REQUEST_ID)

    replace_catalog.assert_not_called()


def test_cursor_override_can_replace_a_removed_cursor_with_a_discovered_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured_catalog = {
        "streams": [
            _stream_entry(
                "users",
                sync_mode="incremental",
                cursor_field=["removed_cursor"],
            )
        ]
    }
    discovered_catalog = {
        "streams": [
            _stream_entry(
                "users",
                sync_mode="incremental",
                cursor_field=["removed_cursor"],
                schema_fields=("id", "new_cursor"),
            )
        ]
    }
    connection = _connection()
    replace_catalog = _set_catalog_and_discover(
        monkeypatch,
        connection,
        configured_catalog=configured_catalog,
        discovered_catalog=discovered_catalog,
        state_messages=[],
    )

    connection.update_stream_configs(
        [
            StreamConfigOverride(
                stream_name="users",
                cursor_field="new_cursor",
            )
        ],
        discover_request_id=_DISCOVER_REQUEST_ID,
    )

    saved_catalog = replace_catalog.call_args.kwargs["configured_catalog_dict"]
    assert saved_catalog["streams"][0]["config"]["cursorField"] == ["new_cursor"]
    replace_catalog.assert_called_once()


def test_source_defined_cursor_does_not_require_user_defined_cursor_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured_catalog = {
        "streams": [
            _stream_entry(
                "users",
                sync_mode="incremental",
                source_defined_cursor=True,
            )
        ]
    }
    discovered_catalog = {
        "streams": [
            _stream_entry(
                "users",
                sync_mode="incremental",
                source_defined_cursor=True,
            )
        ]
    }
    connection = _connection()
    replace_catalog = _set_catalog_and_discover(
        monkeypatch,
        connection,
        configured_catalog=configured_catalog,
        discovered_catalog=discovered_catalog,
        state_messages=[],
    )

    connection.update_stream_configs([], discover_request_id=_DISCOVER_REQUEST_ID)

    replace_catalog.assert_called_once()


def test_plain_override_does_not_validate_existing_cursor_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured_catalog = {
        "streams": [
            _stream_entry(
                "users",
                sync_mode="incremental",
                cursor_field=["removed_cursor"],
            )
        ]
    }
    connection = _connection()
    replace_catalog = _set_catalog_and_discover(
        monkeypatch,
        connection,
        configured_catalog=configured_catalog,
        discovered_catalog={"streams": []},
        state_messages=[],
    )

    connection.update_stream_configs([
        StreamConfigOverride(
            stream_name="users",
            destination_sync_mode=DestinationSyncMode.overwrite,
        )
    ])

    replace_catalog.assert_called_once()


def test_removed_dedup_primary_key_is_rejected_without_writing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured_catalog = {
        "streams": [
            _stream_entry(
                "users",
                destination_sync_mode="append_dedup",
                primary_key=[["removed_key"]],
            )
        ]
    }
    discovered_catalog = {
        "streams": [
            _stream_entry(
                "users",
                destination_sync_mode="append",
                schema_fields=("id",),
            )
        ]
    }
    connection = _connection()
    replace_catalog = _set_catalog_and_discover(
        monkeypatch,
        connection,
        configured_catalog=configured_catalog,
        discovered_catalog=discovered_catalog,
        state_messages=[],
    )

    with pytest.raises(AirbyteLibInputError, match="removed_key") as error:
        connection.update_stream_configs([], discover_request_id=_DISCOVER_REQUEST_ID)

    assert "override_stream_configs" in str(error.value)
    assert "users" in str(error.value)
    replace_catalog.assert_not_called()


def test_discover_application_preserves_api_only_fields_and_applies_overrides_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured_catalog = {
        "streams": [
            _stream_entry("users"),
            _stream_entry("removed"),
        ]
    }
    discovered_catalog = {
        "apiOnlyCatalogField": {"keep": True},
        "streams": [
            {
                **_stream_entry("users", schema_fields=("id", "email")),
                "apiOnlyEntryField": {"keep": True},
                "stream": {
                    **_stream_entry("users", schema_fields=("id", "email"))["stream"],
                    "apiOnlyStreamField": {"keep": True},
                },
                "config": {
                    **_stream_entry("users")["config"],
                    "apiOnlyConfigField": {"keep": True},
                },
            },
            _stream_entry("new"),
        ],
    }
    connection = _connection()
    replace_catalog = _set_catalog_and_discover(
        monkeypatch,
        connection,
        configured_catalog=configured_catalog,
        discovered_catalog=discovered_catalog,
        state_messages=[],
    )

    result = connection.update_stream_configs(
        [
            StreamConfigOverride(
                stream_name="users",
                destination_sync_mode=DestinationSyncMode.overwrite,
            )
        ],
        discover_request_id=_DISCOVER_REQUEST_ID,
    )

    replace_catalog.assert_called_once()
    saved_catalog = replace_catalog.call_args.kwargs["configured_catalog_dict"]
    assert saved_catalog["apiOnlyCatalogField"] == {"keep": True}
    assert saved_catalog["streams"][0]["apiOnlyEntryField"] == {"keep": True}
    assert saved_catalog["streams"][0]["stream"]["apiOnlyStreamField"] == {"keep": True}
    assert saved_catalog["streams"][0]["config"]["apiOnlyConfigField"] == {"keep": True}
    assert saved_catalog["streams"][0]["config"]["destinationSyncMode"] == "overwrite"
    assert result.added_streams == ["new"]
    assert result.removed_streams == ["removed"]
    assert result.applied_source_catalog_id == _SOURCE_CATALOG_ID
