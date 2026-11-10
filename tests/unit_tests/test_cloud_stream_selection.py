# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Regression tests for stream selection PATCH payloads."""

from unittest.mock import MagicMock

import pytest
from airbyte._util import api_util
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte_api import models


def _response(
    streams: list[models.StreamConfiguration] | None,
) -> models.ConnectionResponse:
    return models.ConnectionResponse(
        connection_id="connection-id",
        created_at=0,
        destination_id="destination-id",
        name="name",
        source_id="source-id",
        status=models.ConnectionStatusEnum.ACTIVE,
        workspace_id="workspace-id",
        configurations=models.StreamConfigurations(streams=streams),
        schedule={},
        tags=[],
    )


@pytest.mark.parametrize("selection", [["kept", "new"], [], ["new"]])
def test_selected_streams_preserve_fresh_configuration(
    monkeypatch: pytest.MonkeyPatch, selection: list[str]
) -> None:
    kept = models.StreamConfiguration.from_dict({
        "name": "kept",
        "namespace": "public",
        "syncMode": "incremental_deduped_history",
        "cursorField": ["updated_at"],
        "primaryKey": [["id"]],
        "selectedFields": [{"fieldPath": ["id"]}],
        "mappers": [],
        "includeFiles": False,
        "destinationObjectName": "target",
    })
    connection = CloudConnection._from_connection_response(  # noqa: SLF001
        workspace=CloudWorkspace(workspace_id="workspace-id", bearer_token="token"),
        connection_response=_response([models.StreamConfiguration(name="stale")]),
    )
    get_connection = MagicMock(
        return_value=_response([kept, models.StreamConfiguration(name="drop")])
    )
    monkeypatch.setattr(api_util, "get_connection", get_connection)
    server = MagicMock()
    server.connections.patch_connection.return_value = MagicMock(
        status_code=200,
        connection_response=_response([
            models.StreamConfiguration(name="server-result")
        ]),
    )
    monkeypatch.setattr(
        api_util, "get_airbyte_server_instance", MagicMock(return_value=server)
    )

    assert connection.set_selected_streams(selection) is connection

    request = server.connections.patch_connection.call_args.args[0]
    expected = [
        kept.to_dict() if name == "kept" else {"name": name} for name in selection
    ]
    assert request.connection_patch_request.to_dict() == {
        "configurations": {"streams": expected}
    }
    assert request.connection_id == "connection-id"
    get_connection.assert_called_once()
    assert connection.stream_names == ["server-result"]


@pytest.mark.parametrize("streams", [None, []])
def test_selected_streams_without_existing_streams(
    monkeypatch: pytest.MonkeyPatch, streams: list[models.StreamConfiguration] | None
) -> None:
    connection = CloudConnection(
        workspace=CloudWorkspace(workspace_id="workspace-id", bearer_token="token"),
        connection_id="connection-id",
    )
    monkeypatch.setattr(
        api_util, "get_connection", MagicMock(return_value=_response(streams))
    )
    patch = MagicMock(return_value=_response([models.StreamConfiguration(name="new")]))
    monkeypatch.setattr(api_util, "patch_connection", patch)
    connection.set_selected_streams(["new"])
    assert patch.call_args.kwargs["configurations"].to_dict() == {
        "streams": [{"name": "new"}]
    }


def test_selected_streams_get_failure_does_not_patch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = CloudConnection(
        workspace=CloudWorkspace(workspace_id="workspace-id", bearer_token="token"),
        connection_id="connection-id",
    )
    monkeypatch.setattr(
        api_util, "get_connection", MagicMock(side_effect=RuntimeError("GET failed"))
    )
    patch = MagicMock()
    monkeypatch.setattr(api_util, "patch_connection", patch)
    with pytest.raises(RuntimeError, match="GET failed"):
        connection.set_selected_streams(["kept"])
    patch.assert_not_called()
