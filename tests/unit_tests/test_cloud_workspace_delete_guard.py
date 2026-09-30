# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for CloudWorkspace connector deletion guards."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from airbyte._util import api_util
from airbyte.cloud.connectors import CloudDestination, CloudSource
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteConnectorInUseError


def _workspace() -> CloudWorkspace:
    """Create a workspace with local credentials."""
    return CloudWorkspace(
        workspace_id="workspace-id",
        bearer_token="token",
        api_root="https://api.airbyte.com/v1",
    )


@pytest.mark.parametrize(
    "connector_type",
    [
        pytest.param("source", id="source"),
        pytest.param("destination", id="destination"),
    ],
)
@pytest.mark.parametrize(
    "in_use",
    [pytest.param(True, id="in-use"), pytest.param(False, id="unused")],
)
def test_permanently_delete_connector_guards_and_deletes(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: str,
    in_use: bool,
) -> None:
    """Source and destination deletion is guarded only while connections remain."""
    workspace = _workspace()
    connector_id = f"{connector_type}-id"
    connections = (
        [
            SimpleNamespace(
                connection_id="connection-id",
                name="orders pipeline",
                source_id=connector_id
                if connector_type == "source"
                else "other-source",
                destination_id=connector_id
                if connector_type == "destination"
                else "other-destination",
            )
        ]
        if in_use
        else []
    )
    monkeypatch.setattr(
        workspace, "list_connections", MagicMock(return_value=connections)
    )
    api_delete = f"delete_{connector_type}"
    delete_method = f"permanently_delete_{connector_type}"
    delete = MagicMock()
    monkeypatch.setattr(api_util, api_delete, delete)

    connector: str | CloudSource | CloudDestination = connector_id
    if not in_use and connector_type == "source":
        connector = CloudSource(workspace=workspace, connector_id=connector_id)
        connector._connector_info = SimpleNamespace(  # noqa: SLF001  # Seed cache.
            name="source name",
        )
    elif not in_use and connector_type == "destination":
        connector = CloudDestination(workspace=workspace, connector_id=connector_id)
        connector._connector_info = SimpleNamespace(  # noqa: SLF001  # Seed cache.
            name="destination name",
        )

    if in_use:
        with pytest.raises(AirbyteConnectorInUseError) as exc_info:
            getattr(workspace, delete_method)(connector, safe_mode=False)

        assert exc_info.value.get_message() == (
            f"The {connector_type} '{connector_id}' is used by 1 connection(s) and cannot be deleted."
        )
        assert exc_info.value.guidance == "Delete those connections first."
        assert exc_info.value.connector_id == connector_id
        assert exc_info.value.connector_type == connector_type
        assert exc_info.value.connection_ids == ["connection-id"]
        assert exc_info.value.context == {
            "connections": [
                {"connection_id": "connection-id", "name": "orders pipeline"}
            ],
        }
        delete.assert_not_called()
    else:
        getattr(workspace, delete_method)(connector, safe_mode=False)

        delete.assert_called_once()
        assert (
            delete.call_args.kwargs[
                "source_id" if connector_type == "source" else "destination_id"
            ]
            == connector_id
        )


@pytest.mark.parametrize(
    "connector_type",
    [
        pytest.param("source", id="source"),
        pytest.param("destination", id="destination"),
    ],
)
@pytest.mark.parametrize(
    "delete_through_connection",
    [
        pytest.param(True, id="connection-method"),
        pytest.param(False, id="workspace-method"),
    ],
)
def test_cascade_delete_connector_passes_in_use_guard_after_connection_delete(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: str,
    delete_through_connection: bool,
) -> None:
    """Both cascade APIs check either connector type after connection deletion."""
    workspace = _workspace()
    connector_id = f"{connector_type}-id"
    connection = CloudConnection(
        workspace=workspace,
        connection_id="connection-id",
        source=connector_id if connector_type == "source" else "other-source",
        destination=connector_id
        if connector_type == "destination"
        else "other-destination",
    )
    connection._connection_info = SimpleNamespace(  # noqa: SLF001  # Seed cache.
        name="orders pipeline",
    )
    deleted: list[str] = []

    def delete_connection(**kwargs: object) -> None:
        """Record the connection deletion."""
        _ = kwargs
        deleted.append("connection")

    def list_connections() -> list[SimpleNamespace]:
        """Return no connections after the connection has been deleted."""
        assert deleted == ["connection"]
        return []

    def delete_connector(**kwargs: object) -> None:
        """Record the connector deletion."""
        _ = kwargs
        deleted.append(connector_type)

    monkeypatch.setattr(api_util, "delete_connection", delete_connection)
    monkeypatch.setattr(api_util, f"delete_{connector_type}", delete_connector)
    monkeypatch.setattr(workspace, "list_connections", list_connections)
    if delete_through_connection:
        if connector_type == "source":
            connection.permanently_delete(cascade_delete_source=True)
        else:
            connection.permanently_delete(cascade_delete_destination=True)
    else:
        if connector_type == "source":
            workspace.permanently_delete_connection(
                connection,
                cascade_delete_source=True,
                safe_mode=False,
            )
        else:
            workspace.permanently_delete_connection(
                connection,
                cascade_delete_destination=True,
                safe_mode=False,
            )

    assert deleted == ["connection", connector_type]
