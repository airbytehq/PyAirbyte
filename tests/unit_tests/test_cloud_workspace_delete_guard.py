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
from airbyte.exceptions import PyAirbyteInputError


def _workspace() -> CloudWorkspace:
    """Create a workspace with local credentials."""
    return CloudWorkspace(
        workspace_id="workspace-id",
        bearer_token="token",
        api_root="https://api.airbyte.com/v1",
    )


@pytest.mark.parametrize(
    ("connector_type", "delete_method", "api_delete"),
    [
        pytest.param(
            "source", "permanently_delete_source", "delete_source", id="source"
        ),
        pytest.param(
            "destination",
            "permanently_delete_destination",
            "delete_destination",
            id="destination",
        ),
    ],
)
def test_permanently_delete_connector_rejects_in_use_connector(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: str,
    delete_method: str,
    api_delete: str,
) -> None:
    """A source or destination used by a connection cannot be deleted."""
    workspace = _workspace()
    connector_id = f"{connector_type}-id"
    connection = SimpleNamespace(
        connection_id="connection-id",
        name="orders pipeline",
        source_id=connector_id if connector_type == "source" else "other-source",
        destination_id=connector_id
        if connector_type == "destination"
        else "other-destination",
    )
    monkeypatch.setattr(
        workspace, "list_connections", MagicMock(return_value=[connection])
    )
    delete = MagicMock()
    monkeypatch.setattr(api_util, api_delete, delete)

    with pytest.raises(PyAirbyteInputError) as exc_info:
        getattr(workspace, delete_method)(connector_id, safe_mode=False)

    assert exc_info.value.get_message() == (
        f"The {connector_type} '{connector_id}' is used by 1 connection(s) and cannot be deleted."
    )
    assert exc_info.value.guidance == "Delete those connections first."
    assert exc_info.value.context == {
        "connector_id": connector_id,
        "connector_type": connector_type,
        "connections": [{"connection_id": "connection-id", "name": "orders pipeline"}],
    }
    delete.assert_not_called()


@pytest.mark.parametrize(
    ("connector_type", "delete_method", "api_delete"),
    [
        pytest.param(
            "source", "permanently_delete_source", "delete_source", id="source"
        ),
        pytest.param(
            "destination",
            "permanently_delete_destination",
            "delete_destination",
            id="destination",
        ),
    ],
)
def test_permanently_delete_unused_connector_proceeds(
    monkeypatch: pytest.MonkeyPatch,
    connector_type: str,
    delete_method: str,
    api_delete: str,
) -> None:
    """Unused sources and destinations are deleted."""
    workspace = _workspace()
    monkeypatch.setattr(workspace, "list_connections", MagicMock(return_value=[]))
    delete = MagicMock()
    monkeypatch.setattr(api_util, api_delete, delete)
    connector_id = f"{connector_type}-id"

    connector: str | CloudSource | CloudDestination = connector_id
    if connector_type == "source":
        connector = CloudSource(workspace=workspace, connector_id=connector_id)
        connector._connector_info = SimpleNamespace(name="source name")  # noqa: SLF001
    elif connector_type == "destination":
        connector = CloudDestination(workspace=workspace, connector_id=connector_id)
        connector._connector_info = SimpleNamespace(name="destination name")  # noqa: SLF001

    getattr(workspace, delete_method)(connector, safe_mode=False)

    delete.assert_called_once()
    assert (
        delete.call_args.kwargs[
            "source_id" if connector_type == "source" else "destination_id"
        ]
        == connector_id
    )


@pytest.mark.parametrize(
    "delete_through_connection",
    [
        pytest.param(True, id="connection-method"),
        pytest.param(False, id="workspace-method"),
    ],
)
def test_cascade_delete_source_passes_in_use_guard_after_connection_delete(
    monkeypatch: pytest.MonkeyPatch,
    delete_through_connection: bool,
) -> None:
    """Both cascade APIs check for remaining connections after connection deletion."""
    workspace = _workspace()
    connection = CloudConnection(
        workspace=workspace,
        connection_id="connection-id",
        source="source-id",
        destination="destination-id",
    )
    connection._connection_info = SimpleNamespace(name="orders pipeline")  # noqa: SLF001
    deleted: list[str] = []

    def delete_connection(**kwargs: object) -> None:
        """Record the connection deletion."""
        _ = kwargs
        deleted.append("connection")

    def list_connections() -> list[SimpleNamespace]:
        """Return no connections after the connection has been deleted."""
        assert deleted == ["connection"]
        return []

    def delete_source(**kwargs: object) -> None:
        """Record the source deletion."""
        _ = kwargs
        deleted.append("source")

    monkeypatch.setattr(api_util, "delete_connection", delete_connection)
    monkeypatch.setattr(api_util, "delete_source", delete_source)
    monkeypatch.setattr(workspace, "list_connections", list_connections)

    if delete_through_connection:
        connection.permanently_delete(cascade_delete_source=True)
    else:
        workspace.permanently_delete_connection(
            connection,
            cascade_delete_source=True,
            safe_mode=False,
        )

    assert deleted == ["connection", "source"]
