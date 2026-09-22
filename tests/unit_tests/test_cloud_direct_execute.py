# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for direct entity/action execution on `CloudSource` and `CloudDestination`."""

from __future__ import annotations

from typing import Any

import pytest

from airbyte.agents import _api_util as agents_api_util
from airbyte.agents.models import AgentExecuteResult
from airbyte.cloud import workspaces as cloud_workspaces
from airbyte.cloud.connectors import CloudDestination, CloudSource
from airbyte.cloud.models import (
    SQL_PASSTHROUGH_DESTINATION_DIALECTS,
    CloudDestinationInfo,
    CloudSourceInfo,
)
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import (
    AirbyteError,
    AirbyteExternalAccessNotEnabledError,
    PyAirbyteInputError,
)


SNOWFLAKE_DEFINITION_ID = next(iter(SQL_PASSTHROUGH_DESTINATION_DIALECTS))
SNOWFLAKE_DIALECT = SQL_PASSTHROUGH_DESTINATION_DIALECTS[SNOWFLAKE_DEFINITION_ID]


def _make_workspace(monkeypatch: pytest.MonkeyPatch) -> CloudWorkspace:
    """Return a `CloudWorkspace` whose organization lookup is stubbed."""
    workspace = CloudWorkspace(
        workspace_id="workspace-id",
        bearer_token="token",
    )
    monkeypatch.setattr(
        CloudWorkspace,
        "_organization_info",
        property(lambda _self: {"organizationId": "organization-id"}),
    )
    return workspace


def _patch_context_layer(
    monkeypatch: pytest.MonkeyPatch, *, available: bool = True
) -> None:
    """Answer whether the workspace's API roots have a Context layer API."""
    monkeypatch.setattr(
        cloud_workspaces.deployment,
        "is_agents_api_available",
        lambda **_: available,
    )


def _patch_execute(
    monkeypatch: pytest.MonkeyPatch,
    responses: list[dict[str, Any]] | dict[str, Any],
    *,
    error: Exception | None = None,
) -> list[dict[str, Any]]:
    """Stub `execute_agent_connector_action` and record each call's kwargs."""
    calls: list[dict[str, Any]] = []
    payloads = responses if isinstance(responses, list) else [responses]

    def fake_execute(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        if error is not None:
            raise error
        return payloads[min(len(calls), len(payloads)) - 1]

    monkeypatch.setattr(agents_api_util, "execute_agent_connector_action", fake_execute)
    return calls


def _seed_source(workspace: CloudWorkspace, source_id: str, name: str) -> CloudSource:
    source = CloudSource(workspace=workspace, connector_id=source_id)
    source._connector_info = CloudSourceInfo(  # noqa: SLF001
        source_id=source_id, name=name, definition_id="source-definition"
    )
    return source


def _seed_destination(
    workspace: CloudWorkspace, destination_id: str, definition_id: str
) -> CloudDestination:
    destination = CloudDestination(workspace=workspace, connector_id=destination_id)
    destination._connector_info = CloudDestinationInfo(  # noqa: SLF001
        destination_id=destination_id, name=destination_id, definition_id=definition_id
    )
    return destination


def test_cloud_source_execute_forwards_to_agents_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": [{"id": 1}]})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    result = source.execute(
        "issues",
        "list",
        {"repository": "airbytehq/PyAirbyte"},
        select_fields=["title"],
        exclude_fields=["body"],
        page_size=10,
        cursor="cursor-1",
        skip_truncation=False,
        intent="find open issues",
    )

    assert isinstance(result, AgentExecuteResult)
    assert result.status == "success"
    assert len(calls) == 1
    call = calls[0]
    assert call["connector_id"] == "source-1"
    assert call["credentials"] is workspace._credentials  # noqa: SLF001
    assert call["organization_id"] == "organization-id"
    assert call["request_body"] == {
        "entity": "issues",
        "action": "list",
        "params": {
            "repository": "airbytehq/PyAirbyte",
            "limit": 10,
            "cursor": "cursor-1",
        },
        "skip_truncation": False,
        "select_fields": ["title"],
        "exclude_fields": ["body"],
        "intent": "find open issues",
    }


@pytest.mark.parametrize("status_code", [403, 404])
def test_cloud_source_execute_translates_missing_external_access(
    monkeypatch: pytest.MonkeyPatch,
    status_code: int,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    _patch_execute(
        monkeypatch,
        {},
        error=AirbyteError(context={"status_code": status_code}),
    )
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    with pytest.raises(AirbyteExternalAccessNotEnabledError) as exc_info:
        source.list_entities("issues")

    assert exc_info.value.connector_id == "source-1"
    assert exc_info.value.connector_name == "GitHub Issues"


def test_cloud_source_execute_propagates_other_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    _patch_execute(
        monkeypatch,
        {},
        error=AirbyteError(context={"status_code": 500}),
    )
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    with pytest.raises(AirbyteError) as exc_info:
        source.list_entities("issues")

    assert not isinstance(exc_info.value, AirbyteExternalAccessNotEnabledError)


def test_direct_action_raises_without_context_layer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch, available=False)
    calls = _patch_execute(monkeypatch, {"status": "success"})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    with pytest.raises(AirbyteExternalAccessNotEnabledError) as exc_info:
        source.list_entities("issues")

    assert exc_info.value.connector_id == "source-1"
    assert calls == []


def test_cloud_source_get_entity_uses_get_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": {"id": "42"}})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    result = source.get_entity("issues", {"issue_id": "42"})

    assert result.status == "success"
    assert calls[0]["request_body"]["action"] == "get"
    assert calls[0]["request_body"]["params"] == {"issue_id": "42"}


def test_cloud_source_iter_entities_follows_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(
        monkeypatch,
        [
            {
                "status": "success",
                "result": [{"id": 1}, {"id": 2}],
                "connector_metadata": {"has_next_page": True, "end_cursor": "cursor-2"},
            },
            {
                "status": "success",
                "result": [{"id": 3}],
                "connector_metadata": {"has_next_page": False},
            },
        ],
    )
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    entities = list(source.iter_entities("issues"))

    assert entities == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert len(calls) == 2
    assert calls[0]["request_body"]["params"].get("cursor") is None
    assert calls[1]["request_body"]["params"]["cursor"] == "cursor-2"


def test_cloud_destination_sql_select_infers_dialect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": [{"n": 1}]})
    destination = _seed_destination(workspace, "destination-1", SNOWFLAKE_DEFINITION_ID)

    result = destination.sql_select("SELECT 1", page_size=5)

    assert isinstance(result, AgentExecuteResult)
    body = calls[0]["request_body"]
    assert body["entity"] == "sql"
    assert body["action"] == "sql_select"
    assert body["params"]["sql"] == "SELECT 1"
    assert body["params"]["sql_dialect"] == SNOWFLAKE_DIALECT
    assert body["params"]["limit"] == 5
    assert body["params"]["workspace_id"] == "workspace-id"


def test_cloud_destination_sql_select_explicit_dialect_wins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": []})
    destination = _seed_destination(
        workspace, "destination-1", "not-a-passthrough-definition"
    )

    destination.sql_select("SELECT 1", sql_dialect="postgres")

    params = calls[0]["request_body"]["params"]
    assert params["sql_dialect"] == "postgres"


def test_cloud_destination_sql_select_rejects_non_passthrough_definition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success"})
    destination = _seed_destination(
        workspace, "destination-1", "not-a-passthrough-definition"
    )

    with pytest.raises(PyAirbyteInputError):
        destination.sql_select("SELECT 1")

    assert calls == []
