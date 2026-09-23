# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for direct entity/action execution on `CloudConnector`."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import PropertyMock, patch

import pytest

from airbyte._direct_connectors import api_util as agents_api_util
from airbyte._util import api_util
from airbyte.agents.models import AgentExecuteResult
from airbyte.cloud import workspaces as cloud_workspaces
from airbyte.cloud.connectors import (
    CloudApiQueryAction,
    CloudConnector,
    CloudDestination,
    CloudSource,
)
from airbyte.cloud.models import (
    SQL_PASSTHROUGH_DESTINATION_DIALECTS,
    CloudDestinationInfo,
    CloudSourceInfo,
)
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import (
    AirbyteError,
    AirbyteExternalAccessNotEnabledError,
    AirbyteMissingResourceError,
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
    response: dict[str, Any],
    *,
    error: Exception | None = None,
) -> list[dict[str, Any]]:
    """Stub `execute_agent_connector_action` and record each call's kwargs."""
    calls: list[dict[str, Any]] = []

    def fake_execute(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        if error is not None:
            raise error
        return response

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


def _patch_external_access(*, enabled: bool) -> Any:
    """Mock `CloudConnector.external_access_enabled` without hitting the API."""
    return patch.object(
        CloudConnector,
        "external_access_enabled",
        new_callable=PropertyMock,
        return_value=enabled,
    )


def test_execute_api_query_forwards_to_agents_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": [{"id": 1}]})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    result = source.execute_api_query(
        "issues",
        api_args={"repository": "airbytehq/PyAirbyte"},
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


def test_execute_api_query_supports_get_and_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": []})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    source.execute_api_query("issues", "get", {"issue_id": "42"})
    source.execute_api_query("issues", "search", {"query": "bug"})

    assert calls[0]["request_body"]["action"] == "get"
    assert calls[0]["request_body"]["params"] == {"issue_id": "42"}
    assert calls[1]["request_body"]["action"] == "search"


def test_execute_api_query_accepts_enum_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": []})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    source.execute_api_query("issues", CloudApiQueryAction.SEARCH, {"query": "bug"})

    assert calls[0]["request_body"]["action"] == "search"


def test_execute_api_action_forwards_to_agents_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": {"id": 1}})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    result = source.execute_api_action(
        "issues", "create", {"title": "New issue"}, intent="file a bug"
    )

    assert isinstance(result, AgentExecuteResult)
    body = calls[0]["request_body"]
    assert body["entity"] == "issues"
    assert body["action"] == "create"
    assert body["params"] == {"title": "New issue"}
    assert body["intent"] == "file a bug"


def test_execute_api_query_rejects_write_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success"})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    with pytest.raises(PyAirbyteInputError):
        source.execute_api_query("issues", "create")  # type: ignore[arg-type]

    assert calls == []


def test_execute_api_action_rejects_read_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success"})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    with pytest.raises(PyAirbyteInputError):
        source.execute_api_action("issues", "get")  # type: ignore[arg-type]

    assert calls == []


def test_execute_direct_action_rejects_write_action_as_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success"})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    with pytest.raises(PyAirbyteInputError, match="read-only"):
        source._execute_direct_action(  # noqa: SLF001
            entity_type="issues",
            action="delete",
            read_only=True,
        )

    assert calls == []


def test_execute_forbidden_raises_not_enabled_when_access_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    _patch_execute(
        monkeypatch,
        {},
        error=AirbyteError(context={"status_code": 403}),
    )
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    with (
        _patch_external_access(enabled=False),
        pytest.raises(AirbyteExternalAccessNotEnabledError) as exc_info,
    ):
        source.execute_api_query("issues")

    assert exc_info.value.connector_id == "source-1"
    assert exc_info.value.connector_name == "GitHub Issues"


def test_execute_not_found_reraises_when_access_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    _patch_execute(
        monkeypatch,
        {},
        error=AirbyteError(context={"status_code": 404}),
    )
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    with (
        _patch_external_access(enabled=True),
        pytest.raises(AirbyteError) as exc_info,
    ):
        source.execute_api_query("issues")

    assert not isinstance(exc_info.value, AirbyteExternalAccessNotEnabledError)


def test_execute_forbidden_reraises_when_access_flag_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    _patch_execute(
        monkeypatch,
        {},
        error=AirbyteError(context={"status_code": 403}),
    )
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    with (
        patch.object(
            CloudConnector,
            "external_access_enabled",
            new_callable=PropertyMock,
            side_effect=AirbyteError(context={"status_code": 500}),
        ),
        pytest.raises(AirbyteError) as exc_info,
    ):
        source.execute_api_query("issues")

    assert not isinstance(exc_info.value, AirbyteExternalAccessNotEnabledError)
    assert exc_info.value.context["status_code"] == 403


def test_execute_propagates_other_errors(
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
        source.execute_api_query("issues")

    assert not isinstance(exc_info.value, AirbyteExternalAccessNotEnabledError)


def test_direct_methods_raise_without_context_layer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch, available=False)
    calls = _patch_execute(monkeypatch, {"status": "success"})
    monkeypatch.setattr(
        agents_api_util,
        "inspect_agent_connector",
        lambda **_: pytest.fail("unexpected inspect call"),
    )
    # `_connector_info` stays unset so any `definition_id`/`name` lookup would hit the
    # public API; the Context layer gate must fire first.
    source = CloudSource(workspace=workspace, connector_id="source-1")
    destination = CloudDestination(workspace=workspace, connector_id="destination-1")

    for call in (
        lambda: source.execute_api_query("issues"),
        lambda: source.execute_api_action("issues", "create"),
        lambda: source.execute_sql_query("SELECT 1"),
        lambda: destination.execute_api_query("issues"),
        lambda: destination.execute_api_action("issues", "create"),
        lambda: destination.execute_sql_query("SELECT 1"),
    ):
        with pytest.raises(AirbyteExternalAccessNotEnabledError):
            call()

    assert calls == []


def test_execute_sql_query_infers_dialect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": [{"n": 1}]})
    destination = _seed_destination(workspace, "destination-1", SNOWFLAKE_DEFINITION_ID)

    result = destination.execute_sql_query("SELECT 1", page_size=5)

    assert isinstance(result, AgentExecuteResult)
    body = calls[0]["request_body"]
    assert body["entity"] == "sql"
    assert body["action"] == "sql_select"
    assert body["params"]["sql"] == "SELECT 1"
    assert body["params"]["sql_dialect"] == SNOWFLAKE_DIALECT
    assert body["params"]["limit"] == 5
    assert body["params"]["workspace_id"] == "workspace-id"


def test_execute_sql_query_explicit_dialect_on_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": []})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    source.execute_sql_query("SELECT 1", sql_dialect="postgres")

    params = calls[0]["request_body"]["params"]
    assert params["sql_dialect"] == "postgres"
    assert calls[0]["connector_id"] == "source-1"


def test_execute_api_query_works_on_destinations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": []})
    destination = _seed_destination(
        workspace, "destination-1", "not-a-passthrough-definition"
    )

    result = destination.execute_api_query("tables", "list")

    assert result.status == "success"
    assert calls[0]["connector_id"] == "destination-1"
    assert calls[0]["request_body"]["action"] == "list"


def test_execute_sql_query_requires_dialect_when_not_inferrable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success"})
    destination = _seed_destination(
        workspace, "destination-1", "not-a-passthrough-definition"
    )

    with pytest.raises(PyAirbyteInputError):
        destination.execute_sql_query("SELECT 1")

    assert calls == []


_MISSING: Any = object()


def _patch_connector_probes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    source: Any = _MISSING,  # noqa: ANN401
    destination: Any = _MISSING,  # noqa: ANN401
) -> list[str]:
    """Stub the `get_source`/`get_destination` probes used for lazy kind resolution.

    A `_MISSING` payload raises `AirbyteMissingResourceError`, mirroring the API's
    not-found response. Returns the ordered list of probes attempted.
    """
    calls: list[str] = []

    def fake_get_source(*, source_id: str, **kwargs: Any) -> Any:  # noqa: ANN401, ARG001
        calls.append("source")
        if source is _MISSING:
            raise AirbyteMissingResourceError(
                resource_name_or_id=source_id,
                resource_type="source",
            )
        return source

    def fake_get_destination(*, destination_id: str, **kwargs: Any) -> Any:  # noqa: ANN401, ARG001
        calls.append("destination")
        if destination is _MISSING:
            raise AirbyteMissingResourceError(
                resource_name_or_id=destination_id,
                resource_type="destination",
            )
        return destination

    monkeypatch.setattr(api_util, "get_source", fake_get_source)
    monkeypatch.setattr(api_util, "get_destination", fake_get_destination)
    return calls


def _source_payload(connector_id: str, name: str = "Gong") -> SimpleNamespace:
    """Return a duck-typed `SourceResponse` for the kind probe."""
    return SimpleNamespace(
        source_id=connector_id,
        name=name,
        definition_id="source-gong",
    )


def _destination_payload(
    connector_id: str,
    definition_id: str = "destination-snowflake",
) -> SimpleNamespace:
    """Return a duck-typed `DestinationResponse` for the kind probe."""
    return SimpleNamespace(
        destination_id=connector_id,
        name="Snowflake",
        definition_id=definition_id,
        configuration=None,
    )


def test_untyped_connector_resolves_as_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    probes = _patch_connector_probes(monkeypatch, source=_source_payload("connector-1"))
    connector = workspace.get_connector("connector-1")

    assert isinstance(connector, CloudConnector)
    assert connector.connector_type == "source"
    assert connector.connector_type == "source"
    assert connector.name == "Gong"

    assert probes == ["source"]


def test_untyped_connector_resolves_as_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    probes = _patch_connector_probes(
        monkeypatch,
        destination=_destination_payload("connector-1"),
    )
    connector = workspace.get_connector("connector-1")

    assert connector.connector_type == "destination"
    assert connector.connector_type == "destination"

    assert probes == ["source", "destination"]


def test_untyped_connector_raises_when_neither_probe_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    probes = _patch_connector_probes(monkeypatch)
    connector = workspace.get_connector("connector-1")

    with pytest.raises(AirbyteMissingResourceError):
        connector.connector_type

    assert probes == ["source", "destination"]


def test_as_cloud_source_and_destination_casts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_connector_probes(monkeypatch, source=_source_payload("connector-1"))
    connector = workspace.get_connector("connector-1")

    source = connector.as_cloud_source()

    assert isinstance(source, CloudSource)
    assert source.connector_id == "connector-1"
    assert source._connector_info is connector._connector_info  # noqa: SLF001
    assert source.as_cloud_source() is source
    with pytest.raises(PyAirbyteInputError, match="not a destination"):
        connector.as_cloud_destination()


def test_as_cloud_destination_on_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_connector_probes(
        monkeypatch,
        destination=_destination_payload("connector-1"),
    )
    connector = workspace.get_connector("connector-1")

    destination = connector.as_cloud_destination()

    assert isinstance(destination, CloudDestination)
    assert destination.as_cloud_destination() is destination
    with pytest.raises(PyAirbyteInputError, match="not a source"):
        connector.as_cloud_source()


def test_untyped_connector_executes_without_kind_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Happy-path execute must not touch `connector_type`/`definition_id`/the flag."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": []})
    monkeypatch.setattr(
        api_util,
        "get_source",
        lambda **_: pytest.fail("execute must not probe the connector kind"),
    )
    monkeypatch.setattr(
        api_util,
        "get_destination",
        lambda **_: pytest.fail("execute must not probe the connector kind"),
    )
    connector = workspace.get_connector("connector-1")

    result = connector.execute_api_query("issues", "list")

    assert result.status == "success"
    assert len(calls) == 1
    assert connector._connector_type is None  # noqa: SLF001


def test_execute_sql_query_infers_dialect_via_untyped_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": []})
    _patch_connector_probes(
        monkeypatch,
        destination=_destination_payload(
            "connector-1",
            definition_id=SNOWFLAKE_DEFINITION_ID,
        ),
    )
    connector = workspace.get_connector("connector-1")

    connector.execute_sql_query("SELECT 1")

    assert calls[0]["request_body"]["params"]["sql_dialect"] == SNOWFLAKE_DIALECT
