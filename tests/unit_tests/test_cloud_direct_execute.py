# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for direct entity/action execution on `CloudConnector`."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import PropertyMock, patch

import pytest

from airbyte._direct_connectors import api_util as agents_api_util
from airbyte._util import api_util
from airbyte._direct_connectors.models import AgentExecuteResult
from airbyte.cloud import workspaces as cloud_workspaces
from airbyte.cloud.connectors import (
    CloudConnector,
    CloudDestination,
    CloudSource,
    ConnectorType,
    ExternalApiReadOnlyAction,
)
from airbyte.cloud.models import (
    _SQL_PASSTHROUGH_DESTINATION_DIALECTS,
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


SNOWFLAKE_DEFINITION_ID = next(iter(_SQL_PASSTHROUGH_DESTINATION_DIALECTS))
SNOWFLAKE_DIALECT = _SQL_PASSTHROUGH_DESTINATION_DIALECTS[SNOWFLAKE_DEFINITION_ID]

_MISSING: Any = object()
"""Sentinel for parameters that should be omitted from a call rather than passed."""


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


@pytest.mark.parametrize(
    ("action_input", "expected_action"),
    [
        pytest.param(_MISSING, "list", id="default_is_list"),
        pytest.param("get", "get", id="string_get"),
        pytest.param("search", "search", id="string_search"),
        pytest.param(
            ExternalApiReadOnlyAction.SEARCH,
            "search",
            id="enum_search",
        ),
    ],
)
def test_execute_api_query_forwards_action(
    monkeypatch: pytest.MonkeyPatch,
    action_input: Any,  # noqa: ANN401
    expected_action: str,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": [{"id": 1}]})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    kwargs: dict[str, Any] = {}
    if action_input is not _MISSING:
        kwargs["action"] = action_input
    result = source.execute_api_query(
        "issues",
        api_args={"repository": "airbytehq/PyAirbyte"},
        select_fields=["title"],
        exclude_fields=["body"],
        page_size=10,
        cursor="cursor-1",
        skip_truncation=False,
        intent="find open issues",
        **kwargs,
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
        "action": expected_action,
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


def test_execute_api_action_forwards_to_agents_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": {"id": 1}})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    result = source.execute_api_action(
        "issues",
        "create",  # type: ignore[arg-type]
        {"title": "New issue"},
        intent="file a bug",
    )

    assert isinstance(result, AgentExecuteResult)
    body = calls[0]["request_body"]
    assert body["entity"] == "issues"
    assert body["action"] == "create"
    assert body["params"] == {"title": "New issue"}
    assert body["intent"] == "file a bug"


@pytest.mark.parametrize(
    ("method_name", "bad_action"),
    [
        pytest.param("execute_api_query", "create", id="query_rejects_write"),
        pytest.param("execute_api_action", "get", id="action_rejects_read"),
    ],
)
def test_execute_rejects_mismatched_action(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    bad_action: str,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success"})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    with pytest.raises(PyAirbyteInputError):
        getattr(source, method_name)("issues", bad_action)  # type: ignore[arg-type]

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


_FLAG_LOOKUP_ERROR: Any = object()
"""Marker: the `external_access_enabled` lookup itself fails."""


@pytest.mark.parametrize(
    ("execute_status", "access_flag", "expected_exc", "expected_status"),
    [
        pytest.param(
            403,
            False,
            AirbyteExternalAccessNotEnabledError,
            None,
            id="forbidden_disabled_raises_not_enabled",
        ),
        pytest.param(
            404,
            True,
            AirbyteError,
            404,
            id="not_found_enabled_reraises",
        ),
        pytest.param(
            403,
            _FLAG_LOOKUP_ERROR,
            AirbyteError,
            403,
            id="flag_lookup_failure_reraises",
        ),
        pytest.param(
            500,
            True,
            AirbyteError,
            500,
            id="other_error_propagates",
        ),
    ],
)
def test_execute_error_handling(
    monkeypatch: pytest.MonkeyPatch,
    execute_status: int,
    access_flag: Any,  # noqa: ANN401
    expected_exc: type[Exception],
    expected_status: int | None,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    _patch_execute(
        monkeypatch,
        {},
        error=AirbyteError(context={"status_code": execute_status}),
    )
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    flag_patch = (
        _patch_external_access(enabled=access_flag)
        if isinstance(access_flag, bool)
        else patch.object(
            CloudConnector,
            "external_access_enabled",
            new_callable=PropertyMock,
            side_effect=AirbyteError(context={"status_code": 500}),
        )
    )
    with flag_patch, pytest.raises(expected_exc) as exc_info:
        source.execute_api_query("issues")

    if expected_exc is AirbyteExternalAccessNotEnabledError:
        assert isinstance(exc_info.value, AirbyteExternalAccessNotEnabledError)
        assert exc_info.value.connector_id == "source-1"
        assert exc_info.value.connector_name == "GitHub Issues"
    else:
        assert isinstance(exc_info.value, AirbyteError)
        assert not isinstance(exc_info.value, AirbyteExternalAccessNotEnabledError)
        assert (exc_info.value.context or {})["status_code"] == expected_status


@pytest.mark.parametrize("method_name", ["execute_api_query", "execute_api_action"])
def test_direct_methods_raise_without_context_layer(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
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
    connectors = (
        CloudSource(workspace=workspace, connector_id="source-1"),
        CloudDestination(workspace=workspace, connector_id="destination-1"),
    )
    method_args = (
        ("issues", "create") if method_name == "execute_api_action" else ("issues",)
    )

    for connector in connectors:
        for method in (method_name, "execute_sql_query"):
            args = method_args if method == method_name else ("SELECT 1",)
            with pytest.raises(AirbyteExternalAccessNotEnabledError):
                getattr(connector, method)(*args)

    assert calls == []


@pytest.mark.parametrize(
    ("kind", "sql_dialect", "expected_dialect"),
    [
        pytest.param(
            "seeded_destination",
            None,
            SNOWFLAKE_DIALECT,
            id="inferred_on_seeded_destination",
        ),
        pytest.param(
            "seeded_source",
            "postgres",
            "postgres",
            id="explicit_on_source",
        ),
        pytest.param(
            "untyped_destination",
            None,
            SNOWFLAKE_DIALECT,
            id="inferred_via_untyped_probe",
        ),
    ],
)
def test_execute_sql_query_dialect(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    sql_dialect: str | None,
    expected_dialect: str,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": [{"n": 1}]})

    if kind == "untyped_destination":
        probes = _patch_connector_probes(
            monkeypatch,
            destination=_destination_payload(
                "connector-1",
                definition_id=SNOWFLAKE_DEFINITION_ID,
            ),
        )
        connector: CloudConnector = workspace.get_connector("connector-1")
    elif kind == "seeded_destination":
        probes = None
        connector = _seed_destination(workspace, "connector-1", SNOWFLAKE_DEFINITION_ID)
    else:
        probes = None
        connector = _seed_source(workspace, "connector-1", "GitHub Issues")

    result = connector.execute_sql_query(
        "SELECT 1",
        sql_dialect=sql_dialect,
        page_size=5,
    )

    assert isinstance(result, AgentExecuteResult)
    body = calls[0]["request_body"]
    assert body["entity"] == "sql"
    assert body["action"] == "sql_select"
    assert body["params"]["sql"] == "SELECT 1"
    assert body["params"]["sql_dialect"] == expected_dialect
    assert body["params"]["limit"] == 5
    assert body["params"]["workspace_id"] == "workspace-id"
    assert calls[0]["connector_id"] == "connector-1"
    if probes is not None:
        assert probes == ["source", "destination"]


def test_execute_api_query_works_on_destinations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"status": "success", "result": []})
    destination = _seed_destination(
        workspace, "destination-1", "not-a-passthrough-definition"
    )

    result = destination.execute_api_query("tables", "list")  # type: ignore[arg-type]

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


@pytest.mark.parametrize(
    ("kind", "expected_type", "expected_probes", "expected_name"),
    [
        pytest.param(
            "source",
            ConnectorType.SOURCE,
            ["source"],
            "Gong",
            id="resolves_as_source",
        ),
        pytest.param(
            "destination",
            ConnectorType.DESTINATION,
            ["source", "destination"],
            None,
            id="resolves_as_destination",
        ),
    ],
)
def test_untyped_connector_resolves_kind(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    expected_type: ConnectorType,
    expected_probes: list[str],
    expected_name: str | None,
) -> None:
    workspace = _make_workspace(monkeypatch)
    probes = _patch_connector_probes(
        monkeypatch,
        **{
            kind: (_source_payload if kind == "source" else _destination_payload)(
                "connector-1"
            )
        },
    )
    connector = workspace.get_connector("connector-1")

    assert isinstance(connector, CloudConnector)
    assert connector.connector_type == expected_type
    # Kind and connector info are cached: repeat reads don't re-probe.
    assert connector.connector_type == expected_type
    if expected_name is not None:
        assert connector.name == expected_name

    assert probes == expected_probes


def test_untyped_connector_raises_when_neither_probe_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    probes = _patch_connector_probes(monkeypatch)
    connector = workspace.get_connector("connector-1")

    with pytest.raises(AirbyteMissingResourceError):
        connector.connector_type

    assert probes == ["source", "destination"]


@pytest.mark.parametrize(
    ("kind", "cast_name", "expected_cls", "mismatch_name", "mismatch_match"),
    [
        pytest.param(
            "source",
            "as_cloud_source",
            CloudSource,
            "as_cloud_destination",
            "not a destination",
            id="casts_to_source",
        ),
        pytest.param(
            "destination",
            "as_cloud_destination",
            CloudDestination,
            "as_cloud_source",
            "not a source",
            id="casts_to_destination",
        ),
    ],
)
def test_as_cloud_subclass_casts(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    cast_name: str,
    expected_cls: type,
    mismatch_name: str,
    mismatch_match: str,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_connector_probes(
        monkeypatch,
        **{
            kind: (_source_payload if kind == "source" else _destination_payload)(
                "connector-1"
            )
        },
    )
    connector = workspace.get_connector("connector-1")

    casted = getattr(connector, cast_name)()

    assert isinstance(casted, expected_cls)
    assert casted.connector_id == "connector-1"
    assert casted._connector_info is connector._connector_info  # noqa: SLF001
    assert getattr(casted, cast_name)() is casted
    with pytest.raises(PyAirbyteInputError, match=mismatch_match):
        getattr(connector, mismatch_name)()


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

    result = connector.execute_api_query("issues", "list")  # type: ignore[arg-type]

    assert result.status == "success"
    assert len(calls) == 1
    assert connector._connector_type is None  # noqa: SLF001
