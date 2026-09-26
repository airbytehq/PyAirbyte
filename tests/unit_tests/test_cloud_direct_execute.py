# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for direct entity/action execution on `CloudConnector`."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest
import requests

from airbyte._direct_connectors import api_util as agents_api_util
from airbyte._util import api_util
from airbyte._direct_connectors.models import ExternalApiExecuteResult
from airbyte.cloud import workspaces as cloud_workspaces
from airbyte.cloud.connectors import (
    CloudConnector,
    CloudDestination,
    CloudSource,
    ConnectorType,
    ExternalApiReadOnlyAction,
    ExternalApiWriteAction,
)
from airbyte._direct_connectors.models import (
    _SQL_PASSTHROUGH_DESTINATION_DIALECTS,
)
from airbyte.cloud.models import (
    CloudDestinationInfo,
    CloudSourceInfo,
)
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.cloud._credentials import _AirbyteCredentials
from airbyte.exceptions import (
    AirbyteCloudApiError,
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
    """Stub HTTP responses while exercising the Cloud execution result boundary."""
    calls: list[dict[str, Any]] = []
    execute = agents_api_util.execute_cloud_connector_action

    def fake_request(**kwargs: Any) -> requests.Response:
        assert kwargs["method"] == "POST"
        call = calls[-1]
        route = (
            "sources"
            if call["connector_type"] == ConnectorType.SOURCE
            else "destinations"
        )
        assert kwargs["url"].endswith(f"/{route}/{call['connector_id']}/execute")
        assert kwargs["json"] == call["request_body"]
        raw_response = requests.Response()
        raw_response.status_code = 200
        raw_response.headers["Content-Type"] = "application/json"
        raw_response._content = json.dumps(response).encode()  # noqa: SLF001
        return raw_response

    monkeypatch.setattr(requests, "request", fake_request)

    def fake_execute(**kwargs: Any) -> ExternalApiExecuteResult:
        calls.append(kwargs)
        if error is not None:
            raise error
        return execute(**kwargs)

    monkeypatch.setattr(agents_api_util, "execute_cloud_connector_action", fake_execute)
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
    calls = _patch_execute(monkeypatch, {"data": [{"id": 1}]})
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

    assert isinstance(result, ExternalApiExecuteResult)
    assert result.status == "success"
    assert len(calls) == 1
    call = calls[0]
    assert call["connector_id"] == "source-1"
    assert call["connector_type"] is ConnectorType.SOURCE
    assert call["credentials"] is workspace._credentials  # noqa: SLF001
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


@pytest.mark.parametrize("action", list(ExternalApiWriteAction))
def test_execute_api_action_is_unsupported(
    monkeypatch: pytest.MonkeyPatch, action: ExternalApiWriteAction
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"data": {"id": 1}})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    with pytest.raises(
        PyAirbyteInputError, match="write actions are not supported yet"
    ):
        source.execute_api_action(
            "issues", action, {"title": "New issue"}, intent="file a bug"
        )

    assert calls == []


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
    calls = _patch_execute(monkeypatch, {"data": None})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    with pytest.raises(PyAirbyteInputError):
        getattr(source, method_name)("issues", bad_action)  # type: ignore[arg-type]

    assert calls == []


def test_execute_direct_action_rejects_write_action_as_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"data": None})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    with pytest.raises(PyAirbyteInputError, match="read-only"):
        source._execute_direct_action(  # noqa: SLF001
            entity_type="issues",
            action="delete",
            read_only=True,
        )

    assert calls == []


@pytest.mark.parametrize("execute_status", [403, 404, 500])
@pytest.mark.parametrize("docs_status", [403, 404, 500])
def test_execute_error_handling(
    monkeypatch: pytest.MonkeyPatch,
    execute_status: int,
    docs_status: int,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    error = AirbyteCloudApiError(status_code=execute_status)
    _patch_execute(monkeypatch, {}, error=error)
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    def unavailable_docs(**_kwargs: Any) -> dict[str, Any]:
        raise AirbyteError(context={"status_code": docs_status})

    monkeypatch.setattr(agents_api_util, "read_cloud_skill_docs", unavailable_docs)
    with pytest.raises(AirbyteError) as exc_info:
        source.execute_api_query("issues")

    assert exc_info.value is error


@pytest.mark.parametrize("method_name", ["execute_api_query", "execute_api_action"])
def test_direct_methods_raise_without_context_layer(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch, available=False)
    calls = _patch_execute(monkeypatch, {"data": None})
    monkeypatch.setattr(
        agents_api_util,
        "read_cloud_skill_docs",
        lambda **_: pytest.fail("unexpected skill docs call"),
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
            expected_error = (
                PyAirbyteInputError
                if method == "execute_api_action"
                else AirbyteExternalAccessNotEnabledError
            )
            with pytest.raises(expected_error):
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
    calls = _patch_execute(monkeypatch, {"data": [{"n": 1}]})

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

    assert isinstance(result, ExternalApiExecuteResult)
    body = calls[0]["request_body"]
    assert body["entity"] == "sql"
    assert body["action"] == "sql_select"
    assert body["params"]["sql"] == "SELECT 1"
    assert body["params"]["sql_dialect"] == expected_dialect
    assert body["params"]["dry_run"] is False
    assert body["params"]["limit"] == 5
    assert body["params"]["workspace_id"] == "workspace-id"
    assert calls[0]["connector_id"] == "connector-1"
    if probes is not None:
        assert probes == ["source", "destination"]


def test_execute_sql_query_dry_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`dry_run=True` is forwarded in the request params."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"data": []})
    connector = _seed_destination(workspace, "connector-1", SNOWFLAKE_DEFINITION_ID)

    connector.execute_sql_query("SELECT 1", dry_run=True)

    assert calls[0]["request_body"]["params"]["dry_run"] is True


def test_execute_api_query_works_on_destinations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"data": []})
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
    calls = _patch_execute(monkeypatch, {"data": None})
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
    connector = workspace.get_connector(connector_id="connector-1")

    assert isinstance(connector, CloudConnector)
    assert connector.connector_type == expected_type
    # Kind and connector info are cached: repeat reads don't re-probe.
    assert connector.connector_type == expected_type
    if expected_name is not None:
        assert connector.name == expected_name

    assert probes == expected_probes


def test_untyped_connector_resolves_kind_from_cached_info(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A `CloudDestinationInfo` cached on an untyped connector resolves kind without API."""
    workspace = _make_workspace(monkeypatch)
    monkeypatch.setattr(
        api_util,
        "get_source",
        lambda **_: pytest.fail("get_source must not be called"),
    )
    monkeypatch.setattr(
        api_util,
        "get_destination",
        lambda **_: pytest.fail("get_destination must not be called"),
    )
    connector = workspace.get_connector(connector_id="connector-1")
    connector._connector_info = CloudDestinationInfo(  # noqa: SLF001
        destination_id="connector-1",
        name="Warehouse",
        definition_id="destination-snowflake",
    )

    assert connector.connector_type == ConnectorType.DESTINATION


def test_untyped_connector_raises_when_neither_probe_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    probes = _patch_connector_probes(monkeypatch)
    connector = workspace.get_connector(connector_id="connector-1")

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
    connector = workspace.get_connector(connector_id="connector-1")

    casted = getattr(connector, cast_name)()

    assert isinstance(casted, expected_cls)
    assert casted.connector_id == "connector-1"
    assert casted._connector_info is connector._connector_info  # noqa: SLF001
    assert getattr(casted, cast_name)() is casted
    with pytest.raises(PyAirbyteInputError, match=mismatch_match):
        getattr(connector, mismatch_name)()


def test_untyped_connector_execute_resolves_kind_for_routing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Execute resolves `connector_type` once, because the Cloud route depends on it."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"data": []})
    probes = _patch_connector_probes(monkeypatch, source=_source_payload("connector-1"))
    connector = workspace.get_connector(connector_id="connector-1")

    result = connector.execute_api_query("issues", "list")  # type: ignore[arg-type]

    assert result.status == "success"
    assert len(calls) == 1
    assert calls[0]["connector_type"] is ConnectorType.SOURCE
    assert probes == ["source"]


@pytest.mark.parametrize(
    ("status_code", "expected_phrase", "expects_upstream_guidance"),
    [
        pytest.param(502, None, True, id="bad_gateway"),
        pytest.param(503, None, True, id="service_unavailable"),
        pytest.param(504, None, True, id="gateway_timeout"),
        pytest.param(401, "Unauthorized", False, id="unauthorized"),
        pytest.param(403, "Forbidden", False, id="forbidden"),
    ],
)
def test_agent_request_error_message_and_guidance(
    status_code: int,
    expected_phrase: str | None,
    expects_upstream_guidance: bool,
) -> None:
    """Upstream 5xx failures get non-credentials guidance and no status phrase."""
    raw_response = requests.Response()
    raw_response.status_code = status_code
    raw_response.url = "https://cloud.airbyte.com/api/v1/sources/source-1/execute"

    message = agents_api_util._error_message(  # noqa: SLF001
        response=raw_response, full_url=raw_response.url
    )
    guidance = agents_api_util._error_guidance(response=raw_response)  # noqa: SLF001

    if expected_phrase is None:
        assert f"{status_code} when accessing" in message
    else:
        assert f"({expected_phrase})" in message
    if expects_upstream_guidance:
        assert guidance is not None
        assert "upstream" in guidance
    else:
        assert guidance is None or "upstream" not in guidance


def test_agent_request_non_2xx_raises_cloud_api_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-2xx Config API response raises `AirbyteCloudApiError` with `status_code`."""
    _patch_context_layer(monkeypatch)
    raw_response = requests.Response()
    raw_response.status_code = 503
    raw_response.url = "https://cloud.airbyte.com/api/v1/sources/source-1/execute"
    monkeypatch.setattr(agents_api_util.requests, "request", lambda **_: raw_response)

    credentials = _AirbyteCredentials.from_auth(
        bearer_token="token",
        public_api_root="https://api.airbyte.com/v1",
        env_vars=False,
    )
    with pytest.raises(AirbyteCloudApiError) as exc_info:
        agents_api_util.make_cloud_agent_request(
            method="POST",
            path="/sources/source-1/execute",
            credentials=credentials,
            json={},
        )

    assert exc_info.value.status_code == 503
    assert "(Service Unavailable)" not in exc_info.value.get_message()
    assert "upstream" in (exc_info.value.guidance or "")


@pytest.mark.parametrize("kind", ["source", "destination"])
@pytest.mark.parametrize(
    "data",
    [
        None,
        False,
        0,
        "payload",
        [],
        [{"id": 1}],
        {"data": [{"n": 1}], "meta": {"end_cursor": "nested-cursor"}},
    ],
)
def test_cloud_execute_preserves_payload(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    data: Any,  # noqa: ANN401
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    _patch_execute(monkeypatch, {"data": data})
    connector = (
        _seed_source(workspace, "source-1", "Source")
        if kind == "source"
        else _seed_destination(workspace, "destination-1", SNOWFLAKE_DEFINITION_ID)
    )

    result = connector.execute_api_query("records")

    assert isinstance(result, ExternalApiExecuteResult)
    assert result.status == "success"
    assert result.result == data
    assert result.has_next_page is False
    assert result.end_cursor is None
    assert result.execution_metadata.model_dump(exclude_none=True) == {}
    if isinstance(data, list):
        assert result.entities == data
    else:
        with pytest.raises(PyAirbyteInputError, match="did not return a list"):
            _ = result.entities


def test_cloud_execute_maps_top_level_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    data = {"data": [{"n": 1}], "meta": {"end_cursor": "nested-cursor"}}
    meta = {"has_next_page": True, "end_cursor": "outer-cursor", "custom": {"total": 2}}
    _patch_execute(monkeypatch, {"data": data, "meta": meta})
    destination = _seed_destination(workspace, "destination-1", SNOWFLAKE_DEFINITION_ID)

    result = destination.execute_sql_query("SELECT 1")

    assert result.result == data
    assert result.connector_metadata.model_dump() == meta
    assert result.has_next_page is True
    assert result.end_cursor == "outer-cursor"


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({}, "missing required `data`"),
        ({"meta": {}}, "missing required `data`"),
        ({"status": "success", "result": []}, "missing required `data`"),
        *[
            ({"data": [], "meta": meta}, "`meta` must be an object")
            for meta in (None, [], "metadata", 1, True)
        ],
    ],
)
def test_cloud_execute_rejects_malformed_envelope(
    monkeypatch: pytest.MonkeyPatch, payload: dict[str, Any], message: str
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    _patch_execute(monkeypatch, payload)
    source = _seed_source(workspace, "source-1", "Source")

    with pytest.raises(AirbyteError, match=message) as exc_info:
        source.execute_api_query("records")

    assert (exc_info.value.context or {})["path"] == "/sources/source-1/execute"
