# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for direct entity/action execution on `CloudConnector`."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
import requests

from airbyte._direct_connectors import api_util as agents_api_util
from airbyte._util import api_util
from airbyte._direct_connectors.models import (
    ExternalApiExecuteResult,
    ExternalSearchResult,
    ExternalSearchStatusResult,
    ExternalSearchStreamFilter,
    ExternalSearchType,
)
from airbyte.cloud import workspaces as cloud_workspaces
from airbyte.cloud.connectors import (
    CloudConnector,
    CloudDestination,
    CloudSource,
    ConnectorFeature,
    ConnectorType,
    ExternalApiReadOnlyAction,
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

    def fake_execute(**kwargs: Any) -> dict[str, Any]:
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


def _patch_direct_access(*, enabled: bool) -> Any:
    """Mock `CloudConnector.is_feature_enabled` without hitting the API."""
    return patch.object(
        CloudConnector,
        "is_feature_enabled",
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


def test_execute_api_action_forwards_to_cloud_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_execute(monkeypatch, {"data": {"id": 1}})
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    result = source.execute_api_action(
        "issues",
        "create",  # type: ignore[arg-type]
        {"title": "New issue"},
        intent="file a bug",
    )

    assert isinstance(result, ExternalApiExecuteResult)
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


_FLAG_LOOKUP_ERROR: Any = object()
"""Marker: the feature lookup itself fails."""


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
        error=AirbyteCloudApiError(
            status_code=execute_status,
            context={"status_code": execute_status},
        ),
    )
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    flag_patch = (
        _patch_direct_access(enabled=access_flag)
        if isinstance(access_flag, bool)
        else patch.object(
            CloudConnector,
            "is_feature_enabled",
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
        assert isinstance(exc_info.value, AirbyteCloudApiError)
        assert exc_info.value.status_code == expected_status


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


_SEARCH_RESPONSE: dict[str, Any] = {
    "hits": [
        {
            "source_id": "source-1",
            "stream_name": "issues",
            "entity_id": "42",
            "entity_data": {"title": "Refund request"},
            "score": 0.91,
            "context": "Refund request from ACME",
        }
    ],
    "metadata": [
        {
            "source_id": "source-1",
            "destination_id": "destination-1",
            "stream_name": "issues",
            "index_name": "issues_semantic",
            "search_time_ms": 12,
        },
        {
            "source_id": "source-1",
            "destination_id": "destination-1",
            "stream_name": "users",
            "index_name": "users_keyword",
            "error": "index unavailable",
            "search_time_ms": 3,
        },
    ],
    "response_time_ms": 20,
}

_SEARCH_STATUS_RESPONSE: dict[str, Any] = {
    "sources": [
        {
            "source_id": "source-1",
            "destination_id": "destination-1",
            "connection_id": "connection-1",
            "streams": [
                {
                    "namespace": None,
                    "name": "issues",
                    "backfill": {
                        "expected_total_records": 100,
                        "collected_records": 100,
                        "indexed_records": 100,
                        "started_at": "2026-09-01T00:00:00Z",
                        "completed_at": "2026-09-01T01:00:00Z",
                        "status": "complete",
                    },
                    "indexes": [
                        {
                            "type": "semantic",
                            "name": "issues_semantic",
                            "indexed_records": 100,
                            "physical_rows": 100,
                            "optimized_physical_rows": 100,
                            "updated_at": "2026-09-01T01:00:00Z",
                            "status": "idle",
                            "storage_size_mb": 1.5,
                        }
                    ],
                }
            ],
        }
    ]
}


def _patch_search(
    monkeypatch: pytest.MonkeyPatch,
    response: dict[str, Any],
    *,
    error: Exception | None = None,
) -> list[dict[str, Any]]:
    """Stub HTTP responses for the Cloud search and search-status routes.

    Returns the recorded `requests.request` kwargs, one per HTTP call.
    """
    calls: list[dict[str, Any]] = []

    def fake_request(**kwargs: Any) -> requests.Response:
        calls.append(kwargs)
        if error is not None:
            raise error
        raw_response = requests.Response()
        raw_response.status_code = 200
        raw_response.headers["Content-Type"] = "application/json"
        raw_response._content = json.dumps(response).encode()  # noqa: SLF001
        return raw_response

    monkeypatch.setattr(requests, "request", fake_request)
    return calls


@pytest.mark.parametrize("kind", ["source", "destination"])
def test_search_routes_by_connector_type(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    connector = (
        _seed_source(workspace, "connector-1", "GitHub Issues")
        if kind == "source"
        else _seed_destination(workspace, "connector-1", SNOWFLAKE_DEFINITION_ID)
    )
    route = "sources" if kind == "source" else "destinations"

    calls = _patch_search(monkeypatch, _SEARCH_RESPONSE)
    connector.execute_search_query("refunds")
    assert calls[0]["method"] == "POST"
    assert calls[0]["url"].endswith(f"/{route}/connector-1/search")

    calls = _patch_search(monkeypatch, _SEARCH_STATUS_RESPONSE)
    connector.get_search_status()
    assert calls[0]["method"] == "GET"
    assert calls[0]["url"].endswith(f"/{route}/connector-1/search-status")
    assert calls[0]["json"] is None


def test_search_request_body_omits_none_and_maps_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_search(monkeypatch, _SEARCH_RESPONSE)
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    source.execute_search_query("refunds")
    source.execute_search_query(
        "refunds",
        search_type=ExternalSearchType.SEMANTIC,
        limit=5,
        streams=[
            {"stream_name": "issues", "fields": ["title"]},
            ExternalSearchStreamFilter(stream_name="users", namespace="public"),
        ],
        lookback_seconds=3600,
        max_context_chars=200,
        min_similarity=0.3,
        max_similarity_diff=0.1,
        destination_id="destination-1",
    )

    assert calls[0]["json"] == {"type": "hybrid", "prompt": "refunds"}
    assert calls[1]["json"] == {
        "type": "semantic",
        "prompt": "refunds",
        "limit": 5,
        "streams": [
            {"stream_name": "issues", "fields": ["title"]},
            {"stream_name": "users", "namespace": "public"},
        ],
        "lookback_s": 3600,
        "max_context_chars": 200,
        "min_similarity": 0.3,
        "max_similarity_diff": 0.1,
        "destination_id": "destination-1",
    }


def test_search_parses_response_and_collects_index_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    _patch_search(monkeypatch, _SEARCH_RESPONSE)
    source = _seed_source(workspace, "source-1", "GitHub Issues")

    result = source.execute_search_query("refunds", search_type="keyword")  # type: ignore[arg-type]

    assert isinstance(result, ExternalSearchResult)
    assert [hit.entity_id for hit in result.hits] == ["42"]
    assert result.hits[0].entity_data == {"title": "Refund request"}
    assert result.hits[0].score == pytest.approx(0.91)
    assert result.response_time_ms == 20
    assert [entry.index_name for entry in result.metadata] == [
        "issues_semantic",
        "users_keyword",
    ]
    assert len(result.warnings) == 1
    assert "users" in result.warnings[0]
    assert "index unavailable" in result.warnings[0]


def test_search_status_parses_response(monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    _patch_search(monkeypatch, _SEARCH_STATUS_RESPONSE)
    destination = _seed_destination(workspace, "destination-1", SNOWFLAKE_DEFINITION_ID)

    result = destination.get_search_status()

    assert isinstance(result, ExternalSearchStatusResult)
    assert result.has_indexes
    stream = result.sources[0].streams[0]
    assert result.sources[0].connection_id == "connection-1"
    assert stream.backfill is not None
    assert stream.backfill.status == "complete"
    assert stream.backfill.backfill_start_time is None
    assert stream.indexes[0].status == "idle"
    assert stream.indexes[0].storage_size_mb == pytest.approx(1.5)


@pytest.mark.parametrize(
    ("method_name", "payload"),
    [
        pytest.param(
            "execute_search_query", {"hits": [{"entity_id": "1"}]}, id="search"
        ),
        pytest.param("get_search_status", {"sources": [{"streams": []}]}, id="status"),
    ],
)
def test_search_rejects_malformed_response(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    payload: dict[str, Any],
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    _patch_search(monkeypatch, payload)
    source = _seed_source(workspace, "source-1", "GitHub Issues")
    args = ("refunds",) if method_name == "execute_search_query" else ()

    with pytest.raises(
        AirbyteError, match="Malformed Airbyte Cloud search"
    ) as exc_info:
        getattr(source, method_name)(*args)

    assert (exc_info.value.context or {})["path"].startswith("/sources/source-1/search")


@pytest.mark.parametrize(
    ("kind", "kwargs", "match"),
    [
        pytest.param(
            "source", {"search_type": "fuzzy"}, "not valid", id="bad_search_type"
        ),
        pytest.param("source", {"limit": 0}, "`limit`", id="zero_limit"),
        pytest.param("source", {"limit": -1}, "`limit`", id="negative_limit"),
        pytest.param(
            "source",
            {"search_type": "keyword", "min_similarity": 0.5},
            "keyword",
            id="min_similarity_with_keyword",
        ),
        pytest.param(
            "source",
            {"search_type": ExternalSearchType.KEYWORD, "max_similarity_diff": 0.1},
            "keyword",
            id="max_similarity_diff_with_keyword",
        ),
        pytest.param(
            "destination",
            {"destination_id": "destination-2"},
            "source-level",
            id="destination_id_on_destination",
        ),
        pytest.param(
            "source", {"streams": [{"fields": ["id"]}]}, "`streams`", id="bad_stream"
        ),
    ],
)
def test_search_input_validation(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    kwargs: dict[str, Any],
    match: str,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls = _patch_search(monkeypatch, _SEARCH_RESPONSE)
    connector = (
        _seed_source(workspace, "connector-1", "GitHub Issues")
        if kind == "source"
        else _seed_destination(workspace, "connector-1", SNOWFLAKE_DEFINITION_ID)
    )

    with pytest.raises(PyAirbyteInputError, match=match):
        connector.execute_search_query("refunds", **kwargs)

    assert calls == []


@pytest.mark.parametrize("method_name", ["execute_search_query", "get_search_status"])
@pytest.mark.parametrize(
    ("status_code", "search_flag", "expected_exc", "expected_status"),
    [
        pytest.param(
            403,
            False,
            AirbyteExternalAccessNotEnabledError,
            None,
            id="forbidden_disabled_raises_not_enabled",
        ),
        pytest.param(404, True, AirbyteError, 404, id="not_found_enabled_reraises"),
        pytest.param(
            403,
            _FLAG_LOOKUP_ERROR,
            AirbyteError,
            403,
            id="flag_lookup_failure_reraises",
        ),
        pytest.param(500, True, AirbyteError, 500, id="other_error_propagates"),
    ],
)
def test_search_error_handling(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    status_code: int,
    search_flag: Any,  # noqa: ANN401
    expected_exc: type[Exception],
    expected_status: int | None,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    _patch_search(
        monkeypatch,
        {},
        error=AirbyteCloudApiError(status_code=status_code),
    )
    source = _seed_source(workspace, "source-1", "GitHub Issues")
    args = ("refunds",) if method_name == "execute_search_query" else ()

    flag_patch = (
        patch.object(CloudConnector, "is_feature_enabled", return_value=search_flag)
        if isinstance(search_flag, bool)
        else patch.object(
            CloudConnector,
            "is_feature_enabled",
            side_effect=AirbyteError(context={"status_code": 500}),
        )
    )
    with flag_patch as flag_mock, pytest.raises(expected_exc) as exc_info:
        getattr(source, method_name)(*args)

    if status_code in {403, 404}:
        flag_mock.assert_called_once_with(ConnectorFeature.SEARCH_INDEXING)
    else:
        flag_mock.assert_not_called()
    if expected_exc is AirbyteExternalAccessNotEnabledError:
        assert isinstance(exc_info.value, AirbyteExternalAccessNotEnabledError)
        assert exc_info.value.connector_id == "source-1"
        assert exc_info.value.connector_name == "GitHub Issues"
        assert "search_indexing" in (exc_info.value.guidance or "")
    else:
        assert not isinstance(exc_info.value, AirbyteExternalAccessNotEnabledError)
        assert isinstance(exc_info.value, AirbyteCloudApiError)
        assert exc_info.value.status_code == expected_status


def test_search_methods_raise_without_context_layer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch, available=False)
    calls = _patch_search(monkeypatch, _SEARCH_RESPONSE)
    # `_connector_info` stays unset so any `connector_type`/`name` lookup would hit the
    # public API; the Context layer gate must fire first.
    connectors = (
        CloudSource(workspace=workspace, connector_id="source-1"),
        CloudDestination(workspace=workspace, connector_id="destination-1"),
        workspace.get_connector("connector-1"),
    )

    for connector in connectors:
        with pytest.raises(AirbyteExternalAccessNotEnabledError):
            connector.execute_search_query("refunds", search_type="fuzzy")  # type: ignore[arg-type]
        with pytest.raises(AirbyteExternalAccessNotEnabledError):
            connector.get_search_status()

    assert calls == []
