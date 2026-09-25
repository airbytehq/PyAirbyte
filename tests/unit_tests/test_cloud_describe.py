# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for `CloudConnector` detail lookups and workspace lookups."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
import requests

from airbyte._direct_connectors import api_util as agents_api_util
from airbyte._direct_connectors import connector_docs
from airbyte.cloud import workspaces as cloud_workspaces
from airbyte.mcp import cloud as cloud_mcp
from airbyte.mcp._docs_results import (
    render_connector_docs_result,
    render_agent_skill_docs_result,
)
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.connectors import (
    CloudConnector,
    CloudDestination,
    CloudSource,
)
from airbyte._direct_connectors.models import (
    DirectAccessGuidance,
    _SQL_PASSTHROUGH_DESTINATION_DIALECTS,
)
from airbyte.cloud.models import (
    CloudConnectionInfo,
    ConnectionSchedule,
    ConnectorFeature,
    CloudDestinationInfo,
    CloudSourceInfo,
)
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import (
    AirbyteCloudApiError,
    AirbyteError,
    AirbyteExternalAccessNotEnabledError,
    PyAirbyteInputError,
)


SNOWFLAKE_DEFINITION_ID = next(iter(_SQL_PASSTHROUGH_DESTINATION_DIALECTS))
BIGQUERY_DEFINITION_ID = next(
    definition_id
    for definition_id, dialect in _SQL_PASSTHROUGH_DESTINATION_DIALECTS.items()
    if dialect == "bigquery"
)

SKILL_DOCS_RESPONSE: dict[str, Any] = {
    "metadata": {
        "id": "connector-source:source-1",
        "kind": "connector_source",
        "title": "GitHub",
        "warnings": ["Partial runtime metadata."],
    },
    "outline": [],
    "section_id": None,
    "content": [],
}

DESTINATION_SKILL_DOCS_RESPONSE: dict[str, Any] = {
    "metadata": {
        "id": "connector-destination:snowflake",
        "kind": "connector_destination",
        "title": "Snowflake",
        "warnings": [],
    },
    "outline": [
        {
            "id": "setup",
            "title": "Setup",
            "summary": "How to configure.",
            "available": True,
        },
        {
            "id": "streams",
            "title": "Streams",
            "summary": "Synced streams.",
            "available": True,
        },
    ],
    "section_id": None,
    "content": [{"type": "paragraph", "text": "Server overview."}],
}


def _content_text(guidance: DirectAccessGuidance) -> str:
    """Flatten all block text/code/items for substring assertions."""
    return json.dumps(guidance.content)


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


def _seed_source(workspace: CloudWorkspace, source_id: str, name: str) -> CloudSource:
    source = CloudSource(workspace=workspace, connector_id=source_id)
    source._connector_info = CloudSourceInfo(  # noqa: SLF001
        source_id=source_id, name=name, definition_id="source-definition"
    )
    return source


def _seed_destination(
    workspace: CloudWorkspace,
    destination_id: str,
    definition_id: str,
    *,
    name: str | None = None,
    configuration: dict[str, Any] | None = None,
) -> CloudDestination:
    destination = CloudDestination(workspace=workspace, connector_id=destination_id)
    destination._connector_info = CloudDestinationInfo(  # noqa: SLF001
        destination_id=destination_id,
        name=name or destination_id,
        definition_id=definition_id,
        configuration=configuration,
    )
    destination._configuration = configuration  # noqa: SLF001
    return destination


def _fake_connection(
    workspace: CloudWorkspace,
    connection_id: str,
    source_id: str,
    destination_id: str,
    *,
    name: str = "sync",
) -> CloudConnection:
    connection = CloudConnection(workspace=workspace, connection_id=connection_id)
    connection._connection_info = CloudConnectionInfo(  # noqa: SLF001
        connection_id=connection_id,
        workspace_id=workspace.workspace_id,
        source_id=source_id,
        destination_id=destination_id,
        name=name,
        configurations=SimpleNamespace(
            streams=[SimpleNamespace(name="issues"), SimpleNamespace(name="repos")]
        ),
        prefix="raw_",
        namespace_definition="destination",
        status="active",
    )
    return connection


def test_integration_name_source(monkeypatch: pytest.MonkeyPatch) -> None:
    """`integration_name` reads the source definition's display name."""
    workspace = _make_workspace(monkeypatch)
    monkeypatch.setattr(
        "airbyte._util.api_util.get_source_definition",
        lambda **_: SimpleNamespace(
            name="GitHub",
            docker_repository="airbyte/source-github",
        ),
    )
    source = _seed_source(workspace, "source-1", "GitHub")

    assert source.integration_name == "GitHub"


def test_integration_name_destination(monkeypatch: pytest.MonkeyPatch) -> None:
    """`integration_name` reads the destination definition's display name."""
    workspace = _make_workspace(monkeypatch)
    monkeypatch.setattr(
        "airbyte._util.api_util.get_destination_definition",
        lambda **_: SimpleNamespace(
            name="Snowflake",
            docker_repository="airbyte/destination-snowflake",
        ),
    )
    destination = _seed_destination(
        workspace,
        "dest-1",
        SNOWFLAKE_DEFINITION_ID,
        name="Warehouse",
    )

    assert destination.integration_name == "Snowflake"


def test_integration_name_raises_on_lookup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A definition lookup failure propagates instead of degrading to a warning."""
    workspace = _make_workspace(monkeypatch)
    monkeypatch.setattr(
        "airbyte._util.api_util.get_source_definition",
        lambda **_: (_ for _ in ()).throw(AirbyteError(message="lookup boom")),
    )
    source = _seed_source(workspace, "source-1", "GitHub")

    with pytest.raises(AirbyteError, match="lookup boom"):
        _ = source.integration_name


def test_direct_access_guidance_id_source_with_context_layer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful docs probe reports the source's deterministic skill ID."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls: list[dict[str, Any]] = []

    def fake_read_docs(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return SKILL_DOCS_RESPONSE

    monkeypatch.setattr(
        agents_api_util,
        "read_cloud_skill_docs",
        fake_read_docs,
    )
    source = _seed_source(workspace, "source-1", "GitHub")

    assert (
        source._direct_access_guidance_id()  # noqa: SLF001
        == "connector-source:source-1"
    )
    assert calls[0]["skill_id"] == "connector-source:source-1"
    assert calls[0]["workspace_id"] == "workspace-id"


def test_direct_access_guidance_id_source_without_context_layer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sources without a Context Layer report no guidance ID; reads raise."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch, available=False)
    source = _seed_source(workspace, "source-1", "GitHub")

    assert source._direct_access_guidance_id() is None  # noqa: SLF001
    with pytest.raises(AirbyteExternalAccessNotEnabledError):
        source.get_direct_access_guidance()


@pytest.mark.parametrize(
    "probe_error",
    [
        pytest.param(AirbyteCloudApiError(status_code=404), id="not_found"),
        pytest.param(AirbyteCloudApiError(status_code=403), id="forbidden"),
    ],
)
def test_direct_access_guidance_id_source_probe_not_enabled(
    monkeypatch: pytest.MonkeyPatch,
    probe_error: AirbyteError,
) -> None:
    """A 403/404 docs probe leaves no guidance ID; reads raise not-enabled."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    monkeypatch.setattr(
        agents_api_util,
        "read_cloud_skill_docs",
        lambda **_: (_ for _ in ()).throw(probe_error),
    )
    source = _seed_source(workspace, "source-1", "GitHub")

    assert source._direct_access_guidance_id() is None  # noqa: SLF001
    with pytest.raises(AirbyteExternalAccessNotEnabledError):
        source.get_direct_access_guidance()


@pytest.mark.parametrize(
    "probe_error",
    [
        pytest.param(AirbyteError(context={"status_code": 500}), id="server_error"),
        pytest.param(AirbyteError(message="malformed docs"), id="malformed"),
        pytest.param(requests.ConnectionError("offline"), id="transport"),
    ],
)
def test_direct_access_guidance_id_source_probe_failure_reraises(
    monkeypatch: pytest.MonkeyPatch,
    probe_error: Exception,
) -> None:
    """Operational docs-probe failures propagate instead of reading as disabled."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    monkeypatch.setattr(
        agents_api_util,
        "read_cloud_skill_docs",
        lambda **_: (_ for _ in ()).throw(probe_error),
    )
    source = _seed_source(workspace, "source-1", "GitHub")

    with pytest.raises(type(probe_error)):
        source._direct_access_guidance_id()  # noqa: SLF001


def test_direct_access_guidance_id_sql_passthrough_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SQL passthrough destinations use the conventional destination skill ID."""
    workspace = _make_workspace(monkeypatch)
    destination = _seed_destination(
        workspace,
        "dest-1",
        SNOWFLAKE_DEFINITION_ID,
        name="Warehouse",
    )

    assert (
        destination._direct_access_guidance_id()  # noqa: SLF001
        == "connector-destination:dest-1"
    )


def test_direct_access_guidance_id_other_destination_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Destinations without SQL passthrough have no direct-access guidance."""
    workspace = _make_workspace(monkeypatch)
    destination = _seed_destination(workspace, "dest-1", "other-definition")

    assert destination._direct_access_guidance_id() is None  # noqa: SLF001


def test_get_direct_access_guidance_non_passthrough_destination_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    destination = _seed_destination(workspace, "dest-1", "other-definition")

    with pytest.raises(
        PyAirbyteInputError, match="does not support direct access docs"
    ):
        destination.get_direct_access_guidance()


@pytest.mark.parametrize(
    "probe_error",
    [
        pytest.param(AirbyteCloudApiError(status_code=404), id="not_found"),
        pytest.param(AirbyteCloudApiError(status_code=403), id="forbidden"),
    ],
)
def test_get_direct_access_guidance_destination_not_enabled_adds_notice(
    monkeypatch: pytest.MonkeyPatch,
    probe_error: AirbyteError,
) -> None:
    """A 403/404 unscoped docs read returns structure-only guidance with a notice."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    monkeypatch.setattr(
        agents_api_util,
        "read_cloud_skill_docs",
        lambda **_: (_ for _ in ()).throw(probe_error),
    )
    connection = _fake_connection(workspace, "conn-1", "source-1", "snowflake")
    monkeypatch.setattr(
        CloudWorkspace, "list_connections", lambda *_, **__: [connection]
    )
    monkeypatch.setattr(CloudWorkspace, "list_sources", lambda *_, **__: [])
    destination = _seed_destination(
        workspace,
        "snowflake",
        SNOWFLAKE_DEFINITION_ID,
        configuration={"database": "DATABASE", "schema": "SCHEMA"},
    )

    notice = connector_docs.SQL_PASSTHROUGH_NOT_ENABLED_NOTICE
    assert "SQL passthrough is not enabled" in notice
    guidance = destination.get_direct_access_guidance()
    assert guidance.content[0] == {"type": "paragraph", "text": notice}
    assert guidance.metadata.warnings == [notice]
    assert render_connector_docs_result(guidance).warnings == [notice]
    assert render_agent_skill_docs_result(guidance).warnings == [notice]
    assert guidance.outline == []
    assert any(block.get("type") == "table" for block in guidance.content)
    assert "execute_external_sql_query" not in _content_text(guidance)
    assert "SHOW TABLES" not in _content_text(guidance)

    with pytest.raises(AirbyteError):
        destination.get_direct_access_guidance(section="streams")


def test_get_direct_access_guidance_destination_forwards_section_to_sonar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Section reads are passed to Sonar verbatim and returned unmodified."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    section_response = {
        **DESTINATION_SKILL_DOCS_RESPONSE,
        "section_id": "streams",
        "content": [{"type": "paragraph", "text": "Streams from Sonar."}],
    }
    read_docs = MagicMock(return_value=section_response)
    monkeypatch.setattr(agents_api_util, "read_cloud_skill_docs", read_docs)
    monkeypatch.setattr(CloudWorkspace, "list_connections", lambda *_, **__: [])
    destination = _seed_destination(
        workspace,
        "snowflake",
        SNOWFLAKE_DEFINITION_ID,
        configuration={"database": "DATABASE", "schema": "SCHEMA"},
    )

    guidance = destination.get_direct_access_guidance(section="streams")
    assert read_docs.call_count == 1
    assert read_docs.call_args.kwargs["section"] == "streams"
    assert guidance.section_id == "streams"
    assert [section.id for section in guidance.outline] == ["setup", "streams"]
    assert guidance.content == [{"type": "paragraph", "text": "Streams from Sonar."}]


@pytest.mark.parametrize(
    ("dialect", "definition_id", "configuration", "expected_location_phrases"),
    [
        pytest.param(
            "snowflake",
            SNOWFLAKE_DEFINITION_ID,
            {"database": "DATABASE", "schema": "SCHEMA"},
            ["Snowflake", "database `DATABASE`", "schema `SCHEMA`"],
            id="snowflake",
        ),
        pytest.param(
            "bigquery",
            BIGQUERY_DEFINITION_ID,
            {"project_id": "PROJ", "dataset_id": "DS"},
            ["BigQuery", "project `PROJ`", "dataset `DS`"],
            id="bigquery",
        ),
    ],
)
def test_get_direct_access_guidance_destination_merges_server_docs(
    monkeypatch: pytest.MonkeyPatch,
    dialect: str,
    definition_id: str,
    configuration: dict[str, Any],
    expected_location_phrases: list[str],
) -> None:
    """A successful docs read gets a short local intro; the outline is unchanged."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    read_docs = MagicMock(return_value=DESTINATION_SKILL_DOCS_RESPONSE)
    monkeypatch.setattr(agents_api_util, "read_cloud_skill_docs", read_docs)
    monkeypatch.setattr(CloudWorkspace, "list_connections", lambda *_, **__: [])
    destination = _seed_destination(
        workspace,
        dialect,
        definition_id,
        configuration=configuration,
    )

    guidance = destination.get_direct_access_guidance()
    assert read_docs.call_count == 1
    assert read_docs.call_args.kwargs["section"] is None
    assert [section.id for section in guidance.outline] == ["setup", "streams"]
    intro = guidance.content[0]
    assert intro["type"] == "paragraph"
    for phrase in expected_location_phrases:
        assert phrase in intro["text"]
    assert guidance.content[-1] == {"type": "paragraph", "text": "Server overview."}
    assert "execute_external_sql_query" not in _content_text(guidance)
    assert "SHOW TABLES" not in _content_text(guidance)
    assert connector_docs.SQL_PASSTHROUGH_NOT_ENABLED_NOTICE not in getattr(  # noqa: B009
        guidance, "warnings", []
    )


def test_get_direct_access_guidance_destination_server_error_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-403/404 docs read failure propagates the original `AirbyteError`."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    monkeypatch.setattr(
        agents_api_util,
        "read_cloud_skill_docs",
        lambda **_: (_ for _ in ()).throw(AirbyteError(context={"status_code": 500})),
    )
    destination = _seed_destination(
        workspace,
        "snowflake",
        SNOWFLAKE_DEFINITION_ID,
        configuration={"database": "DATABASE", "schema": "SCHEMA"},
    )

    with pytest.raises(AirbyteError) as exc_info:
        destination.get_direct_access_guidance()
    assert not isinstance(exc_info.value, AirbyteExternalAccessNotEnabledError)


def test_get_direct_access_guidance_destination_no_context_layer_notices(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without a Context layer API, local guidance carries the unavailable notice."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch, available=False)
    read_docs = MagicMock()
    monkeypatch.setattr(agents_api_util, "read_cloud_skill_docs", read_docs)
    monkeypatch.setattr(CloudWorkspace, "list_connections", lambda *_, **__: [])
    destination = _seed_destination(
        workspace,
        "snowflake",
        SNOWFLAKE_DEFINITION_ID,
        configuration={"database": "DATABASE", "schema": "SCHEMA"},
    )

    notice = connector_docs.SQL_PASSTHROUGH_UNAVAILABLE_NOTICE
    guidance = destination.get_direct_access_guidance()
    read_docs.assert_not_called()
    assert guidance.content[0] == {"type": "paragraph", "text": notice}
    assert guidance.metadata.warnings == [notice]
    assert render_connector_docs_result(guidance).warnings == [notice]
    assert render_agent_skill_docs_result(guidance).warnings == [notice]
    assert guidance.outline == []
    assert "execute_external_sql_query" not in _content_text(guidance)
    assert "SHOW TABLES" not in _content_text(guidance)

    with pytest.raises(PyAirbyteInputError, match="Section-scoped"):
        destination.get_direct_access_guidance(section="streams")


def _patch_list_connectors(
    monkeypatch: pytest.MonkeyPatch,
    workspace: CloudWorkspace,
    connectors: list[CloudConnector],
) -> list[bool]:
    calls: list[bool] = []
    monkeypatch.setattr(
        CloudWorkspace,
        "list_connectors",
        lambda _self, **_: (calls.append(True), connectors)[1],
    )
    return calls


def test_get_connector_by_exact_name(monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = _make_workspace(monkeypatch)
    source = _seed_source(workspace, "source-1", "GitHub Issues")
    destination = _seed_destination(workspace, "dest-1", "def-1", name="Warehouse")
    _patch_list_connectors(monkeypatch, workspace, [source, destination])

    assert workspace.get_connector(name="warehouse") is destination
    assert workspace.get_connector(name="GitHub Issues") is source


def test_get_connector_by_unique_substring(monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = _make_workspace(monkeypatch)
    source = _seed_source(workspace, "source-1", "GitHub Issues")
    _patch_list_connectors(monkeypatch, workspace, [source])

    assert workspace.get_connector(name="issues") is source


def test_get_connector_ambiguous_name_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_list_connectors(
        monkeypatch,
        workspace,
        [
            _seed_source(workspace, "source-1", "GitHub Issues"),
            _seed_source(workspace, "source-2", "GitHub PRs"),
        ],
    )

    with pytest.raises(AirbyteError, match="Multiple connectors"):
        workspace.get_connector(name="github")


def test_get_connector_no_match_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_list_connectors(
        monkeypatch, workspace, [_seed_source(workspace, "source-1", "GitHub")]
    )

    with pytest.raises(AirbyteError, match="No connector found"):
        workspace.get_connector(name="missing")


def test_get_connector_uuid_positional_makes_no_api_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    calls = _patch_list_connectors(monkeypatch, workspace, [])
    connector_id = "123e4567-e89b-12d3-a456-426614174000"

    connector = workspace.get_connector(connector_id)

    assert isinstance(connector, CloudConnector)
    assert connector.connector_id == connector_id
    assert calls == []


def test_get_connector_keyword_id_makes_no_api_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    calls = _patch_list_connectors(monkeypatch, workspace, [])

    connector = workspace.get_connector(connector_id="connector-1")

    assert connector.connector_id == "connector-1"
    assert calls == []


def test_build_connection_details_reads_cached_schedule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`build_connection_details` uses the listed connection's schedule, not a fetch."""
    workspace = _make_workspace(monkeypatch)
    destination = _seed_destination(
        workspace,
        "snowflake",
        SNOWFLAKE_DEFINITION_ID,
        configuration={"database": "DATABASE", "schema": "SCHEMA"},
    )
    connection = CloudConnection(workspace=workspace, connection_id="conn-1")
    connection._connection_info = CloudConnectionInfo(  # noqa: SLF001
        connection_id="conn-1",
        workspace_id="workspace-id",
        source_id="source-1",
        destination_id="snowflake",
        name="sync",
        configurations=SimpleNamespace(streams=[SimpleNamespace(name="issues")]),
        schedule=ConnectionSchedule(schedule_type="manual"),
        status="active",
    )
    monkeypatch.setattr(
        CloudWorkspace, "list_connections", lambda *_, **__: [connection]
    )
    monkeypatch.setattr(
        CloudWorkspace,
        "list_sources",
        lambda *_, **__: [_seed_source(workspace, "source-1", "GitHub")],
    )
    monkeypatch.setattr(
        CloudWorkspace, "list_destinations", lambda *_, **__: [destination]
    )
    get_connection = MagicMock()
    monkeypatch.setattr("airbyte._util.api_util.get_connection", get_connection)

    (info,) = connector_docs.build_connection_details(destination)

    get_connection.assert_not_called()
    assert info.schedule == "manual"


def test_enabled_features_context_layer_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Context-Layer-enabled source reports `direct_access` and `direct_api_query`."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    monkeypatch.setattr(
        agents_api_util,
        "read_cloud_skill_docs",
        lambda **_: SKILL_DOCS_RESPONSE,
    )
    source = _seed_source(workspace, "source-1", "GitHub")

    assert source.enabled_features == frozenset({
        ConnectorFeature.DIRECT_ACCESS,
        ConnectorFeature.DIRECT_API_QUERY,
    })


def test_enabled_features_sql_passthrough_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A SQL passthrough destination reports `direct_access` and `direct_sql_query`."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    monkeypatch.setattr(
        agents_api_util,
        "read_cloud_skill_docs",
        lambda **_: SKILL_DOCS_RESPONSE,
    )
    destination = _seed_destination(
        workspace,
        "snowflake",
        SNOWFLAKE_DEFINITION_ID,
        configuration={"database": "DATABASE", "schema": "SCHEMA"},
    )

    assert destination.enabled_features == frozenset({
        ConnectorFeature.DIRECT_ACCESS,
        ConnectorFeature.DIRECT_SQL_QUERY,
    })


def test_enabled_features_disabled_connector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A source whose docs probe fails reports no features."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    monkeypatch.setattr(
        agents_api_util,
        "read_cloud_skill_docs",
        lambda **_: (_ for _ in ()).throw(AirbyteCloudApiError(status_code=404)),
    )
    source = _seed_source(workspace, "source-1", "GitHub")

    assert source.enabled_features == frozenset()


def test_is_feature_enabled_uses_cached_features(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A populated `_enabled_features` cache short-circuits workspace lookups."""
    workspace = _make_workspace(monkeypatch)
    get_features = MagicMock()
    monkeypatch.setattr(CloudWorkspace, "_get_connector_features", get_features)
    source = _seed_source(workspace, "source-1", "GitHub")
    source._enabled_features = frozenset({ConnectorFeature.DIRECT_ACCESS})  # noqa: SLF001

    assert source.is_feature_enabled(ConnectorFeature.DIRECT_ACCESS)
    assert not source.is_feature_enabled(ConnectorFeature.DIRECT_API_QUERY)
    get_features.assert_not_called()


@pytest.mark.parametrize(
    "feature",
    [ConnectorFeature.DIRECT_API_ACTION, ConnectorFeature.SEARCH_INDEXING],
)
def test_is_feature_enabled_never_enabled_features_short_circuit(
    monkeypatch: pytest.MonkeyPatch,
    feature: ConnectorFeature,
) -> None:
    """Features no connector can report return `False` without a workspace call."""
    workspace = _make_workspace(monkeypatch)
    get_features = MagicMock()
    monkeypatch.setattr(CloudWorkspace, "_get_connector_features", get_features)
    source = _seed_source(workspace, "source-1", "GitHub")

    assert source.is_feature_enabled(feature) is False
    get_features.assert_not_called()


@pytest.mark.parametrize(
    ("seed", "feature"),
    [
        pytest.param("source", ConnectorFeature.DIRECT_SQL_QUERY, id="sql_on_source"),
        pytest.param(
            "destination",
            ConnectorFeature.DIRECT_API_QUERY,
            id="api_query_on_destination",
        ),
    ],
)
def test_is_feature_enabled_wrong_connector_kind_short_circuits(
    monkeypatch: pytest.MonkeyPatch,
    seed: str,
    feature: ConnectorFeature,
) -> None:
    """Kind-specific features return `False` on the wrong connector type, no call."""
    workspace = _make_workspace(monkeypatch)
    get_features = MagicMock()
    monkeypatch.setattr(CloudWorkspace, "_get_connector_features", get_features)
    connector = (
        _seed_source(workspace, "source-1", "GitHub")
        if seed == "source"
        else _seed_destination(workspace, "snowflake", SNOWFLAKE_DEFINITION_ID)
    )

    assert connector.is_feature_enabled(feature) is False
    get_features.assert_not_called()


@pytest.mark.parametrize("status", [403, 404])
def test_unavailable_features_warn_without_caching_absence(
    monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    read_docs = MagicMock(
        side_effect=[
            AirbyteError(context={"status_code": status, "response_text": "secret"}),
            SKILL_DOCS_RESPONSE,
        ]
    )
    monkeypatch.setattr(agents_api_util, "read_cloud_skill_docs", read_docs)
    source = _seed_source(workspace, "source-1", "GitHub")
    warnings: list[str] = []

    assert source.get_enabled_features(warnings=warnings) is None
    assert source._enabled_features is None  # noqa: SLF001
    assert len(warnings) == 1
    assert "unavailable" in warnings[0]
    assert "secret" not in warnings[0]
    assert "not enabled" not in warnings[0]
    assert ConnectorFeature.DIRECT_API_QUERY in source.enabled_features
    assert read_docs.call_count == 2
    assert source.enabled_features
    assert read_docs.call_count == 2


@pytest.mark.parametrize(
    "failure",
    [
        AirbyteError(context={"status_code": 403, "response_text": "secret"}),
        AirbyteError(context={"status_code": 404, "response_text": "secret"}),
        AirbyteError(context={"status_code": 401, "response_text": "secret"}),
        AirbyteError(context={"status_code": 500, "response_text": "secret"}),
        requests.Timeout("secret"),
        ValueError("secret"),
    ],
)
def test_describe_survives_unknown_features_and_recovers(
    monkeypatch: pytest.MonkeyPatch, failure: Exception
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    read_docs = MagicMock(side_effect=[failure, SKILL_DOCS_RESPONSE])
    monkeypatch.setattr(agents_api_util, "read_cloud_skill_docs", read_docs)
    source = _seed_source(workspace, "source-1", "GitHub")
    source._connector_definition = SimpleNamespace(name="GitHub")  # noqa: SLF001
    monkeypatch.setattr(connector_docs, "build_connection_details", lambda _: [])

    result = cloud_mcp._describe_cloud_connector(  # noqa: SLF001
        source,
        with_config=False,
        with_replication_details=True,
        with_direct_access_guidance=True,
        with_data_replication_docs=False,
    )

    assert result.connector_id == "source-1"
    assert result.replication_details == []
    assert result.enabled_features == "unknown"
    assert result.warnings
    assert all("secret" not in warning for warning in result.warnings)
    assert read_docs.call_count == 1
    assert source._enabled_features is None  # noqa: SLF001
    assert ConnectorFeature.DIRECT_API_QUERY in source.enabled_features
    assert read_docs.call_count == 2


def test_malformed_probe_is_not_cached_or_silently_filtered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    source = _seed_source(workspace, "source-1", "GitHub")
    monkeypatch.setattr(workspace, "list_sources", lambda: [source])
    monkeypatch.setattr(workspace, "list_destinations", lambda: [])
    read_docs = MagicMock(side_effect=[{}, SKILL_DOCS_RESPONSE])
    monkeypatch.setattr(agents_api_util, "read_cloud_skill_docs", read_docs)

    with pytest.raises(ValueError):
        workspace.list_connectors(feature_filter=ConnectorFeature.DIRECT_ACCESS)
    assert source._enabled_features is None  # noqa: SLF001
    assert workspace.list_connectors(feature_filter=ConnectorFeature.DIRECT_ACCESS) == [
        source
    ]
    assert read_docs.call_count == 2


@pytest.mark.parametrize("section", [None, "overview", "sql-passthrough"])
def test_destination_server_guidance_is_not_duplicated(
    monkeypatch: pytest.MonkeyPatch, section: str | None
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    server_section = (
        "actions.record.sql_select" if section == "sql-passthrough" else section
    )
    server_response = {
        "metadata": {
            "id": "connector-destination:dest-1",
            "kind": "connector_destination",
            "title": "Warehouse",
        },
        "outline": [
            {
                "id": "actions.record.sql_select",
                "title": "SQL select",
                "available": True,
            }
        ],
        "section_id": server_section,
        "content": [{"type": "paragraph", "text": "Authoritative SQL instructions."}],
    }
    read_docs = MagicMock(return_value=server_response)
    monkeypatch.setattr(agents_api_util, "read_cloud_skill_docs", read_docs)
    destination = _seed_destination(workspace, "dest-1", SNOWFLAKE_DEFINITION_ID)
    # A successful server docs response must not trigger local configuration/connection reads.
    monkeypatch.setattr(
        connector_docs,
        "_destination_load_context",
        MagicMock(side_effect=AssertionError("local SQL rendering")),
    )
    monkeypatch.setattr(
        workspace,
        "list_connections",
        MagicMock(side_effect=AssertionError("local connection listing")),
    )

    docs = destination.get_direct_access_guidance(section=section)

    assert read_docs.call_count == 1
    assert read_docs.call_args.kwargs["section"] == server_section
    assert docs.content == [
        {"type": "paragraph", "text": "Authoritative SQL instructions."}
    ]
    assert docs.section_id == section
    assert {entry.id for entry in docs.outline} == {"actions.record.sql_select"}


@pytest.mark.parametrize("cache_path", ["fresh", "inspect", "features"])
def test_describe_stringifies_structured_probe_warnings(
    monkeypatch: pytest.MonkeyPatch, cache_path: str
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    warning = {"code": "partial", "message": "Partial metadata"}
    response = {
        **SKILL_DOCS_RESPONSE,
        "metadata": {**SKILL_DOCS_RESPONSE["metadata"], "warnings": [warning]},
    }
    read_docs = MagicMock(return_value=response)
    monkeypatch.setattr(agents_api_util, "read_cloud_skill_docs", read_docs)
    source = _seed_source(workspace, "source-1", "GitHub")
    source._connector_definition = SimpleNamespace(name="GitHub")  # noqa: SLF001
    if cache_path == "inspect":
        source._context_layer_inspect(warnings=[])  # noqa: SLF001
    elif cache_path == "features":
        source.get_enabled_features()

    result = cloud_mcp._describe_cloud_connector(  # noqa: SLF001
        source,
        with_config=False,
        with_replication_details=False,
        with_direct_access_guidance=False,
        with_data_replication_docs=False,
    )

    assert result.warnings == [str(warning)]
    assert result.model_dump()["warnings"] == [str(warning)]
    assert read_docs.call_count == 1
