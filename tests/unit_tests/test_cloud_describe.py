# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for `CloudConnector.describe()` and related workspace lookups."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import PropertyMock, patch

import pytest
import requests

from airbyte._direct_connectors import api_util as agents_api_util
from airbyte._direct_connectors.models import CloudConnectorDetails
from airbyte.cloud import workspaces as cloud_workspaces
from airbyte.cloud.connections import CloudConnection
from airbyte.cloud.connectors import (
    CloudConnector,
    CloudDestination,
    CloudSource,
    ConnectorFeature,
)
from airbyte._direct_connectors.models import (
    _SQL_PASSTHROUGH_DESTINATION_DIALECTS,
)
from airbyte.cloud.models import (
    CloudConnectionInfo,
    CloudDestinationInfo,
    CloudSourceInfo,
)
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import (
    AirbyteConnectorNotRegisteredError,
    AirbyteError,
    PyAirbyteInputError,
)


SNOWFLAKE_DEFINITION_ID = next(iter(_SQL_PASSTHROUGH_DESTINATION_DIALECTS))

INSPECT_RESPONSE: dict[str, Any] = {
    "connector_id": "source-1",
    "name": "GitHub",
    "workspace_id": "workspace-id",
    "source_definition_name": "GitHub",
    "docs_skill_id": "connector:github",
    "context_store_readiness": {
        "supported_context_store_entities": [{"entity": "issues", "suggested": True}]
    },
    "warnings": ["Partial runtime metadata."],
}


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
    schedule: str = "every 24 hours",
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
        namespace_definition="source",
        schedule_description=schedule,
        status="active",
    )
    return connection


def test_describe_source_with_external_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`describe` fills inspect-derived fields when external access is enabled."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    inspect_calls: list[dict[str, Any]] = []

    def fake_inspect(**kwargs: Any) -> dict[str, Any]:
        inspect_calls.append(kwargs)
        return INSPECT_RESPONSE

    monkeypatch.setattr(agents_api_util, "inspect_agent_connector", fake_inspect)
    source = _seed_source(workspace, "source-1", "GitHub")
    source._enabled_features = frozenset(  # noqa: SLF001
        {ConnectorFeature.EXTERNAL_ACCESS, ConnectorFeature.SEARCH_INDEXING}
    )

    details = source.describe()

    assert isinstance(details, CloudConnectorDetails)
    assert details.connector_id == "source-1"
    assert details.connector_type == "source"
    assert details.connector_name == "GitHub"
    assert details.connector_definition_id == "source-definition"
    assert details.external_access_enabled is True
    assert details.search_indexing_enabled is True
    assert details.integration_name == "GitHub"
    assert details.docs_skill_id == "connector:github"
    assert details.context_store_readiness is not None
    assert (
        details.context_store_readiness.supported_context_store_entities[0].entity
        == "issues"
    )
    assert details.warnings == ["Partial runtime metadata."]
    assert inspect_calls and inspect_calls[0]["connector_id"] == "source-1"


def test_describe_source_inspect_failure_warns_without_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failing `inspect` call appends a warning instead of raising."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)

    def fake_inspect(**_kwargs: Any) -> dict[str, Any]:
        raise AirbyteError(
            message="Inspect failed.",
            context={"status_code": 404},
        )

    monkeypatch.setattr(agents_api_util, "inspect_agent_connector", fake_inspect)
    source = _seed_source(workspace, "source-1", "GitHub")
    source._enabled_features = frozenset({ConnectorFeature.EXTERNAL_ACCESS})  # noqa: SLF001

    details = source.describe()

    assert details.integration_name is None
    assert details.docs_skill_id is None
    assert details.context_store_readiness is None
    assert len(details.warnings) == 1
    assert "inspect" in details.warnings[0]


def test_describe_without_external_access_skips_inspect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    monkeypatch.setattr(
        agents_api_util,
        "inspect_agent_connector",
        lambda **_: pytest.fail("inspect must not be called"),
    )
    source = _seed_source(workspace, "source-1", "GitHub")
    source._enabled_features = frozenset()  # noqa: SLF001

    details = source.describe()

    assert details.external_access_enabled is False
    assert details.docs_skill_id is None


def test_describe_sql_passthrough_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SQL-passthrough destinations use local docs metadata and never call inspect."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    monkeypatch.setattr(
        agents_api_util,
        "inspect_agent_connector",
        lambda **_: pytest.fail("inspect must not be called for destinations"),
    )
    destination = _seed_destination(
        workspace,
        "dest-1",
        SNOWFLAKE_DEFINITION_ID,
        name="Warehouse",
        configuration={"database": "analytics", "schema": "raw"},
    )
    destination._enabled_features = frozenset({ConnectorFeature.EXTERNAL_ACCESS})  # noqa: SLF001

    connection = _fake_connection(
        workspace, "conn-1", "source-1", "dest-1", schedule="manual"
    )
    other_connection = _fake_connection(workspace, "conn-2", "source-1", "dest-2")
    monkeypatch.setattr(
        CloudWorkspace, "list_connections", lambda _self: [connection, other_connection]
    )
    monkeypatch.setattr(
        CloudWorkspace,
        "list_sources",
        lambda _self: [_seed_source(workspace, "source-1", "GitHub")],
    )
    monkeypatch.setattr(
        CloudWorkspace,
        "list_destinations",
        lambda _self: [destination],
    )

    details = destination.describe(with_replication_details=True)

    assert details.connector_type == "destination"
    assert details.integration_name == "Snowflake"
    assert details.docs_skill_id is not None
    assert details.docs_skill_id.startswith("connector-destination:")
    assert details.replication_details is not None
    assert len(details.replication_details) == 1
    info = details.replication_details[0]
    assert info.connection_id == "conn-1"
    assert info.source_name == "GitHub"
    assert info.destination_name == "Warehouse"
    assert info.schedule == "manual"
    assert info.stream_names == ["issues", "repos"]
    assert info.table_prefix == "raw_"
    assert info.destination_database == "analytics"
    assert info.destination_schema == "raw"


def test_describe_with_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`with_config` adds the definition name and the redacted connector config."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)

    monkeypatch.setattr(
        "airbyte._util.api_util.get_source_definition",
        lambda **_: SimpleNamespace(
            name="GitHub",
            docker_repository="airbyte/source-github",
        ),
    )
    source = _seed_source(workspace, "source-1", "GitHub")
    source._enabled_features = frozenset()  # noqa: SLF001

    details = source.describe(with_config=True)

    assert details.connector_definition_name == "GitHub"
    assert details.config is None


def test_describe_with_direct_access_guidance_failure_warns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A docs failure under `with_direct_access_guidance` appends a warning."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)

    def fake_read_docs(**_kwargs: Any) -> dict[str, Any]:
        raise AirbyteError(message="Docs unavailable.")

    monkeypatch.setattr(agents_api_util, "read_agent_skill_docs", fake_read_docs)
    source = _seed_source(workspace, "source-1", "GitHub")
    source._enabled_features = frozenset({ConnectorFeature.EXTERNAL_ACCESS})  # noqa: SLF001
    monkeypatch.setattr(
        agents_api_util,
        "inspect_agent_connector",
        lambda **_: INSPECT_RESPONSE,
    )

    details = source.describe(with_direct_access_guidance=True)

    assert details.direct_access_guidance is None
    assert any("docs" in warning for warning in details.warnings)


def test_get_direct_access_guidance_non_passthrough_destination_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    destination = _seed_destination(workspace, "dest-1", "other-definition")

    with pytest.raises(
        PyAirbyteInputError, match="does not support direct access docs"
    ):
        destination.get_direct_access_guidance()


def test_iter_api_entities_follows_cursors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`iter_api_entities` pages until `has_next_page` is false."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls: list[dict[str, Any]] = []
    pages = {
        None: {
            "status": "success",
            "result": [{"id": 1}, {"id": 2}],
            "connector_metadata": {"has_next_page": True, "end_cursor": "cursor-1"},
        },
        "cursor-1": {
            "status": "success",
            "result": [{"id": 3}],
            "connector_metadata": {"has_next_page": False, "end_cursor": "cursor-2"},
        },
    }

    def fake_execute(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return pages[kwargs["request_body"].get("params", {}).get("cursor")]

    monkeypatch.setattr(agents_api_util, "execute_agent_connector_action", fake_execute)
    source = _seed_source(workspace, "source-1", "GitHub")

    records = list(source.iter_api_entities("issues", api_args={"repository": "repo"}))

    assert [record["id"] for record in records] == [1, 2, 3]
    assert len(calls) == 2
    assert calls[1]["request_body"]["params"]["cursor"] == "cursor-1"


def test_iter_api_entities_respects_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls: list[dict[str, Any]] = []

    def fake_execute(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {
            "status": "success",
            "result": [{"id": 1}, {"id": 2}],
            "connector_metadata": {"has_next_page": True, "end_cursor": "cursor-1"},
        }

    monkeypatch.setattr(agents_api_util, "execute_agent_connector_action", fake_execute)
    source = _seed_source(workspace, "source-1", "GitHub")

    records = list(source.iter_api_entities("issues", limit=3))

    assert [record["id"] for record in records] == [1, 2, 1]
    assert len(calls) == 2


def test_iter_api_entities_stops_on_repeated_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls: list[dict[str, Any]] = []

    def fake_execute(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {
            "status": "success",
            "result": [{"id": len(calls)}],
            "connector_metadata": {"has_next_page": True, "end_cursor": "cursor-1"},
        }

    monkeypatch.setattr(agents_api_util, "execute_agent_connector_action", fake_execute)
    source = _seed_source(workspace, "source-1", "GitHub")

    records = list(source.iter_api_entities("issues"))

    assert len(records) == 2
    assert len(calls) == 2


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


def test_describe_passthrough_docs_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`with_direct_access_guidance` renders the built-in SQL destination docs."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch, available=False)
    monkeypatch.setattr(
        CloudWorkspace,
        "list_connections",
        lambda _self: [_fake_connection(workspace, "conn-1", "source-1", "dest-1")],
    )
    monkeypatch.setattr(
        CloudWorkspace,
        "list_sources",
        lambda _self: [_seed_source(workspace, "source-1", "GitHub")],
    )

    with patch.object(
        CloudConnector,
        "external_access_enabled",
        new_callable=PropertyMock,
        return_value=True,
    ):
        destination = _seed_destination(
            workspace,
            "dest-1",
            SNOWFLAKE_DEFINITION_ID,
            name="Warehouse",
            configuration={"database": "analytics", "schema": "raw"},
        )

        details = destination.describe(with_direct_access_guidance=True)

    assert details.direct_access_guidance is not None
    assert details.direct_access_guidance.skill_id == details.docs_skill_id
    assert details.direct_access_guidance.content


def test_describe_direct_access_guidance_transport_failure_warns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transport error under `with_direct_access_guidance` appends a warning."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    source = _seed_source(workspace, "source-1", "GitHub")
    source._enabled_features = frozenset()  # noqa: SLF001
    monkeypatch.setattr(
        CloudConnector,
        "get_direct_access_guidance",
        lambda _self, **_: (_ for _ in ()).throw(requests.Timeout("docs timed out")),
    )

    details = source.describe(with_direct_access_guidance=True)

    assert details.direct_access_guidance is None
    assert any(
        "Direct access docs are unavailable" in warning for warning in details.warnings
    )


def test_describe_with_config_configuration_failure_warns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A config fetch failure under `with_config` warns but keeps the definition name."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
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
        configuration={"database": "analytics", "schema": "raw"},
    )
    destination._enabled_features = frozenset()  # noqa: SLF001
    monkeypatch.setattr(
        CloudDestination,
        "configuration",
        property(
            lambda _self: (_ for _ in ()).throw(AirbyteError(message="config boom"))
        ),
    )

    details = destination.describe(with_config=True)

    assert details.connector_definition_name == "Snowflake"
    assert details.config is None
    assert any(
        "Connector configuration lookup failed" in warning
        for warning in details.warnings
    )


def test_iter_api_entities_limit_zero_makes_no_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`limit=0` yields nothing without calling the API."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    calls: list[dict[str, Any]] = []

    def fake_execute(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"status": "success", "result": [{"id": 1}]}

    monkeypatch.setattr(agents_api_util, "execute_agent_connector_action", fake_execute)
    source = _seed_source(workspace, "source-1", "GitHub")

    assert list(source.iter_api_entities("issues", limit=0)) == []
    assert calls == []


def test_iter_api_entities_negative_limit_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A negative `limit` is rejected."""
    workspace = _make_workspace(monkeypatch)
    source = _seed_source(workspace, "source-1", "GitHub")

    with pytest.raises(PyAirbyteInputError, match="limit"):
        list(source.iter_api_entities("issues", limit=-1))


def test_describe_inspect_transport_failure_warns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transport error from Context Layer `inspect` degrades to a warning."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    monkeypatch.setattr(
        agents_api_util,
        "inspect_agent_connector",
        lambda **_: (_ for _ in ()).throw(requests.ConnectionError("conn down")),
    )
    source = _seed_source(workspace, "source-1", "GitHub")
    source._enabled_features = frozenset({ConnectorFeature.EXTERNAL_ACCESS})  # noqa: SLF001

    details = source.describe()

    assert any("Connector inspect failed" in warning for warning in details.warnings)


def test_describe_direct_access_guidance_input_error_warns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A `PyAirbyteError` under `with_direct_access_guidance` appends a warning."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    source = _seed_source(workspace, "source-1", "GitHub")
    source._enabled_features = frozenset()  # noqa: SLF001
    monkeypatch.setattr(
        CloudConnector,
        "get_direct_access_guidance",
        lambda _self, **_: (_ for _ in ()).throw(
            PyAirbyteInputError(message="bad docs")
        ),
    )

    details = source.describe(with_direct_access_guidance=True)

    assert details.direct_access_guidance is None
    assert any(
        "Direct access docs are unavailable" in warning for warning in details.warnings
    )


def test_describe_data_replication_docs_registry_error_warns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A registry miss under `with_data_replication_docs` appends a warning."""
    workspace = _make_workspace(monkeypatch)
    _patch_context_layer(monkeypatch)
    source = _seed_source(workspace, "source-1", "GitHub")
    source._enabled_features = frozenset()  # noqa: SLF001
    monkeypatch.setattr(
        CloudConnector,
        "get_data_replication_docs",
        lambda _self, **_: (_ for _ in ()).throw(
            AirbyteConnectorNotRegisteredError(message="unregistered")
        ),
    )

    details = source.describe(with_data_replication_docs=True)

    assert details.data_replication_docs is None
    assert any(
        "Data replication docs are unavailable" in warning
        for warning in details.warnings
    )
