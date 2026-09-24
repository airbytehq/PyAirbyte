# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for `CloudWorkspace` direct-access guidance helpers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from airbyte import exceptions as exc
from airbyte._direct_connectors import api_util as agents_api_util
from airbyte._direct_connectors.models import (
    DirectAccessGuidance,
    DirectAccessGuidanceIndexEntry,
    _SQL_PASSTHROUGH_DESTINATION_DIALECTS,
)
from airbyte.cloud import workspaces as cloud_workspaces
from airbyte.cloud.connectors import CloudDestination
from airbyte.cloud.models import CloudDestinationInfo
from airbyte.cloud.workspaces import CloudWorkspace


SNOWFLAKE_DEFINITION_ID = next(iter(_SQL_PASSTHROUGH_DESTINATION_DIALECTS))

SKILL_DOCS_RESPONSE: dict[str, Any] = {
    "metadata": {
        "id": "connector:github",
        "kind": "connector_source",
        "title": "GitHub",
        "warnings": [],
    },
    "outline": [
        {
            "id": "setup",
            "title": "Setup",
            "summary": "How to configure.",
            "available": True,
        },
    ],
    "section_id": None,
    "content": [{"type": "paragraph", "text": "Hello"}],
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


def test_list_guidance_derives_index_from_connectors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`_list_guidance` derives one entry per source plus SQL passthrough destinations."""
    workspace = _make_workspace(monkeypatch)
    monkeypatch.setattr(
        workspace,
        "list_sources",
        lambda **_: [
            SimpleNamespace(connector_id="source-1", name="GitHub"),
            SimpleNamespace(connector_id="source-2", name="Slack"),
        ],
    )
    monkeypatch.setattr(
        workspace,
        "list_destinations",
        lambda **_: [
            SimpleNamespace(
                connector_id="dest-1",
                name="Snowflake",
                definition_id=SNOWFLAKE_DEFINITION_ID,
            ),
            SimpleNamespace(
                connector_id="dest-2",
                name="Other",
                definition_id="not-a-passthrough-definition",
            ),
        ],
    )

    infos = workspace._list_guidance()  # noqa: SLF001

    assert [info.id for info in infos] == [
        "connector-source:source-1",
        "connector-source:source-2",
        "connector-destination:dest-1",
    ]
    assert all(isinstance(info, DirectAccessGuidanceIndexEntry) for info in infos)
    assert infos[0].title == "GitHub"
    assert infos[0].kind == "connector_source"
    assert infos[2].kind == "connector_destination"


def test_get_agent_skill_docs_reads_docs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`get_agent_skill_docs` returns the skill's parsed `DirectAccessGuidance`."""
    workspace = _make_workspace(monkeypatch)
    calls: list[dict[str, Any]] = []

    def fake_read_docs(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return SKILL_DOCS_RESPONSE

    monkeypatch.setattr(agents_api_util, "read_cloud_skill_docs", fake_read_docs)

    guidance = workspace.get_agent_skill_docs("connector:github")

    assert isinstance(guidance, DirectAccessGuidance)
    assert guidance.metadata.id == "connector:github"
    assert guidance.metadata.title == "GitHub"
    assert calls[0]["skill_id"] == "connector:github"
    assert calls[0]["workspace_id"] == "workspace-id"


def test_get_agent_skill_docs_passes_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`get_agent_skill_docs` passes the section through to the Agents API."""
    workspace = _make_workspace(monkeypatch)
    calls: list[dict[str, Any]] = []

    def fake_read_docs(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return SKILL_DOCS_RESPONSE

    monkeypatch.setattr(agents_api_util, "read_cloud_skill_docs", fake_read_docs)

    docs = workspace.get_agent_skill_docs("connector:github", section="setup")

    assert docs.metadata.id == "connector:github"
    assert docs.metadata.title == "GitHub"
    assert docs.outline[0].id == "setup"
    assert calls[0]["skill_id"] == "connector:github"
    assert calls[0]["section"] == "setup"


def test_get_agent_skill_docs_destination_prefix_delegates_raw_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`connector-destination:` IDs read server docs unchanged, no enrichment."""
    workspace = _make_workspace(monkeypatch)
    destination = MagicMock()
    destination.read_agent_skill_docs.return_value = (
        DirectAccessGuidance.model_validate(SKILL_DOCS_RESPONSE)
    )
    calls: list[str] = []
    monkeypatch.setattr(
        CloudWorkspace,
        "get_destination",
        lambda _self, destination_id: (calls.append(destination_id), destination)[1],
    )

    def fail_read_docs(**_kwargs: Any) -> dict[str, Any]:
        raise AssertionError(
            "Workspace docs read must not run for destination skill IDs"
        )

    monkeypatch.setattr(agents_api_util, "read_cloud_skill_docs", fail_read_docs)

    docs = workspace.get_agent_skill_docs(
        "connector-destination:destination-1", section="streams"
    )

    assert calls == ["destination-1"]
    destination.read_agent_skill_docs.assert_called_once_with(section="streams")
    assert docs.content == SKILL_DOCS_RESPONSE["content"]
    assert [section.id for section in docs.outline] == ["setup"]


def test_get_agent_skill_docs_destination_errors_propagate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A not-enabled destination read re-raises instead of falling back."""
    workspace = _make_workspace(monkeypatch)
    destination = MagicMock()
    destination.read_agent_skill_docs.side_effect = (
        exc.AirbyteExternalAccessNotEnabledError(connector_id="destination-1")
    )
    monkeypatch.setattr(
        CloudWorkspace,
        "get_destination",
        lambda _self, _destination_id: destination,
    )

    with pytest.raises(exc.AirbyteExternalAccessNotEnabledError):
        workspace.get_agent_skill_docs("connector-destination:destination-1")


def test_get_agent_skill_docs_connector_id_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`connector_id` resolves through `get_connector` and forwards `section`."""
    workspace = _make_workspace(monkeypatch)
    connector = MagicMock()
    connector.read_agent_skill_docs.return_value = DirectAccessGuidance.model_validate(
        SKILL_DOCS_RESPONSE
    )
    get_connector = MagicMock(return_value=connector)
    monkeypatch.setattr(CloudWorkspace, "get_connector", get_connector)

    docs = workspace.get_agent_skill_docs(connector_id="connector-1", section="setup")

    get_connector.assert_called_once_with("connector-1")
    connector.read_agent_skill_docs.assert_called_once_with(section="setup")
    assert docs.metadata.id == "connector:github"


def test_get_direct_access_guidance_section_without_context_layer_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A section-scoped destination read fails when there is no Context layer API."""
    workspace = _make_workspace(monkeypatch)
    monkeypatch.setattr(
        cloud_workspaces.deployment,
        "is_agents_api_available",
        lambda **_: False,
    )
    destination = CloudDestination(workspace=workspace, connector_id="dest-1")
    destination._connector_info = CloudDestinationInfo(  # noqa: SLF001
        destination_id="dest-1",
        name="Warehouse",
        definition_id=SNOWFLAKE_DEFINITION_ID,
    )

    with pytest.raises(exc.PyAirbyteInputError, match="Section-scoped"):
        destination.get_direct_access_guidance(section="streams")


def test_get_agent_skill_docs_requires_exactly_one_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Passing both or neither of `docs_skill_id`/`connector_id` raises."""
    workspace = _make_workspace(monkeypatch)

    with pytest.raises(exc.PyAirbyteInputError, match="exactly one"):
        workspace.get_agent_skill_docs()
    with pytest.raises(exc.PyAirbyteInputError, match="exactly one"):
        workspace.get_agent_skill_docs("connector:github", connector_id="connector-1")
