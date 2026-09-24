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


@pytest.mark.parametrize(
    ("docs_skill_id", "connector_id", "lookup_method", "expected_lookup_id"),
    [
        pytest.param(
            "connector-destination:destination-1",
            None,
            "get_destination",
            "destination-1",
            id="destination_skill_id",
        ),
        pytest.param(
            None,
            "connector-1",
            "get_connector",
            "connector-1",
            id="connector_id",
        ),
    ],
)
def test_get_agent_skill_docs_delegates_raw_read(
    monkeypatch: pytest.MonkeyPatch,
    docs_skill_id: str | None,
    connector_id: str | None,
    lookup_method: str,
    expected_lookup_id: str,
) -> None:
    """Skill/connector IDs resolve to a connector and read server docs unchanged."""
    workspace = _make_workspace(monkeypatch)
    connector = MagicMock()
    connector.read_agent_skill_docs.return_value = DirectAccessGuidance.model_validate(
        SKILL_DOCS_RESPONSE
    )
    lookup = MagicMock(return_value=connector)
    monkeypatch.setattr(CloudWorkspace, lookup_method, lookup)

    def fail_read_docs(**_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("Workspace docs read must not run for connector skill IDs")

    monkeypatch.setattr(agents_api_util, "read_cloud_skill_docs", fail_read_docs)

    docs = workspace.get_agent_skill_docs(
        docs_skill_id, connector_id=connector_id, section="setup"
    )

    lookup.assert_called_once_with(expected_lookup_id)
    connector.read_agent_skill_docs.assert_called_once_with(section="setup")
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


def test_get_agent_skill_docs_requires_exactly_one_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Passing both or neither of `docs_skill_id`/`connector_id` raises."""
    workspace = _make_workspace(monkeypatch)

    with pytest.raises(exc.PyAirbyteInputError, match="exactly one"):
        workspace.get_agent_skill_docs()
    with pytest.raises(exc.PyAirbyteInputError, match="exactly one"):
        workspace.get_agent_skill_docs("connector:github", connector_id="connector-1")
