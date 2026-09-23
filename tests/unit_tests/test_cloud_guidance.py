# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for `CloudWorkspace` direct-access guidance helpers."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from airbyte._direct_connectors import api_util as agents_api_util
from airbyte._direct_connectors.models import (
    DirectAccessGuidance,
    DirectAccessGuidanceIndexEntry,
)
from airbyte.cloud.workspaces import CloudWorkspace


SKILL_INFO = {
    "id": "connector:github",
    "kind": "connector_source",
    "title": "GitHub",
    "summary": "GitHub usage docs.",
    "tags": ["github", "connector"],
}
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


def test_list_guidance_follows_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    """`_list_guidance` returns `DirectAccessGuidanceIndexEntry` objects across result pages."""
    workspace = _make_workspace(monkeypatch)
    calls: list[dict[str, Any]] = []
    pages = [
        {"data": [SKILL_INFO], "next_cursor": "cursor-1"},
        {"data": [{**SKILL_INFO, "id": "connector:slack"}], "next_cursor": None},
    ]

    def fake_list_skills(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return pages[len(calls) - 1]

    monkeypatch.setattr(agents_api_util, "list_agent_skills", fake_list_skills)

    infos = workspace._list_guidance()  # noqa: SLF001

    assert [info.id for info in infos] == ["connector:github", "connector:slack"]
    assert all(isinstance(info, DirectAccessGuidanceIndexEntry) for info in infos)
    assert infos[0].title == "GitHub"
    assert infos[0].kind == "connector_source"
    assert len(calls) == 2
    assert calls[1]["cursor"] == "cursor-1"


def test_get_direct_access_guidance_reads_docs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`get_direct_access_guidance` returns the skill's parsed `DirectAccessGuidance`."""
    workspace = _make_workspace(monkeypatch)
    calls: list[dict[str, Any]] = []

    def fake_read_docs(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return SKILL_DOCS_RESPONSE

    monkeypatch.setattr(agents_api_util, "read_agent_skill_docs", fake_read_docs)

    guidance = workspace.get_direct_access_guidance("connector:github")

    assert isinstance(guidance, DirectAccessGuidance)
    assert guidance.metadata.id == "connector:github"
    assert guidance.metadata.title == "GitHub"
    assert calls[0]["skill_id"] == "connector:github"


def test_get_direct_access_guidance_passes_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`get_direct_access_guidance` passes the section through to the Agents API."""
    workspace = _make_workspace(monkeypatch)
    calls: list[dict[str, Any]] = []

    def fake_read_docs(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return SKILL_DOCS_RESPONSE

    monkeypatch.setattr(agents_api_util, "read_agent_skill_docs", fake_read_docs)

    docs = workspace.get_direct_access_guidance("connector:github", section="setup")

    assert docs.metadata.id == "connector:github"
    assert docs.metadata.title == "GitHub"
    assert docs.outline[0].id == "setup"
    assert calls[0]["skill_id"] == "connector:github"
    assert calls[0]["section"] == "setup"


def test_list_guidance_passes_resolved_organization_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`_list_guidance` sends the workspace's resolved organization ID to the API."""
    workspace = _make_workspace(monkeypatch)
    calls: list[dict[str, Any]] = []

    def fake_list_skills(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"data": [], "next_cursor": None}

    monkeypatch.setattr(agents_api_util, "list_agent_skills", fake_list_skills)

    workspace._list_guidance()  # noqa: SLF001

    assert calls[0]["organization_id"] == "organization-id"


def test_get_direct_access_guidance_destination_prefix_served_locally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`connector-destination:` IDs resolve locally through `get_destination`."""
    workspace = _make_workspace(monkeypatch)
    destination = MagicMock()
    destination.get_direct_access_guidance.return_value = SKILL_DOCS_RESPONSE[
        "metadata"
    ] and DirectAccessGuidance.model_validate(SKILL_DOCS_RESPONSE)
    calls: list[str] = []
    monkeypatch.setattr(
        CloudWorkspace,
        "get_destination",
        lambda _self, destination_id: (calls.append(destination_id), destination)[1],
    )

    def fail_read_docs(**_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("Agents API must not be called for destination skill IDs")

    monkeypatch.setattr(agents_api_util, "read_agent_skill_docs", fail_read_docs)

    docs = workspace.get_direct_access_guidance(
        "connector-destination:destination-1", section="sql"
    )

    assert calls == ["destination-1"]
    destination.get_direct_access_guidance.assert_called_once_with(section="sql")
    assert docs.metadata.id == "connector:github"
