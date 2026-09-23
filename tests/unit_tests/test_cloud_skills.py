# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Unit tests for `CloudWorkspace` skills and `CloudSkill`."""

from __future__ import annotations

from typing import Any

import pytest

from airbyte._direct_connectors import api_util as agents_api_util
from airbyte.cloud import CloudSkill
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


def test_list_skills_follows_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    """`list_skills` returns `CloudSkill` objects across all result pages."""
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

    skills = workspace.list_skills()

    assert [skill.skill_id for skill in skills] == [
        "connector:github",
        "connector:slack",
    ]
    assert all(isinstance(skill, CloudSkill) for skill in skills)
    assert skills[0].title == "GitHub"
    assert skills[0].kind == "connector_source"
    assert len(calls) == 2
    assert calls[1]["cursor"] == "cursor-1"


def test_get_skill_returns_lazy_skill(monkeypatch: pytest.MonkeyPatch) -> None:
    """`get_skill` makes no API call until docs or info are read."""
    workspace = _make_workspace(monkeypatch)
    monkeypatch.setattr(
        agents_api_util,
        "list_agent_skills",
        lambda **_: pytest.fail("get_skill must not call the API"),
    )

    skill = workspace.get_skill("connector:github")

    assert isinstance(skill, CloudSkill)
    assert skill.skill_id == "connector:github"


def test_read_skill_docs(monkeypatch: pytest.MonkeyPatch) -> None:
    """`read_skill_docs` and `CloudSkill.read_docs` return parsed docs."""
    workspace = _make_workspace(monkeypatch)
    calls: list[dict[str, Any]] = []

    def fake_read_docs(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return SKILL_DOCS_RESPONSE

    monkeypatch.setattr(agents_api_util, "read_agent_skill_docs", fake_read_docs)

    docs = workspace.read_skill_docs("connector:github", section="setup")

    assert docs.metadata.id == "connector:github"
    assert docs.metadata.title == "GitHub"
    assert docs.outline[0].id == "setup"
    assert calls[0]["skill_id"] == "connector:github"
    assert calls[0]["section"] == "setup"

    skill = workspace.get_skill("connector:github")
    assert skill.read_docs().metadata.title == "GitHub"
    # `info` is populated from the docs response without a second call pattern change.
    assert skill.info.id == "connector:github"


def test_read_docs_markdown(monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = _make_workspace(monkeypatch)
    monkeypatch.setattr(
        agents_api_util,
        "read_agent_skill_docs",
        lambda **_: SKILL_DOCS_RESPONSE,
    )

    markdown = workspace.get_skill("connector:github").read_docs_markdown()

    assert "Hello" in markdown
