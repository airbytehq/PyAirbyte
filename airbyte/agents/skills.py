# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Airbyte Agents skills: reusable documentation served by the Agents API.

> ## ⚠️ Experimental Interface
>
> **The Airbyte Agents Python interfaces are experimental.** Class names, method signatures,
> and result models may change or be removed without notice between minor versions of
> PyAirbyte. Pin an exact PyAirbyte version if you depend on them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte.agents import _api_util
from airbyte.agents.models import AgentSkillDocs, AgentSkillInfo, AgentSkillList


if TYPE_CHECKING:
    from airbyte.cloud._credentials import _AirbyteCredentials


class AgentSkill:
    """A skill on the Airbyte Agents platform.

    Get one from `AgentWorkspace.get_skill()` or `AgentWorkspace.list_skills()` rather than
    constructing it directly.
    """

    def __init__(
        self,
        skill_id: str,
        *,
        credentials: _AirbyteCredentials,
        workspace_id: str | None = None,
        info: AgentSkillInfo | None = None,
    ) -> None:
        """Initialize an `AgentSkill`. Prefer `AgentWorkspace.get_skill()`."""
        self.skill_id = skill_id
        """The skill ID."""

        self._credentials = credentials
        self._workspace_id = workspace_id
        self._info = info

    @property
    def info(self) -> AgentSkillInfo:
        """The skill's metadata, fetched from the Agents API if not already known."""
        if self._info is None:
            self._info = self.read_docs().metadata
        return self._info

    @property
    def title(self) -> str | None:
        """The human-readable skill title."""
        return self.info.title

    @property
    def kind(self) -> str | None:
        """The skill category, for example `static` or `connector_source`."""
        return self.info.kind

    def read_docs(self, *, section: str | None = None) -> AgentSkillDocs:
        """Read this skill's docs, optionally scoped to a single section.

        Omit `section` for metadata, guidance, and the outline of available sections, or
        pass an exact section `id` from the outline to read that section.
        """
        docs = AgentSkillDocs.model_validate(
            _api_util.read_agent_skill_docs(
                skill_id=self.skill_id,
                credentials=self._credentials,
                organization_id=self._credentials.organization_id,
                workspace_id=self._workspace_id,
                section=section,
            )
        )
        self._info = docs.metadata
        return docs


def list_skills(
    *,
    credentials: _AirbyteCredentials,
    workspace_id: str | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> AgentSkillList:
    """List the skills available to a workspace or organization.

    Pass `limit` to cap the page size and the `next_cursor` of a previous result as
    `cursor` to fetch the next page.
    """
    return AgentSkillList.model_validate(
        _api_util.list_agent_skills(
            credentials=credentials,
            organization_id=credentials.organization_id,
            workspace_id=workspace_id,
            limit=limit,
            cursor=cursor,
        )
    )


def search_skills(
    query: str,
    *,
    credentials: _AirbyteCredentials,
    workspace_id: str | None = None,
    limit: int | None = None,
    cursor: str | None = None,
) -> AgentSkillList:
    """Search skills by keyword, returning a page of matching skills."""
    return AgentSkillList.model_validate(
        _api_util.search_agent_skills(
            query=query,
            credentials=credentials,
            organization_id=credentials.organization_id,
            workspace_id=workspace_id,
            limit=limit,
            cursor=cursor,
        )
    )
