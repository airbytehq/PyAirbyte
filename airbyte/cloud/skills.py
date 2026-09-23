# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Cloud-side access to the Agents API skills catalog.

Skills are reusable documentation served by the Agents (Context layer) API. They are
exposed on `CloudWorkspace` as `CloudSkill` objects so that connector workflows can read
them without going through the `airbyte.agents` interface.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

from airbyte._direct_connectors import api_util as _api_util
from airbyte._direct_connectors.docs_markdown import render_docs_content_markdown
from airbyte._direct_connectors.models import CloudSkillDocs


if TYPE_CHECKING:
    from airbyte._direct_connectors.models import CloudSkillInfo
    from airbyte.cloud.workspaces import CloudWorkspace


class CloudSkill:
    """A skill available to a Cloud workspace through the Agents API.

    Get one from `CloudWorkspace._get_skill()` or `CloudWorkspace._list_skills()` rather than
    constructing it directly. These workspace methods are private: skills are surfaced
    through `CloudConnector.get_direct_access_docs()` and the MCP agents tools.
    """

    def __init__(
        self,
        workspace: CloudWorkspace,
        skill_id: str,
        *,
        info: CloudSkillInfo | None = None,
    ) -> None:
        """Initialize a `CloudSkill`. Prefer `CloudWorkspace._get_skill()`."""
        self.workspace = workspace
        """The workspace the skill is read through."""

        self.skill_id = skill_id
        """The skill ID."""

        self._info = info

    @property
    def info(self) -> CloudSkillInfo:
        """The skill's metadata, fetched from the Agents API if not already known."""
        if self._info is None:
            self._info = self.read_docs(format="blocks").metadata
        return self._info

    @property
    def title(self) -> str | None:
        """The human-readable skill title."""
        return self.info.title

    @property
    def kind(self) -> str | None:
        """The skill category, for example `static` or `connector_source`."""
        return self.info.kind

    @overload
    def read_docs(
        self,
        *,
        section: str | None = None,
        format: Literal["markdown"] = "markdown",  # Specified public name.
    ) -> str: ...

    @overload
    def read_docs(
        self,
        *,
        section: str | None = None,
        format: Literal["blocks"],  # Specified public name.
    ) -> CloudSkillDocs: ...

    def read_docs(
        self,
        *,
        section: str | None = None,
        format: Literal["markdown", "blocks"] = "markdown",  # noqa: A002  # Specified name.
    ) -> str | CloudSkillDocs:
        """Read this skill's docs, optionally scoped to a single section.

        Omit `section` for metadata, guidance, and the outline of available sections, or
        pass an exact section `id` from the outline to read that section. By default the
        docs are returned rendered as a single Markdown document; pass `format="blocks"`
        for the `CloudSkillDocs` model.
        """
        docs = CloudSkillDocs.model_validate(
            _api_util.read_agent_skill_docs(
                skill_id=self.skill_id,
                credentials=self.workspace._credentials,  # noqa: SLF001
                organization_id=self.workspace._resolve_agents_organization_id(),  # noqa: SLF001
                workspace_id=self.workspace.workspace_id,
                section=section,
            )
        )
        if self._info is None:
            self._info = docs.metadata
        if format == "blocks":
            return docs
        return render_docs_content_markdown(docs.content)
