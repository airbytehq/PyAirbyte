# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Docs-result models and renderers for the Cloud MCP tools.

This module provides the `CloudConnectorDocsResult` and `AgentSkillDocsResult`
result models plus the helpers that render `DirectAccessGuidance` into them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from airbyte._direct_connectors.docs_markdown import render_docs_content_markdown
from airbyte._direct_connectors.models import (
    DirectAccessGuidanceSection,  # noqa: TC001 - needed at runtime for Pydantic field types
)


if TYPE_CHECKING:
    from airbyte._direct_connectors.models import DirectAccessGuidance


class CloudConnectorDocsResult(BaseModel):
    """Connector docs rendered for agent consumption by the Cloud MCP tools.

    Returned by `describe_cloud_connector`.
    """

    model_config = ConfigDict(extra="allow")

    skill_id: str | None = None
    """The docs skill ID, for example `connector-source:<id>`."""

    title: str | None = None
    """The human-readable docs title."""

    content: str
    """The docs body, rendered as Markdown."""

    outline: list[DirectAccessGuidanceSection] = Field(default_factory=list)
    """The sections available in the docs."""

    section_id: str | None = None
    """The requested section ID, or `None` for the default docs response."""

    warnings: list[str] = Field(default_factory=list)
    """Non-fatal issues reported while reading or rendering the docs."""


class AgentSkillDocsResult(BaseModel):
    """Docs for a single agent skill (skills for agents), returned by `get_agent_skill_docs`."""

    model_config = ConfigDict(extra="allow")

    skill_id: str | None = None
    """The docs skill ID, for example `connector-source:<id>`."""

    title: str | None = None
    """The human-readable docs title."""

    content: str
    """The docs body, rendered as Markdown."""

    outline: list[DirectAccessGuidanceSection] = Field(default_factory=list)
    """The sections available in the docs."""

    section_id: str | None = None
    """The requested section ID, or `None` for the default docs response."""

    warnings: list[str] = Field(default_factory=list)
    """Non-fatal issues reported while reading or rendering the docs."""


def render_connector_docs_result(docs: DirectAccessGuidance) -> CloudConnectorDocsResult:
    """Render `DirectAccessGuidance` into a `CloudConnectorDocsResult`."""
    return CloudConnectorDocsResult(
        skill_id=docs.metadata.id,
        title=docs.metadata.title,
        content=render_docs_content_markdown(docs.content),
        outline=docs.outline,
        section_id=docs.section_id,
        warnings=[str(warning) for warning in docs.metadata.warnings],
    )


def render_agent_skill_docs_result(docs: DirectAccessGuidance) -> AgentSkillDocsResult:
    """Render `DirectAccessGuidance` into an `AgentSkillDocsResult`."""
    return AgentSkillDocsResult(
        skill_id=docs.metadata.id,
        title=docs.metadata.title,
        content=render_docs_content_markdown(docs.content),
        outline=docs.outline,
        section_id=docs.section_id,
        warnings=[str(warning) for warning in docs.metadata.warnings],
    )
