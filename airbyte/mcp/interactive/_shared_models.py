# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Shared models for interactive MCP tools."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class SupportLevel(str, Enum):
    """Connector support levels ordered by precedence."""

    ARCHIVED = "archived"
    COMMUNITY = "community"
    CERTIFIED = "certified"

    @property
    def precedence(self) -> int:
        """Return numeric precedence for support-level comparisons."""
        return _SUPPORT_LEVEL_PRECEDENCE[self]

    @classmethod
    def parse(cls, value: str) -> SupportLevel:
        """Parse a support level keyword or legacy integer precedence value."""
        try:
            return cls(value)
        except ValueError:
            pass
        try:
            return next(
                level
                for level, precedence in _SUPPORT_LEVEL_PRECEDENCE.items()
                if precedence == int(value)
            )
        except (ValueError, StopIteration):
            valid_kw = ", ".join(f"`{member.value}`" for member in cls)
            valid_int = ", ".join(
                f"`{precedence}`" for precedence in _SUPPORT_LEVEL_PRECEDENCE.values()
            )
            raise ValueError(
                f"Unrecognized support level: {value!r}. "
                f"Expected keyword ({valid_kw}) or integer ({valid_int})."
            ) from None


class ConnectorType(str, Enum):
    """Connector type: `source` or `destination`."""

    SOURCE = "source"
    DESTINATION = "destination"

    @classmethod
    def parse(cls, value: str) -> ConnectorType:
        """Parse a connector type value."""
        try:
            return cls(value)
        except ValueError:
            valid = ", ".join(f"`{member.value}`" for member in cls)
            raise ValueError(
                f"Unrecognized connector type: {value!r}. Expected one of: {valid}."
            ) from None


_SUPPORT_LEVEL_PRECEDENCE: dict[SupportLevel, int] = {
    SupportLevel.ARCHIVED: 100,
    SupportLevel.COMMUNITY: 200,
    SupportLevel.CERTIFIED: 300,
}


class PublicConnectorFilters(BaseModel):
    """Filters applied to the public connector catalog listing."""

    certified: bool = Field(
        default=False,
        description=(
            "When `True`, only certified connectors are returned. "
            "Shorthand for `support_level='certified'`."
        ),
    )
    support_level: str | None = Field(
        default=None,
        description="Exact support level filter.",
    )
    min_support_level: str | None = Field(
        default=None,
        description="Minimum support level threshold.",
    )
    connector_type: str | None = Field(
        default=None,
        description="Connector type filter.",
    )
    search: str = Field(default="", description="Case-insensitive search string.")
    limit: int | None = Field(default=None, description="Maximum returned connectors.")


class PublicConnectorSummary(BaseModel):
    """Connector summary from the public registry."""

    connector_name: str = Field(description="Canonical connector name.")
    display_name: str = Field(description="Human-readable connector name.")
    connector_type: ConnectorType = Field(description="Connector type.")
    definition_id: str | None = Field(default=None, description="Connector definition ID.")
    docker_repository: str = Field(description="Docker repository.")
    docker_image_tag: str | None = Field(default=None, description="Docker image tag.")
    support_level: str | None = Field(default=None, description="Support level.")
    release_stage: str | None = Field(default=None, description="Release stage.")
    source_type: str | None = Field(default=None, description="Connector subtype.")
    documentation_url: str | None = Field(
        default=None,
        description="Connector documentation URL.",
    )
    release_date: str | None = Field(default=None, description="Release date.")
    github_issue_label: str | None = Field(
        default=None,
        description="GitHub issue label for the connector.",
    )


class PublicConnectorListResult(BaseModel):
    """Result for public connector catalog listing."""

    registry_url: str = Field(description="Registry URL used for the listing.")
    connector_count: int = Field(description="Number of matching connectors.")
    filters: PublicConnectorFilters = Field(description="Applied filters.")
    connectors: list[PublicConnectorSummary] = Field(description="Matching connectors.")
