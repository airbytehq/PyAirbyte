# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Public response models for Airbyte Cloud APIs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field


class _WorkspaceResponseLike(Protocol):
    workspace_id: str
    name: str
    data_residency: str
    notifications: object


class CloudWorkspaceInfo(BaseModel):
    """Information about an Airbyte workspace."""

    model_config = ConfigDict(populate_by_name=True)

    workspace_id: str = Field(alias="workspaceId")
    """The workspace ID."""

    name: str
    """The workspace name."""

    data_residency: str | None = Field(default=None, alias="dataResidency")
    """The data residency setting for the workspace, if available."""

    organization_id: str | None = Field(default=None, alias="organizationId")
    """The organization ID for the workspace, if available."""

    notifications: dict[str, object | None] = Field(default_factory=dict)
    """Workspace notification settings."""

    @classmethod
    def from_api_response(cls, workspace: _WorkspaceResponseLike) -> CloudWorkspaceInfo:
        """Create a public model from an internal API workspace response."""
        return cls(
            workspace_id=workspace.workspace_id,
            name=workspace.name,
            data_residency=workspace.data_residency,
            organization_id=getattr(workspace, "organization_id", None),
            notifications=_notifications_to_dict(workspace.notifications),
        )

    @classmethod
    def from_mapping(cls, workspace: Mapping[str, object]) -> CloudWorkspaceInfo:
        """Create a public model from a workspace mapping."""
        return cls.model_validate(workspace)

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable dictionary."""
        return self.model_dump(mode="json")


def _notifications_to_dict(notifications: object) -> dict[str, object | None]:
    """Convert workspace notification settings into a dictionary."""
    if notifications is None:
        return {}
    if is_dataclass(notifications) and not isinstance(notifications, type):
        return {str(key): value for key, value in asdict(notifications).items()}
    if isinstance(notifications, Mapping):
        return {str(key): value for key, value in notifications.items()}
    return {}
