# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Public response models for Airbyte Cloud APIs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field


class _WorkspaceResponseLike(Protocol):
    workspace_id: str
    name: str
    data_residency: str
    notifications: object


class _ConnectionResponseLike(Protocol):
    connection_id: str
    workspace_id: str
    source_id: str
    destination_id: str
    name: str
    configurations: Any
    prefix: str | None
    status: object


class _JobResponseLike(Protocol):
    job_id: int
    status: object
    bytes_synced: int | None
    rows_synced: int | None
    start_time: str


class _SourceResponseLike(Protocol):
    source_id: str
    name: str


class _DestinationResponseLike(Protocol):
    destination_id: str
    name: str


class _DeclarativeSourceDefinitionResponseLike(Protocol):
    id: str
    name: str
    manifest: dict[str, Any] | None
    version: object


class JobStatusEnum(str, Enum):
    """Status values for an Airbyte Cloud job."""

    PENDING = "pending"
    RUNNING = "running"
    INCOMPLETE = "incomplete"
    FAILED = "failed"
    SUCCEEDED = "succeeded"
    CANCELLED = "cancelled"


class JobTypeEnum(str, Enum):
    """Job type values for Airbyte Cloud jobs."""

    SYNC = "sync"
    RESET = "reset"
    REFRESH = "refresh"
    CLEAR = "clear"


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


class CloudConnectionInfo(BaseModel):
    """Information about an Airbyte Cloud connection."""

    connection_id: str
    """The connection ID."""

    workspace_id: str
    """The workspace ID."""

    source_id: str
    """The source ID."""

    destination_id: str
    """The destination ID."""

    name: str
    """The connection name."""

    configurations: Any
    """Stream configuration details for the connection."""

    prefix: str | None = None
    """The destination table prefix."""

    status: str
    """The connection status."""

    @classmethod
    def from_api_response(cls, connection: _ConnectionResponseLike) -> CloudConnectionInfo:
        """Create a public model from an internal API connection response."""
        return cls(
            connection_id=connection.connection_id,
            workspace_id=connection.workspace_id,
            source_id=connection.source_id,
            destination_id=connection.destination_id,
            name=connection.name,
            configurations=connection.configurations,
            prefix=connection.prefix,
            status=_enum_value(connection.status),
        )


class CloudJobInfo(BaseModel):
    """Information about an Airbyte Cloud job."""

    job_id: int
    """The job ID."""

    status: JobStatusEnum
    """The job status."""

    bytes_synced: int | None = None
    """The number of bytes synced by the job, if available."""

    rows_synced: int | None = None
    """The number of rows synced by the job, if available."""

    start_time: str
    """The job start time."""

    @classmethod
    def from_api_response(cls, job: _JobResponseLike) -> CloudJobInfo:
        """Create a public model from an internal API job response."""
        return cls(
            job_id=job.job_id,
            status=JobStatusEnum(_enum_value(job.status)),
            bytes_synced=job.bytes_synced,
            rows_synced=job.rows_synced,
            start_time=job.start_time,
        )


class CloudSourceInfo(BaseModel):
    """Information about an Airbyte Cloud source."""

    source_id: str
    """The source ID."""

    name: str
    """The source name."""

    @classmethod
    def from_api_response(cls, source: _SourceResponseLike) -> CloudSourceInfo:
        """Create a public model from an internal API source response."""
        return cls(source_id=source.source_id, name=source.name)


class CloudDestinationInfo(BaseModel):
    """Information about an Airbyte Cloud destination."""

    destination_id: str
    """The destination ID."""

    name: str
    """The destination name."""

    @classmethod
    def from_api_response(
        cls,
        destination: _DestinationResponseLike,
    ) -> CloudDestinationInfo:
        """Create a public model from an internal API destination response."""
        return cls(destination_id=destination.destination_id, name=destination.name)


class CloudCustomSourceDefinitionInfo(BaseModel):
    """Information about a custom Airbyte Cloud source definition."""

    definition_id: str
    """The source definition ID."""

    name: str
    """The source definition name."""

    manifest: dict[str, Any] | None = None
    """The source definition manifest."""

    version: str
    """The source definition version."""

    @classmethod
    def from_api_response(
        cls,
        definition: _DeclarativeSourceDefinitionResponseLike,
    ) -> CloudCustomSourceDefinitionInfo:
        """Create a public model from an internal API source definition response."""
        return cls(
            definition_id=definition.id,
            name=definition.name,
            manifest=definition.manifest,
            version=str(definition.version),
        )


def _notifications_to_dict(notifications: object) -> dict[str, object | None]:
    """Convert workspace notification settings into a dictionary."""
    if notifications is None:
        return {}
    if is_dataclass(notifications) and not isinstance(notifications, type):
        return {str(key): value for key, value in asdict(notifications).items()}
    if isinstance(notifications, Mapping):
        return {str(key): value for key, value in notifications.items()}
    return {}


def _enum_value(value: object) -> str:
    """Return the string value for an enum-like object."""
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)
