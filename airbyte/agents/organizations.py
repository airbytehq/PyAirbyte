# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Airbyte Agents organizations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte.agents import _api_util
from airbyte.agents.models import AgentWorkspaceInfo
from airbyte.agents.workspaces import AgentWorkspace
from airbyte.cloud._credentials import _AirbyteCredentials
from airbyte.cloud.organizations import CloudOrganization
from airbyte.exceptions import AirbyteError, PyAirbyteInputError


if TYPE_CHECKING:
    from airbyte.secrets.base import SecretString


class AgentOrganization:
    """An organization on the Airbyte Agents platform.

    ```python
    from airbyte import agents

    organization = agents.AgentOrganization.from_env()
    workspace = organization.get_workspace(name="my-workspace")
    ```
    """

    def __init__(
        self,
        *,
        organization_id: str | None = None,
        client_id: str | SecretString | None = None,
        client_secret: str | SecretString | None = None,
        bearer_token: str | SecretString | None = None,
    ) -> None:
        """Initialize an `AgentOrganization`.

        Credentials fall back to the `AIRBYTE_CLOUD_*` environment variables when they are
        not passed explicitly. The organization ID is optional: the Agents API infers it
        when the credentials belong to exactly one organization.
        """
        self._credentials = _AirbyteCredentials.from_auth(
            organization_id=organization_id,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            env_vars=not (client_id or client_secret or bearer_token),
        )

        self.organization_id: str | None = self._credentials.organization_id
        """The organization ID, when known."""

    @classmethod
    def from_env(cls, organization_id: str | None = None) -> AgentOrganization:
        """Create an `AgentOrganization` from the `AIRBYTE_CLOUD_*` environment variables."""
        return cls(organization_id=organization_id)

    def list_workspaces(self) -> list[AgentWorkspace]:
        """List the workspaces visible to these credentials in the Agents API."""
        return [
            self._as_workspace(info)
            for info in (
                AgentWorkspaceInfo.model_validate(record)
                for record in _api_util.list_agent_workspaces(
                    credentials=self._credentials,
                    organization_id=self.organization_id,
                )
            )
        ]

    def get_workspace(
        self,
        *,
        workspace_id: str | None = None,
        name: str | None = None,
    ) -> AgentWorkspace:
        """Get a workspace in this organization, by ID or by name.

        Lookup by `workspace_id` does not call the Agents API. Lookup by `name` lists the
        organization's workspaces and matches the name exactly.
        """
        if bool(workspace_id) == bool(name):
            raise PyAirbyteInputError(
                message="Exactly one of `workspace_id` or `name` is required.",
                guidance="Provide either `workspace_id` or `name`, but not both.",
            )

        if workspace_id:
            return self._as_workspace(AgentWorkspaceInfo(id=workspace_id))

        matches = [workspace for workspace in self.list_workspaces() if workspace.name == name]
        if not matches:
            raise AirbyteError(
                message="No workspace found with the given name.",
                guidance="Use `list_workspaces()` to see the available workspaces.",
                context={"name": name},
            )
        if len(matches) > 1:
            raise AirbyteError(
                message="Multiple workspaces matched the given name.",
                guidance="Pass `workspace_id` instead of `name`.",
                context={"name": name, "match_count": len(matches)},
            )
        return matches[0]

    def as_cloud_organization(self) -> CloudOrganization:
        """Return this organization as an `airbyte.cloud.CloudOrganization`.

        Every Agents organization is also a Cloud organization, so this conversion always
        succeeds without calling either API.
        """
        if not self.organization_id:
            raise PyAirbyteInputError(
                message="Organization ID is required to convert to a `CloudOrganization`.",
                guidance=(
                    "Provide `organization_id`, or set the `AIRBYTE_CLOUD_ORGANIZATION_ID` "
                    "environment variable."
                ),
            )
        return CloudOrganization(
            organization_id=self.organization_id,
            client_id=self._credentials.client_id,
            client_secret=self._credentials.client_secret,
            bearer_token=self._credentials.bearer_token,
            public_api_root=self._credentials.public_api_root,
            config_api_root=self._credentials.config_api_root,
        )

    @classmethod
    def from_cloud_organization(
        cls,
        cloud_organization: CloudOrganization,
    ) -> AgentOrganization:
        """Return a Cloud organization as an `AgentOrganization`.

        Whether the organization can actually execute connector actions depends on its
        Airbyte Agents subscription, which is only knowable per workspace. Use
        `AgentWorkspace.from_cloud_workspace()` for an authoritative eligibility check.
        """
        credentials = cloud_organization._credentials  # noqa: SLF001  # Same-domain conversion.
        return cls(
            organization_id=cloud_organization.organization_id,
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            bearer_token=credentials.bearer_token,
        )

    def _as_workspace(self, info: AgentWorkspaceInfo) -> AgentWorkspace:
        """Build an `AgentWorkspace` from workspace info, reusing these credentials."""
        return AgentWorkspace(
            workspace_id=info.id,
            organization_id=info.organization_id or self.organization_id,
            name=info.name,
            client_id=self._credentials.client_id,
            client_secret=self._credentials.client_secret,
            bearer_token=self._credentials.bearer_token,
        )
