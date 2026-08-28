# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Airbyte Agents organizations."""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

from airbyte.agents import _api_util
from airbyte.agents.models import AgentWorkspaceInfo
from airbyte.agents.workspaces import AgentWorkspace
from airbyte.cloud._credentials import _AirbyteCredentials
from airbyte.cloud.organizations import CloudOrganization
from airbyte.exceptions import AirbyteError, PyAirbyteInputError


if TYPE_CHECKING:
    from airbyte.secrets.base import SecretString


class _WorkspaceLookup(NamedTuple):
    """What to look a workspace up by, once the lookup arguments have been validated.

    Both fields are set when the caller passed a positional value that could be either an
    ID or a name, in which case an ID match takes precedence over a name match.
    """

    workspace_id: str | None
    name: str | None


def _resolve_workspace_lookup(
    id_or_name: str | None,
    /,
    *,
    workspace_id: str | None,
    name: str | None,
) -> _WorkspaceLookup:
    """Validate workspace lookup arguments and return what to look the workspace up by.

    Exactly one of `workspace_id`, `name`, or the positional `id_or_name` is required. A
    blank value is rejected rather than treated as an omitted argument.
    """
    all_args = {"id_or_name": id_or_name, "workspace_id": workspace_id, "name": name}

    blank_args = sorted(
        key for key, value in all_args.items() if value is not None and not value.strip()
    )
    if blank_args:
        raise PyAirbyteInputError(
            message="Workspace lookup arguments cannot be blank.",
            guidance="Omit the argument entirely, or pass a non-blank value.",
            context={"blank_args": blank_args},
        )

    provided = sorted(key for key, value in all_args.items() if value)
    if len(provided) != 1:
        raise PyAirbyteInputError(
            message="Exactly one workspace lookup argument is required.",
            guidance="Pass a workspace ID or name positionally, or as `workspace_id` or `name`.",
            context={"provided": provided},
        )

    if id_or_name:
        return _WorkspaceLookup(workspace_id=id_or_name, name=id_or_name)

    return _WorkspaceLookup(workspace_id=workspace_id, name=name)


class AgentOrganization:
    """An organization on the Airbyte Agents platform.

    ```python
    from airbyte import agents

    organization = agents.AgentOrganization.from_env()
    workspace = organization.get_workspace("my-workspace")  # by ID or name (case insensitive)
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
            # Mirrors `CloudWorkspace.__init__`: any explicit credential disables env
            # fallback, since an env bearer token plus explicit client creds is rejected
            # as mutually exclusive auth.
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
            self._workspace_from_info(info)
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
        id_or_name: str | None = None,
        /,
        *,
        workspace_id: str | None = None,
        name: str | None = None,
    ) -> AgentWorkspace:
        """Get a workspace in this organization, by ID or by name.

        Pass a single positional value to look the workspace up by either its ID or its
        name, or name the argument to be explicit.

        Lookup by an explicit `workspace_id` does not call the Agents API. Every other form
        lists the organization's workspaces and matches on ID first, then on an exact name,
        ignoring case.
        """
        lookup = _resolve_workspace_lookup(
            id_or_name,
            workspace_id=workspace_id,
            name=name,
        )

        if lookup.workspace_id and not lookup.name:
            return self._workspace_from_info(AgentWorkspaceInfo(id=lookup.workspace_id))

        workspaces = self.list_workspaces()
        if lookup.workspace_id:
            id_matches = [
                workspace
                for workspace in workspaces
                if workspace.workspace_id == lookup.workspace_id
            ]
            if id_matches:
                return id_matches[0]

        name_lower = (lookup.name or "").lower()
        matches = [
            workspace
            for workspace in workspaces
            if workspace.name and workspace.name.lower() == name_lower
        ]
        if not matches:
            raise AirbyteError(
                message="No workspace found with the given ID or name.",
                guidance="Use `list_workspaces()` to see the available workspaces.",
                context={"name": lookup.name},
            )
        if len(matches) > 1:
            raise AirbyteError(
                message="Multiple workspaces matched the given name.",
                guidance="Pass `workspace_id` instead of `name`.",
                context={"name": lookup.name, "match_count": len(matches)},
            )
        return matches[0]

    def as_cloud_organization(self) -> CloudOrganization:
        """Return this organization as an `airbyte.cloud.CloudOrganization`.

        Every Agents organization is also a Cloud organization, so this conversion needs no
        API call. It requires a known organization ID, and raises `PyAirbyteInputError`
        when the ID is unknown.
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

        Raises `PyAirbyteInputError` when the Cloud organization uses non-public Cloud API
        roots, since an `AgentOrganization` cannot carry them.
        """
        credentials = cloud_organization._credentials  # noqa: SLF001  # Same-domain conversion.
        _api_util.check_public_cloud_api_roots(credentials)
        return cls(
            organization_id=cloud_organization.organization_id,
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            bearer_token=credentials.bearer_token,
        )

    def _workspace_from_info(self, info: AgentWorkspaceInfo) -> AgentWorkspace:
        """Build an `AgentWorkspace` from workspace info, reusing these credentials."""
        return AgentWorkspace(
            workspace_id=info.id,
            organization_id=info.organization_id or self.organization_id,
            name=info.name,
            client_id=self._credentials.client_id,
            client_secret=self._credentials.client_secret,
            bearer_token=self._credentials.bearer_token,
        )
