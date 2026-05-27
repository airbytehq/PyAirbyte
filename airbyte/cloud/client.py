# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte Cloud client."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, overload

from airbyte import exceptions as exc
from airbyte._util import api_util
from airbyte.cloud._credentials import _AirbyteCredentials
from airbyte.cloud.models import CloudWorkspaceInfo
from airbyte.cloud.organizations import CloudOrganization
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteMissingResourceError


if TYPE_CHECKING:
    from collections.abc import Callable

    from airbyte.secrets.base import SecretString


@dataclass(init=False, kw_only=True)
class CloudClient:
    """Authenticated client for Airbyte Cloud and self-managed Airbyte APIs."""

    _credentials: _AirbyteCredentials

    def __init__(
        self,
        *,
        client_id: str | SecretString | None = None,
        client_secret: str | SecretString | None = None,
        bearer_token: str | SecretString | None = None,
        public_api_root: str | None = None,
        config_api_root: str | None = None,
        workspace_id: str | None = None,
        organization_id: str | None = None,
    ) -> None:
        """Initialize a `CloudClient` from explicit auth values."""
        self._credentials = _AirbyteCredentials.from_auth(
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            public_api_root=public_api_root,
            config_api_root=config_api_root,
            workspace_id=workspace_id,
            organization_id=organization_id,
            env_vars=False,
        )

    @property
    def client_id(self) -> SecretString | None:
        """OAuth client ID used for authentication."""
        return self._credentials.client_id

    @property
    def client_secret(self) -> SecretString | None:
        """OAuth client secret used for authentication."""
        return self._credentials.client_secret

    @property
    def bearer_token(self) -> SecretString | None:
        """Bearer token used for authentication."""
        return self._credentials.bearer_token

    @property
    def public_api_root(self) -> str:
        """Airbyte Public API root."""
        return self._credentials.public_api_root

    @property
    def config_api_root(self) -> str | None:
        """Airbyte Config API root."""
        return self._credentials.config_api_root

    @property
    def organization_id(self) -> str | None:
        """Default organization ID for organization-scoped operations."""
        return self._credentials.organization_id

    @classmethod
    def from_auth(
        cls,
        *,
        env_vars: bool = False,
        organization_id: str | None = None,
        client_id: str | SecretString | None = None,
        client_secret: str | SecretString | None = None,
        bearer_token: str | SecretString | None = None,
        public_api_root: str | None = None,
        config_api_root: str | None = None,
    ) -> CloudClient:
        """Create a client from explicit inputs and optionally environment variables.

        When `env_vars` is True, environment variables are checked as a fallback
        after any explicitly provided values.
        """
        credentials = _AirbyteCredentials.from_auth(
            organization_id=organization_id,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            public_api_root=public_api_root,
            config_api_root=config_api_root,
            env_vars=env_vars,
        )
        return cls._from_credentials(credentials)

    @classmethod
    def _from_credentials(cls, credentials: _AirbyteCredentials) -> CloudClient:
        """Create a client from resolved Cloud credentials."""
        return cls(
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            bearer_token=credentials.bearer_token,
            public_api_root=credentials.public_api_root,
            config_api_root=credentials.config_api_root,
            workspace_id=credentials.workspace_id,
            organization_id=credentials.organization_id,
        )

    def get_workspace(self, workspace_id: str | None = None) -> CloudWorkspace:
        """Create a `CloudWorkspace` using this client's credentials."""
        resolved_workspace_id = workspace_id or self._credentials.workspace_id
        if not resolved_workspace_id:
            raise exc.PyAirbyteInputError(
                message="Workspace ID is required.",
                guidance="Provide a workspace ID.",
            )

        credentials = self._credentials.with_workspace_id(resolved_workspace_id)
        return CloudWorkspace(
            workspace_id=credentials.workspace_id,
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            bearer_token=credentials.bearer_token,
            api_root=credentials.public_api_root,
            config_api_root=credentials.config_api_root,
        )

    def create_workspace(
        self,
        *,
        name: str,
        organization_id: str | None = None,
        region_id: str | None = None,
    ) -> CloudWorkspaceInfo:
        """Create an Airbyte workspace."""
        resolved_organization_id = organization_id or self.organization_id
        workspace = api_util.create_workspace(
            name=name,
            organization_id=resolved_organization_id,
            region_id=region_id,
            api_root=self.public_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )
        return CloudWorkspaceInfo.from_api_response(workspace)

    def rename_workspace(
        self,
        workspace_id: str,
        *,
        name: str,
    ) -> CloudWorkspaceInfo:
        """Rename an Airbyte workspace."""
        workspace = api_util.rename_workspace(
            workspace_id=workspace_id,
            name=name,
            api_root=self.public_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )
        return CloudWorkspaceInfo.from_api_response(workspace)

    def permanently_delete_workspace(
        self,
        workspace_id: str,
        *,
        workspace_name: str | None = None,
        safe_mode: bool = True,
    ) -> None:
        """Permanently delete an Airbyte workspace if it has no connections.

        When `safe_mode` is enabled, the workspace name must contain `delete-me`
        or `deleteme`. This also checks for existing connections before deleting
        and raises `AirbyteWorkspaceNotEmptyError` if the workspace is not empty.
        """
        api_util.permanently_delete_workspace(
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            api_root=self.public_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
            safe_mode=safe_mode,
        )

    @overload
    def list_workspaces(
        self,
        name: str | None = None,
        *,
        organization_id: None = None,
        name_contains: str | None = None,
        name_filter: Callable[[str], bool] | None = None,
        limit: int | None = None,
    ) -> list[CloudWorkspaceInfo]:
        raise NotImplementedError

    @overload
    def list_workspaces(
        self,
        name: str | None = None,
        *,
        organization_id: str,
        name_contains: str | None = None,
        name_filter: Callable[[str], bool] | None = None,
        limit: int | None = None,
    ) -> list[CloudWorkspaceInfo]:
        raise NotImplementedError

    def list_workspaces(
        self,
        name: str | None = None,
        *,
        organization_id: str | None = None,
        name_contains: str | None = None,
        name_filter: Callable[[str], bool] | None = None,
        limit: int | None = None,
    ) -> list[CloudWorkspaceInfo]:
        """List workspaces available to this client."""
        if organization_id is not None or self.organization_id is not None:
            resolved_organization_id = organization_id or self.organization_id
            if not resolved_organization_id:
                raise exc.PyAirbyteInputError(
                    message="Organization ID is required.",
                    guidance="Provide an organization ID.",
                )
            workspaces = api_util.list_workspaces_in_organization(
                organization_id=resolved_organization_id,
                api_root=self.public_api_root,
                config_api_root=self.config_api_root,
                client_id=self.client_id,
                client_secret=self.client_secret,
                bearer_token=self.bearer_token,
                name_contains=name_contains or name,
                limit=None if name_filter is not None else limit,
            )
            workspace_infos = [
                CloudWorkspaceInfo.from_mapping(workspace) for workspace in workspaces
            ]
            if name_filter is not None:
                workspace_infos = [
                    workspace for workspace in workspace_infos if name_filter(workspace.name)
                ]
                if limit is not None:
                    workspace_infos = workspace_infos[:limit]
            return workspace_infos
        if name_contains is not None:
            if name_filter is not None:
                raise exc.PyAirbyteInputError(
                    message="You can provide name_contains or name_filter, but not both."
                )
            name_substring = name_contains

            def name_filter(workspace_name: str) -> bool:
                return name_substring in workspace_name

        return [
            CloudWorkspaceInfo.from_api_response(workspace)
            for workspace in api_util.list_workspaces(
                workspace_id="",
                api_root=self.public_api_root,
                name=name,
                name_filter=name_filter,
                client_id=self.client_id,
                client_secret=self.client_secret,
                bearer_token=self.bearer_token,
                limit=limit,
            )
        ]

    def get_organization(
        self,
        organization_id: str | None = None,
        *,
        organization_name: str | None = None,
    ) -> CloudOrganization:
        """Resolve an organization by ID or exact name."""
        resolved_organization_id = organization_id or self.organization_id
        if resolved_organization_id and organization_name:
            raise exc.PyAirbyteInputError(
                message="Provide either organization ID or organization name."
            )
        if not resolved_organization_id and not organization_name:
            raise exc.PyAirbyteInputError(
                message="Organization ID or organization name is required."
            )

        organizations = api_util.list_organizations_for_user(
            api_root=self.public_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )
        if resolved_organization_id:
            matching_organizations = [
                organization
                for organization in organizations
                if organization.organization_id == resolved_organization_id
            ]
        else:
            matching_organizations = [
                organization
                for organization in organizations
                if organization.organization_name == organization_name
            ]

        if not matching_organizations:
            raise AirbyteMissingResourceError(
                resource_type="organization",
                resource_name_or_id=resolved_organization_id or organization_name,
            )
        if len(matching_organizations) > 1:
            raise exc.PyAirbyteInputError(
                message="Organization name matches multiple organizations.",
                context={"organization_name": organization_name},
            )

        organization = matching_organizations[0]

        organization_credentials = self._credentials.with_organization_id(
            organization.organization_id
        )
        return CloudOrganization(
            organization_id=organization.organization_id,
            organization_name=organization.organization_name,
            email=organization.email,
            client_id=organization_credentials.client_id,
            client_secret=organization_credentials.client_secret,
            bearer_token=organization_credentials.bearer_token,
            public_api_root=organization_credentials.public_api_root,
            config_api_root=organization_credentials.config_api_root,
        )
