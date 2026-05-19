# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""PyAirbyte Cloud client."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, overload

from airbyte import exceptions as exc
from airbyte._util import api_util
from airbyte.cloud._credentials import (
    CREDENTIALS_FILE_PATH,
    CloudLoginResult,
    _AirbyteCredentials,
)
from airbyte.cloud._credentials import logout as remove_credentials_file
from airbyte.cloud.organizations import CloudOrganization
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.exceptions import AirbyteMissingResourceError
from airbyte.secrets.base import SecretString


if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from airbyte._util import api_imports


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
        self._credentials = _AirbyteCredentials(
            client_id=SecretString(client_id) if client_id else None,
            client_secret=SecretString(client_secret) if client_secret else None,
            bearer_token=SecretString(bearer_token) if bearer_token else None,
            public_api_root=public_api_root or api_util.CLOUD_API_ROOT,
            config_api_root=config_api_root,
            workspace_id=workspace_id,
            organization_id=organization_id,
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
    def from_env(
        cls,
        *,
        client_id: str | SecretString | None = None,
        client_secret: str | SecretString | None = None,
        bearer_token: str | SecretString | None = None,
        organization_id: str | None = None,
        public_api_root: str | None = None,
        config_api_root: str | None = None,
    ) -> CloudClient:
        """Create a client from shared environment and credentials-file resolution."""
        credentials = _AirbyteCredentials.from_auth(
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            organization_id=organization_id,
            public_api_root=public_api_root,
            config_api_root=config_api_root,
        )
        return cls._from_credentials(credentials)

    @classmethod
    def from_auth(
        cls,
        *,
        organization_id: str | None = None,
        client_id: str | SecretString | None = None,
        client_secret: str | SecretString | None = None,
        bearer_token: str | SecretString | None = None,
        public_api_root: str | None = None,
        config_api_root: str | None = None,
        credentials_file_path: Path = CREDENTIALS_FILE_PATH,
    ) -> CloudClient:
        """Create a client from explicit inputs, env vars, and credentials file."""
        credentials = _AirbyteCredentials.from_auth(
            organization_id=organization_id,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            public_api_root=public_api_root,
            config_api_root=config_api_root,
            credentials_file_path=credentials_file_path,
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

    def login(
        self,
        *,
        interactive: bool | None = None,
        credentials_file_path: Path = CREDENTIALS_FILE_PATH,
    ) -> CloudLoginResult:
        """Log in to Airbyte and persist local credentials."""
        if interactive is True:
            raise NotImplementedError("Interactive Airbyte Cloud login is not implemented.")
        if self.client_id is not None and self.client_secret is not None:
            return self._credentials.login(credentials_file_path=credentials_file_path)
        if interactive is False:
            raise exc.PyAirbyteInputError(
                message="Client ID and client secret are both required.",
                guidance="Provide both client ID and client secret for non-interactive login.",
            )

        raise NotImplementedError("Interactive Airbyte Cloud login is not implemented.")

    def logout(
        self,
        *,
        credentials_file_path: Path = CREDENTIALS_FILE_PATH,
    ) -> None:
        """Log out by removing locally stored credentials."""
        remove_credentials_file(credentials_file_path=credentials_file_path)

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

    @overload
    def list_workspaces(
        self,
        name: str | None = None,
        *,
        organization_id: None = None,
        name_contains: str | None = None,
        name_filter: Callable[[str], bool] | None = None,
        limit: int | None = None,
    ) -> list[api_imports.WorkspaceResponse]:
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
    ) -> list[dict[str, object]]:
        raise NotImplementedError

    def list_workspaces(
        self,
        name: str | None = None,
        *,
        organization_id: str | None = None,
        name_contains: str | None = None,
        name_filter: Callable[[str], bool] | None = None,
        limit: int | None = None,
    ) -> list[api_imports.WorkspaceResponse] | list[dict[str, object]]:
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
            if name_filter is not None:
                workspaces = [
                    workspace
                    for workspace in workspaces
                    if name_filter(str(workspace.get("name", "")))
                ]
                if limit is not None:
                    workspaces = workspaces[:limit]
            return workspaces
        if name_contains is not None:
            name = name_contains
        return api_util.list_workspaces(
            workspace_id="",
            api_root=self.public_api_root,
            name=name,
            name_filter=name_filter,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
            limit=limit,
        )

    def get_organization(
        self,
        organization_id: str | None = None,
        *,
        organization_name: str | None = None,
    ) -> CloudOrganization:
        """Resolve an organization by ID or exact name."""
        if organization_id and organization_name:
            raise exc.PyAirbyteInputError(
                message="Provide either organization ID or organization name."
            )
        if not organization_id and not organization_name:
            raise exc.PyAirbyteInputError(
                message="Organization ID or organization name is required."
            )

        organizations = api_util.list_organizations_for_user(
            api_root=self.public_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )
        if organization_id:
            matching_organizations = [
                organization
                for organization in organizations
                if organization.organization_id == organization_id
            ]
        else:
            matching_organizations = [
                organization
                for organization in organizations
                if organization.organization_name == organization_name
            ]

        if not matching_organizations:
            raise AirbyteMissingResourceError(resource_type="organization")
        if len(matching_organizations) > 1:
            raise exc.PyAirbyteInputError(
                message="Organization name matches multiple organizations."
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
