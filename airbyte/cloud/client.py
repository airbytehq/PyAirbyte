# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Client facade for authenticated Airbyte Cloud API access."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from airbyte._util import api_imports, api_util
from airbyte.cloud import workspaces as cloud_workspaces
from airbyte.cloud.credentials import (
    CREDENTIALS_FILE_PATH,
    CloudCredentials,
    CloudLoginResult,
    login_with_client_credentials,
    resolve_cloud_credentials,
)
from airbyte.cloud.credentials import logout as remove_credentials_file
from airbyte.exceptions import AirbyteMissingResourceError, PyAirbyteInputError
from airbyte.secrets.base import SecretString


if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


@dataclass(kw_only=True)
class CloudClient:
    """Authenticated client for Airbyte Cloud and self-managed Airbyte APIs."""

    client_id: SecretString | None = None
    client_secret: SecretString | None = None
    bearer_token: SecretString | None = None
    public_api_root: str = api_util.CLOUD_API_ROOT
    config_api_root: str | None = None
    organization_id: str | None = None

    def __post_init__(self) -> None:
        """Wrap provided secret values."""
        if self.client_id is not None:
            self.client_id = SecretString(self.client_id)
        if self.client_secret is not None:
            self.client_secret = SecretString(self.client_secret)
        if self.bearer_token is not None:
            self.bearer_token = SecretString(self.bearer_token)

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
        credentials = resolve_cloud_credentials(
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            organization_id=organization_id,
            public_api_root=public_api_root,
            config_api_root=config_api_root,
        )
        return cls.from_credentials(credentials)

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
        credentials = resolve_cloud_credentials(
            organization_id=organization_id,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            public_api_root=public_api_root,
            config_api_root=config_api_root,
            credentials_file_path=credentials_file_path,
        )
        return cls.from_credentials(credentials)

    @classmethod
    def get_workspace_from_auth(
        cls,
        workspace_id: str | None = None,
        *,
        organization_id: str | None = None,
        client_id: str | SecretString | None = None,
        client_secret: str | SecretString | None = None,
        bearer_token: str | SecretString | None = None,
        public_api_root: str | None = None,
        config_api_root: str | None = None,
        credentials_file_path: Path = CREDENTIALS_FILE_PATH,
    ) -> cloud_workspaces.CloudWorkspace:
        """Create a workspace from explicit inputs, env vars, and credentials file."""
        credentials = resolve_cloud_credentials(
            workspace_id=workspace_id,
            organization_id=organization_id,
            client_id=client_id,
            client_secret=client_secret,
            bearer_token=bearer_token,
            public_api_root=public_api_root,
            config_api_root=config_api_root,
            credentials_file_path=credentials_file_path,
        )
        return cls.from_credentials(credentials).get_workspace(credentials.workspace_id)

    def login(
        self,
        *,
        interactive: bool | None = None,
        credentials_file_path: Path = CREDENTIALS_FILE_PATH,
    ) -> CloudLoginResult:
        """Log in to Airbyte and persist local credentials."""
        if interactive is True:
            # TK-TODO: Implement and verify interactive browser login before merging this PR.
            raise NotImplementedError("Interactive Airbyte Cloud login is not implemented.")
        if self.client_id is not None and self.client_secret is not None:
            return login_with_client_credentials(
                client_id=str(self.client_id),
                client_secret=str(self.client_secret),
                airbyte_api_root=self.public_api_root,
                config_api_root=self.config_api_root,
                credentials_file_path=credentials_file_path,
            )
        if interactive is False:
            raise PyAirbyteInputError(
                message="Client ID and client secret are both required.",
                guidance="Provide both client ID and client secret for non-interactive login.",
            )

        # TK-TODO: Implement and verify interactive browser login before merging this PR.
        raise NotImplementedError("Interactive Airbyte Cloud login is not implemented.")

    def logout(
        self,
        *,
        credentials_file_path: Path = CREDENTIALS_FILE_PATH,
    ) -> None:
        """Log out by removing locally stored credentials."""
        remove_credentials_file(credentials_file_path=credentials_file_path)

    @classmethod
    def from_credentials(cls, credentials: CloudCredentials) -> CloudClient:
        """Create a client from resolved Cloud credentials."""
        return cls(
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            bearer_token=credentials.bearer_token,
            public_api_root=credentials.public_api_root,
            config_api_root=credentials.config_api_root,
            organization_id=credentials.organization_id,
        )

    @classmethod
    def from_explicit_credentials(
        cls,
        *,
        client_id: str | SecretString | None = None,
        client_secret: str | SecretString | None = None,
        bearer_token: str | SecretString | None = None,
        public_api_root: str | None = None,
        config_api_root: str | None = None,
        organization_id: str | None = None,
    ) -> CloudClient:
        """Create a client from explicit credential values only."""
        return cls.from_credentials(
            CloudCredentials(
                client_id=SecretString(client_id) if client_id else None,
                client_secret=SecretString(client_secret) if client_secret else None,
                bearer_token=SecretString(bearer_token) if bearer_token else None,
                public_api_root=public_api_root or api_util.CLOUD_API_ROOT,
                config_api_root=config_api_root,
                organization_id=organization_id,
            )
        )

    def get_workspace(self, workspace_id: str | None = None) -> cloud_workspaces.CloudWorkspace:
        """Create a `CloudWorkspace` using this client's credentials."""
        if not workspace_id:
            raise PyAirbyteInputError(
                message="Workspace ID is required.",
                guidance="Provide a workspace ID.",
            )

        return cloud_workspaces.CloudWorkspace(
            workspace_id=workspace_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
            api_root=self.public_api_root,
            config_api_root=self.config_api_root,
        )

    def list_workspaces(
        self,
        name: str | None = None,
        *,
        name_filter: Callable[[str], bool] | None = None,
    ) -> list[api_imports.WorkspaceResponse]:
        """List workspaces available to this client."""
        return api_util.list_workspaces(
            workspace_id="",
            api_root=self.public_api_root,
            name=name,
            name_filter=name_filter,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )

    def list_workspaces_in_organization(
        self,
        organization_id: str | None = None,
        *,
        name_contains: str | None = None,
        max_items_limit: int | None = None,
    ) -> list[dict[str, object]]:
        """List workspaces in an organization using the Config API."""
        resolved_organization_id = organization_id or self.organization_id
        if not resolved_organization_id:
            raise PyAirbyteInputError(
                message="Organization ID is required.",
                guidance="Provide an organization ID.",
            )

        return api_util.list_workspaces_in_organization(
            organization_id=resolved_organization_id,
            api_root=self.public_api_root,
            config_api_root=self.config_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
            name_contains=name_contains,
            max_items_limit=max_items_limit,
        )

    def get_organization(
        self,
        organization_id: str | None = None,
        *,
        organization_name: str | None = None,
    ) -> cloud_workspaces.CloudOrganization:
        """Resolve an organization by ID or exact name."""
        if organization_id and organization_name:
            raise PyAirbyteInputError(
                message="Provide either organization ID or organization name."
            )
        if not organization_id and not organization_name:
            raise PyAirbyteInputError(message="Organization ID or organization name is required.")

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
            raise PyAirbyteInputError(message="Organization name matches multiple organizations.")

        organization = matching_organizations[0]

        return cloud_workspaces.CloudOrganization(
            organization_id=organization.organization_id,
            organization_name=organization.organization_name,
            email=organization.email,
            api_root=self.public_api_root,
            config_api_root=self.config_api_root,
            client_id=self.client_id,
            client_secret=self.client_secret,
            bearer_token=self.bearer_token,
        )
