# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Internal credential resolution for Airbyte Cloud authentication."""

from __future__ import annotations

from dataclasses import dataclass, replace

from airbyte.constants import CLOUD_API_ROOT
from airbyte.exceptions import AirbyteNoCloudCredentialsError, PyAirbyteInputError
from airbyte.secrets.base import SecretString
from airbyte.settings import AirbyteCloudSettings


@dataclass(frozen=True)
class _AirbyteCredentials:
    """Resolved credentials and API roots for Airbyte control-plane APIs."""

    client_id: SecretString | None
    client_secret: SecretString | None
    bearer_token: SecretString | None
    public_api_root: str
    config_api_root: str | None
    workspace_id: str | None = None
    organization_id: str | None = None

    @classmethod
    def from_auth(
        cls,
        *,
        workspace_id: str | None = None,
        organization_id: str | None = None,
        client_id: str | SecretString | None = None,
        client_secret: str | SecretString | None = None,
        bearer_token: str | SecretString | None = None,
        public_api_root: str | None = None,
        config_api_root: str | None = None,
        env_vars: bool = True,
    ) -> _AirbyteCredentials:
        """Resolve Airbyte Cloud credentials from inputs and optionally env vars.

        When `env_vars` is True (default), process environment and `./.env` settings are checked
        as a fallback after explicit inputs.
        """
        settings = AirbyteCloudSettings() if env_vars else None
        resolved_bearer_token = _first_value(
            str(bearer_token) if bearer_token is not None else None,
            str(settings.bearer_token)
            if settings is not None and settings.bearer_token is not None
            else None,
        )
        resolved_client_id = _first_value(
            str(client_id) if client_id is not None else None,
            str(settings.client_id)
            if settings is not None and settings.client_id is not None
            else None,
        )
        resolved_client_secret = _first_value(
            str(client_secret) if client_secret is not None else None,
            str(settings.client_secret)
            if settings is not None and settings.client_secret is not None
            else None,
        )

        if resolved_bearer_token and (resolved_client_id or resolved_client_secret):
            raise PyAirbyteInputError(
                message="Cannot use both client credentials and bearer token authentication.",
                guidance=(
                    "Provide either client_id and client_secret together, "
                    "or bearer_token alone, but not both."
                ),
            )
        if bool(resolved_client_id) != bool(resolved_client_secret):
            raise PyAirbyteInputError(
                message="Client ID and client secret are both required.",
                guidance="Provide both client ID and client secret, or use a bearer token.",
            )
        if not resolved_bearer_token and not resolved_client_id:
            raise AirbyteNoCloudCredentialsError(_env_vars=env_vars)

        return cls(
            client_id=SecretString(resolved_client_id) if resolved_client_id else None,
            client_secret=SecretString(resolved_client_secret) if resolved_client_secret else None,
            bearer_token=SecretString(resolved_bearer_token) if resolved_bearer_token else None,
            public_api_root=_first_value(
                public_api_root,
                settings.api_url if settings is not None else None,
            )
            or CLOUD_API_ROOT,
            config_api_root=_first_value(
                config_api_root,
                settings.config_api_url if settings is not None else None,
            ),
            workspace_id=_first_value(
                workspace_id,
                settings.workspace_id if settings is not None else None,
            ),
            organization_id=_first_value(
                organization_id,
                settings.organization_id if settings is not None else None,
            ),
        )

    def with_workspace_id(self, workspace_id: str | None) -> _AirbyteCredentials:
        """Return credentials scoped to a workspace."""
        return replace(self, workspace_id=workspace_id)

    def with_organization_id(self, organization_id: str | None) -> _AirbyteCredentials:
        """Return credentials scoped to an organization."""
        return replace(self, organization_id=organization_id)


def _first_value(*values: str | None) -> str | None:
    """Return the first non-empty string value."""
    for value in values:
        if value:
            return value
    return None
