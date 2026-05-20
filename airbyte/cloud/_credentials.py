# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Internal credential resolution for Airbyte Cloud authentication."""

from __future__ import annotations

from dataclasses import dataclass, replace

from airbyte.constants import (
    CLOUD_API_ROOT,
    CLOUD_API_ROOT_ENV_VAR,
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CLIENT_SECRET_ENV_VAR,
    CLOUD_CONFIG_API_ROOT_ENV_VAR,
    CLOUD_ORGANIZATION_ID_ENV_VAR,
    CLOUD_WORKSPACE_ID_ENV_VAR,
)
from airbyte.exceptions import PyAirbyteInputError
from airbyte.secrets.base import SecretString
from airbyte.secrets.util import try_get_secret


CLIENT_ID_ENV_VAR = "AIRBYTE_CLIENT_ID"
CLIENT_SECRET_ENV_VAR = "AIRBYTE_CLIENT_SECRET"
WORKSPACE_ID_ENV_VAR = "AIRBYTE_WORKSPACE_ID"
ORGANIZATION_ID_ENV_VAR = "AIRBYTE_ORGANIZATION_ID"
PUBLIC_API_ROOT_ENV_VAR = "AIRBYTE_API_ROOT"
BEARER_TOKEN_ENV_VAR = "AIRBYTE_BEARER_TOKEN"
CONFIG_API_ROOT_ENV_VAR = "AIRBYTE_CONFIG_API_ROOT"


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

        When `env_vars` is True (default), environment variables are checked as a
        fallback after explicit inputs.
        """
        resolved_bearer_token = _first_value(
            str(bearer_token) if bearer_token is not None else None,
            _env_value(BEARER_TOKEN_ENV_VAR, CLOUD_BEARER_TOKEN_ENV_VAR) if env_vars else None,
        )
        resolved_client_id = _first_value(
            str(client_id) if client_id is not None else None,
            _env_value(CLIENT_ID_ENV_VAR, CLOUD_CLIENT_ID_ENV_VAR) if env_vars else None,
        )
        resolved_client_secret = _first_value(
            str(client_secret) if client_secret is not None else None,
            _env_value(CLIENT_SECRET_ENV_VAR, CLOUD_CLIENT_SECRET_ENV_VAR) if env_vars else None,
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
            guidance = (
                "Set Airbyte Cloud credentials in environment variables."
                if env_vars
                else "Provide either bearer_token or both client_id and client_secret."
            )
            raise PyAirbyteInputError(
                message="No Airbyte credentials found.",
                guidance=guidance,
            )

        return cls(
            client_id=SecretString(resolved_client_id) if resolved_client_id else None,
            client_secret=SecretString(resolved_client_secret) if resolved_client_secret else None,
            bearer_token=SecretString(resolved_bearer_token) if resolved_bearer_token else None,
            public_api_root=_first_value(
                public_api_root,
                _env_value(PUBLIC_API_ROOT_ENV_VAR, CLOUD_API_ROOT_ENV_VAR) if env_vars else None,
            )
            or CLOUD_API_ROOT,
            config_api_root=_first_value(
                config_api_root,
                _env_value(CONFIG_API_ROOT_ENV_VAR, CLOUD_CONFIG_API_ROOT_ENV_VAR)
                if env_vars
                else None,
            ),
            workspace_id=_first_value(
                workspace_id,
                _env_value(WORKSPACE_ID_ENV_VAR, CLOUD_WORKSPACE_ID_ENV_VAR) if env_vars else None,
            ),
            organization_id=_first_value(
                organization_id,
                _env_value(ORGANIZATION_ID_ENV_VAR, CLOUD_ORGANIZATION_ID_ENV_VAR)
                if env_vars
                else None,
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


def _env_value(*names: str) -> str | None:
    """Return the first available environment variable value."""
    for name in names:
        value = try_get_secret(name, default=None)
        if value:
            return str(value)
    return None
