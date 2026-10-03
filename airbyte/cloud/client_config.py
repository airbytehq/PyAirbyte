# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Cloud client configuration for Airbyte Cloud API authentication.

This module provides the CloudClientConfig class for managing authentication
credentials and API configuration when connecting to Airbyte Cloud, OSS, or
Enterprise instances.

Two authentication methods are supported (mutually exclusive):
1. OAuth2 client credentials (client_id + client_secret)
2. Bearer token authentication

Example usage with client credentials:
    ```python
    from airbyte.cloud.client_config import CloudClientConfig

    config = CloudClientConfig(
        client_id="your-client-id",
        client_secret="your-client-secret",
    )
    ```

Example usage with bearer token:
    ```python
    from airbyte.cloud.client_config import CloudClientConfig

    config = CloudClientConfig(
        bearer_token="your-bearer-token",
    )
    ```

Example using environment variables:
    ```python
    from airbyte.cloud.client_config import CloudClientConfig

    # Resolves through airbyte.settings.AirbyteCloudSettings from process environment
    # variables and .env (process environment takes precedence).
    # Generic aliases include AIRBYTE_CLIENT_ID, AIRBYTE_CLIENT_SECRET,
    # AIRBYTE_BEARER_TOKEN, AIRBYTE_WORKSPACE_ID, and AIRBYTE_ORGANIZATION_ID.
    config = CloudClientConfig.from_env()
    ```

Cloud credentials and URLs are read from the process environment and `./.env` by
`airbyte.settings.AirbyteCloudSettings`. Process environment values take precedence, and these
settings do not use registered secret managers or interactive prompts. Generic credential aliases
(`AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_BEARER_TOKEN`, `AIRBYTE_WORKSPACE_ID`, and
`AIRBYTE_ORGANIZATION_ID`) take precedence over their `AIRBYTE_CLOUD_*` alternatives.
"""

from __future__ import annotations

from dataclasses import dataclass

from airbyte._util import api_util
from airbyte.cloud._auth import (
    resolve_cloud_api_url,
    resolve_cloud_bearer_token,
    resolve_cloud_client_id,
    resolve_cloud_client_secret,
    resolve_cloud_config_api_url,
)
from airbyte.exceptions import PyAirbyteInputError
from airbyte.secrets.base import SecretString


@dataclass
class CloudClientConfig:
    """Client configuration for Airbyte Cloud API.

    This class encapsulates the authentication and API configuration needed to connect
    to Airbyte Cloud, OSS, or Enterprise instances. It supports two mutually
    exclusive authentication methods:

    1. OAuth2 client credentials flow (client_id + client_secret)
    2. Bearer token authentication

    Exactly one authentication method must be provided. Providing both or neither
    will raise a validation error.

    Attributes:
        client_id: OAuth2 client ID for client credentials flow.
        client_secret: OAuth2 client secret for client credentials flow.
        bearer_token: Pre-generated bearer token for direct authentication.
        api_root: The API root URL. Defaults to Airbyte Cloud API.
        config_api_root: The Config API root URL.
    """

    client_id: SecretString | None = None
    """OAuth2 client ID for client credentials authentication."""

    client_secret: SecretString | None = None
    """OAuth2 client secret for client credentials authentication."""

    bearer_token: SecretString | None = None
    """Bearer token for direct authentication (alternative to client credentials)."""

    api_root: str = api_util.CLOUD_API_ROOT
    """The API root URL. Defaults to Airbyte Cloud API."""

    config_api_root: str | None = None
    """The Config API root URL."""

    def __post_init__(self) -> None:
        """Validate credentials and ensure secrets are properly wrapped."""
        # Wrap secrets in SecretString if they aren't already
        if self.client_id is not None:
            self.client_id = SecretString(self.client_id)
        if self.client_secret is not None:
            self.client_secret = SecretString(self.client_secret)
        if self.bearer_token is not None:
            self.bearer_token = SecretString(self.bearer_token)

        # Validate mutual exclusivity
        has_client_credentials = self.client_id is not None or self.client_secret is not None
        has_bearer_token = self.bearer_token is not None

        if has_client_credentials and has_bearer_token:
            raise PyAirbyteInputError(
                message="Cannot use both client credentials and bearer token authentication.",
                guidance=(
                    "Provide either client_id and client_secret together, "
                    "or bearer_token alone, but not both."
                ),
            )

        if has_client_credentials and (self.client_id is None or self.client_secret is None):
            # If using client credentials, both must be provided
            raise PyAirbyteInputError(
                message="Incomplete client credentials.",
                guidance=(
                    "When using client credentials authentication, "
                    "both client_id and client_secret must be provided."
                ),
            )

        if not has_client_credentials and not has_bearer_token:
            raise PyAirbyteInputError(
                message="No authentication credentials provided.",
                guidance=(
                    "Provide either client_id and client_secret together for OAuth2 "
                    "client credentials flow, or bearer_token for direct authentication."
                ),
            )

    @property
    def uses_bearer_token(self) -> bool:
        """Return True if using bearer token authentication."""
        return self.bearer_token is not None

    @property
    def uses_client_credentials(self) -> bool:
        """Return True if using client credentials authentication."""
        return self.client_id is not None and self.client_secret is not None

    @classmethod
    def from_env(
        cls,
        *,
        api_root: str | None = None,
        config_api_root: str | None = None,
    ) -> CloudClientConfig:
        """Create a CloudClientConfig from `AirbyteCloudSettings`.

        Credentials and URLs are resolved from process environment variables and `./.env` through
        `airbyte.settings.AirbyteCloudSettings`. Process environment values take precedence; this
        does not consult registered secret managers or prompt interactively.

        Environment variable aliases:
            - `AIRBYTE_CLIENT_ID` or `AIRBYTE_CLOUD_CLIENT_ID`: OAuth client ID.
            - `AIRBYTE_CLIENT_SECRET` or `AIRBYTE_CLOUD_CLIENT_SECRET`: OAuth client secret.
            - `AIRBYTE_BEARER_TOKEN` or `AIRBYTE_CLOUD_BEARER_TOKEN`: Bearer token.
            - `AIRBYTE_WORKSPACE_ID` or `AIRBYTE_CLOUD_WORKSPACE_ID`: Workspace ID.
            - `AIRBYTE_ORGANIZATION_ID` or `AIRBYTE_CLOUD_ORGANIZATION_ID`: Organization ID.
            - `AIRBYTE_CLOUD_API_URL`: Optional. The API root URL (defaults to Airbyte Cloud).
            - `AIRBYTE_CLOUD_CONFIG_API_URL`: Optional. The Config API root URL.

        The method will first check for a bearer token. If not found, it will
        attempt to use client credentials.

        Args:
            api_root: The API root URL. If not provided, will be resolved from
                `airbyte.settings.AirbyteCloudSettings`, or default to
                the Airbyte Cloud API.
            config_api_root: The Config API root URL. If not provided, will be resolved
                from `airbyte.settings.AirbyteCloudSettings`.

        Returns:
            A CloudClientConfig instance configured with credentials from the environment.

        Raises:
            PyAirbyteSecretNotFoundError: If required credentials are not found in
                the environment or `./.env`.
        """
        resolved_api_root = resolve_cloud_api_url(api_root)
        resolved_config_api_root = resolve_cloud_config_api_url(config_api_root)

        # Try bearer token first
        bearer_token = resolve_cloud_bearer_token()
        if bearer_token:
            return cls(
                bearer_token=bearer_token,
                api_root=resolved_api_root,
                config_api_root=resolved_config_api_root,
            )

        # Fall back to client credentials
        return cls(
            client_id=resolve_cloud_client_id(),
            client_secret=resolve_cloud_client_secret(),
            api_root=resolved_api_root,
            config_api_root=resolved_config_api_root,
        )
