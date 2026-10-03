# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Authentication-related constants and utilities for the Airbyte Cloud."""

from airbyte.constants import (
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CLIENT_SECRET_ENV_VAR,
    CLOUD_WORKSPACE_ID_ENV_VAR,
)
from airbyte.exceptions import PyAirbyteSecretNotFoundError
from airbyte.secrets.base import SecretString
from airbyte.settings import AirbyteCloudSettings


def resolve_cloud_bearer_token(
    input_value: str | SecretString | None = None,
    /,
) -> SecretString | None:
    """Get the Airbyte Cloud bearer token from the environment or `./.env`.

    Unlike other resolve functions, this returns None if no bearer token is found,
    since bearer token authentication is optional (client credentials can be used instead).

    Args:
        input_value: Optional explicit bearer token value. If provided, it will be
            returned directly (wrapped in SecretString if needed).

    Returns:
        The bearer token as a SecretString, or None if not found.
    """
    if input_value is not None:
        return SecretString(input_value)
    return AirbyteCloudSettings().bearer_token


def resolve_cloud_client_secret(
    input_value: str | SecretString | None = None,
    /,
) -> SecretString:
    """Get the Airbyte Cloud client secret from the environment or `./.env`."""
    if input_value is not None and input_value != "":  # noqa: PLC1901
        return SecretString(input_value)
    settings = AirbyteCloudSettings()
    if settings.client_secret is None:
        raise PyAirbyteSecretNotFoundError(
            secret_name=CLOUD_CLIENT_SECRET_ENV_VAR,
            sources=["env", "dotenv"],
        )
    return settings.client_secret


def resolve_cloud_client_id(
    input_value: str | SecretString | None = None,
    /,
) -> SecretString:
    """Get the Airbyte Cloud client ID from the environment or `./.env`."""
    if input_value is not None and input_value != "":  # noqa: PLC1901
        return SecretString(input_value)
    settings = AirbyteCloudSettings()
    if settings.client_id is None:
        raise PyAirbyteSecretNotFoundError(
            secret_name=CLOUD_CLIENT_ID_ENV_VAR,
            sources=["env", "dotenv"],
        )
    return settings.client_id


def resolve_cloud_api_url(
    input_value: str | None = None,
    /,
) -> str:
    """Get the Airbyte Cloud API URL from the environment or `./.env`."""
    return input_value or AirbyteCloudSettings().api_url


def resolve_cloud_workspace_id(
    input_value: str | None = None,
    /,
) -> str:
    """Get the Airbyte Cloud workspace ID from the environment or `./.env`."""
    if input_value is not None and input_value != "":  # noqa: PLC1901
        return input_value
    settings = AirbyteCloudSettings()
    if settings.workspace_id is None:
        raise PyAirbyteSecretNotFoundError(
            secret_name=CLOUD_WORKSPACE_ID_ENV_VAR,
            sources=["env", "dotenv"],
        )
    return settings.workspace_id


def resolve_cloud_config_api_url(
    input_value: str | None = None,
    /,
) -> str | None:
    """Get the Airbyte Cloud Config API URL from the environment or `./.env`.

    The Config API is a separate internal API used for certain operations like
    connector builder projects and custom source definitions.

    Returns:
        The Config API URL if set via environment, `./.env`, or input, otherwise None.
    """
    return input_value or AirbyteCloudSettings().config_api_url
