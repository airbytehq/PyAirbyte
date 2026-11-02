# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Authentication-related constants and utilities for the Airbyte Cloud."""

from airbyte import constants
from airbyte.secrets import SecretString
from airbyte.secrets.util import get_secret, try_get_secret


def resolve_cloud_bearer_token(
    input_value: str | SecretString | None = None,
    /,
) -> SecretString | None:
    """Get the Airbyte Cloud bearer token from the environment.

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

    result = try_get_secret(constants.CLOUD_BEARER_TOKEN_ENV_VAR, default=None)
    if result:
        return SecretString(result)
    return None


def resolve_cloud_client_secret(
    input_value: str | SecretString | None = None,
    /,
) -> SecretString:
    """Get the Airbyte Cloud client secret from the environment."""
    return get_secret(constants.CLOUD_CLIENT_SECRET_ENV_VAR, default=input_value)


def resolve_cloud_client_id(
    input_value: str | SecretString | None = None,
    /,
) -> SecretString:
    """Get the Airbyte Cloud client ID from the environment."""
    return get_secret(constants.CLOUD_CLIENT_ID_ENV_VAR, default=input_value)


def resolve_cloud_api_url(
    input_value: str | None = None,
    /,
) -> str:
    """Get the Airbyte Cloud API URL from the environment, or return the default."""
    return str(
        try_get_secret(constants.CLOUD_API_ROOT_ENV_VAR, default=input_value)
        or constants.CLOUD_API_ROOT
    )


def resolve_cloud_workspace_id(
    input_value: str | None = None,
    /,
) -> str:
    """Get the Airbyte Cloud workspace ID from the environment, or return None if not set."""
    return str(get_secret(constants.CLOUD_WORKSPACE_ID_ENV_VAR, default=input_value))


def resolve_cloud_config_api_url(
    input_value: str | None = None,
    /,
) -> str | None:
    """Get the Airbyte Cloud Config API URL from the environment, or return None if not set.

    The Config API is a separate internal API used for certain operations like
    connector builder projects and custom source definitions.

    Returns:
        The Config API URL if set via environment variable or input, None otherwise.
    """
    result = try_get_secret(constants.CLOUD_CONFIG_API_ROOT_ENV_VAR, default=input_value)
    if result:
        return str(result)
    return None
