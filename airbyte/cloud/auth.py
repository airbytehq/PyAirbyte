# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Authentication-related constants and utilities for the Airbyte Cloud."""

from airbyte import constants
from airbyte.secrets import SecretString
from airbyte.secrets.util import get_secret, try_get_secret


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
