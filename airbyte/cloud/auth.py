# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Authentication-related constants and utilities for the Airbyte Cloud."""

from airbyte.secrets import SecretString
from airbyte.secrets.util import get_secret, try_get_secret


CLOUD_CLIENT_ID_ENV_VAR: str = "AIRBYTE_CLOUD_CLIENT_ID"
"""The environment variable name for the Airbyte Cloud client ID."""

CLOUD_CLIENT_SECRET_ENV_VAR: str = "AIRBYTE_CLOUD_CLIENT_SECRET"
"""The environment variable name for the Airbyte Cloud client secret."""

CLOUD_API_ROOT_ENV_VAR: str = "AIRBYTE_CLOUD_API_URL"
"""The environment variable name for the Airbyte Cloud API URL."""

CLOUD_WORKSPACE_ID_ENV_VAR: str = "AIRBYTE_CLOUD_WORKSPACE_ID"
"""The environment variable name for the Airbyte Cloud workspace ID."""

CLOUD_API_ROOT: str = "https://api.airbyte.com/v1"
"""The Airbyte Cloud API root URL.

This is the root URL for the Airbyte Cloud API. It is used to interact with the Airbyte Cloud API
and is the default API root for the `CloudWorkspace` class.
- https://reference.airbyte.com/reference/getting-started
"""

CLOUD_CONFIG_API_ROOT: str = "https://cloud.airbyte.com/api/v1"
"""Internal-Use API Root, aka Airbyte "Config API".

Documentation:
- https://docs.airbyte.com/api-documentation#configuration-api-deprecated
- https://github.com/airbytehq/airbyte-platform-internal/blob/master/oss/airbyte-api/server-api/src/main/openapi/config.yaml
"""


def resolve_cloud_client_secret(
    input_value: str | SecretString | None = None,
    /,
) -> SecretString:
    """Get the Airbyte Cloud client secret from the environment."""
    return get_secret(CLOUD_CLIENT_SECRET_ENV_VAR, default=input_value)


def resolve_cloud_client_id(
    input_value: str | SecretString | None = None,
    /,
) -> SecretString:
    """Get the Airbyte Cloud client ID from the environment."""
    return get_secret(CLOUD_CLIENT_ID_ENV_VAR, default=input_value)


def resolve_cloud_api_url(
    input_value: str | None = None,
    /,
) -> str:
    """Get the Airbyte Cloud API URL from the environment, or return the default."""
    return str(try_get_secret(CLOUD_API_ROOT_ENV_VAR, default=input_value) or CLOUD_API_ROOT)


def resolve_cloud_workspace_id(
    input_value: str | None = None,
    /,
) -> str:
    """Get the Airbyte Cloud workspace ID from the environment, or return None if not set."""
    return str(get_secret(CLOUD_WORKSPACE_ID_ENV_VAR, default=input_value))
