# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Credential resolution for the Airbyte CLI.

Resolution order:
1. Explicit CLI flags (`--client-id`, `--client-secret`)
2. Short env vars: `AIRBYTE_CLIENT_ID` / `AIRBYTE_CLIENT_SECRET`
3. Long env vars: `AIRBYTE_CLOUD_CLIENT_ID` / `AIRBYTE_CLOUD_CLIENT_SECRET`
4. Credentials file: `~/.airbyte/credentials` (YAML with `client_id` / `client_secret`)
5. Error if none found
"""

from __future__ import annotations

import os

from airbyte.cloud.credentials import CREDENTIALS_FILE_PATH, read_credentials_file
from airbyte.cloud.workspaces import CloudWorkspace
from airbyte.constants import (
    CLOUD_API_ROOT,
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CLIENT_SECRET_ENV_VAR,
    CLOUD_CONFIG_API_ROOT,
    CLOUD_CONFIG_API_ROOT_ENV_VAR,
    CLOUD_WORKSPACE_ID_ENV_VAR,
)
from airbyte.exceptions import PyAirbyteInputError
from airbyte.secrets.base import SecretString


# Short-form env var names (preferred for CLI usage)
CLI_CLIENT_ID_ENV_VAR = "AIRBYTE_CLIENT_ID"
CLI_CLIENT_SECRET_ENV_VAR = "AIRBYTE_CLIENT_SECRET"
CLI_WORKSPACE_ID_ENV_VAR = "AIRBYTE_WORKSPACE_ID"
CLI_API_URL_ENV_VAR = "AIRBYTE_API_URL"
CLI_BEARER_TOKEN_ENV_VAR = "AIRBYTE_BEARER_TOKEN"
CLI_CONFIG_API_ROOT_ENV_VAR = "AIRBYTE_CONFIG_API_ROOT"


def _read_credentials_file() -> dict[str, str]:
    """Read credentials from `~/.airbyte/credentials` if the file exists."""
    return read_credentials_file(CREDENTIALS_FILE_PATH)


def resolve_client_id(explicit: str | None = None) -> str:
    """Resolve the Airbyte client ID.

    Resolution order: explicit arg, short env var, long env var, credentials file.
    """
    if explicit:
        return explicit

    from_short_env = os.environ.get(CLI_CLIENT_ID_ENV_VAR)
    if from_short_env:
        return from_short_env

    from_long_env = os.environ.get(CLOUD_CLIENT_ID_ENV_VAR)
    if from_long_env:
        return from_long_env

    creds = _read_credentials_file()
    from_file = creds.get("client_id")
    if from_file:
        return str(from_file)

    raise PyAirbyteInputError(
        message="No Airbyte client ID found.",
        guidance=(
            f"Set the `{CLI_CLIENT_ID_ENV_VAR}` environment variable, "
            f"or create a credentials file at {CREDENTIALS_FILE_PATH} "
            "with a `client_id` key."
        ),
    )


def resolve_client_secret(explicit: str | None = None) -> str:
    """Resolve the Airbyte client secret.

    Resolution order: explicit arg, short env var, long env var, credentials file.
    """
    if explicit:
        return explicit

    from_short_env = os.environ.get(CLI_CLIENT_SECRET_ENV_VAR)
    if from_short_env:
        return from_short_env

    from_long_env = os.environ.get(CLOUD_CLIENT_SECRET_ENV_VAR)
    if from_long_env:
        return from_long_env

    creds = _read_credentials_file()
    from_file = creds.get("client_secret")
    if from_file:
        return str(from_file)

    raise PyAirbyteInputError(
        message="No Airbyte client secret found.",
        guidance=(
            f"Set the `{CLI_CLIENT_SECRET_ENV_VAR}` environment variable, "
            f"or create a credentials file at {CREDENTIALS_FILE_PATH} "
            "with a `client_secret` key."
        ),
    )


def resolve_workspace_id(explicit: str | None = None) -> str:
    """Resolve the Airbyte workspace ID.

    Resolution order: explicit arg, short env var, long env var, credentials file.
    """
    if explicit:
        return explicit

    from_short_env = os.environ.get(CLI_WORKSPACE_ID_ENV_VAR)
    if from_short_env:
        return from_short_env

    from_long_env = os.environ.get(CLOUD_WORKSPACE_ID_ENV_VAR)
    if from_long_env:
        return from_long_env

    creds = _read_credentials_file()
    from_file = creds.get("workspace_id")
    if from_file:
        return str(from_file)

    raise PyAirbyteInputError(
        message="No Airbyte workspace ID found.",
        guidance=(
            f"Set the `{CLI_WORKSPACE_ID_ENV_VAR}` environment variable, "
            f"or create a credentials file at {CREDENTIALS_FILE_PATH} "
            "with a `workspace_id` key."
        ),
    )


def resolve_api_url(explicit: str | None = None) -> str:
    """Resolve the Airbyte API URL.

    Resolution order: explicit arg, short env var, long env var, default.
    """
    if explicit:
        return explicit

    from_short_env = os.environ.get(CLI_API_URL_ENV_VAR)
    if from_short_env:
        return from_short_env

    from_long_env = os.environ.get("AIRBYTE_CLOUD_API_URL")
    if from_long_env:
        return from_long_env

    creds = _read_credentials_file()
    from_file = creds.get("airbyte_api_root") or creds.get("api_url")
    if from_file:
        return str(from_file)

    return CLOUD_API_ROOT


def resolve_config_api_root(explicit: str | None = None) -> str:
    """Resolve the Airbyte Config API root URL."""
    if explicit:
        return explicit

    from_short_env = os.environ.get(CLI_CONFIG_API_ROOT_ENV_VAR)
    if from_short_env:
        return from_short_env

    from_long_env = os.environ.get(CLOUD_CONFIG_API_ROOT_ENV_VAR)
    if from_long_env:
        return from_long_env

    creds = _read_credentials_file()
    from_file = creds.get("config_api_root")
    if from_file:
        return str(from_file)

    return CLOUD_CONFIG_API_ROOT


def resolve_bearer_token(explicit: str | None = None) -> str | None:
    """Resolve an Airbyte bearer token if one is available."""
    if explicit:
        return explicit

    from_short_env = os.environ.get(CLI_BEARER_TOKEN_ENV_VAR)
    if from_short_env:
        return from_short_env

    from_long_env = os.environ.get(CLOUD_BEARER_TOKEN_ENV_VAR)
    if from_long_env:
        return from_long_env

    creds = _read_credentials_file()
    from_file = creds.get("bearer_token")
    if from_file:
        return str(from_file)

    return None


def create_cloud_workspace(
    *,
    workspace_id: str,
    client_id: str | None = None,
    client_secret: str | None = None,
    api_url: str | None = None,
) -> CloudWorkspace:
    """Create a `CloudWorkspace` from CLI authentication inputs."""
    bearer_token = resolve_bearer_token()
    resolved_api_url = resolve_api_url(api_url)
    if bearer_token and not client_id and not client_secret:
        return CloudWorkspace(
            workspace_id=workspace_id,
            bearer_token=SecretString(bearer_token),
            api_root=resolved_api_url,
        )

    return CloudWorkspace(
        workspace_id=workspace_id,
        client_id=SecretString(resolve_client_id(client_id)),
        client_secret=SecretString(resolve_client_secret(client_secret)),
        api_root=resolved_api_url,
    )
