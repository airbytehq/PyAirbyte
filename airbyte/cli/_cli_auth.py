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
from pathlib import Path
from typing import Any

import yaml

from airbyte.constants import (
    CLOUD_API_ROOT,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CLIENT_SECRET_ENV_VAR,
    CLOUD_WORKSPACE_ID_ENV_VAR,
)
from airbyte.exceptions import PyAirbyteInputError


# Short-form env var names (preferred for CLI usage)
CLI_CLIENT_ID_ENV_VAR = "AIRBYTE_CLIENT_ID"
CLI_CLIENT_SECRET_ENV_VAR = "AIRBYTE_CLIENT_SECRET"
CLI_WORKSPACE_ID_ENV_VAR = "AIRBYTE_WORKSPACE_ID"
CLI_API_URL_ENV_VAR = "AIRBYTE_API_URL"

CREDENTIALS_FILE_PATH = Path("~/.airbyte/credentials").expanduser()


def _read_credentials_file() -> dict[str, Any]:
    """Read credentials from `~/.airbyte/credentials` if the file exists.

    The file is expected to be YAML with `client_id` and `client_secret` keys.

    Returns an empty dict if the file does not exist or cannot be parsed.
    """
    if not CREDENTIALS_FILE_PATH.exists():
        return {}

    try:
        content = CREDENTIALS_FILE_PATH.read_text(encoding="utf-8").strip()
        parsed = yaml.safe_load(content) if content else {}
    except (OSError, yaml.YAMLError):
        return {}

    if not content:
        return {}

    if not isinstance(parsed, dict):
        return {}

    return parsed


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
    from_file = creds.get("api_url")
    if from_file:
        return str(from_file)

    return CLOUD_API_ROOT
