# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Credential file helpers for Airbyte Cloud authentication."""

from __future__ import annotations

from dataclasses import dataclass
from os import environ
from pathlib import Path

import yaml

from airbyte._util.api_util import get_bearer_token
from airbyte.constants import (
    CLOUD_API_ROOT,
    CLOUD_API_ROOT_ENV_VAR,
    CLOUD_BEARER_TOKEN_ENV_VAR,
    CLOUD_CLIENT_ID_ENV_VAR,
    CLOUD_CLIENT_SECRET_ENV_VAR,
    CLOUD_CONFIG_API_ROOT,
    CLOUD_CONFIG_API_ROOT_ENV_VAR,
    CLOUD_ORGANIZATION_ID_ENV_VAR,
    CLOUD_WORKSPACE_ID_ENV_VAR,
)
from airbyte.exceptions import PyAirbyteInputError
from airbyte.secrets.base import SecretString


CREDENTIALS_FILE_PATH = Path("~/.airbyte/credentials").expanduser()
CLIENT_ID_ENV_VAR = "AIRBYTE_CLIENT_ID"
CLIENT_SECRET_ENV_VAR = "AIRBYTE_CLIENT_SECRET"
WORKSPACE_ID_ENV_VAR = "AIRBYTE_WORKSPACE_ID"
ORGANIZATION_ID_ENV_VAR = "AIRBYTE_ORGANIZATION_ID"
PUBLIC_API_ROOT_ENV_VAR = "AIRBYTE_API_ROOT"
BEARER_TOKEN_ENV_VAR = "AIRBYTE_BEARER_TOKEN"
CONFIG_API_ROOT_ENV_VAR = "AIRBYTE_CONFIG_API_ROOT"


@dataclass(frozen=True)
class CloudLoginResult:
    """Result of a successful non-interactive Cloud login."""

    credentials_file_path: Path
    airbyte_api_root: str
    config_api_root: str


@dataclass(frozen=True)
class CloudCredentials:
    """Resolved credentials and API roots for Airbyte control-plane APIs."""

    client_id: SecretString | None
    client_secret: SecretString | None
    bearer_token: SecretString | None
    public_api_root: str
    config_api_root: str | None
    workspace_id: str | None = None
    organization_id: str | None = None


def _as_string_mapping(parsed: object) -> dict[str, str]:
    """Return a string-only mapping from parsed YAML content."""
    if not isinstance(parsed, dict):
        return {}

    result: dict[str, str] = {}
    for key, value in parsed.items():
        if isinstance(key, str) and value is not None:
            result[key] = str(value)

    return result


def _first_value(*values: str | None) -> str | None:
    """Return the first non-empty string value."""
    for value in values:
        if value:
            return value
    return None


def _env_value(*names: str) -> str | None:
    """Return the first available environment variable value."""
    for name in names:
        value = environ.get(name)
        if value:
            return value
    return None


def read_credentials_file(
    credentials_file_path: Path = CREDENTIALS_FILE_PATH,
) -> dict[str, str]:
    """Read Airbyte credentials from a YAML credentials file."""
    if not credentials_file_path.exists():
        return {}

    try:
        content = credentials_file_path.read_text(encoding="utf-8").strip()
        parsed = yaml.safe_load(content) if content else {}
    except (OSError, yaml.YAMLError):
        return {}

    return _as_string_mapping(parsed)


def resolve_cloud_credentials(
    *,
    workspace_id: str | None = None,
    organization_id: str | None = None,
    client_id: str | SecretString | None = None,
    client_secret: str | SecretString | None = None,
    bearer_token: str | SecretString | None = None,
    public_api_root: str | None = None,
    config_api_root: str | None = None,
    credentials_file_path: Path = CREDENTIALS_FILE_PATH,
) -> CloudCredentials:
    """Resolve Airbyte Cloud credentials from inputs, env vars, and credentials file."""
    credentials_file = read_credentials_file(credentials_file_path)
    resolved_bearer_token = _first_value(
        str(bearer_token) if bearer_token is not None else None,
        _env_value(BEARER_TOKEN_ENV_VAR, CLOUD_BEARER_TOKEN_ENV_VAR),
        credentials_file.get("bearer_token"),
    )
    resolved_client_id = _first_value(
        str(client_id) if client_id is not None else None,
        _env_value(CLIENT_ID_ENV_VAR, CLOUD_CLIENT_ID_ENV_VAR),
        credentials_file.get("client_id"),
    )
    resolved_client_secret = _first_value(
        str(client_secret) if client_secret is not None else None,
        _env_value(CLIENT_SECRET_ENV_VAR, CLOUD_CLIENT_SECRET_ENV_VAR),
        credentials_file.get("client_secret"),
    )

    if resolved_bearer_token and (resolved_client_id or resolved_client_secret):
        resolved_client_id = None
        resolved_client_secret = None
    elif bool(resolved_client_id) != bool(resolved_client_secret):
        raise PyAirbyteInputError(
            message="Client ID and client secret are both required.",
            guidance="Provide both client ID and client secret, or use a bearer token.",
        )
    elif not resolved_bearer_token and not resolved_client_id:
        raise PyAirbyteInputError(
            message="No Airbyte credentials found.",
            guidance=(
                "Set Airbyte Cloud credentials in environment variables or "
                f"create a credentials file at {credentials_file_path}."
            ),
        )

    return CloudCredentials(
        client_id=SecretString(resolved_client_id) if resolved_client_id else None,
        client_secret=SecretString(resolved_client_secret) if resolved_client_secret else None,
        bearer_token=SecretString(resolved_bearer_token) if resolved_bearer_token else None,
        public_api_root=_first_value(
            public_api_root,
            _env_value(PUBLIC_API_ROOT_ENV_VAR, CLOUD_API_ROOT_ENV_VAR),
            credentials_file.get("airbyte_api_root"),
            credentials_file.get("public_api_root"),
            credentials_file.get("api_url"),
        )
        or CLOUD_API_ROOT,
        config_api_root=_first_value(
            config_api_root,
            _env_value(CONFIG_API_ROOT_ENV_VAR, CLOUD_CONFIG_API_ROOT_ENV_VAR),
            credentials_file.get("config_api_root"),
        ),
        workspace_id=_first_value(
            workspace_id,
            _env_value(WORKSPACE_ID_ENV_VAR, CLOUD_WORKSPACE_ID_ENV_VAR),
            credentials_file.get("workspace_id"),
        ),
        organization_id=_first_value(
            organization_id,
            _env_value(ORGANIZATION_ID_ENV_VAR, CLOUD_ORGANIZATION_ID_ENV_VAR),
            credentials_file.get("organization_id"),
        ),
    )


def write_credentials_file(
    credentials: dict[str, str],
    credentials_file_path: Path = CREDENTIALS_FILE_PATH,
) -> None:
    """Write Airbyte credentials to a YAML credentials file."""
    credentials_file_path.parent.mkdir(parents=True, exist_ok=True)
    credentials_file_path.write_text(
        yaml.safe_dump(dict(credentials), sort_keys=True),
        encoding="utf-8",
    )
    credentials_file_path.chmod(0o600)


def _raise_interactive_login_unavailable() -> None:
    """Raise an error for the unsupported browser login flow."""
    raise PyAirbyteInputError(
        message="Interactive Airbyte Cloud login is not implemented.",
        guidance=(
            "Provide `--client-id` and `--client-secret` for non-interactive login. "
            "The browser login protocol has not been published in repo docs."
        ),
    )


def _validate_client_credentials(
    *,
    client_id: str | None,
    client_secret: str | None,
) -> tuple[str, str]:
    """Validate and return client credentials for non-interactive login."""
    if not client_id and not client_secret:
        _raise_interactive_login_unavailable()

    if not client_id or not client_secret:
        raise PyAirbyteInputError(
            message="Client ID and client secret are both required.",
            guidance="Provide both `--client-id` and `--client-secret`.",
        )

    return client_id, client_secret


def _resolve_login_roots(
    *,
    airbyte_api_root: str | None,
    config_api_root: str | None,
) -> tuple[str, str]:
    """Resolve Cloud or self-managed API roots for login."""
    if airbyte_api_root is not None or config_api_root is not None:
        if airbyte_api_root is not None and config_api_root is not None:
            return airbyte_api_root, config_api_root

        missing_roots: list[str] = []
        if not airbyte_api_root:
            missing_roots.append("airbyte_api_root")
        if not config_api_root:
            missing_roots.append("config_api_root")
        raise PyAirbyteInputError(
            message="Self-managed login requires both API roots.",
            context={"missing": ", ".join(missing_roots)},
            guidance="Provide both `--public-api-root` and `--config-api-root`.",
        )

    return CLOUD_API_ROOT, CLOUD_CONFIG_API_ROOT


def login_with_client_credentials(
    *,
    client_id: str | None = None,
    client_secret: str | None = None,
    airbyte_api_root: str | None = None,
    config_api_root: str | None = None,
    credentials_file_path: Path = CREDENTIALS_FILE_PATH,
) -> CloudLoginResult:
    """Log in using client credentials and persist a bearer token."""
    resolved_client_id, resolved_client_secret = _validate_client_credentials(
        client_id=client_id,
        client_secret=client_secret,
    )
    resolved_airbyte_api_root, resolved_config_api_root = _resolve_login_roots(
        airbyte_api_root=airbyte_api_root,
        config_api_root=config_api_root,
    )
    bearer_token = get_bearer_token(
        client_id=SecretString(resolved_client_id),
        client_secret=SecretString(resolved_client_secret),
        api_root=resolved_airbyte_api_root,
    )

    credentials = read_credentials_file(credentials_file_path)
    credentials.update(
        {
            "airbyte_api_root": resolved_airbyte_api_root,
            "bearer_token": str(bearer_token),
            "config_api_root": resolved_config_api_root,
        }
    )
    write_credentials_file(credentials, credentials_file_path)

    return CloudLoginResult(
        credentials_file_path=credentials_file_path,
        airbyte_api_root=resolved_airbyte_api_root,
        config_api_root=resolved_config_api_root,
    )


def logout(
    *,
    credentials_file_path: Path = CREDENTIALS_FILE_PATH,
) -> None:
    """Remove locally stored Airbyte credentials."""
    if credentials_file_path.exists():
        credentials_file_path.unlink()
