# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Credential file helpers for Airbyte Cloud authentication."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from airbyte._util.api_util import get_bearer_token
from airbyte.constants import CLOUD_API_ROOT, CLOUD_CONFIG_API_ROOT
from airbyte.exceptions import PyAirbyteInputError
from airbyte.secrets.base import SecretString


CREDENTIALS_FILE_PATH = Path("~/.airbyte/credentials").expanduser()


@dataclass(frozen=True)
class CloudLoginResult:
    """Result of a successful non-interactive Cloud login."""

    credentials_file_path: Path
    airbyte_api_root: str
    config_api_root: str


def _as_string_mapping(parsed: object) -> dict[str, str]:
    """Return a string-only mapping from parsed YAML content."""
    if not isinstance(parsed, dict):
        return {}

    result: dict[str, str] = {}
    for key, value in parsed.items():
        if isinstance(key, str) and value is not None:
            result[key] = str(value)

    return result


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
            guidance="Provide both `--airbyte-api-root` and `--config-api-root`.",
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
