# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Secret manager that retrieves secrets from environment variables and `.env` files."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from dotenv import dotenv_values

from airbyte.secrets.base import SecretManager, SecretSourceEnum, SecretString


if TYPE_CHECKING:
    from pathlib import Path


class EnvVarSecretManager(SecretManager):
    """Secret manager that retrieves secrets from environment variables."""

    name = SecretSourceEnum.ENV.value

    def get_secret(self, secret_name: str) -> SecretString | None:
        """Get a named secret from the environment."""
        if secret_name not in os.environ:
            return None

        return SecretString(os.environ[secret_name])

    def _should_ignore(self, env_var_name: str) -> bool:
        """Determine if a given environment variable should be ignored."""
        if env_var_name in {
            "PYTHONPATH",  # Ignore Python path
            "PATH",  # Ignore system path
            "HOME",  # Ignore home directory
            "USER",  # Ignore user name
            "SHELL",  # Ignore shell type
            "LC_CTYPE",  # Ignore locale settings
            "VIRTUAL_ENV",  # Ignore virtual environment
            "ARROW_DEFAULT_MEMORY_POOL",  # Ignore Arrow memory pool setting
            "__CF_USER_TEXT_ENCODING",  # Ignore macOS specific encoding variable
        }:
            return True

        ignored_prefixes = (
            "PYAIRBYTE_",  # Ignore Airbyte-specific environment variables
            "AIRBYTE_",  # Ignore Airbyte-specific environment variables
            "GCP_GSM_",  # Ignore Google Cloud Secret Manager variables
            "AWS_SECRET_",  # Ignore AWS Secrets Manager variables
        )

        # Ignore if variable starts with any ignored prefix
        return bool(any(env_var_name.startswith(prefix) for prefix in ignored_prefixes))

    def list_secrets(self) -> list[str]:
        """List all secrets available in the environment."""
        # Return all environment variable names as a list
        return sorted([key for key in os.environ if not self._should_ignore(key)])

    def is_secret_available(self, secret_name: str) -> bool:
        """Check if a secret is available in the environment."""
        # Check if the secret name exists in the environment variables
        return secret_name in os.environ


class DotenvSecretManager(SecretManager):
    """Secret manager that retrieves secrets from a `.env` file."""

    def __init__(self, dotenv_path: Path | None = None) -> None:
        """Initialize the DotenvSecretManager with an optional path to a `.env` file."""
        super().__init__()
        self.dotenv_path: Path | None = dotenv_path
        self.name = str(dotenv_path) if dotenv_path else SecretSourceEnum.DOTENV.value

    def get_secret(self, secret_name: str) -> SecretString | None:
        """Get a named secret from the `.env` file."""
        try:
            dotenv_vars: dict[str, str | None] = dotenv_values(
                dotenv_path=self.dotenv_path,
            )
        except Exception:
            # Can't locate or parse a .env file
            return None

        if secret_name not in dotenv_vars:
            # Secret not found
            return None

        return SecretString(dotenv_vars[secret_name])

    def list_secrets(self) -> list[str]:
        """List all secrets available in the `.env` file."""
        if self.dotenv_path is None:
            try:
                dotenv_keys = dotenv_values().keys()
            except Exception:
                # Can't locate or parse default .env file. This is common if no .env file exists.
                # Treat as empty.
                return []
            else:
                return list(dotenv_keys)

        # When a specific dotenv file is provided, we should expect it to exist.
        return list(dotenv_values(dotenv_path=self.dotenv_path).keys())

    def is_secret_available(self, secret_name: str) -> bool:
        """Check if a secret is available in the `.env` file."""
        return secret_name in (self.list_secrets() or [])
