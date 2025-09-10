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


class DotenvSecretManager(SecretManager):
    """Secret manager that retrieves secrets from a `.env` file."""

    dotenv_path: Path | None = None

    @property
    def name(self) -> str:  # type: ignore[override]
        """Get name of secret manager."""
        return str(self.dotenv_path) if self.dotenv_path else SecretSourceEnum.ENV.value

    def __init__(
        self,
        dotenv_path: Path | None = None,
    ) -> None:
        """Initialize a new .env Secret Manager, with optionally specified file path."""
        self.dotenv_path = dotenv_path

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
