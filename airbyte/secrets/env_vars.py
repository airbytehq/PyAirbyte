# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Secret manager that retrieves secrets from environment variables and `.env` files."""

from __future__ import annotations

import os

from dotenv import dotenv_values

from airbyte.secrets.base import SecretManager, SecretSourceEnum, SecretString


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

    name = SecretSourceEnum.DOTENV.value

    def get_secret(self, secret_name: str) -> SecretString | None:
        """Get a named secret from the `.env` file."""
        try:
            dotenv_vars: dict[str, str | None] = dotenv_values()
        except Exception:
            # Can't locate or parse a .env file
            return None

        if secret_name not in dotenv_vars:
            # Secret not found
            return None

        return SecretString(dotenv_vars[secret_name])
