# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Secret manager that prompts the user to enter a secret."""

from __future__ import annotations

import contextlib
from getpass import getpass

from airbyte.secrets.base import SecretManager, SecretSourceEnum, SecretString


class SecretsPrompt(SecretManager):
    """Secret manager that prompts the user to enter a secret."""

    name = SecretSourceEnum.PROMPT.value

    def get_secret(
        self,
        secret_name: str,
    ) -> SecretString | None:
        """Prompt the user to enter a secret.

        As a security measure, the secret is not echoed to the terminal when typed.
        """
        with contextlib.suppress(Exception):
            return SecretString(getpass(f"Enter the value for secret '{secret_name}': "))

        return None
