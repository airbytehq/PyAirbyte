# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Secrets manager for Google Colab user secrets."""

from __future__ import annotations

from airbyte.secrets.base import SecretManager, SecretSourceEnum, SecretString


class ColabSecretManager(SecretManager):
    """Secret manager that retrieves secrets from Google Colab user secrets."""

    name = SecretSourceEnum.GOOGLE_COLAB.value

    def __init__(self) -> None:
        try:
            from google.colab import (  # pyright: ignore[reportMissingImports]  # noqa: PLC0415
                userdata as colab_userdata,
            )

            self.colab_userdata = colab_userdata
        except ImportError:
            self.colab_userdata = None

        super().__init__()

    def get_secret(self, secret_name: str) -> SecretString | None:
        """Get a named secret from Google Colab user secrets."""
        if self.colab_userdata is None:
            # The module doesn't exist. We probably aren't in Colab.
            return None

        try:
            return SecretString(self.colab_userdata.get(secret_name))
        except Exception:
            # Secret name not found. Continue.
            return None
