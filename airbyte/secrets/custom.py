# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Custom secret manager that retrieves secrets from a custom source."""

from __future__ import annotations

from abc import ABC

from airbyte.secrets.base import SecretManager
from airbyte.secrets.config import clear_secret_sources, register_secret_manager


class CustomSecretManager(SecretManager, ABC):
    """Custom secret manager that retrieves secrets from a custom source.

    This class is a convenience class that can be used to create custom secret
    managers. By default, custom secrets managers are auto-registered during
    creation.
    """

    auto_register = True
    replace_existing = False
    as_backup = False

    def __init__(self) -> None:
        """Initialize the custom secret manager."""
        super().__init__()
        if self.auto_register:
            self.register()

    def register(
        self,
        *,
        replace_existing: bool | None = None,
        as_backup: bool | None = None,
    ) -> None:
        """Register the secret manager as global secret source.

        This makes the secret manager available to the `get_secret` function and
        allows it to be used automatically as a source for secrets.

        If `replace_existing` is `True`, the secret manager will replace all existing
        secrets sources, including the default secret managers such as environment
        variables, dotenv files, and Google Colab secrets. If `replace_existing` is
        None or not provided, the default behavior will be used from the `replace_existing`
        of the class (`False` unless overridden by the subclass).
        """
        if replace_existing is None:
            replace_existing = self.replace_existing

        if as_backup is None:
            as_backup = self.as_backup

        if replace_existing:
            clear_secret_sources()

        register_secret_manager(
            self,
            as_backup=as_backup,
            replace_existing=replace_existing,
        )
