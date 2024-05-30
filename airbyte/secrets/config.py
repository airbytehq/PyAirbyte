# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""This module provides customization of how PyAirbyte locates secrets."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbyte._util import meta
from airbyte.secrets.base import SecretManager
from airbyte.secrets.env_vars import DotenvSecretManager, EnvVarSecretManager
from airbyte.secrets.google_colab import ColabSecretManager
from airbyte.secrets.prompt import SecretsPrompt


if TYPE_CHECKING:
    from airbyte.secrets.base import SecretSourceEnum
    from airbyte.secrets.custom import CustomSecretManager


_SECRETS_SOURCES: list[SecretManager] = []


def _get_secret_sources() -> list[SecretManager]:
    """Initialize the default secret sources."""
    if len(_SECRETS_SOURCES) == 0:
        # Initialize the default secret sources
        _SECRETS_SOURCES.extend(
            [
                EnvVarSecretManager(),
                DotenvSecretManager(),
            ]
        )
        if meta.is_colab():
            _SECRETS_SOURCES.append(ColabSecretManager())

        if meta.is_interactive():
            _SECRETS_SOURCES.append(SecretsPrompt())

    return _SECRETS_SOURCES.copy()


# Ensure the default secret sources are initialized
_ = _get_secret_sources()


def register_secret_manager(
    secret_manager: CustomSecretManager,
    *,
    as_backup: bool = False,
    replace_existing: bool = False,
) -> None:
    """Register a custom secret manager."""
    if replace_existing:
        clear_secret_sources()

    if as_backup:
        # Add to end of list
        _SECRETS_SOURCES.append(secret_manager)
    else:
        # Add to beginning of list
        _SECRETS_SOURCES.insert(0, secret_manager)


def clear_secret_sources() -> None:
    """Clear all secret sources."""
    _SECRETS_SOURCES.clear()


def disable_secret_source(source: SecretManager | SecretSourceEnum) -> None:
    """Disable one of the default secrets sources.

    This function can accept either a `SecretManager` instance, a `SecretSourceEnum` enum value, or
    a string representing the name of the source to disable.
    """
    if isinstance(source, SecretManager) and source in _SECRETS_SOURCES:
        _SECRETS_SOURCES.remove(source)
        return

    # Else, remove by name
    for s in _SECRETS_SOURCES:
        if s.name == str(source):
            _SECRETS_SOURCES.remove(s)
