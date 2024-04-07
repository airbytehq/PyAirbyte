# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
"""Secrets management for PyAirbyte."""
from __future__ import annotations

import contextlib
import os
import warnings
from abc import ABC, abstractmethod
from enum import Enum
from getpass import getpass
from typing import Any, cast

from dotenv import dotenv_values

from airbyte import exceptions as exc
from airbyte._util import meta


try:
    from google.colab import userdata as colab_userdata
except ImportError:
    colab_userdata = None


class SecretSourceEnum(str, Enum):
    ENV = "env"
    DOTENV = "dotenv"
    GOOGLE_COLAB = "google_colab"

    PROMPT = "prompt"


_SECRETS_SOURCES: list[SecretManager] = []


class SecretManager(ABC):
    """Abstract base class for secret managers.

    Secret managers are used to retrieve secrets from a secret store.

    By registering a secret manager, PyAirbyte can automatically locate and
    retrieve secrets from the secret store when needed. This allows you to
    securely store and access sensitive information such as API keys, passwords,
    and other credentials without hardcoding them in your code.

    To create a custom secret manager, subclass this class and implement the
    `get_secret` method. By default, the secret manager will be automatically
    registered as a global secret source, but will not replace any existing
    secret sources. To customize this behavior, override the `auto_register` and
    `replace_existing` attributes in your subclass as needed.

    Note: Registered secrets managers always have priority over the default
    secret sources such as environment variables, dotenv files, and Google Colab
    secrets. If multiple secret managers are registered, the last one registered
    will take priority.
    """

    replace_existing = False
    as_backup = False

    def __init__(self, name: str | None = None) -> None:
        """Instantiate the new secret manager."""

        self.name: str = (  # Default to the class name if no name is provided
            name or self.__class__.__name__
        )

    @abstractmethod
    def get_secret(self, secret_name: str) -> str | None:
        """Get a named secret from the secret manager.

        This method should be implemented by subclasses to retrieve secrets from
        the secret store. If the secret is not found, the method should return `None`.
        """
        ...

    def __str__(self) -> str:
        return self.name

    def __eq__(self, value: object) -> bool:
        if isinstance(value, SecretManager):
            return self.name == value.name

        if isinstance(value, str):
            return self.name == value

        if isinstance(value, SecretSourceEnum):
            return self.name == str(value)

        return super().__eq__(value)


class CustomSecretManager(SecretManager, ABC):
    """Custom secret manager that retrieves secrets from a custom source.

    This class is a convenience class that can be used to create custom secret
    managers. By default, custom secrets managers are auto-registered during
    creation.
    """

    auto_register = True
    replace_existing = False
    as_backup = False

    def __init__(self, name: str | None = None) -> None:
        super().__init__(name)
        if self.auto_register:
            self.register()

    def register(self, *, replace_existing: bool | None = None) -> None:
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

        if replace_existing:
            _SECRETS_SOURCES.clear()

        if self.as_backup:
            # Add to end of list
            _SECRETS_SOURCES.append(self)
        else:
            # Add to beginning of list
            _SECRETS_SOURCES.insert(0, self)


class EnvVarSecretManager(CustomSecretManager):
    """Secret manager that retrieves secrets from environment variables."""

    name = str(SecretSourceEnum.ENV)

    def get_secret(self, secret_name: str) -> str | None:
        """Get a named secret from the environment."""
        if secret_name not in os.environ:
            return None

        return os.environ[secret_name]


class DotenvSecretManager(CustomSecretManager):
    """Secret manager that retrieves secrets from a `.env` file."""

    name = str(SecretSourceEnum.DOTENV)

    def get_secret(self, secret_name: str) -> str | None:
        """Get a named secret from the `.env` file."""
        try:
            dotenv_vars: dict[str, str | None] = dotenv_values()
        except Exception:
            # Can't locate or parse a .env file
            return None

        if secret_name not in dotenv_vars:
            # Secret not found
            return None

        return dotenv_vars[secret_name]


class ColabSecretManager(CustomSecretManager):
    """Secret manager that retrieves secrets from Google Colab user secrets."""

    name = str(SecretSourceEnum.GOOGLE_COLAB)

    def get_secret(self, secret_name: str) -> str | None:
        """Get a named secret from Google Colab user secrets."""
        if colab_userdata is None:
            # The module doesn't exist. We probably aren't in Colab.
            return None

        try:
            return colab_userdata.get(secret_name)
        except Exception:
            # Secret name not found. Continue.
            return None


class SecretsPrompt(CustomSecretManager):
    """Secret manager that prompts the user to enter a secret."""

    name = str(SecretSourceEnum.PROMPT)

    def get_secret(
        self,
        secret_name: str,
    ) -> str | None:
        with contextlib.suppress(Exception):
            return getpass(f"Enter the value for secret '{secret_name}': ")

        return None


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


def register_secret_manager(secret_manager: CustomSecretManager) -> None:
    """Register a custom secret manager."""
    secret_manager.register()


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


def get_secret(
    secret_name: str,
    /,
    *,
    sources: list[SecretManager | SecretSourceEnum] | None = None,
    allow_prompt: bool = True,
    **kwargs: dict[str, Any],
) -> str:
    """Get a secret from the environment.

    The optional `sources` argument of enum type `SecretSourceEnum` or list of `SecretSourceEnum`
    options. If left blank, the `sources` arg will be `SecretSourceEnum.ANY`. If `source` is set to
    a specific source, then only that source will be checked. If a list of `SecretSourceEnum`
    entries is passed, then the sources will be checked using the provided ordering.

    If `prompt` to `True` or if SecretSourceEnum.PROMPT is declared in the `source` arg, then the
    user will be prompted to enter the secret if it is not found in any of the other sources.
    """
    if "source" in kwargs:
        warnings.warn(
            message="The `source` argument is deprecated. Use the `sources` argument instead.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        sources = kwargs.pop("source")

    available_sources: dict[str, SecretManager] = {}
    for available_source in _get_secret_sources():
        # Add available sources to the dict. Order matters.
        available_sources[available_source.name] = available_source

    if sources is None:
        # If ANY is in the list, then we don't need to check any other sources.
        # This is the default behavior.
        sources = list(available_sources.values())

    elif not isinstance(sources, list):
        sources = [sources]

    # Replace any SecretSourceEnum strings with the matching SecretManager object
    for source in sources:
        if isinstance(source, SecretSourceEnum):
            if source not in available_sources:
                raise exc.PyAirbyteInputError(
                    guidance="Invalid secret source name.",
                    input_value=source,
                    context={
                        "Available Sources": list(available_sources.keys()),
                    },
                )

            sources[sources.index(source)] = available_sources[source]

    secret_managers = cast(list[SecretManager], sources)

    if SecretSourceEnum.PROMPT in secret_managers:
        prompt_source = secret_managers.pop(
            secret_managers.index(SecretSourceEnum.PROMPT),
        )

        if allow_prompt:
            # Always check prompt last. Add it to the end of the list.
            secret_managers.append(prompt_source)

    for secret_mgr in secret_managers:
        val = secret_mgr.get_secret(secret_name)
        if val:
            return val

    raise exc.PyAirbyteSecretNotFoundError(
        secret_name=secret_name,
        sources=[str(s) for s in available_sources],
    )


__all__ = [
    "get_secret",
    "SecretSourceEnum",
    "SecretManager",
    "CustomSecretManager",
]
