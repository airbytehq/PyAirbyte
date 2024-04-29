# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Base classes and methods for working with secrets in PyAirbyte."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from enum import Enum
from typing import cast

from airbyte import exceptions as exc


class SecretSourceEnum(str, Enum):
    ENV = "env"
    DOTENV = "dotenv"
    GOOGLE_COLAB = "google_colab"
    GOOGLE_GSM = "google_gsm"  # Not enabled by default

    PROMPT = "prompt"


class SecretString(str):
    """A string that represents a secret.

    This class is used to mark a string as a secret. When a secret is printed, it
    will be masked to prevent accidental exposure of sensitive information.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "<SecretString: ****>"

    def is_empty(self) -> bool:
        """Check if the secret is an empty string."""
        return len(self) == 0

    def is_json(self) -> bool:
        """Check if the secret string is a valid JSON string."""
        try:
            json.loads(self)
        except (json.JSONDecodeError, Exception):
            return False

        return True

    def __bool__(self) -> bool:
        """Override the boolean value of the secret string.

        Always returns `True` without inspecting contents."""
        return True

    def parse_json(self) -> dict:
        """Parse the secret string as JSON."""
        try:
            return json.loads(self)
        except json.JSONDecodeError as ex:
            raise exc.PyAirbyteInputError(
                message="Failed to parse secret as JSON.",
                context={
                    "Message": ex.msg,
                    "Position": ex.pos,
                    "SecretString_Length": len(self),  # Debug secret blank or an unexpected format.
                },
            ) from None


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

    def __init__(self) -> None:
        """Instantiate the new secret manager."""
        if not hasattr(self, "name"):
            # Default to the class name if no name is provided
            self.name: str = self.__class__.__name__

    @abstractmethod
    def get_secret(self, secret_name: str) -> SecretString | None:
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

    def __hash__(self) -> int:
        return hash(self.name)


class SecretHandle:
    """A handle for a secret in a secret manager.

    This class is used to store a reference to a secret in a secret manager.
    The secret is not retrieved until the `get_value()` or `parse_json()` methods are
    called.
    """

    def __init__(
        self,
        parent: SecretManager,
        secret_name: str,
    ) -> None:
        """Instantiate a new secret handle."""
        self.parent = parent
        self.secret_name = secret_name

    def get_value(self) -> SecretString:
        """Get the secret from the secret manager.

        Subclasses can optionally override this method to provide a more optimized code path.
        """
        return cast(SecretString, self.parent.get_secret(self.secret_name))

    def parse_json(self) -> dict:
        """Parse the secret as JSON.

        This method is a convenience method to parse the secret as JSON without
        needing to call `get_value()` first. If the secret is not a valid JSON
        string, a `PyAirbyteInputError` will be raised.
        """
        return self.get_value().parse_json()
